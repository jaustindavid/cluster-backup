#!/usr/bin/env python3

"""
I scan a local folder, populate a persistent_dict
I maintain file state for that folder; a Clientlet
    uses me to know what files he owns

TODONE: 
    make me a subclass of PersistentDict
TODO:
    return directories (trailing /)

"""

import os, logging, threading
import config, utils, elapsed
from persistent_dict import PersistentDict
from utils import logger_str
from file_state import FileState

class Scanner(PersistentDict):
    def __init__(self, context, path, **kwargs):
        self.context = context
        self.path = os.path.expanduser(path)
        if "name" in kwargs:
            name = kwargs["name"]
        else:
            name = context

        if "checksums" in kwargs:
            self.checksums = kwargs['checksums']
        else:
            self.checksums = True

        self.config = config.Config.instance()
        self.pd_filename = f".cb.{context}.json.bz2"
        lazy_write = utils.str_to_duration(self.config.get(context, "LAZY WRITE", 5))
        super().__init__(f"{self.path}/{self.pd_filename}",
                            lazy_write=lazy_write, **kwargs) 
        self.logger = logging.getLogger(logger_str(__class__) + " " + name)
        # self.logger.setLevel(logging.INFO)
        self.ignored_suffixes = {}
        self.report_timer = elapsed.ElapsedTimer()
        self.stat = stats.Statistic(buckets=(0, 5, 10, 30))


    def report(self, restart = False):
        self.stat.incr(1)
        if restart:
            self.nfiles = 0
            self.report_timer.reset()
            self.report_timer.once_every(5)
            self.stat.set(0)
            return
        self.nfiles += 1
        if self.report_timer.once_every(15):
            self.logger.info(f"scanned {self.nfiles} files")


    def ignore(self, suffix):
        if suffix not in self.ignored_suffixes:
            self.ignored_suffixes.append(suffix)


    # returns a list
    def build_ignorals(self):
        ignorals = [ self.pd_filename ]
        global_ignore_suffix = self.config.get("global", "ignore suffix")
        if type(global_ignore_suffix) is str:
            ignorals.append(global_ignore_suffix)
        elif global_ignore_suffix is not None:
            ignorals += global_ignore_suffix
        local_ignore_suffix = self.config.get(self.context, "ignore suffix")
        if type(local_ignore_suffix) is str:
            ignorals.append(local_ignore_suffix)
        elif local_ignore_suffix is not None:
            ignorals += local_ignore_suffix
        return ignorals


    def ignoring(self, ignorals, filename):
        # always ignore state files
        if filename.endswith(self.pd_filename):
            return True
        for suffix in ignorals:
           # we only ignore suffixes "magically"
            if filename.endswith(suffix):
                return True
        return False


    def scan(self, **kwargs):
        if "turbo" in kwargs and kwargs['turbo']:
            turbo = True
            self.logger.info("Starting \"turbo\" scan")
        else:
            turbo = False

        if not os.path.exists(self.path):
            self.logger.warn(f"cannot scan: {self.path} does not exist")
            # return True

        ignorals = self.build_ignorals()
        self.report(True)
        gen_checksums = not turbo and self.checksums
        changed = self.scandir(".", ignorals, gen_checksums)
        if self.removeDeleteds():
            changed = True
        self.write() # should be redundant
        if turbo:
            self.logger.info("Finished \"turbo\" scan")
        self.logger.debug(f"Finished scan: {self.stat.qps()}")
        return changed


    # recursively scan a directory; populate self.states
    # FQDE = fully qualified directory entry: a full path for
    # the file (relative to the master/slave base dir)
    # because os.chdir and Thread don't get along nicely
    # (all threads get os.chdir()'d) the path is relative
    # and should get self.path prepended for the purposes
    # of a stat (but not in the recorded state)
    def scandir(self, path, ignorals, gen_checksums = True):
        changed = False
        if path.startswith("./"):
            path = path[2:]
        self.logger.debug(f"scanning path {path}, ignoring {ignorals}")
        try:
            direntries = os.listdir(f"{self.path}/{path}")
        except (FileNotFoundError, PermissionError):
            return None
        for dirent in sorted(direntries):
            if self.ignoring(ignorals, dirent):
                continue
            self.report()
            fqde = f"{path}/{dirent}"
            if os.path.isdir(f"{self.path}/{fqde}"):
                self.logger.debug(f"is a directory")
                if self.scandir(fqde, ignorals, gen_checksums):
                    changed = True
                continue
            if not fqde in self:
                self.update(fqde, gen_checksums)
                changed = True
            else:
                actualState = FileState(fqde, False, prefix=self.path)
                if actualState.maybechanged(self[fqde]) or \
                    (gen_checksums and self[fqde]["checksum"] == "deferred"):
                    self.update(fqde, gen_checksums)
                    changed = True
                else:
                    # ... probably same.  preserve the old one (touch it)
                    self.touch(fqde)
        return changed


    # update one file
    def update(self, fqde, gen_checksums=True):
        try:
            actualState = FileState(fqde, gen_checksums, 
                                    prefix=self.path)
            self[fqde] = actualState.to_dict()
        except FileNotFoundError:
            if fqde in self:
                del self[fqde]


    def drop(self, filename):
        if filename in self:
            pathname = f"{self.path}/{filename}"
            self.logger.debug(f"dropping {filename}; before={self.consumption()}")
            if os.path.exists(pathname):
                del self[filename]
                os.remove(pathname)
                self.scan(turbo=True) # TODO: this is expensive
                self.write()
                self.logger.debug(f"dropped {filename}; after={self.consumption()}")
                self.audit()
        else:
            self.logger.warn(f"I don't have {filename}")


    def removeDeleteds(self):
        changed = False
        for fqde in self.clean_keys():
            self.logger.debug(f"removed: {fqde}")
            del self[fqde]
            changed = True
        return changed


    def DEADcontains_p(self, filename):
        # return self.states.contains_p(filename)
        return filename in self.data


    def DEADconsumption(self):
        total = 0
        # for filename, state in self.states.items():
        for filename, state in self.items():
            total += state["size"]
        return total


    def audit(self):
        report = "Scanner: {len(self.keys()} items, {utils.bytes_to_str(self.consumption())}"
        for filename in sorted(self.keys()):
            report += f"filename: {utils.bytes_to_str(self.consumption())}"
        return report


    def __str__(self):
        string = f"Scanner<{self.path}> {self.items()}"
        return string

import stats
class ScannerLite(PersistentDict):
    def __init__(self, context, path, pd_path=None, name=None, 
                    loglevel=logging.INFO, **kwargs):
        self.context = context
        self.path = os.path.expanduser(path)
        if not name:
            name = context

        self.config = config.Config.instance()
        lazy_write = utils.get_interval(self.config, "LAZY WRITE", (context))
        self.pd_filename = f".cb.{context}-lite.json.bz2"
        if pd_path:
            pd_file = f"{pd_path}/{self.pd_filename}"
        else:
            pd_file = f"{self.path}/{self.pd_filename}"
        super().__init__(pd_file, lazy_write=lazy_write) 
        self.logger = logging.getLogger(logger_str(__class__) + " " + name)
        self.logger.setLevel(loglevel)
        self.ignored_suffixes = {}
        self.stat = stats.Statistic(buckets=(0, 5, 10, 30))
        self.report_timer = elapsed.ElapsedTimer()


    def report(self, restart = False):
        self.stat.incr(1)
        if restart:
            self.nfiles = 0
            self.report_timer.reset()
            self.report_timer.once_every(5)
            self.stat.set(0)
            return
        self.nfiles += 1
        if self.report_timer.once_every(15):
            self.logger.info(f"scanned {self.nfiles} files")


    def ignore(self, suffix):
        if suffix not in self.ignored_suffixes:
            self.ignored_suffixes.append(suffix)


    # returns a list
    def build_ignorals(self):
        ignorals = [ self.pd_filename ]
        global_ignore_suffix = self.config.get("global", "ignore suffix")
        if type(global_ignore_suffix) is str:
            ignorals.append(global_ignore_suffix)
        elif global_ignore_suffix is not None:
            ignorals += global_ignore_suffix
        local_ignore_suffix = self.config.get(self.context, "ignore suffix")
        if type(local_ignore_suffix) is str:
            ignorals.append(local_ignore_suffix)
        elif local_ignore_suffix is not None:
            ignorals += local_ignore_suffix
        return ignorals


    def ignoring(self, ignorals, filename):
        # always ignore state files
        if filename.endswith(self.pd_filename) \
            or filename.endswith(self.pd_filename + ".tmp"):
            return True
        for suffix in ignorals:
           # we only ignore suffixes "magically"
            if filename.endswith(suffix):
                return True
        return False


    def scan(self, **kwargs):
        if not os.path.exists(self.path):
            self.logger.debug(f"cannot scan: {self.path} does not exist")
            return False

        self.logger.debug("Starting scan")
        ignorals = self.build_ignorals()
        self.report(True)
        changed = self.scandir(".", ignorals)
        if self.removeDeleteds():
            changed = True
        self.write() # should be redundant
        self.logger.debug(f"Finished scan: {self.stat.qps()}")
        return changed


    # recursively scan a directory; populate self.states
    # FQDE = fully qualified directory entry: a full path for
    # the file (relative to the master/slave base dir)
    # because os.chdir and Thread don't get along nicely
    # (all threads get os.chdir()'d) the path is relative
    # and should get self.path prepended for the purposes
    # of a stat (but not in the recorded state)
    def scandir(self, path, ignorals):
        if path.startswith("./"):
            path = path[2:]
        self.logger.debug(f"scanning path {path}, ignoring {ignorals}")
        try:
            direntries = os.listdir(f"{self.path}/{path}")
        except (FileNotFoundError, PermissionError):
            return None
        for dirent in sorted(direntries):
            if self.ignoring(ignorals, dirent):
                continue
            self.report()
            if path == ".":
                fqde = dirent
            else:
                fqde = f"{path}/{dirent}"
            if os.path.isdir(f"{self.path}/{fqde}"):
                self.logger.debug(f"is a directory")
                if self.scandir(fqde, ignorals):
                    changed = True
            else:
                self.update(fqde)


    # update one file
    def update(self, fqde):
        state = FileState(fqde, False, prefix=self.path)
        self[fqde] = state.data["size"]


    def drop(self, filename):
        if filename in self:
            pathname = f"{self.path}/{filename}"
            # self.logger.debug(f"dropping {filename}; before={self.consumption()}")
            if os.path.exists(pathname):
                del self[filename]
                os.remove(pathname)
                # self.scan(turbo=True) # TODO: this is expensive
                # self.write()
                # self.logger.debug(f"dropped {filename}; after={self.consumption()}")
                # self.audit()
        else:
            self.logger.warn(f"I don't have {filename}")


    def removeDeleteds(self):
        changed = False
        for fqde in self.clean_keys():
            self.logger.debug(f"removed: {fqde}")
            del self[fqde]
            changed = True
        return changed


    def consumption(self):
        total = 0
        filenames = list(self.keys())
        for filename, size in self.items():
            total += size
        return total


    def audit(self):
        report = "Scanner: {len(self.keys()} items, {utils.bytes_to_str(self.consumption())}"
        for filename in sorted(self.keys()):
            report += f"filename: {utils.bytes_to_str(self.consumption())}"
        return report


    def __str__(self):
        string = f"Scanner<{self.path}> {self.items()}"
        return string


