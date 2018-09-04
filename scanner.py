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

        self.config = config.Config.instance()
        self.pd_filename = f".cb.{context}.json.bz2"
        lazy_write = utils.str_to_duration(self.config.get(context, "LAZY WRITE", 5))
        super().__init__(f"{self.path}/{self.pd_filename}", lazy_write=lazy_write) 
        self.logger = logging.getLogger(logger_str(__class__) + " " + name)
        self.logger.setLevel(logging.INFO)
        self.ignored_suffixes = {}
        self.report_timer = elapsed.ElapsedTimer()


    def report(self, restart = False):
        if restart:
            self.nfiles = 0
            self.report_timer.reset()
            self.report_timer.once_every(5)
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
        if False and filename.endswith(self.pd_filename):
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
        # else:
        #   self.logger.debug(f"scanning {self.path}")

        ignorals = self.build_ignorals()
        # cwd = os.getcwd()  # this behaves badly in a Thread
        # self.logger.debug(f"coming from {cwd}")
        # os.chdir(self.path)
        lock = threading.RLock()
        lock.acquire()
        self.report(True)
        gen_checksums = not turbo
        changed = self.scandir(".", ignorals, gen_checksums)
        lock.release()
        # os.chdir(cwd)
        if self.removeDeleteds():
            changed = True
        # self.states.write() # should be redundant
        self.write() # should be redundant
        if turbo:
            self.logger.info("Finished \"turbo\" scan")
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
            # self.logger.debug(f"stat(3)ing {fqde}")
            if os.path.isdir(f"{self.path}/{fqde}"):
                self.logger.debug(f"is a directory")
                if self.scandir(fqde, ignorals, gen_checksums):
                    changed = True
                continue
            if not self.contains_p(fqde):
                # no FQDE: (maybe) checksum & write it
                # self.logger.debug(f"new: {fqde}")
                # actualState = FileState(fqde, gen_checksums, prefix=self.path)
                # self[fqde] = actualState.to_dict()
                # self.logger.debug(self[fqde]["size"])
                self.update(fqde, gen_checksums)
                changed = True
            else:
                # TODO: look for bitrot?
                # "if the file has maybechanged OR
                #  if I don't have a checksum + I care about checksums"
                actualState = FileState(fqde, False, prefix=self.path)
                if actualState.maybechanged(self[fqde]) or \
                    (gen_checksums and self[fqde]["checksum"] == "deferred"):
                    # ... maybe changed.  (maybe) checksum + write
                    # self.logger.debug(f"changed: {fqde}")
                    # self.logger.debug(f"old: {self[fqde]}")
                    # self.logger.debug(f"new: {actualState}")
                    # actualState = FileState(fqde, gen_checksums, 
                    #                         prefix=self.path)
                    # self[fqde] = actualState.to_dict()
                    self.update(fqde, gen_checksums)
                    changed = True
                else:
                    # ... probably same.  preserve the old one (touch it)
                    self.touch(fqde)
        return changed


    # update one file
    def update(self, fqde, gen_checksums=True):
        # self.logger.debug(f"changed: {fqde}")
        # self.logger.debug(f"old: {self[fqde]}")
        actualState = FileState(fqde, gen_checksums, 
                                prefix=self.path)
        # self.logger.debug(f"new: {actualState}")
        self[fqde] = actualState.to_dict()


    def drop(self, filename):
        if self.contains_p(filename):
            pathname = f"{self.path}/{filename}"
            self.logger.debug(f"dropping {filename}; before={self.consumption()}")
            if os.path.exists(pathname):
                del self[filename]
                os.remove(pathname)
                self.scan()
                self.write()
                self.logger.debug(f"dropped {filename}; after={self.consumption()}")
                self.audit()
        else:
            self.logger.warn(f"I don't have {filename}")


    def removeDeleteds(self):
        changed = False
        # for fqde in self.states.clean_keys():
        for fqde in self.clean_keys():
            self.logger.debug(f"removed: {fqde}")
            del self[fqde]
            changed = True
        # self.states.write()
        self.write()
        return changed


    def contains_p(self, filename):
        # return self.states.contains_p(filename)
        return filename in self.data


    def consumption(self):
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
