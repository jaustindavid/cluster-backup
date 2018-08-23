#!/usr/bin/env python3

"""
I scan a local folder, populate a persistent_dict
I maintain file state for that folder; a Clientlet
    uses me to know what files he owns

TODO: make me a subclass of PersistentDict
"""

import os, logging
import config, utils
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
        self.pd_filename = f".cb.{context}.json"
        lazy_write = self.config.get(context, "LAZY WRITE", 5)
        # self.logger.debug(f"lazy write: {lazy_write}")
        # self.states = PersistentDict(f"{self.path}/{self.pd_filename}", \
        #                                 lazy_write)
        super().__init__(f"{self.path}/{self.pd_filename}", lazy_write) 
        self.logger = logging.getLogger(logger_str(__class__) + " " + name)
        self.logger.setLevel(logging.INFO)
        self.ignored_suffixes = {}


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
        if not os.path.exists(self.path):
            self.logger.warn(f"cannot scan: {self.path} does not exist")
            # return True
        # else:
        #   self.logger.debug(f"scanning {self.path}")

        ignorals = self.build_ignorals()
        # cwd = os.getcwd()  # this behaves badly in a Thread
        # self.logger.debug(f"coming from {cwd}")
        # os.chdir(self.path)
        changed = self.scandir(".", ignorals)
        # os.chdir(cwd)
        if self.removeDeleteds():
            changed = True
        # self.states.write() # should be redundant
        self.write() # should be redundant
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
            fqde = f"{path}/{dirent}"
            # self.logger.debug(f"scanning {fqde}")
            if os.path.isdir(fqde):
                if self.scandir(fqde, ignorals, gen_checksums):
                    changed = True
                continue
            # if not self.states.contains_p(fqde):
            if not self.contains_p(fqde):
                # no FQDE: (maybe) checksum & write it
                self.logger.debug(f"new: {fqde}")
                actualState = FileState(fqde, gen_checksums, prefix=self.path)
                # self.states.set(fqde, actualState.to_dict())
                self[fqde] = actualState.to_dict()
                changed = True
            else:
                # FQDE: no checksum...
                # TODO: this is broken ??
                actualState = FileState(fqde, False, prefix=self.path)
                # if actualState.maybechanged(self.states.get(fqde)):
                if actualState.maybechanged(self[fqde]):
                    # ... maybe changed.  (maybe) checksum + write
                    self.logger.debug(f"changed: {fqde}")
                    self.logger.debug(f"old: {self[fqde]}")
                    self.logger.debug(f"new: {actualState}")
                    actualState = FileState(fqde, gen_checksums, 
                                            prefix=self.path)
                    # self.states.set(fqde, actualState.to_dict())
                    self[fqde] = actualState.to_dict()
                    changed = True
                else:
                    # ... probably same.  preserve the old one (touch it)
                    # self.states.touch(fqde)
                    self.touch(fqde)
        return changed


    def drop(self, filename):
        if self.contains_p(filename):
            pathname = f"{self.path}/{filename}"
            self.logger.debug(f"dropping {filename}; before={self.consumption()}")
            assert os.path.exists(pathname)
            # self.states.delete(filename)
            self.delete(filename)
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
            # self.states.delete(fqde)
            self.delete(fqde)
            changed = True
        # self.states.write()
        self.write()
        return changed


    def contains_p(self, filename):
        # return self.states.contains_p(filename)
        return filename in self.data


    # def get(self, filename):
    # return self.states.get(filename)

    # def keys(self):
        # return self.states.keys()


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
