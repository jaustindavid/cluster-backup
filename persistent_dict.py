#! python 3.x

"""
from persistent_dict import PersistentDict

pd = PersistentDict("dict.json")
pd.startTransaction()
pd.set()
pd.set()
pd.closeTransaction()

If kwargs "cls" is provided, this will be a dict of the named class.
The class should provide serialize(), deserialize(data), and set(value)
functions.
"""

import os, json, logging, threading, bz2
from utils import logger_str
import elapsed, config

class PersistentDict:
    def __init__(self, filename, *args, **kwargs):
        self.logger = logging.getLogger(logger_str(__class__))
        self.logger.setLevel(logging.INFO)
        self.masterFilename = filename
        self.args = args
        self.kwargs = kwargs
        if 'lazy_write' in kwargs:
            self.lazy_timer = kwargs['lazy_write']
        else:
            self.lazy_timer = 0
        self.data = {}
        if 'cls' in kwargs:
            self.cls = kwargs['cls']
        else:
            self.cls = None
        self.transactionName = None
        self.dirty = False
        self.read()
        self.clear_dirtybits()
        self.timer = elapsed.ElapsedTimer()
        # TODO: "metadata" is really just a hidden prefix
        if "metadata" in kwargs:
            self.metadata_key = kwargs["metadata"]
        else:
            self.metadata_key = "__metadata__"


    # destructor
    def __del__(self):
        return # causes problems with file IO in some cases, racey
        if self.dirty:
            self.logger.debug("one last write")
            self.write()


    def read(self, verbose = False):
        if self.transactionName is not None:
            filename = f"{self.masterFilename}.{self.transactionName}"
        else:
            filename = self.masterFilename
        self.logger.debug(f"reading from {filename}")
        if not os.path.exists(filename):
            self.logger.debug("whoopsie, no file")
            return None
        try:
        # https://stackoverflow.com/questions/39450065/python-3-read-write-compressed-json-objects-from-to-gzip-file
            with bz2.open(filename, "r") as statefile:
                data = json.loads(statefile.read().decode('utf-8'))
                self.data = self.classify(data)
                if self.data is None:
                    self.logger.debug("json.load() -> self.data is None")
                    self.data = {}
        except json.decoder.JSONDecodeError:
            os.rename(filename, f"{filename}.busted")
            self.logger.warn(f"whoopsie, JSONDecodeError;" \
                        f" saved in {filename}.busted")
            self.data = {}
        self.logger.debug(f"read {len(self.data.items())} items")
        self.dirty = False


    def write(self, verbose = False):
        lock = threading.RLock()
        lock.acquire()
        if self.transactionName is not None:
            filename = f"{self.masterFilename}.{self.transactionName}"
        else:
            filename = self.masterFilename
        # self.logger.debug(f"writing data: {filename}")
        self.mkdir(filename)
        with bz2.open(f"{filename}.tmp", "w") as statefile:
            statefile.write(json.dumps(self.de_classify(), \
                        sort_keys=True, indent=4).encode('utf-8'))
        os.rename(f"{filename}.tmp", filename)
        self.dirty = False
        lock.release()


    def classify(self, data):
        if self.cls:
            for key in data.keys():
                data[key] = self.cls().deserialize(data[key])
        return data


    def de_classify(self):
        data = {}
        if self.cls:
            for key in self.data.keys():
                data[key] = self.data[key].serialize()
            return data
        return self.data


    def lazy_write(self):
        if self.lazy_timer == 0 or self.timer.elapsed() > self.lazy_timer:
            # self.logger.debug("lazy timer expired; writing")
            self.write()
            self.timer.reset()


    def mkdir(self, filename):
        dir = os.path.dirname(filename)
        if dir is not "" and not os.path.exists(dir):
            os.makedirs(dir)


    def startTransaction(self, transactionName = "tmp"):
        self.transactionName = transactionName
        self.write()


    def closeTransaction(self, verbose = False):
        filename = f"{self.masterFilename}.{self.transactionName}"
        os.rename(filename, self.masterFilename)


    def touch(self, key):
        self.dirtybits[key] = 1


    def set(self, key, value):
        self[key] = value


    def __setitem__(self, key, value):
        lock = threading.RLock()
        lock.acquire()
        if self.cls:
            if key not in self.data:
                self.data[key] = self.cls(*self.args, **self.kwargs)
            self.data[key].set(value)
        else:
            self.data[key] = value
        self.touch(key)
        self.dirty = True
        self.lazy_write()
        lock.release()


    def __getitem__(self, key):
        return self.data[key]


    def __contains__(self, key):
        return key in self.data


    def __delete__(self, key):
        del self.data[key]


    def __iter__(self):
        return iter(self.data)


    def __len__(self):
        return len(self.data)


    def keys(self):
        lock = threading.RLock()
        lock.acquire()
        keys = self.data.keys()
        lock.release()
        return keys


    def delete(self, key):
        lock = threading.RLock()
        lock.acquire()
        del self.data[key]
        self.lazy_write()
        if key in self.dirtybits:
            del self.dirtybits[key]
        lock.release()


    def items(self):
        lock = threading.RLock()
        lock.acquire()
        if self.metadata_key in self.data:
            dupe = self.data.copy()
            del dupe[self.metadata_key]
            return dupe.items()
        lock.release()
        return self.data.items()
            

    def contains_p(self, key):
        return key in self.data


    def clear_dirtybits(self):
        self.dirtybits = {}


    # returns the keys which haven't been touched
    # https://stackoverflow.com/questions/3462143/get-difference-between-two-lists
    def clean_keys(self):
        return [key for key in self.data if key not in self.dirtybits]


if __name__ == "__main__":
    import hashlib
    pd = PersistentDict("pd.json", lazy_write=60)
    for key in range(100000):
        h = hashlib.sha256()
        h.update(f"this is a {key}".encode())
        pd.set(key, h.hexdigest())
        if key % 1000 == 0:
            print(key)
