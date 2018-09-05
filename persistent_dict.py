#! python 3.x

"""
from persistent_dict import PersistentDict

pd = PersistentDict("dict.json")
pd["key"] = "value"

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
        self.lock = threading.Lock()
        self.read()
        self.clear_dirtybits()
        self.timer = elapsed.ElapsedTimer()


    def read(self, verbose = False):
        filename = self.masterFilename
        self.logger.debug(f"reading from {filename}")
        if not os.path.exists(filename):
            self.logger.debug("whoopsie, no file")
            return None
        self.lock.acquire()
        try:
        # https://stackoverflow.com/questions/39450065/python-3-read-write-compressed-json-objects-from-to-gzip-file
            with bz2.open(filename, "r") as statefile:
                data = json.loads(statefile.read().decode('utf-8'))
                self.data = self.classify(data)
                if self.data is None:
                    self.logger.debug("json.load() -> self.data is None")
                    self.data = {}
        except (json.decoder.JSONDecodeError, EOFError):
            os.rename(filename, f"{filename}.busted")
            self.logger.warn(f"whoopsie, JSONDecodeError;" \
                        f" saved in {filename}.busted")
            self.data = {}
        self.logger.debug(f"read {len(self.data.items())} items")
        self.lock.release()


    def write(self, verbose = False):
        filename = self.masterFilename
        self.mkdir(filename)
        with bz2.open(f"{filename}.tmp", "w") as statefile:
            statefile.write(json.dumps(self.de_classify(), \
                        sort_keys=True, indent=4).encode('utf-8'))
        os.rename(f"{filename}.tmp", filename)


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
            self.lock.acquire()
            if self.lazy_timer == 0 or self.timer.elapsed() > self.lazy_timer:
                self.write()
                self.timer.reset()
            self.lock.release()


    def mkdir(self, filename):
        dir = os.path.dirname(filename)
        if dir is not "" and not os.path.exists(dir):
            os.makedirs(dir)


    def touch(self, key):
        self.dirtybits[key] = 1


    def set(self, key, value):
        self[key] = value


    def __setitem__(self, key, value):
        # self.lock.acquire()
        if self.cls:
            if key not in self.data:
                self.data[key] = self.cls(*self.args, **self.kwargs)
            self.data[key].set(value)
        else:
            self.data[key] = value
        self.touch(key)
        self.lazy_write()
        # self.lock.release()


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
        # self.lock.acquire()
        keys = self.data.keys()
        # self.lock.release()
        return keys


    def __delitem__(self, key):
        # self.lock.acquire()
        del self.data[key]
        self.lazy_write()
        if key in self.dirtybits:
            del self.dirtybits[key]
        # self.lock.release()


    def items(self):
        # self.lock.acquire()
        items = self.data.items()
        # self.lock.release()
        return items
            

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
