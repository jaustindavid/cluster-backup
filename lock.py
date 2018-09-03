import persistent_dict, time, json

"""
from lock import Lock

l = Lock(depth=1)
if not l:
    l["client1"] = True # or l.set("client1")
    ... work with client1
    del l["client1"]

Also works in a dict:
locks = {}
locks['file1'] = Lock(depth=1).set("client1")
if locks['file1']:
    print("whoops, it's locked")



A Lock holds one (or more) values in a list, and keeps a timer (a float).
If tested, the lock will be empty (False) if the timer has expired
"""

class Lock:
    def __init__(self, *args, **kwargs):
        self.data = {}
        self.depth = 0
        self.expiry = 0
        if 'depth' in kwargs:
            self.depth = kwargs['depth']
        if 'expiry' in kwargs:
            self.expiry = kwargs['expiry']


    # returns a JSON-serlializable thing
    def serialize(self):
        return {'depth': self.depth, 
                'expiry': self.expiry, 
                'data': self.data }


    # takes a JSON-deserialized thing and turns it into self
    def deserialize(self, data):
        self.depth = data['depth']
        self.expiry = data['expiry']
        self.data = data['data']
        return self


    def _update(self):
        self.data = { key: start \
                     for key, start in self.data.items() \
                        if self.expiry == 0 or \
                            start+self.expiry > time.time() }
                

    def set(self, key):
        self[key] = "ignored"


    def __setitem__(self, key, ignored_value):
        self._update()
        if not key in self.data and self.depth > 0 \
            and len(self.data) >= self.depth:
            raise KeyError
        self.data[key] = time.time()


    def __bool__(self):
        self._update()
        return self.depth is not 0 and len(self) >= self.depth


    def __delitem__(self, key):
        del self.data[key]


    def __contains__(self, key):
        self._update()
        return key in self.data


    def __len__(self):
        self._update()
        return len(self.data)


    def __iter__(self):
        self._update()
        return iter(self.data.keys())


    def __repr__(self):
        self._update()
        return f"Lock{list(self.data.keys())}"
