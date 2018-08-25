#!/user/bin/env python3

"""
    import locker
    locks = locker.Locker(5) # a 5-second lock

    locker["foo"] = "data"
    if locker["foo"]:
        print("True!  But don't assign it None or False or 0, that's silly")
    locker["foo"] = "something else":
    if locker["foo"] == "something else":
        print("This would never happen")
    else:
        print("because this is not valid (lock is still held)")
    if locker["foo"].holds("data"):
        print("Not a thing!  Use == ")
    time.sleep(6)
    if locker["foo"].holds("data"):
        print("No longer true :((")
    locker["foo"].release()  # not a thing!!!
    locker["foo"] = "bar"
    del locker["foo"]
"""

import time
import elapsed

class Locker:
    def __init__(self, expiry):
        self.timers = elapsed.ExpiringDict(expiry)
        self.data = {}


    def __getitem__(self, key):
        if self.timers[key]:
            return self.data[key]
        else:
            return None


    # if I have this key *and* the data is the same, renew
    #                   , not expired, and different data, reject
    # if it's a new key, make it
    def __setitem__(self, key, value):
        if key in self.data:
            if self.data[key] == value:
                self.timers[key] = "whatever"
                return True
            if self.timers[key]:
                return False
        self.timers[key] = "whatever"
        self.data[key] = value
        return True


    def __delitem__(self, key):
        del self.data[key]
        del self.timers[key]


    def __contains__(self, key):
        if key in self.timers and self.timers[key]:
            return key in self.data
        else:
            return False

