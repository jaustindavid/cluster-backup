#! python3.x

"""
usage:
    from elapsed import ElapsedTimer
    ...
    timer = ElapsedTimer()
    while True:
        print(f"It's been {timer.elapsed()}s since start")
        if timer.once_every(10):
            print("This happens only once every 10s")
        time.sleep(1)

    ...
    timer.reset() 
    print(f"0: {int(timer.elapsed())}; True: {timer.once_every(5)}")
    print(f"0: {int(timer.elapsed())}; False: {timer.once_every(5)}")
    time.sleep(5)
    print(f"5: {int(timer.elapsed())}; True: {timer.once_every(5)}")
"""

import time

class ElapsedTimer:
    def __init__(self):
        self.reset()

    def elapsed(self):
        return time.time() - self.start

    def reset(self):
        self.start = time.time()
        self.last = 0

    def once_every(self, interval):
        if (self.last + interval) < time.time():
            self.last = time.time()
            return True
        return False


# TODO
"""
    ed = ExpiringDict(3)
    ed[1] = "whatever" # set or reset a timer 
    if ed[1]:   # True when this timer expires

    ed.expired() # list of expired keys
"""
class ExpiringDict:
    def __init__(self, expiry, sense=True):
        self.expiry = expiry
        self.sense = sense  # what to return on expiration
        self.data = {}
    

    def __getitem__(self, key):
        if self.data[key].elapsed() > self.expiry:
            return self.sense
        else:
            return not self.sense


    def __setitem__(self, key, value):
        if key in self.data:
            self.data[key].reset()
        else:
            self.data[key] = ElapsedTimer()


    def __delete__(self, key):
        del self.data[key]


    def __contains__(self, key):
        return key in self.data


    def expired(self):
        return [ key for key in self.data if self[key] ]


if __name__ == "__main__":
    from elapsed import ElapsedTimer

    timer = ElapsedTimer()
    while timer.elapsed() < 5:
        print(f"It's been {timer.elapsed():3.1f}s since start")
        if timer.once_every(2):
            print("This happens only once every 2s")
        time.sleep(1)

