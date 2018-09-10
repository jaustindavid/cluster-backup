#!/usr/bin/env python3

import time, random

# a single event counter
class Statistic:
    def __init__(self, value=0, *args, **kwargs):
        if 'buckets' in kwargs:
            self.buckets = kwargs['buckets']
        else:
            self.buckets = (0, 60, 300)
        self.set(value)


    def set(self, value):
        self.data = {}
        self.starts = {}
        for bucket in self.buckets:
            self.data[bucket] = value
            self.starts[bucket] = time.time()


    def incr(self, other):
        for bucket in self.buckets:
            if bucket and self.starts[bucket] + bucket < time.time():
               self.data[bucket] = 0
               self.starts[bucket] = time.time()
            self.data[bucket] += other
        return self.data[0]


    def __int__(self):
        return self.data[0]


    def qps(self):
        times = {}
        for bucket in self.buckets:
            # print(f"{bucket}: count={self.data[bucket]}")
            q = self.data[bucket]
            s = time.time() - self.starts[bucket]
            times[bucket] = q/s
        return times


# holds multiple event counters
class Stats:
    def __init__(self, *args, **kwargs):
        self.data = {}
        self.args = args
        self.kwargs = kwargs


    def reset(self):
        for stat in self.data:
            self.data[stat].set(0)


    def __setitem__(self, key, value):
        if key not in self.data:
            self.data[key] = Statistic(*self.args, **self.kwargs)
        self.data[key].set(value)


    # get is deliberately magic; if someone tries to use
    # a new Statistic, start at default
    def __getitem__(self, key):
        if key not in self.data:
            self.data[key] = Statistic(*self.args, **self.kwargs)
        return self.data[key]


    def __contains__(self, key):
        return key in self.data


    def __delete__(self, key):
        del self.data[key]


    def __iter__(self):
        return iter(self.data)



if __name__ == "__main__":
    buckets = (0, 1, 2, 5, 10)
    # stats = Stats(buckets=(0, 5))
    stats = {}
    stats['tightloop'] = Statistic(buckets=buckets)
    start = time.time()
    while (time.time() < start+10):
        stats['tightloop'].incr(1)
        time.sleep(random.randrange(1, 10)/100)
    print(f"qps: {stats['tightloop'].qps()}")
