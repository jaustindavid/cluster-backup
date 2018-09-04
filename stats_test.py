#!/usr/bin/env python3

import unittest, time
import stats

class TestMethods(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_stats(self):
        s = stats.Statistic()
        start = time.time()
        for i in range(0, 100):
            s.incr(1)
        time.sleep(1)
        self.assertEqual(int(s.qps()[0]), int(s.qps()[60]))
        self.assertEqual(round(s.qps()[60]/10, 0), 10)
        s = stats.Statistic(buckets=(0,10))
        start = time.time()
        for i in range(0, 100):
            s.incr(1)
        time.sleep(1)
        self.assertEqual(int(s.qps()[0]), int(s.qps()[10]))
        self.assertEqual(round(s.qps()[0]/10, 0), 10)


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
