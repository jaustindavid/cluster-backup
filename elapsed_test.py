#!/usr/bin/env python3

import unittest, elapsed, time

class TestTimerMethods(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass


    def test_timer(self):
        start = int(time.time())
        timer = elapsed.ElapsedTimer()
        self.assertEquals(timer.once_every(10), True)
        self.assertEquals(timer.once_every(10), False)
        time.sleep(3)
        print(f"So far {time.time() - start}s elapsed")
        self.assertEquals(int(timer.elapsed()), 3)
        timer.reset()
        self.assertEquals(timer.once_every(10), True)
        self.assertEquals(int(timer.elapsed()), 0)
        time.sleep(3)
        print(f"So far {time.time() - start}s elapsed")
        self.assertEquals(int(timer.elapsed()), 3)
        self.assertEquals(timer.once_every(10), False)
        time.sleep(7)
        print(f"So far {time.time() - start}s elapsed")
        self.assertEquals(timer.once_every(10), True)


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTimerMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
