#!/usr/bin/env python3

import unittest, time
from locker import Locker

class TestTimerMethods(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass


    def test_locker(self):
        locks = Locker(0.25)
        locks["foo"] = "bar"
        self.assertTrue(locks["foo"])
        with self.assertRaises(KeyError):
            c = locks["bar"]
        locks["foo"] = "baz"
        self.assertNotEquals(locks["foo"], "baz")
        self.assertEquals(locks["foo"], "bar")
        self.assertTrue("foo" in locks)
        self.assertFalse("foot" in locks)
        time.sleep(0.3)
        self.assertFalse(locks["foo"])
        del locks["foo"]
        with self.assertRaises(KeyError):
            self.assertFalse(locks["foo"])

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTimerMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
