#!/usr/bin/env python3

import unittest, json, logging
from lock import *
from persistent_dict import *

class TestMethods(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        if os.path.exists("test.json.bz2"):
            os.remove("test.json.bz2")


    def test_lock(self):
        lock = Lock(depth=2, expiry=0.25)
        lock['one'] = 0
        self.assertFalse(lock)
        lock['two'] = 1
        self.assertTrue(lock)
        with self.assertRaises(KeyError):
            lock['three'] = False

        self.assertTrue('one' in lock)
        time.sleep(0.5)
        self.assertFalse('one' in lock)
        lock['one'] = 0
        self.assertTrue('one' in lock)
        lock['one'] = 0         # should not raise
        lock['three'] = False   # should not raise
        self.assertEquals(len(lock), 2)
        with self.assertRaises(KeyError):
            lock['two'] = True
        del lock['one']
        lock['two'] = True
        self.assertFalse('one' in lock)
        self.assertTrue('two' in lock)


    def test_table(self):
        locks = {}
        locks['filename'] = Lock()
        locks['filename']['client 1'] = True
        self.assertEquals(len(locks), 1)
        self.assertTrue('client 1' in locks['filename'])
        self.assertFalse("client 2" in locks['filename'])
        # locktable = LockTable("test.json.bz2", depth=2, expiry=0.25)
        locktable = PersistentDict("test.json.bz2", depth=2, expiry=0.25, cls=Lock)
        self.assertEquals(len(locktable), 0)
        self.assertFalse("random lock" in locktable)
        locktable["filename"] = "client 1"
        print(locktable.data)
        self.assertEquals(len(locktable), 1)
        self.assertTrue("client 1" in locktable['filename'])
        self.assertFalse("client 2" in locktable['filename'])
        self.assertEquals(len(locktable['filename']), 1)
        time.sleep(0.3)
        self.assertEquals(len(locktable['filename']), 0)
        self.assertFalse("client 1" in locktable['filename'])
        locktable["filename"] = "client 1"
        locktable["filename"] = "client 2"
        self.assertEquals(len(locktable['filename']), 2)
        self.assertEquals(list(locktable['filename']), ["client 1", "client 2"])
        del locktable['filename']['client 1']
        self.assertEquals(list(locktable['filename']), ["client 2"])
        locktable["filename"] = "client 3"
        locktable.write()
        locks2 = PersistentDict("test.json.bz2", depth=2, expiry=0.25, cls=Lock)
        print(locks2.data)
        self.assertTrue("client 3" in locks2['filename'])



if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
