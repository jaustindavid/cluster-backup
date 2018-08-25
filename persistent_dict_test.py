#!/usr/local/bin/python3.6

import unittest, persistent_dict, os, time, logging

class TestCacheMethods(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s',
                            level=logging.DEBUG)
        pass

    def tearDown(self):
        if os.path.exists("testfile.txt"):
            os.remove("testfile.txt")


    def test_set(self):
        pd = persistent_dict.PersistentDict("testfile.txt")
        pd.set("thing", 1)
        pd.set("thing two", "two")
        self.assertEquals(pd.get("thing"), 1)
        self.assertEquals(pd.get("thing two"), "two")


    def test_lazyio(self):
        pd = persistent_dict.PersistentDict("testfile.txt", 5)
        pd.set("thing", 1)
        pd.set("thing two", "two")
        pd2 = persistent_dict.PersistentDict("testfile.txt")
        print(pd.data)
        print(pd2.data)
        with self.assertRaises(KeyError):
            pd2.get("thing two")
        time.sleep(6)
        pd.set("thing", 2)
        pd2.read()
        self.assertEquals(pd2.get("thing two"), "two")


    def test_io(self):
        pd = persistent_dict.PersistentDict("testfile.txt")
        pd.set("thing", 1)
        pd.set("thing two", "two")
        pd2 = persistent_dict.PersistentDict("testfile.txt")
        self.assertEquals(pd2.get("thing two"), "two")

    def test_dirty(self):
        pd = persistent_dict.PersistentDict("testfile.txt")
        pd.set("one", 1)
        pd.set("two", 1)
        pd.set("three", 1)
        # print(pd.items())
        pd.clear_dirtybits()
        pd.set("three", 3)
        pd.set("two", 2)
        self.assertEquals(pd.clean_keys()[0], "one")

    def test_metadata(self):
        metadata = {1: "one", 3: "three"}
        pd = persistent_dict.PersistentDict("testfile.txt", \
                                            metadata_key="__metadata__")
        pd.set("one", 1)
        pd.set("red", 2)
        self.assertEquals(len(pd.items()), 2)

        pd.set("__metadata__", metadata)
        # print(pd.items())
        # print(f"{pd.items()}: {len(pd.items())}")
        self.assertEquals(len(pd.items()), 2)

        more_data = pd.get("__metadata__")
        self.assertEquals(more_data, metadata)


    def test_item(self):
        pd = persistent_dict.PersistentDict("testfile.txt")
        pd.set("one", "1")
        self.assertEquals(pd.get("one"), "1")
        self.assertEquals(pd["one"], "1")
        pd["two"] = "2"
        self.assertEquals(pd.get("two"), "2")
        self.assertTrue("two" in pd)

        

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCacheMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
