#!/usr/bin/env python3

import unittest, utils

class TestMethods(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass


    def test_bytes(self):
        self.assertEquals(utils.str_to_bytes("27"), 27)
        self.assertEquals(utils.str_to_bytes("27k"), 27*1024)
        self.assertEquals(utils.str_to_bytes("27T"), 27*2**40)
        self.assertEquals(utils.str_to_bytes("27mbT"), 27*2**20)
        self.assertEquals(utils.str_to_bytes("27gbT"), 27*2**30)
        self.assertEquals(utils.bytes_to_str(27*2**30), "27.000G")
        self.assertEquals(utils.bytes_to_str(27*2**30+11*2**10), "27.000G")
        self.assertEquals(utils.bytes_to_str(27*2**30+11*2**10), "27.000G")
        self.assertEquals(utils.bytes_to_str(27*2**20), "27.000M")
        self.assertEquals(utils.str_to_bytes("1.2k"), 1228)
        

    def test_durations(self):
        self.assertEquals(utils.str_to_duration("42"), 42)
        self.assertEquals(utils.str_to_duration("60m"), 60*60)
        self.assertEquals(utils.str_to_duration("27h5m"), 27*3600+5*60)
        self.assertEquals(utils.str_to_duration("2d5m8"), 2*24*3600+5*60+8)
        self.assertEquals(utils.duration_to_str(60*60),"60m")
        self.assertEquals(utils.duration_to_str(3*60*60+11),"3h11s")
        self.assertEquals(utils.duration_to_str(8*60+3*60*60*24+11),"3d8m11s")

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
