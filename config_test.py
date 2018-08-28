#!/usr/bin/env python3

import unittest, logging
import config

class TestCacheMethods(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s', level=logging.DEBUG)
        cfg = config.Config.instance()
        cfg.init("test-config.txt", "source", "backup", hostname="localhost")

    def tearDown(self):
        pass


    def test_config(self):
        cfg = config.Config.instance()
        source_contexts = list(cfg.get_contexts_for_key("source").keys())
        self.assertTrue(len(source_contexts)>0)


    def test_unpacking(self):
        cfg = config.Config.instance()
        contexts = cfg.get_contexts_for_key_and_target("backup", "localhost")
        self.assertTrue(len(contexts)>0)
        contexts = cfg.get_contexts_for_key_and_target("source", "localhost")
        self.assertTrue(len(contexts)>0)


    def test_options(self):
        cfg = config.Config.instance()
        cfg.init("config.txt", "source", "backup")
        self.assertEquals(cfg.get("global", "rescan"), "1d")
        self.assertEquals(cfg.get("global", "BLOCKSIZE"), "128K")
        self.assertEquals(cfg.get("global", "NBLOCKS"), "10")
        self.assertEquals(cfg.get("global", "IO_RATELIMIT"), "10MB/s")


        

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCacheMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
