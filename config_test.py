#!/usr/bin/env python3

import unittest, config

class TestCacheMethods(unittest.TestCase):

    def setUp(self):
        cfg = config.Config.instance()
        cfg.init("test-config.txt", "source", "backup", hostname="localhost")

    def tearDown(self):
        pass


    def test_config(self):
        cfg = config.Config.instance()
        print("config:", cfg.data)
        self.assertEquals(cfg.get("91cf8c76", "backup"), \
                           "localhost:~/cb/test/backup3")

    def test_unpacking(self):
        cfg = config.Config.instance()
        print("config:", cfg.data)
        contexts = cfg.get_contexts_for_key_and_target("backup", "localhost")
        print(contexts)
        contexts = cfg.get_contexts_for_key_and_target("source", "localhost")
        print(contexts)

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCacheMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
