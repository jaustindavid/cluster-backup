#!/usr/bin/env python3

import unittest, scanner, config, logging, os, shutil
import subprocess

class TestMethods(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s',
                            level=logging.DEBUG)
        if not os.path.exists("/tmp/scanner-test"):
            os.mkdir("/tmp/scanner-test")
            cwd = os.getcwd()
            os.chdir("/tmp/scanner-test")
            subprocess.call(["tar", "xfvz", "/Users/austind/src/cb/scanner-test.tgz"])
            os.chdir(cwd)


    def tearDown(self):
        # shutil.rmtree('/tmp/scanner-test')
        os.remove("/tmp/scanner-test/.cb.test_scanner.json")
        pass


    def test_config(self):
        cfg = config.Config.instance()
        cfg.init("test-config.txt", "source", "backup", hostname="localhost")
        contexts = list(cfg.get_contexts_for_key("source").keys())
        print(contexts)
        context = contexts[0]
        path = config.path_for(cfg.get(context, "source"))
        print(path)
        s = scanner.Scanner(context, path)
        s.scan()
        filename = list(s.keys())[0]
        self.assertTrue(os.path.exists(f"{path}/{filename}"))


    def test_drop(self):
        s = scanner.Scanner("test_scanner", "/tmp/scanner-test")
        s.scan()
        files = list(s.keys())
        self.assertEquals(s["source3/1kb.zero"]["size"], 1024)
        s.drop("./one.zero")
        self.assertFalse(os.path.exists("/tmp/scanner-test/one.zero"))
        print(s.keys())
        self.assertEquals(s.consumption(), 10240)


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
