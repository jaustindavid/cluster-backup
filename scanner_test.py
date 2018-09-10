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
            subprocess.call(["tar", "xfz", f"{cwd}/scanner-test.tgz"])
            os.chdir(cwd)


    def tearDown(self):
        try:
            shutil.rmtree('/tmp/scanner-test')
            os.remove("/tmp/scanner-test/.cb.test_scanner.json.bz2")
        except FileNotFoundError:
            pass


    def test_config(self):
        cfg = config.Config.instance()
        cfg.init("test-config.txt", "source", "backup", hostname="localhost")
        contexts = list(cfg.get_contexts_for_key("source").keys())
        print(contexts)
        context = contexts[1]
        path = config.path_for(cfg.get(context, "source"))
        print(path)
        s = scanner.Scanner(context, path)
        s.scan()
        filename = list(s.keys())[0]
        self.assertTrue(os.path.exists(f"{path}/{filename}"))


    def test_drop(self):
        s = scanner.Scanner("test_scanner", "/tmp/scanner-test")
        s.scan()
        print(s.items())
        self.assertEquals(s["./one.zero"]["size"], 10240)
        self.assertEquals(s["directory/three.zero"]["size"], 10240)
        self.assertTrue(os.path.exists("/tmp/scanner-test/one.zero"))
        self.assertEquals(s.consumption(), 30720)
        s.drop("./one.zero")
        self.assertFalse(os.path.exists("/tmp/scanner-test/one.zero"))
        self.assertEquals(s.consumption(), 20480)


    def test_turbo(self):
        s = scanner.Scanner("test_scanner", "/tmp/scanner-test")
        s.scan(turbo=True)
        print(s.items())
        self.assertEquals(s['./one.zero']['size'], 10240)
        self.assertEquals(s['./one.zero']['checksum'], "deferred")
        s.scan()
        self.assertNotEquals(s['./one.zero']['checksum'], "deferred")


    def test_qps(self):
        import elapsed
        timer = elapsed.ElapsedTimer()
        dir = "/users/austind/src/cb/test/source1"
        s = scanner.Scanner("test", dir, checksums=False)
        print("starting QPS test")
        s.scan(turbo=True)
        print(f"So far: {timer.elapsed():5.2f}s")
        s.scan()
        print(f"Total: {timer.elapsed():5.2f}s")
        os.remove(f"{dir}/.cb.test.json.bz2")


    def test_qps_lite(self):
        import elapsed
        timer = elapsed.ElapsedTimer()
        dir = "/users/austind/src/cb/test/source1"
        s = scanner.ScannerLite("test", dir, checksums=False)
        print("starting QPS test")
        s.scan()
        print(f"Total: {timer.elapsed():5.2f}s")
        os.remove(f"{dir}/.cb.test-lite.json.bz2")


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
