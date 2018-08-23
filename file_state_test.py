#!/usr/local/bin/python3.6

import unittest, localState, os, subprocess, shutil

class TestCacheMethods(unittest.TestCase):

    def setUp(self):
        global tempdir
        # print(f"Building {tempdir}")
        subprocess.call(["mkdir", "-p", "tmp"])
        with open("/dev/zero") as zero:
            one_k = zero.read(1024);
        for i in range(0,10):
            filename = f"tmp/file_{i}.1k"
            outfile = open(filename, "w+")
            outfile.write(one_k)

    def tearDown(self):
        global tempdir
        shutil.rmtree(tempdir)

    def test_1_empty_scandir(self):
        files = localState.scandir(f"{tempdir}/foo")
        self.assertEquals(files["__total__"], 0)
        self.assertEquals(len(list(files)), 1)

    def test_2_scandir(self):
        global tempdir
        files = localState.scandir(tempdir)
        self.assertEquals(files["__total__"], 10240)
        for i in range(0,10):
            self.assertEquals(files[f"{tempdir}/file_{i}.1k"].st_size, 1024)

    def test_3_lru_files_to_size_target(self):
        global tempdir
        targets = localState.lru_files_to_size_target(localState.scandir(tempdir), 9500)
        self.assertEquals(len(targets), 1)
        targets = localState.lru_files_to_size_target(localState.scandir(tempdir))
        self.assertEquals(len(targets), 10)

    def test_4_cleanup(self):
        global tempdir
        targets = localState.lru_files_to_size_target(localState.scandir(tempdir), 9500)
        localState.cleanup(targets)
        targets = localState.lru_files_to_size_target(localState.scandir(tempdir), 9500)
        self.assertEquals(len(targets), 0)
        targets = localState.lru_files_to_size_target(localState.scandir(tempdir), 8500)
        self.assertEquals(len(targets), 1)
        self.assertEquals(targets[0], f"{tempdir}/file_1.1k")
        targets = localState.lru_files_to_size_target(localState.scandir(tempdir))
        localState.cleanup(targets)
        targets = localState.lru_files_to_size_target(localState.scandir(tempdir))
        self.assertEquals(len(targets), 0)


    def test_5_read(self):
        with open("file.txt", "w") as file:
            file.write('{"key": "value"}')
        ps = localState.PersistentState("file.txt")
        data = ps.read()
        self.assertEquals(data["key"], "value")

    def test_6_write(self):
        ps = localState.PersistentState("file.txt")
        ps.write({"key": "value"})
        with open("file.txt", "r") as file:
            contents = file.read()
        self.assertEquals(contents, '{"key": "value"}')


tempdir = "tmp"

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCacheMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
