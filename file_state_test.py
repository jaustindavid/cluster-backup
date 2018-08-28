#!/usr/local/bin/python3.6

import unittest, os, subprocess, shutil, logging
import config, file_state

class TestMethods(unittest.TestCase):

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
        logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s',
                            level=logging.DEBUG)


    def tearDown(self):
        global tempdir
        shutil.rmtree(tempdir)


    def test_escape(self):
        string = r'this is (a neato) string & stuff'
        self.assertEquals(file_state.escape_special_chars(string), \
                            r'this\ is\ \(a\ neato\)\ string\ \&\ stuff')


    def test_rsync(self):
        global tempdir
        cfg = config.Config.instance()
        cfg.set("global", "verbose", "yes")
        source = f"{tempdir}/file_0.1k"
        dest = f"{tempdir}/copy_file_0.1k"
        exitvalue = file_state.rsync(source, dest)
        self.assertEquals(exitvalue, 0)
        self.assertTrue(os.path.exists(dest))

        dest = f"{tempdir}/copy' file_0.1k"
        exitvalue = file_state.rsync(source, dest)
        self.assertEquals(exitvalue, 0)
        self.assertTrue(os.path.exists(dest))

# 2018-08-25 15:20:14,904 [rsync] ['rsync', '-a', '--inplace', '--partial', '--timeout', '180', "mini:/Volumes/Docs_ZFS/BitTorrent\\ Sync/hacking/Drew's\\ things/things/3d\\ printer/cuttlefish/M_cuttlefish_upright_80pct.gcode.gz", "/mnt/data/austin/cluster-backups/a55fde13/BitTorrent Sync/hacking/Drew's things/things/3dprinter/cuttlefish/M_cuttlefish_upright_80pct.gcode.gz", '-v', '--progress']

        source = "mini:/Volumes/Docs_ZFS/BitTorrent Sync/hacking/Drew's things/things/3d printer/cuttlefish/M_cuttlefish_upright_80pct.gcode.gz"
        dest = "/tmp" # "/mnt/data/austin/cluster-backups/a55fde13/BitTorrent Sync/hacking/Drew's things/things/3dprinter/cuttlefish/M_cuttlefish_upright_80pct.gcode.gz"
        exitvalue = file_state.rsync(source, dest)
        self.assertEquals(exitvalue, 0)
        self.assertTrue(os.path.exists(dest))



tempdir = "tmp"

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
