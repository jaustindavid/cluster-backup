#!/usr/bin/env python3

import unittest, logging, os, shutil, time
import server, config
from file_state import sum_sha256
import subprocess

class TestMethods(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s',
                            level=logging.DEBUG)

    def tearDown(self):
        pass


    def test_1_random_sample(self):
        logger = logging.getLogger()
        # logger.setLevel(logging.INFO)
        cfg = config.Config.instance()
        cfg.init("test-config.txt", "source", "backup", hostname="localhost")
        source_contexts = list(cfg.get_contexts_for_key("source").keys())
        context = source_contexts[0]
        servlet = server.Servlet(context)

        self.assertEquals(len(servlet.random_subset([], 11)), 0)

        data = [ "zero", "one", "two", "three", "four", \
                "five", "six", "seven", "eight", "nine" ]
        logger.debug(f"data: {data}")
        sampled = servlet.random_subset(data, 10)
        logger.debug(f"sample(10): {sampled}")
        self.assertEquals(len(sampled), 10)

        sampled = servlet.random_subset(data, 6)
        self.assertEquals(len(sampled), 6)
        logger.debug(f"sample(6): {sampled}")

        while len(sampled) > 0:
            datum = sampled[0]
            sampled = sampled[1:]
            self.assertTrue(datum not in sampled)


    def test_2_least_served(self):
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        cfg = config.Config.instance()
        cfg.init("test-config.txt", "source", "backup", hostname="localhost")
        source_contexts = list(cfg.get_contexts_for_key("source").keys())
        context = source_contexts[0]
        servlet = server.Servlet(context)
        servlet.scanner.scan()
        servlet.update_files()

        candidates = list(servlet.scanner.keys())

        # check varying sizes (only one file should be served)
        bigfile = servlet.least_served(candidates, 2**30)
        logger.debug(f"got {bigfile}: {servlet.scanner[bigfile]['size']}")
        self.assertTrue(servlet.scanner[bigfile]['size'] < 2**30)
        otherfile = servlet.least_served(candidates, 2**30)
        self.assertTrue(servlet.scanner[otherfile]['size'] < 2**30)
        self.assertEquals(bigfile, otherfile)


    def test_3_request(self):
        logger = logging.getLogger()
        # logger.setLevel(logging.INFO)
        cfg = config.Config.instance()
        cfg.init("test-config.txt", "source", "backup", hostname="localhost")
        source_contexts = list(cfg.get_contexts_for_key("source").keys())
        context = source_contexts[0]
        servlet = server.Servlet(context)
        servlet.scanner.scan()
        servlet.update_files()

        client_contexts = list(cfg.get_contexts_for_key("backup").keys())
        client0 = client_contexts[0]
        client1 = client_contexts[1]

        # claim the biggest & confirm the next-biggest is served
        bigfile = servlet.request((client0, 2**30))[0]
        hash = servlet.scanner[bigfile]["checksum"]
        response = servlet.claim([client0, bigfile, hash])
        self.assertTrue(response)
        response = servlet.claim([client1, bigfile, hash])
        self.assertTrue(response)
        otherfile = servlet.request((client0, 2**30))[0]
        self.assertTrue(servlet.scanner[otherfile]['size'] < 2**30)
        self.assertNotEquals(bigfile, otherfile)
        logger.debug(f"{client0} got {otherfile}: {servlet.scanner[otherfile]['size']}")
        logger.debug("\n\n\n")

        # claim all the files
        filecount = 2 # claimed 2 so far
        for client in client0, client1:
            response = True
            while response:
                response = servlet.request((client, 2**30))
                if response:
                    filename = response[0]
                    hash = servlet.scanner[filename]["checksum"]
                    response = servlet.claim([client, filename, hash])
                    self.assertTrue(response)
                    filecount += 1
                else:
                    break
        self.assertEquals(filecount, 40)

        # I should get a big one now
        client2 = client_contexts[2]
        bigfile = servlet.request((client2, 2**30))[0]
        logger.debug(f"got {bigfile}: {servlet.scanner[bigfile]['size']}")
        self.assertTrue(servlet.scanner[bigfile]['size'] < 2**30)
        otherfile = servlet.request((client2, 50*2**20))[0]
        logger.debug(f"got {otherfile}: {servlet.scanner[otherfile]['size']}")
        self.assertTrue(servlet.scanner[otherfile]['size'] < 50*2**20)
        self.assertNotEquals(bigfile, otherfile)


    def test_locking(self):
        return
        cfg = config.Config.instance()
        cfg.init("test-config.txt", "source", "backup", hostname="localhost")
        source_contexts = list(cfg.get_contexts_for_key("source").keys())
        context = source_contexts[0]
        servlet = server.Servlet(context)
        servlet.scanner.scan()
        servlet.update_files()

        client_contexts = list(cfg.get_contexts_for_key("backup").keys())
        client0 = client_contexts[0]
        client1 = client_contexts[1]

        # request (not claim) a big one from two clients: better be 2 files
        bigfile = servlet.request((client0, 2**30))[0]
        otherfile = servlet.request((client1, 2**30))[0]
        self.assertNotEquals(bigfile, otherfile)

        # same thing -- I should get the same answers
        bigfile2 = servlet.request((client0, 2**30))[0]
        otherfile2 = servlet.request((client1, 2**30))[0]
        self.assertEquals(bigfile, bigfile2)
        self.assertEquals(otherfile, otherfile2)


        # requests over time will break locks
        bigfile = servlet.request((client0, 2**30))[0]
        time.sleep(6)
        otherfile = servlet.request((client1, 2**30))[0]
        self.assertEquals(bigfile, otherfile)



if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
