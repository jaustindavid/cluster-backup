#!/usr/bin/env python3

import unittest, logging
import client_lite, config

class TestMethods(unittest.TestCase):

    def setUp(self):
        cfg = config.Config.instance()
        cfg.init("test-config.txt", "source", "backup", hostname="localhost")

    def tearDown(self):
        pass


    def test_generate_priority_list(self):
        cfg = config.Config.instance()
        context = list(cfg.get_contexts_for_key("backup").keys())[0]
        clientlet = client_lite.Clientlet(context)
        clientlet.get_metadata()

        # test empty input
        pl = clientlet.generate_priority_list({})
        self.assertEquals(pl, [])
        source_contexts = cfg.get_contexts_for_key("source")
        source_context = list(source_contexts.keys())[0]
        files = {   'fifteen': ( 1500, 0 ),
                    'ten': ( 1000, 3 ),
                    'twenty': ( 2000, 1 ),
                    'five': ( 500, 0 ),
                    'thirty': ( 3000, 2 ),
                }
        inventory = {}

        # test single-source input
        inventory[source_context] = files
        pl = clientlet.generate_priority_list(inventory)
        # priorities should be in increasing order
        for i in range(0, len(pl) - 1):
            self.assertTrue(pl[i].ratio <= pl[i+1].ratio)
        self.assertEquals(len(pl), 5)

        # test mingled-source input
        source_context = list(source_contexts.keys())[1]
        inventory[source_context] = files
        pl = clientlet.generate_priority_list(inventory)
        # priorities should be in increasing order
        for i in range(0, len(pl) - 1):
            self.assertTrue(pl[i].ratio <= pl[i+1].ratio)
        self.assertEquals(len(pl), 10)


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
