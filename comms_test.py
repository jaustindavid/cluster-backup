#!/usr/bin/env python3

import unittest, comms, logging

class TestMethods(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s', level=logging.DEBUG)
        pass

    def tearDown(self):
        pass


    def test_construction(self):
        c = comms.Communique("This is one string")
        self.assertTrue(c)

        c = comms.Communique(None)
        self.assertFalse(c)

        c = comms.Communique(None, "more")
        self.assertFalse(c)

        c = comms.Communique("")
        self.assertFalse(c)

        c = comms.Communique("None")
        self.assertTrue(c)

        c = comms.Communique("", "a list")
        self.assertFalse(c)

        c = comms.Communique(None, "a list")
        self.assertFalse(c)

        c = comms.Communique("filename", 0)
        self.assertTrue(c)
        self.assertEquals(str(c), "filename @@ 0")


    def test_indexing(self):
        c = comms.Communique("test string")
        self.assertEquals(c[0], "test string")

        c = comms.Communique("test string", "with more")
        self.assertEquals(c[0], "test string")
        self.assertEquals(c[1], "with more")

        c = comms.Communique("test string", "123")
        self.assertEquals(c[1], 123)


    def test_serialize(self):
        c = comms.Communique("test")
        self.assertEquals(str(c), "test")

        c = comms.Communique("test", "string with spaces")
        self.assertEquals(str(c), "test @@ string with spaces")

        c = comms.Communique("test", "string with spaces", "and more")
        self.assertEquals(str(c), "test @@ string with spaces @@ and more")


    def test_specials(self):
        c = comms.Communique("test", "string with spaces", special=";")
        self.assertEquals(str(c), "test;string with spaces")


    def test_negatives(self):
        c = comms.Communique("ack")
        self.assertTrue(c)

        c = comms.Communique("nack", negatives=("nack", "NACK", "__none__"))
        self.assertFalse(c)

        c = comms.Communique("NACK", negatives=("nack", "NACK", "__none__"))
        self.assertFalse(c)

        c = comms.Communique("__none__", negatives=("nack", "NACK", "__none__"))
        self.assertFalse(c)


    def test_deserialize(self):
        c = comms.Communique.build("test @@ string with spaces")
        self.assertEquals(c[0], "test")
        self.assertEquals(c[1], "string with spaces")

        c = comms.Communique.build("1")
        self.assertEquals(c[0], 1)

        data = "__none__"
        c = comms.Communique.build(data, negatives=("nack", "__none__"))
        self.assertFalse(c)


    def test_len(self):
        c = comms.Communique.build("test @@ string with spaces")
        self.assertEquals(len(c), 2)
        
        c = comms.Communique("ack")
        self.assertEquals(len(c), 1)

        c = comms.Communique("nack")
        self.assertEquals(len(c), 1)


    def test_append(self):
        c = comms.Communique("command", "source", "client")
        args = list()
        c.append(args)
        self.assertEquals(str(c), "command @@ source @@ client")
        args = [ "filename" ]
        c.append(args)
        self.assertEquals(str(c), "command @@ source @@ client @@ filename")



if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
