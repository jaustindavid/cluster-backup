#!/usr/bin/env python3

import unittest, json, logging, _thread, sys
from datagram import *

class TestMethods(unittest.TestCase):

    def echo_server(self):
        try:
            s = DatagramServer("localhost", 1492)
            while True:
                datagram = s.accept()
                datagram.set(bool=not datagram)
                datagram.send()
                datagram.close()
        except OSError:
            pass


    def setUp(self):
        logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s', level=logging.DEBUG)
        _thread.start_new_thread(self.echo_server, ())


    def tearDown(self):
        pass


    def test_construction(self):
        dg = Datagram("This is one string")
        self.assertTrue(dg)

        dg = Datagram("This is one string", bool=False)
        self.assertFalse(dg)

        dg = Datagram("test", "string with spaces")
        self.assertTrue(dg)
        self.assertEquals(dg[0], "test")
        self.assertEquals(dg[1], "string with spaces")


    def test_indexing(self):
        dg = Datagram("test", "string with spaces")
        self.assertTrue(dg)
        self.assertEquals(dg[0], "test")
        self.assertEquals(dg[1], "string with spaces")


    def test_echo(self):
        data = "an arbitrary string"
        datagram = Datagram(data, server="localhost", port=1492)
        self.assertTrue(datagram.send())
        echo = datagram.receive()
        self.assertEquals(data, echo)

        data = { '1': "a key", 'two': "second key" }
        datagram = Datagram(data, server="localhost", port=1492)
        self.assertTrue(datagram.send())
        echo = datagram.receive()
        self.assertEquals(data, echo)
        self.assertFalse(datagram)

        datagram = Datagram(data, bool=False, server="localhost", port=1492)
        self.assertTrue(datagram.send())
        echo = datagram.receive()
        self.assertEquals(data, echo)
        print(f"{datagram.data}")
        self.assertTrue(datagram)


    def test_1huge_echo(self):
        data = "an arbitrary string"
        buffer = []
        for i in range(0, 100000):
            buffer.append(f"{i}{data}{i}")
        datagram = Datagram(buffer, server="localhost", port=1492)
        self.assertTrue(datagram.send())
        echo = datagram.receive()
        self.assertEquals(buffer, echo)
        self.assertEquals(echo[999], "999an arbitrary string999")
        print(f"JFYI, buffer was {sys.getsizeof(buffer)} bytes!")


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
