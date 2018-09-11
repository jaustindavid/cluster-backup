#!/usr/bin/env python3

import unittest, json, logging, _thread, sys
from datagram import *

class TestMethods(unittest.TestCase):

    def echo_server(self):
        try:
            s = DatagramServer("localhost", 1492)
            while True:
                with s.accept(name='echo server') as datagram:
                    while datagram:
                        got = datagram.value()
                        returned = [ "ack", got ]
                        # print(f"GOT >{got}< RETURNING >{returned}<")
                        datagram.send(returned)
                        datagram.receive()
        except OSError:
            pass


    def setUp(self):
        logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s', level=logging.DEBUG)
        _thread.start_new_thread(self.echo_server, ())


    def tearDown(self):
        pass


    def test_construction(self):
        dg = Datagram("This is one string")
        self.assertFalse(dg)

        dg = Datagram("test", "string with spaces")
        self.assertFalse(dg)
        self.assertEquals(dg[0], "test")
        self.assertEquals(dg[1], "string with spaces")


    def test_indexing(self):
        dg = Datagram("test", "string with spaces")
        self.assertEquals(dg[0], "test")
        self.assertEquals(dg[1], "string with spaces")


    def test_echo(self):
        data = "an arbitrary string"
        datagram = Datagram(data, server="localhost", port=1492)
        self.assertTrue(datagram.send())
        echo = datagram.receive()
        self.assertEquals(echo[0], "ack")
        self.assertEquals(data, echo[1])

        data = { '1': "a key", 'two': "second key" }
        datagram = Datagram(data, server="localhost", port=1492)
        self.assertTrue(datagram.send())
        echo = datagram.receive()
        print(f"echoed: {echo}")
        self.assertTrue(datagram)
        self.assertEquals(echo[0], "ack")
        self.assertEquals(data, echo[1])


    def test_back_n_forth(self):
        datagram = Datagram(server="localhost", port=1492)
        self.assertTrue(datagram.ping())
        for i in range(0, 99):
            r = datagram.send(f"hello {i}")
            self.assertTrue(r)
            r = datagram.receive()
            self.assertTrue(r)
            self.assertEquals(r[0], "ack")
            self.assertEquals(r[1], f"hello {i}")



    def test_huge_echo(self):
        data = "an arbitrary string"
        buffer = []
        N = 1000
        for i in range(0, N):
            buffer.append(f"{i}{data}{i}")
        logging.getLogger().setLevel(logging.INFO)
        datagram = Datagram(buffer, server="localhost", port=1492)
        self.assertTrue(datagram.send())
        echo = datagram.receive()
        print(f"echoed: {echo} (type: {type(echo)})")
        self.assertEquals(buffer, echo[1])
        for i in range(0, N):
            self.assertEquals(echo[1][i], buffer[i])
        print(f"JFYI, buffer was {sys.getsizeof(buffer)} bytes!")


    def test_set(self):
        datagram = Datagram(None)
        datagram.set("one")
        self.assertEquals(datagram, "one")
        
        datagram.set("one", "two")
        self.assertEquals(datagram.value(), ("one", "two"))


    def test_connected(self):
        datagram = Datagram(None)
        self.assertFalse(datagram.connected())
        self.assertFalse(datagram.ping())
        datagram = Datagram("Stuff", server="localhost", port=1492)
        self.assertFalse(datagram.connected())
        self.assertTrue(datagram.ping())
        self.assertTrue(datagram.ping())
        datagram = Datagram("Stuff", server="localhost", port=1493)
        self.assertFalse(datagram.connected())
        self.assertFalse(datagram.ping())
        


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
