#!/usr/bin/env python3

import unittest, json, comms, logging

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

        c = comms.Communique(None, bool=False)
        self.assertFalse(c)

        c = comms.Communique(None, bool=True)
        self.assertTrue(c)

        c = comms.Communique("This is one string", bool=False)
        self.assertFalse(c)

        c = comms.Communique("")
        self.assertFalse(c)

        c = comms.Communique("None")
        self.assertTrue(c)

        c = comms.Communique(0)
        self.assertFalse(c)

        c = comms.Communique("test", "string with spaces")
        self.assertTrue(c)
        self.assertEquals(c[0], "test")
        self.assertEquals(c[1], "string with spaces")


    def test_indexing(self):
        c = comms.Communique("test string")
        self.assertEquals(c[0], "test string")

        c = comms.Communique(["test string"])
        self.assertEquals(c[0], "test string")

        c = comms.Communique(("test string", "with more"))
        self.assertEquals(c[0], "test string")
        self.assertEquals(c[1], "with more")

        c = comms.Communique("test string", "123")
        self.assertEquals(c[1], 123)



    def test_serialize(self):
        data = ("test", "string with spaces")
        c = comms.Communique(data)

        string = str(c)
        self.assertEquals(str(c), json.dumps(data))

        see = comms.Communique.build(string)
        self.assertEquals(see, c)
        self.assertEquals(see[0], data[0])
        self.assertEquals(see[0], "test")

        data2 = ("tEst", "string with spaces")
        see2 = comms.Communique(data2)
        self.assertNotEquals(see, see2)



    def test_len(self):
        dump = json.dumps(["test", "string with spaces"])
        c = comms.Communique.build(dump)
        self.assertEquals(len(c), 2)
        
        c = comms.Communique("ack")
        self.assertEquals(len(c), 1)

        c = comms.Communique("nack")
        self.assertEquals(len(c), 1)


    def test_append(self):
        c = comms.Communique("command", "source", "client")
        print(str(c))
        self.assertEquals(len(c), 3)
        args = list()
        c.append(args)
        c.append("filename")
        self.assertEquals(len(c), 4)
        print(f"len(c) = {len(c)}")
        print(f"str(c) = {str(c)}")
        c = comms.Communique("first")
        self.assertEquals(len(c), 1)
        print(f"len(c) = {len(c)}")
        print(f"str(c) = {str(c)}")
        c.append("second")
        print(f"len(c) = {len(c)}")
        print(f"str(c) = {str(c)}")
        self.assertEquals(len(c), 2)


    def test_eq(self):
        data = "ack"
        self.assertEquals(data, "ack")
        c = comms.Communique(data)
        self.assertEquals(c, data)

        c = comms.Communique("ack")
        print(f"{str(c)} == {'ack'} ?")
        self.assertTrue(c == "ack")
        self.assertEquals(c, "ack")


    def test_for(self):
        data = [ "one item" ]
        c = comms.Communique(data)
        print(f"one element: {str(c)}")
        i = 0
        for element in c:
            self.assertTrue(type(element) is str)
            self.assertEquals(element, data[i])
            i += 1
        data = [ "ayeeeee", "beeeee", "seeeeee", "duheeeeeee" ]
        c = comms.Communique(data)
        print(f"len(c) = {len(c)}")
        print(f"str(c) = {str(c)}")
        for item in c:
            print(f"item: {item}")
        data = [ "one thing" ]
        c = comms.Communique(data)
        print(f"len(c) = {len(c)}")
        print(f"str(c) = {str(c)}")
        i = 0
        for item in c:
            i += 1
            print(f"item: {item}")
        self.assertEquals(i, 1)
        data = "one string"
        c = comms.Communique(data)
        print(f"len(c) = {len(c)}")
        print(f"str(c) = {str(c)}")
        i = 0
        for item in c:
            i += 1
            print(f"item: {item}")
        self.assertEquals(i, 1)
        c.append("plus one more")
        print(f"len(c) = {len(c)}")
        print(f"str(c) = {str(c)}")
        i = 0
        for item in c:
            i += 1
            print(f"item: {item}")
        self.assertEquals(i, 2)


    def test_negatives(self):
        data = "True"
        c = comms.Communique(data, negatives=("True",))
        self.assertFalse(c)
        

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
