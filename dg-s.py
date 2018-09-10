#!/usr/bin/env python3

from datagram import *
import _thread

PORT = 5004
def echo_server():
    s = DatagramServer("localhost", PORT)
    while True:
        with s.accept(name='echo server') as datagram:
            while datagram:
                got = datagram.value()
                returned = [ "ack", got ]
                # print(f"GOT >{got}< RETURNING >{returned}<")
                datagram.send(returned)
                datagram.receive()

logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s', 
                    level=logging.DEBUG)

s = DatagramServer("localhost", 5005)

print(f"Listening on {PORT}")
echo_server()
