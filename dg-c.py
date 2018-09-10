#!/usr/bin/env python3

import time
from datagram import *

logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s',
                    level=logging.DEBUG)

buffer = "a" * 1000

PORT=5004

with Datagram(buffer, server="localhost", port=PORT) as datagram:
    print(f"ping: {datagram.ping()}")
    if datagram.send(): # or send(server="localhost", port=5000)
        print(datagram.receive())
        buffer = "b" * 1000
        if datagram.send(buffer):
            print(datagram.receive())
        else:
            print("receive failed")

time.sleep(5)
