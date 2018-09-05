#! python

import time
from datagram import *

logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s',
                    level=logging.DEBUG)

buffer = "a" * 1000

# datagram = Datagram(buffer, server="localhost", port=5000)
with Datagram(buffer, server="localhost", port=5005) as datagram:
    print(datagram.ping())
    if datagram.send(): # or send(server="localhost", port=5000)
        print(datagram.receive())
        buffer = "b" * 1000
        datagram.send(buffer)
        print("done")

time.sleep(5)
