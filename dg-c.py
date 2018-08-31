#! python

from datagram import *
import time

logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s',
                    level=logging.DEBUG)

datagram = Datagram("Hello, World!", server="localhost", port=5000)
if datagram.send(): # or send(server="localhost", port=5000)
    print(datagram.receive())
    datagram.set(datagram.value().lower())
    datagram.send()

time.sleep(5)
