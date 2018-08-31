#! python

from datagram import *

def handler(datagram):
    print(datagram.value())
    datagram.send(datagram.value().upper())
    datagram.close()


logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s', 
                    level=logging.DEBUG)

s = DatagramServer("localhost", 5000)

while True:
    # datagram = s.accept()
    with s.accept() as datagram:
        # _thread.start_new_thread(handler, (datagram))
        message = datagram.value().upper()
        datagram.send(message.upper())
        print(datagram.receive())
