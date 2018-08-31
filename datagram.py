#! python


"""
# Server (threaded or non)
s = DatagramServer("localhost", 5000)

while True:
    datagram = s.accept()
    # _thread.start_new_thread(handler, (datagram))
    datagram.respond(datagram.value().upper())


# Client

datagram = Datagram("Hello, World!", server="localhost", port=5000)
if datagram.send(): # or send(server="localhost", port=5000)
    print(datagram.response())

################

A Datagram is an arbitrarily-large object which can be sent
to a DatagramServer, and a response Datagram will be returned.

Network communication is *always* reciprocated: send() does a (single)
receive, and vice-versa: the server always acks messages (or otherwise
replies)

It can act like an indexable list or dict, or like a scalar.
len() acts like a list, so len(Datagram("contents is a string")) == 1
to avoid list confusion.  

A Datagram has a bool() value, which can be explicitly set or
implicitly based on its contents.

"""


import logging, json, zlib, socket

class Datagram:
    def __init__(self, *contents, **kwargs):
        self.truthiness = None
        self.contents = None
        self.connection = None
        self.logger = logging.getLogger(str(__class__))

        if "connection" in kwargs:
            # slurp the universe from the connection
            # ignore basically all other arguments
            self.connection = kwargs['connection']
            self.logger.debug("New from socket")
            self.receive()
            return

        if "server" in kwargs:
            assert "port" in kwargs, "Required: server, port"
            self.server = kwargs['server']
            self.port = kwargs['port']


        if "bool" in kwargs:
            self.truthiness = kwargs["bool"]

        self.set(*contents)


    def value(self):
        self.logger.debug(f"I have {self.contents}")
        return self.contents


    def __getitem__(self, key):
        if type(self.contents) is str and key == 0:
            value = self.contents
        else:
            value = self.contents[key]
        # print(f"{key}: {value}")
        if type(value) is str and value.isdigit():
            return int(value)
        return value


    def __bool__(self):
        if self.truthiness is not None:
            return self.truthiness
        if type(self.contents) is str:
            return bool(self.contents and self.contents != "")
        return bool(self.contents)
        

    def __len__(self):
        if type(self.contents) is str:
            return 1
        return len(self.contents)


    def __eq__(self, item):
        if type(item) is __class__:
            return str(self) == str(item)
        return self.contents == item


    def __iter__(self):
        if type(self.contents) is str:
            return iter([ str(self.contents) ])
        else:
            return iter(self.contents)


    def serialize(self):
        return json.dumps(self.contents)


    def deserialize(self, data):
        self.contents = json.loads(data)


    def set(self, *contents):
        if contents:
            self.logger.debug(f"new contents: {contents}")
            if len(contents) == 1:
                # print(f"Unpacking {contents}")
                self.contents = contents[0]
            else:
                # print(f"NOT unpacking {contents}")
                self.contents = contents
    

    def _get_connection(self, **kwargs):
        if not self.connection:
            if "server" in kwargs:
                server = kwargs['server']
                port = kwargs['port']
            else:
                server = self.server
                port = self.port

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.connection = sock
            try:
                sock.connect((server, port))
                # sock.setblocking(False)
                # sock.settimeout(300)
            except BrokenPipeError:
                self.logger.exception(f"got that broken pipe")
                self.close()
                return None
            except ConnectionRefusedError:
                self.logger.exception(f"Connection refused...")
                self.close()
                return None
        return self.connection


    # initiates a NEW connection (or re-uses an existing one)
    # then sends my serialized contents
    #   if datagram.send(): # or send(server="localhost", port=5000)
    #       datagram.receive()
    def send(self, *contents, **kwargs):
        BUFFER_SIZE = 1024

        self.set(*contents)
        data = self.serialize()
        self.logger.debug(f"Sending {data}")

        sock = self._get_connection(**kwargs)
        if sock:
            try:
                sock.sendall(bytes(data, 'ascii'))
            except socket.timeout:
                self.logger.exception("timed out in send()")
                self.close()
                return False
            except BrokenPipeError:
                self.logger.exception(f"got that broken pipe")
                self.close()
                return False
            except ConnectionRefusedError:
                self.logger.exception(f"Connection refused...")
                self.close()
                return False
        return True
            

    # slurps a lot of data down a connection, deserializes it
    # and returns it for good measure
    def receive(self):
        self.logger.debug("Receiving")
        BUFFER_SIZE = 1024
        sock = self.connection
        data = str(sock.recv(BUFFER_SIZE), 'ascii')
        sock.setblocking(False)
        while True:
            try:
                buffer = str(sock.recv(BUFFER_SIZE), 'ascii')
                self.logger.debug(f"got {buffer}")
            except BlockingIOError:
                buffer = None
            if not buffer:
                break
            data = data + buffer
        sock.setblocking(True)
        self.logger.debug(f"data is {data}")
        self.deserialize(data)

        return self.contents


    def close(self):
        self.logger.debug("closing connection")
        self.connection.close()
        self.connetion = None


"""
# Server (threaded or non)
ds = DatagramServer("localhost", 5000)

while True:
    datagram = ds.accept()
    # _thread.start_new_thread(handler, (datagram))
    datagram.respond(datagram.value().upper())
"""

class DatagramServer:
    def __init__(self, host, port, **kwargs):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.socket.bind((self.host, self.port))
        self.socket.listen(10)


    def accept(self):
        conn, addr = self.socket.accept()
        dgram = Datagram(connection=conn)
        return dgram
