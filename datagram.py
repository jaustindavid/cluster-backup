#! python


"""
# Server (threaded or non)
s = DatagramServer("localhost", 5000)

while True:
    with s.accept() as datagram
        datagram.send(datagram.value().upper())



# Client

with Datagram("Hello, World!", server="localhost", port=5000) as datagram:
    if datagram.send(): # or send(server="localhost", port=5000)
        print(datagram.receive())

################

A Datagram is an arbitrarily-large object which can be sent
to a DatagramServer, and a response Datagram will be returned.

Network communication appears atomic; the entire datagram is send()'t
or receive()'d in one blocking call.  If possible the socket is held
open for subsequent calls.

It can act like an indexable list or dict, or like a scalar.
len() acts like a list, so len(Datagram("contents is a string")) == 1
to avoid list confusion.  

A Datagram has a bool() value, which can be explicitly set or
implicitly based on its contents.
"""


import logging, json, zlib, socket

class Datagram:
    def __init__(self, *contents, **kwargs):
        self.data = {}
        self.data['truthiness'] = None
        self.connection = None
        self.logger = logging.getLogger(str(__class__)[8:-2])

        if "connection" in kwargs:
            # slurp the universe from the connection
            # ignore all other arguments
            self.connection = kwargs['connection']
            self.logger.debug("New from socket")
            self.receive()
            return

        if "server" in kwargs:
            assert "port" in kwargs, "Required: server, port"
            self.server = kwargs['server']
            self.port = kwargs['port']


        if "bool" in kwargs:
            self.data['truthiness'] = kwargs["bool"]

        self.set(*contents)


    # with s.accept() as datagram:
    def __enter__(self):
        return self

    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


    def value(self):
        self.logger.debug(f"I have {self.data['contents']}")
        return self.data['contents']


    def __getitem__(self, key):
        if type(self.data['contents']) is str and key == 0:
            value = self.data['contents']
        else:
            value = self.data['contents'][key]
        # print(f"{key}: {value}")
        if type(value) is str and value.isdigit():
            return int(value)
        return value


    def __bool__(self):
        if self.data['truthiness'] is not None:
            return self.data['truthiness']
        if type(self.data['contents']) is str:
            return bool(self.data['contents'] and self.data['contents'] != "")
        return bool(self.data['contents'])
        

    def __len__(self):
        if type(self.data['contents']) is str:
            return 1
        return len(self.data['contents'])


    def __eq__(self, item):
        if type(item) is __class__:
            return str(self) == str(item)
        return self.data['contents'] == item


    def __iter__(self):
        if type(self.data['contents']) is str:
            return iter([ str(self.data['contents']) ])
        else:
            return iter(self.data['contents'])


    def serialize(self):
        return zlib.compress(bytes(json.dumps(self.data), 'ascii'))


    def deserialize(self, data):
        self.data = json.loads(zlib.decompress(data))


    def set(self, *contents, **kwargs):
        if "bool" in kwargs:
            self.data['truthiness'] = kwargs['bool']
        if contents:
            self.logger.debug(f"new contents: {contents}")
            if len(contents) == 1:
                # print(f"Unpacking {contents}")
                self.data['contents'] = contents[0]
            else:
                # print(f"NOT unpacking {contents}")
                self.data['contents'] = contents
    

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



    # alias for "send"
    def respond(self, *contents, **kwargs):
        return self.send(*contents, **kwargs)


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
                # sock.sendall(bytes(data, 'ascii'))
                sock.sendall(data)
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
    def receive(self, **kwargs):
        BUFFER_SIZE = 1024
        self.logger.debug("Receiving")
        sock = self._get_connection(**kwargs)
        # data = str(sock.recv(BUFFER_SIZE), 'ascii')
        if not sock:
            return None

        data = sock.recv(BUFFER_SIZE)
        sock.setblocking(False)
        while True:
            try:
                # buffer = str(sock.recv(BUFFER_SIZE), 'ascii')
                buffer = sock.recv(BUFFER_SIZE)
                self.logger.debug(f"got {buffer}")
            except BlockingIOError:
                buffer = None
            if not buffer:
                break
            data = data + buffer
        sock.setblocking(True)
        self.logger.debug(f"data is {data}")
        self.deserialize(data)

        return self.data['contents']


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
