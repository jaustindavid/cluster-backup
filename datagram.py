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

A Datagram has a bool() value, which reflects whether it has a 
valid network socket.  Use .ping() to refresh this without affecting
contents
"""


import logging, json, zlib, socket

class Datagram:
    def __init__(self, *contents, **kwargs):
        self.data = {}
        self.connection = self.server = self.port = None
        if 'name' in kwargs:
            self.name = kwargs['name']
        else:
            self.name = f"{str(__class__)[8:-2]} {id(self)}"
        self.logger = logging.getLogger(self.name)
        # self.logger.setLevel(logging.INFO)

        self.compressing = "compress" in kwargs

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
        else:
            self.server = self.port = None

        self.set(*contents)


    # with s.accept() as datagram:
    def __enter__(self):
        return self

    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


    def value(self):
        if not self.compressing:
            self.logger.debug(f"I have {str(self.data)[:200]}...")
        return self.data


    # am I connected?
    def connected(self):
        return self.connection is not None

    
    # ping the other side & return T/F
    def ping(self):
        if not self._send(b"PING") or self._receive() != b"PONG":
            self.close()
        return self.connected()


    def __getitem__(self, key):
        if type(self.data) is str and key == 0:
            value = self.data
        else:
            value = self.data[key]
        if type(value) is str and value.isdigit():
            return int(value)
        return value


    def __bool__(self):
        return self.connected()
        

    def __len__(self):
        if not self.data:
            return 0
        if type(self.data) is str:
            return 1
        return len(self.data)


    def __eq__(self, item):
        if type(item) is __class__:
            return str(self) == str(item)
        return self.data == item


    def __iter__(self):
        if not self.data:
            return iter([])
        if type(self.data) is str:
            return iter([ str(self.data) ])
        else:
            return iter(self.data)

    
    def __str__(self):
        dg = "Datagram"
        if self.server:
            dg += f"({self.server}:{self.port})"
        ds = str(self.data)
        if len(ds) > 20:
            ds = ds[:20] + "..."
        return f"{dg}[{ds}]"


    def serialize(self):
        if self.compressing:
            return zlib.compress(bytes(json.dumps(self.data), 'ascii'))
        else:
            return bytes(json.dumps(self.data), 'ascii')



    def deserialize(self, data):
        if not data:
            return
        try:
            if self.compressing:
                self.data = json.loads(zlib.decompress(data))
            else:
                self.data = json.loads(data)
        except (zlib.error, json.decoder.JSONDecodeError):
            self.logger.exception("Can't deserialize... (handling)")
            self.data = None


    def set(self, *contents, **kwargs):
        # if "bool" in kwargs:
        #     self.data['truthiness'] = kwargs['bool']
        if contents:
            self.logger.debug(f"new contents: {str(contents)[:200]}...")
            if len(contents) == 1:
                self.data = contents[0]
            else:
                self.data = contents


    def _get_connection(self, **kwargs):
        if not self.connection:
            if "server" in kwargs:
                server = kwargs['server']
                port = kwargs['port']
            else:
                server = self.server
                port = self.port

            if self.server is not None and self.port is not None:
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
        if self.compressing:
            self.logger.debug(f"Sending {len(data)} compressed bytes")
        else:
            self.logger.debug(f"Sending {len(data)} bytes: {str(data)[:200]}")

        return self._send(data)


    # low(er)-level "send"; takes hunk of data, returns T/F
    def _send(self, data, **kwargs):
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
        data = self._receive(**kwargs)
        self.deserialize(data)
        return self.data


    # low(er)-level "receive"; returns a hunk of data
    def _receive(self, **kwargs):
        BUFFER_SIZE = 1024
        self.logger.debug("Receiving")
        sock = self._get_connection(**kwargs)
        # data = str(sock.recv(BUFFER_SIZE), 'ascii')
        if not sock:
            return None

        try:
            data = sock.recv(BUFFER_SIZE)
            # intercepting PING, low-level reponse ;-)
            if data == b"PING":
                self.logger.debug("got a PING")
                self._send(b"PONG")
                return self._receive(**kwargs)
            sock.setblocking(False)
            while True:
                try:
                    buffer = sock.recv(BUFFER_SIZE)
                except BlockingIOError:
                    buffer = None
                # self.logger.debug(f"got {buffer}")
                if not buffer:
                    break
                data = data + buffer
            sock.setblocking(True)
        except ConnectionResetError:
            self.logger.debug("Connection closed :(")
            self.close()
            return None
        if self.compressing:
            self.logger.debug(f"data is {len(data)} compressed bytes")
        else:
            self.logger.debug(f"data is {len(data)} bytes: {str(data)[:200]}")
        return data


    def close(self):
        self.logger.debug("closing connection")
        if self.connection:
            self.connection.close()
            self.connection = None



"""
# Server (threaded or non)
ds = DatagramServer("localhost", 5000)

while True:
    datagram = ds.accept()
    # _thread.start_new_thread(handler, (datagram))
    datagram.respond(datagram.value().upper())


long-lived server thread:
    while datagram:
        request = datagram.value()
        if request:
            self.logger.debug(f"received {request}")
            response = self.handle(request)
            self.logger.debug(f"returning {response}")
            datagram.respond(response)
            datagram.receive()
    self.logger.debug(f"closing connection")
    datagram.close()
"""

class DatagramServer:
    def __init__(self, host, port, **kwargs):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.socket.bind((self.host, self.port))
        self.socket.listen(10)


    def accept(self, **kwargs):
        conn, addr = self.socket.accept()
        dgram = Datagram(connection=conn, **kwargs)
        return dgram
