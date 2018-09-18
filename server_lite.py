#!/usr/bin/env python3

import _thread, time
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer

import config, stats, scanner, lock, utils, elapsed
from datagram import *
from persistent_dict import PersistentDict


 #####
#     # ###### #####  #    # #      ###### #####
#       #      #    # #    # #      #        #
 #####  #####  #    # #    # #      #####    #
      # #      #####  #    # #      #        #
#     # #      #   #   #  #  #      #        #
 #####  ###### #    #   ##   ###### ######   #
"""
Servlet protocol:
    metadata(): returns a dict({'copies': ##, 'rescan': ##})
    list(): returns a dict( { filename : [ size, nclaims ] , })
    claim(client, [filename,]): increments the nclaims for each filename
        returns "ack" or None
    unclaim(client, [filename, ]): decrements the nclaims for each filename
        returns "ack" or None
"""
class Servlet(Thread):
    def __init__(self, context):
        super().__init__()
        self.context = context

        logger_str = f"{utils.logger_str(__class__)} {context}"
        self.logger = logging.getLogger(logger_str)
        # self.logger.setLevel(logging.INFO)

        self.config = config.Config.instance()
        self.copies = int(self.config.get(self.context, "copies", 2))
        self.path = config.path_for(self.config.get(self.context, "source"))
        self.scanner = scanner.ScannerLite(self.context, self.path)
        self.rescan = utils.get_interval(self.config, "rescan", self.context)

        lazy_write = self.config.get(context, "LAZY WRITE", 5)
        lazy_write = utils.str_to_duration(lazy_write)
        # self.clients: { filename : { client: expiry_time, } }
        clients_state = f"/tmp/cb.{context}-clients.json.bz2"
        self.clients = PersistentDict(clients_state, lazy_write=5)
        self.stats = stats.Stats()
        self.handling = False


    def expire_claims(self):
        expires = 0
        if True or self.logger.getEffectiveLevel() < logging.DEBUG:
            for filename in self.clients:
                for client, stamp in self.clients[filename].items():
                    if stamp < time.time():
                        expires += 1
        if expires:
            self.logger.warn(f"Warning: about to expire {expires} files")
        for filename in self.clients:
            self.clients[filename] = { client: stamp \
                for client, stamp in self.clients[filename].items() \
                    if stamp > time.time() }


    # metadata(): returns a dict({'copies': ##, 'rescan': ##})
    def handle_metadata(self, args):
        return { 'copies': self.copies, 'rescan': self.rescan }


    # list(): returns a dict( { filename : [ size, nclaims ] , })
    def handle_list(self, args):
        client = args[0]
        self.logger.debug(f"Listing all for {client}")
        listing = {}
        self.expire_claims()
        for filename in self.scanner:
            size = self.scanner[filename]
            if filename in self.clients:
                nclients = len(self.clients[filename])
            else:
                nclients = 0
            listing[filename] = [ size, nclients ]
        self.stats['files listed'].incr(len(listing))
        self.logger.debug(f"Returning {len(listing)} to {client}: {str(listing)[:200]}...")
        return listing


    # claim(client, [filename,]): increments the nclaims for each filename
    #    returns "ack" or None
    def handle_claim(self, args):
        client, files = args[:2]
        n = len(files)
        self.logger.debug(f"claiming {n} files for client {client}")
        for filename in files:
            if filename not in self.clients:
                self.clients[filename] = {}
            self.clients[filename][client] = time.time() + self.rescan
        self.stats['files claimed'].incr(len(files))
        self.logger.debug(str(self.clients.data)[:200])
        return "ack"


    # unclaim(client, [filename, ]): decrements the nclaims for each filename
    #     returns "ack" or None
    def handle_unclaim(self, args):
        client, files = args[:2]

        n = len(files)
        self.logger.debug(f"unclaiming {n} files for client {client}")
        for filename in files:
            if filename in self.clients:
                if len(self.clients[filename]) < self.copies:
                    self.logger.warn(f"WARNING: {client} dropping {filename} prematurely\n" * 10)
                if client in self.clients[filename]:
                    del self.clients[filename][client]
        self.stats['files unclaimed'].incr(n)
        return "ack"
        

    # unclaim_all(client): deletes all claims for this client
    #     returns "ack" or None
    def handle_unclaim_all(self, args):
        client = args[0]

        for filename in self.clients.keys():
            if client in self.clients[filename]:
                del self.clients[filename][client]
                self.stats['files unclaimed'].incr(1)
        return "ack" 


    def histogram(self):
        hist = f"{len(self.scanner)} total files, need {self.copies} copies\n"
        buckets = { 0: 0 }
        bucketsize = { 0: 0 }
        self.expire_claims()

        with self.scanner:
            scanned_files = self.scanner.keys()
        for filename in scanned_files:
            size = self.scanner[filename]
            if filename in self.clients:
                bucket = len(self.clients[filename])
                if bucket not in buckets:
                    buckets[bucket] = 0
                    bucketsize[bucket] = 0
                buckets[bucket] += 1
                bucketsize[bucket] += size
            else:
                buckets[0] += 1
                bucketsize[0] += size
        for bucket in sorted(buckets.keys(), reverse=True):
            if buckets[bucket]:
                size = utils.bytes_to_str(bucketsize[bucket])
                hist += f"{buckets[bucket]:6d} files, {size.rjust(8)}: {'## ' * bucket}"
                if bucket < self.copies:
                    missing = self.copies - bucket
                    hist += "__ " * missing
            hist += "\n"
        return hist


    def dump(self):
        message = ""
        for filename in self.clients:
            message += f"{filename}: "
            for client in sorted(self.clients[filename].keys()):
                stamp = self.clients[filename][client]
                if stamp < time.time():
                    message += f"{client}! "
                else:
                    message += f"{client} "
            message += "\n"
        return message
                

    def audit(self):
        self.logger.info(f"Auditing:\n{self.histogram()}")
        for statistic in self.stats:
            self.logger.debug(f"{statistic}: {self.stats[statistic].qps()}")
        # self.logger.log(5, f"\n{self.dump()}")
        # self.clients.lazy_write()



    # handle an incoming action(args)
    # called in parallel from many serving threads
    def handle(self, action, args):
        if not self.handling:
            return None
        # self.logger.debug(f"requested: {action} ({args})")
        actions = { 'list':         self.handle_list,
                    'claim':        self.handle_claim,
                    'unclaim':      self.handle_unclaim,
                    'unclaim all':  self.handle_unclaim_all,
                    'metadata':     self.handle_metadata,
                   }
        response = actions[action](args)
        # self.logger.debug(f"responding: {action} {args} -> {response}")
        return response


    # Server will call into my datagram functions; I just brood
    def run(self):
        self.bailout = False
        # pre-scan
        self.scanner.scan()
        self.logger.info("Ready to serve")
        self.handling = True
        while not self.bailout:
            timer = elapsed.ElapsedTimer()
            self.config.load()
            self.rescan = utils.get_interval(self.config, "rescan", self.context)
            self.scanner.scan()
            sleepy_time = max(self.rescan - timer.elapsed(), 10)
            sleep_msg = utils.duration_to_str(sleepy_time)
            self.logger.info(f"sleeping {sleep_msg} til next rescan")
            time.sleep(sleepy_time)


 #####
#     # ###### #####  #    # ###### #####
#       #      #    # #    # #      #    #
 #####  #####  #    # #    # #####  #    #
      # #      #####  #    # #      #####
#     # #      #   #   #  #  #      #   #
 #####  ###### #    #   ##   ###### #    #


# https://ruslanspivak.com/lsbaws-part1/
class WebServer(Thread):
    def __init__(self, servlets, port=8888):
        super().__init__()
        self.servlets = {}
        self.port = port
        self.logger = logging.getLogger(utils.logger_str(__class__))


    def run(self):
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen_socket.bind(("", self.port))
        listen_socket.listen(1)
        self.logger.info(f"Serving HTTP on 0:{self.port}")
        while True:
            client_connection, client_address = listen_socket.accept()
            request = client_connection.recv(1024)
            self.logger.debug(request)

            http_response = """\
        HTTP/1.1 200 OK

        Hello, World!
        """
            client_connection.sendall(bytes(http_response, 'ascii'))
            client_connection.close()
        

class Server(Thread):
    def __init__(self, hostname):
        super().__init__()
        self.hostname = hostname
        self.config = config.Config.instance()
        self.logger = logging.getLogger(utils.logger_str(__class__))
        # self.logger.setLevel(logging.INFO)
        self.contexts = self.get_contexts()
        self.servlets = {}
        self.build_servlets()
        self.stats = stats.Stats()


    def get_contexts(self):
        all_contexts = self.config.get_contexts_for_key("source")
        contexts = {}
        for context in all_contexts:
            source = self.config.get(context, "source")
            if source.startswith(self.hostname + ":"):
                contexts[context] = source
        return contexts


    def build_servlets(self):
        for context in self.contexts:
            self.servlets[context] = Servlet(context)


    def auditor(self):
        timer = elapsed.ElapsedTimer()
        while True:
            time.sleep(15)
            self.logger.info(f"aggregate qps: {self.stats['handler'].qps()}")
            self.logger.info("Servlet status update: ")
            for context, servlet in self.servlets.items():
                self.logger.info(f"{context} qps: {self.stats[context].qps()}")
                servlet.audit()


    # client sends to a specific server context
    # [ action, server context, client context, arguments ]
    def handle(self, request):
        action, server_context = request[:2]
        args = request[2:]
        if server_context not in self.servlets:
            return None
        self.logger.log(5, f"acting: {server_context} => {action}({args})")
        self.stats[server_context].incr(1)
        response = self.servlets[server_context].handle(action, args)

        return response


    def handler(self, datagram):
        while datagram:
            self.stats['handler'].incr(1)
            request = datagram.value()
            if request:
                self.logger.debug(f"received {str(request)[:140]}...")
                self.logger.log(5, f"received {str(request)}...")
                response = self.handle(request)
                self.logger.debug(f"returning {str(response)[:140]}...")
                self.logger.log(5, f"returning {str(response)}...")
                datagram.respond(response)
                datagram.receive()
            else:
                break
        self.logger.debug(f"closing connection")
        datagram.close()


    def serve(self):
        ADDRESS = self.hostname
        PORT = int(self.config.get("global", "PORT", "5005"))
        dgserver = DatagramServer(ADDRESS, PORT)
        self.logger.info(f"Listening on {PORT}")
        while True:
            datagram = dgserver.accept(compress=True)
            _thread.start_new_thread(self.handler, (datagram,))


    # busy guy: all servlets should scan forever, and
    # server still has to serve forever
    def run(self):
        if not self.servlets:
            self.logger.info("No serving tasks; exiting Server")
            return
        for context, servlet in self.servlets.items():
            servlet.start()
        _thread.start_new_thread(self.auditor, ())
        # ws = WebServer(self.servlets)
        # ws.start()
        self.serve() # forever



################################
#
#          M A I N
#
################################


import getopt, platform, sys, logging, os


def getopts():
    options = {}
    options["hostname"] = platform.node()
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:c:v")
    except getopt.GetoptError as err:
        print(err)
        sys.exit(1)
    options["verbose"] = False
    for opt, arg in opts:
        if opt == "-c":
            options["configfile"] = arg
        elif opt == "-h":
            options["hostname"] = arg
        elif opt == "-v":
            options["verbose"] = True
        else:
            assert False, "Unhandled option"

    return options


def main(args):
    logger = logging.getLogger(__name__)
    logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s',
                            level=logging.DEBUG)
    logger.setLevel(logging.INFO)

    cfg = config.Config.instance()
    options = getopts()
    if options['verbose']:
        logger.setLevel(logging.DEBUG)
    assert os.path.exists(options['configfile']), \
        f"Can't read {options['configfile']}"
    assert type(options["hostname"]) is str

    cfg.init(options['configfile'], "source", "backup")

    s = Server(options['hostname'])
    s.start()

    try:
        # wait for them to finish (if ever)
        s.join()
    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv)
