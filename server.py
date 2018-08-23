#!/usr/bin/env python3

import sys, random, time, socket, logging, os
from threading import Thread
import config, elapsed, scanner, persistent_dict
from utils import logger_str
from comms import Communique

"""
A host will have a single Server, which can serve API requests
for multiple filesystems (contexts), by proxying to a Servlet per

One Servlet per filesystem to run the per-fs scanner

The Server will *not* start a handler thread for each incoming 
connection; a connection just calls into the relevant servlet.
Calls should be VERY FAST, they're just clients checking in.
It's TCP datagrams, basically.

internal data:
  files[filename][backup client, backup client ... ]

Internal data is note preserved; clients will inform me of their
state(s), and I'll confirm it, when I start up.  

TODO: 
* return the rsync commands needed to rebuild a given source
* coverage > 100%
"""


class Servlet(Thread):
    def __init__(self, context):
        super().__init__()
        self.context = context
        
        self.logger = logging.getLogger(logger_str(__class__) + " " + context)
        self.logger.setLevel(logging.INFO)
        self.config = config.Config.instance()
        self.copies = int(self.config.get(self.context, "copies", 2))
        self.path = config.path_for(self.config.get(self.context, "source"))
        self.scanner = scanner.Scanner(self.context, self.path)

        self.files = dict() # NOT persistent!
        self.bailout = False
        self.drops = 0
        self.claims = 0


    def run(self):
        self.bailout = False
        while not self.bailout:
            self.scanner.scan()
            self.update_files()
            self.audit()
            time.sleep(10)

    
    def update_files(self):
        for filename, stuff in self.scanner.items():
            if not filename in self.files:
                self.files[filename] = []


    def stop(self):
        self.bailout = True


    def audit(self):
        if self.claims > 0:
            ratio = 100-100*self.drops/self.claims
        else:
            ratio = 0
        message = f"audit: need {self.copies}; {ratio:5.2f}% {self.drops}:{self.claims} drops:claims\n"
        ncopies = {}
        coverage = 0
        for filename in self.files:
            # self.logger.debug(f"{filename}: {len(self.files[filename])} copies")
            count = len(self.files[filename])
            if count not in ncopies:
                ncopies[count] = 1
            else:
                ncopies[count] += 1
            if count >= self.copies:
                coverage += 1
            elif count > 0:
                coverage += count/self.copies
        if len(self.files) > 0:
            self.logger.info(f"coverage: {100*coverage/len(self.files):5.2f}%")

        for count in sorted(ncopies.keys(), reverse=True):
            message += f"{count} copies: {ncopies[count]} files, "
        message += "\n"
        for filename in self.files:
            message += f"{filename}:{len(self.files[filename])} {sorted(self.files[filename])}\n"
        self.logger.info(message)



    # client claims filename (not a lock)
    def claim(self, args):
        client, filename, checksum = args[:3]
        filestate = self.scanner.get(filename)
        self.logger.debug(f"{client} claims file {filename}")
        if checksum != filestate["checksum"]:
            self.logger.warn(f"{client} has the wrong checksum!\n" * 10)
            return Communique("NACK", truthiness=False)

        if client not in self.files[filename]:
            self.files[filename].append(client)
            self.claims += 1
        return Communique("ack")


    # client releases their claim on filename
    def unclaim(self, args):
        client, filename = args[:2]
        self.logger.debug(f"files: {self.files}, client: {client}, filename: {filename}")
        if filename and len(self.files) > 0 and filename in self.files \
            and client in self.files[filename]:
            self.files[filename].remove(client)
            self.drops += 1
        return Communique("ack")


    # client wants a file: return the least-served which isn't on client
    # but, if possible, match a size-hint (if they ask for 100mb, try not
    # to offer 1gb)

    def underserved_for(self, client):
        files = []
        for filename in self.files:
            if client not in self.files[filename]:
                if len(self.files[filename]) < self.copies:
                    self.logger.debug(f"request: {filename} is not held by client")
                    files.append(filename)
        if len(files) > 0:
            return sorted(files, key=lambda filename: len(self.files[filename]))
        else:
            return None


    def available_for(self, client, sizehint=0):
        files = []
        for filename in self.files:
            if client not in self.files[filename]:
                files.append(filename)

        if len(files) == 0:
            self.logger.debug(f"no files available for {client}")
            return None

        self.logger.debug(f"request: candidates are {files}")
        files = sorted(files, reverse=True, key=lambda \
                        filename: int(self.scanner[filename]["size"]))
        files = sorted(files, 
                        key=lambda filename: len(self.files[filename]))
        self.logger.debug(f"request: candidates are now {files}")
        smalls = []
        for filename in files:
            if sizehint and int(self.scanner[filename]["size"]) < int(sizehint):
                smalls.append(filename)
        if len(smalls) > 0:
            return smalls
        else:
            return files


# return a file:
# - filter out files held by client
# - sort by underserved (fewest first)
# - sort by size (largest first)
# return the largest file under sizehint
#   but if I have to, return an oversized file
#   (to inspire the agent to MAYBE rebalance)

    def request(self, args):
        client, sizehint = args[:2]
        self.logger.debug("looking for an underserved file")
        files = self.underserved_for(client)
        self.logger.debug(files)

        if files:
            filename = random.choice(files)
            self.logger.debug(f"trying to return {filename}")
            if self.files[filename]:
                self.logger.debug(f"file data is {self.scanner[filename]}")
                return Communique(filename, str(self.scanner[filename]["size"]))
            else:
                return Communique(filename, 0)

        self.logger.debug("no underserved; giving any")
        files = self.available_for(client, sizehint)
        if files:
            filename = random.choice(files)
            return Communique(filename, str(self.scanner[filename]["size"]))
        else:
            return Communique("__none__", truthiness=False)



    # return a file which needs a backup, but is NOT held
    # by client
    # cf https://stackoverflow.com/questions/4113307/pythonic-way-to-select-list-elements-with-different-probability
    def underserved(self, args):
        client = args[0]
        self.logger.debug(f"Underserved for {client}?")
        self.audit()
        files = []
        for filename in self.files:
            if client not in self.files[filename] and \
                len(self.files[filename]) < self.copies:
                    # weighted random... TODO make this ~O(1)
                    files.append(filename)
                    # return filename
        if len(files) > 0:
            # TODO: return the whole list
            file = random.choice(files)
            self.logger.debug(f"returning {file}")
            return files
        self.logger.debug("I got nothin'")
        return None


    # should return the most-overserved file which is held
    # by the named client
    def overserved(self, args):
        client = args[0]
        files = {}
        for filename in self.files:
            self.logger.debug(f"{filename}: {len(self.files[filename])}/{self.copies}")
            if client in self.files[filename]: 
                self.logger.debug(f"client match for {filename}")
                if len(self.files[filename]) > self.copies:
                    self.logger.debug(f"overserved file: {filename}")
                    files[filename] = len(self.files[filename])
        if len(files.keys()) > 0:
            filenames = sorted(files.keys(), key=lambda x: files[x])
            # TODO: return the whole list
            return Communique(filenames)
        else:
            self.logger.debug("no overserved files :/")
            return None


    """ return one of (in order of preference):
            underserved: I need some coverage from you
            available  : you COULD take some of my files, NBD
            overserved : I've got more than enough coverage, you can drop some
            just right : you have all my files, none are overserved
    """
    def status(self, args):
        client = args[0]
        if self.underserved(args):
            return "underserved"
        if self.request((client, 0)):
            return "available"
        if self.overserved(args):
            return "overserved"
        return "just right"


    # handle a datagram request: returns a string
    def handle(self, action, args):
        self.logger.debug(f"requested: {action} ({args})")
        actions = {"request":       self.request,
                    "claim":        self.claim,
                    "unclaim":      self.unclaim,
                    "overserved":   self.overserved,
                    "underserved":  self.underserved,
                    "status":       self.status
                   }
        response = actions[action](args)
        self.logger.debug(f"responding: {type(response)}: {response}")
        return str(response)


class Server:
    def __init__(self, hostname):
        self.hostname = hostname
        self.config = config.Config.instance()
        self.logger = logging.getLogger(logger_str(__class__))
        self.logger.setLevel(logging.INFO)
        self.contexts = self.get_contexts()
        self.servlets = {}
        self.build_servlets()
        

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


    # client sends to a specific server context
    # action @@ server context @@ client context @@ arguments
    # action: context arg1, arg2
    def handle(self, request):
        # self.logger.debug(f"requested: {request}")
        tokens = request.split(" @@ ")
        # self.logger.debug(tokens)
        action, server_context = tokens[:2]
        args = tokens[2:]
        if server_context not in self.servlets:
            return "__none__"
        self.logger.debug(f"acting: {server_context} => {action}({args})")
        # print(self.servlets)
        response = self.servlets[server_context].handle(action, args)

        # print(f"responding: {response}")
        if not response:
            return "__none__"
        return str(response)


    # https://wiki.python.org/moin/TcpCommunication
    def serve(self):
        ADDRESS = self.hostname
        PORT = int(self.config.get("global", "PORT", "5005"))
        BUFFER_SIZE = 1024

        timer = elapsed.ElapsedTimer()

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        s.bind((ADDRESS, PORT))
        s.listen(10)
        self.logger.debug(f"Listenging on {PORT}")
        while True:
            conn, addr = s.accept()
            self.logger.debug(f"Connecting address: {addr}")
            with conn:
                data = str(conn.recv(1024), 'ascii')
                if data:
                    response = bytes(self.handle(data), 'ascii')
                conn.sendall(response)
                conn.close()
            if timer.once_every(30):
                for context, servlet in self.servlets.items():
                    servlet.audit()



    # busy guy: all servlets should scan forever, and
    # server still has to serve forever
    def run(self):
        for context, servlet in self.servlets.items():
            servlet.start()
        self.serve()



##########################
#
#      M A I N
#
##########################

import getopt, platform



def getopts():
    options = {}
    options["hostname"] = platform.node()
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:c:")
    except getopt.GetoptError as err:
        print(err)
        sys.exit(1)
    for opt, arg in opts:
        if opt == "-c":
            options["configfile"] = arg
        elif opt == "-h":
            options["hostname"] = arg
        else:
            assert False, "Unhandled option"

    return options


def main(args):
    logger = logging.getLogger(__name__)
    logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s',
                            level=logging.DEBUG)
    logger.setLevel(logging.DEBUG)
    cfg = config.Config.instance()
    options = getopts()
    assert os.path.exists(options["configfile"])
    assert type(options["hostname"]) is str
    cfg.init(options["configfile"], "source", "backup")
    s = Server(options["hostname"])
    s.run()


if __name__ == "__main__":
    main(sys.argv)
