#!/usr/bin/env python3

import sys, random, time, socket, logging, os, _thread
from threading import Thread
# from multiprocessing import Process as Thread

import config, elapsed, scanner, persistent_dict, utils, locker
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
* test restoration / recovery
* coverage > 100%
* collapse the audit: files with common client lists should be 1 line
* arbitrarily long inventory() chain
* encourage clients to balance among themselves
"""


class Servlet(Thread):
    def __init__(self, context):
        super().__init__()
        self.context = context
        
        self.logger = logging.getLogger(utils.logger_str(__class__) \
                        + " " + context)
        # self.logger.setLevel(logging.INFO)
        self.config = config.Config.instance()
        self.copies = int(self.config.get(self.context, "copies", 2))
        self.path = config.path_for(self.config.get(self.context, "source"))
        self.scanner = scanner.Scanner(self.context, self.path)

        self.files = dict() # NOT persistent!  On startup assume nothing
        self.drains = elapsed.ExpiringDict(300) # NOT persistent!
        self.locks = locker.Locker(5)
            # TODO: timers should relate to a configurable cycle time
        self.bailout = False
        self.stats = {'claims':0, 'drops':0}
        self.handling = False


    # Server will call into my datagram functions; I just brood
    def run(self):
        self.bailout = False
        # TODO: turbo prescan
        self.scanner.scan(turbo=True)
        self.logger.info("Ready to serve")
        self.handling = True
        while not self.bailout:
            self.scanner.scan()
            self.update_files()
            self.heartbeat()
            sleep_time = utils.str_to_duration( \
                            self.config.get(self.context, "rescan"))
            self.logger.info("Scanning complete; will re-scan in " \
                                + f"{utils.duration_to_str(sleep_time)}")
            time.sleep(sleep_time)

    
    def update_files(self):
        for filename, stuff in self.scanner.items():
            if not filename in self.files:
                self.files[filename] = []


    def stop(self):
        self.bailout = True


    def heartbeat(self):
        if self.stats['claims'] > 0:
            ratio = 100-100*self.stats['drops']/self.stats['claims']
        else:
            ratio = 0
        message = f"Efficiency: {ratio:5.2f}%, " \
                + f"{self.stats['drops']}:{self.stats['claims']} drops:claims\n"
        ncopies = {}
        clientelle = {}
        coverage = 0
        nblocks = 0
        for filename in self.files:
            count = len(self.files[filename])
            nblocks += count
            if count not in ncopies:
                ncopies[count] = 1
            else:
                ncopies[count] += 1
            if count >= self.copies:
                coverage += 1
            elif count > 0:
                coverage += count/self.copies
        if len(self.files) > 0:
            # "coverage" shows the portion of dataset with "enough" copies
            # ... but > 100%, shows the amount of copies out there 
            #   :. coverage = 150% -> 1.5x requested number of copies
            #   ... not neccessarily "1.5 copies of everything"
            if coverage == len(self.files): 
                # all files covered; show the *actual* coverge
                coverage = nblocks / (len(self.files) * self.copies)
                message += f"coverage: {nblocks / (len(self.files) * self.copies):5.1f}x"
            else:
                message += f"coverage: {100*coverage/len(self.files):5.2f}%"
            message += f" of {self.copies}\n"
        self.logger.info(message)


    def audit(self):
        self.heartbeat()
        message = "\n"
        ncopies = {}
        clientelle = {}
        coverage = 0
        nblocks = 0
        for filename in self.files:
            # self.logger.debug(f"{filename}: {len(self.files[filename])} copies")
            clients = " + ".join(sorted(self.files[filename]))
            if not clients:
                clients = "none"
            if clients not in clientelle:
                clientelle[clients] = 1
            else:
                clientelle[clients] += 1
        for clients in sorted(clientelle.keys(), \
                            key = lambda c : len(c), \
                            reverse = True):
            message += f"[{clients}]: {clientelle[clients]} files\n"
        self.logger.info(message)


    def dump_files(self, filelist):
        message = ""
        i = 0
        for filename in filelist:
            message += f"{filename}:{len(self.files[filename])} "
            i += 1
            if i > 20:
                message += "..."
                break
        self.logger.debug(message)


    # client claims filename (not a lock)
    # a few cases: 
    #   - not a file: drop it
    #   - not valid checksum because I haven't computed it: keep it
    #   - not valid checksum because it's wrong: update it
    #   - valid checksum (few cases): keep it
    def claim(self, args):
        client, filename, checksum = args[:3]
        if filename not in self.scanner:
            self.logger.warn("Client has a file, I don't; deleted?")
            return Communique("drop", truthiness=False)
            return Communique("NACK", truthiness=False)

        filestate = self.scanner[filename]
        self.logger.debug(f"{client} claims file {filename}")
        if filestate["checksum"] == "deferred":
            self.logger.debug("I have a deferred checksum; let it go (for now)")
            # return Communique("keep", truthiness=True)
            # fallthrough
        elif checksum != filestate["checksum"]:
            self.logger.warn(f"{client} has a different checksum ...")
            self.scanner.update(filename)
            filestate = self.scanner[filename]
            if checksum != filestate["checksum"]:
                self.logger.warn(f"{client} has the wrong checksum!\n" * 10)
                return Communique("update", truthiness=False)
            else:
                self.logger.warn(f"{client} was right; I'm straight now")
                return Communique("keep", truthiness=True)

        if filename not in self.files:
            self.logger.warn(f"I'm learning about {filename}")
            self.files[filename] = [ client ]
        elif client not in self.files[filename]:
                self.files[filename].append(client)
        self.stats['claims'] += 1
        self.release(filename)
        # return Communique("keep", truthiness=True)
        return Communique("ack")


    # client releases their claim on filename
    def unclaim(self, args):
        client, filename = args[:2]
        # self.logger.debug(f"files: {self.files}, client: {client}, filename: {filename}")
        if filename and len(self.files) > 0 and filename in self.files \
            and client in self.files[filename]:
            self.files[filename].remove(client)
            self.stats['drops'] += 1
        return Communique("ack")


    # client wants to (gracefully) drop a file
    # ... just set a timer.  Client should re-ask if it's OK
    def drain(self, args):
        client, filename = args[:2]
        self.logger.debug(f"client: {client} requests drain {filename}")
        self.drains[f"{client}:{filename}"] = 1
        return Communique("ack")


    # client asks for an inventory of the things I think he has
    # returns a list of filenames
    def inventory(self, args):
        client = args[0]
        self.logger.debug(f"{client} wants inventory")
        files = []
        for filename in self.files:
            if client in self.files[filename]:
                files.append(filename)
            # else:
            #     self.logger.debug(f"{client} not in {filename}")
        # TODO: make this better
        c = Communique(files)
        self.logger.debug(f"inventory: {c}")
        return c


    # client wants a file: return the least-served which isn't on client
    # but, if possible, match a size-hint (if they ask for 100mb, try not
    # to offer 1gb)
    def DEADunderserved_for(self, client):
        files = []
        for filename in self.files:
            if client not in self.files[filename]:
                if len(self.files[filename]) < self.copies:
                    # self.logger.debug(f"request: {filename} is not held by client")
                    files.append(filename)
        if len(files) > 0:
            files = sorted(files, key=lambda filename: len(self.files[filename]))
            return self.random_subset(files, 10)
        else:
            return None


    def DEADavailable_for(self, client, sizehint=0):
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


    def lockname(self, filename):
        return utils.hash(filename)


    # a temporary hold on a file we've offered to a client,
    # to prevent us (briefly) offering to another
    def hold(self, filename, client):
        # self.logger.debug(f"{filename} given to {client} (maybe)")
        self.locks[self.lockname(filename)] = client


    # release a hold 
    def release(self, filename):
        if self.lockname(filename) in self.locks:
            del self.locks[self.lockname(filename)]


    # is there a hold? 
    def held(self, filename, client):
        ret = self.lockname(filename) in self.locks \
                and self.locks[self.lockname(filename)] != client
        # self.logger.debug(f"{filename} lock held against {client} ? {ret}")
        return ret


    # return least-served file smaller than sizehint
    def least_served(self, candidates, sizehint):
        if self.scanner[candidates[0]]["size"] > sizehint:
            # they're all too big; pick one
            return random.choice(candidates)
        
        for filename in sorted(candidates, reverse=True, \
                        key = lambda f: self.scanner[f]["size"]):
            if self.scanner[filename]["size"] < int(sizehint):
                return filename
        # fallthrough: returns the last checked file
        return filename


    # serve a file request: the (if possible) largest, not-locked,
    # unowned-by-client file
    #  ANY underserved file is a candidate
    def request(self, args):
        client, sizehint = args[:2]
        self.update_files()  # TODO: fix the race condition behind this
        filelist = [ filename for filename in self.scanner.keys() \
                        if client not in self.files[filename] \
                            and not self.held(filename, client) ]
        filelist = sorted(filelist, key=lambda f: self.scanner[f]["size"])
        filelist = sorted(filelist, key=lambda f: len(self.files[f]))
        self.logger.debug(f"pulled a list for {client}:")
        self.dump_files(filelist)
        if not filelist: # no files for this client
            return Communique("__none__", negatives=("__none__",))
        target = len(self.files[filelist[0]]) + 1
        if target < self.copies:  # implies underserved; expand to 
            target = self.copies  # consider any underserved file
        candidates = [ filename for filename in filelist \
                        if len(self.files[filename]) < target ]
        self.logger.debug(f"targeted list for {client}:")
        self.dump_files(candidates)
        filename = self.least_served(candidates, int(sizehint))
        self.logger.debug(f"least_served gives {filename}")
        if filename:
            self.hold(filename, client)
            return Communique(filename, str(self.scanner[filename]["size"]))
        else:
            return Communique("__none__", negatives=("__none__",))


# return a file not held by client
# - if any unserved exist, return one 
#   - small enough, if possible
# - one of the least-served
#   but if I have to, return an oversized file
#   (to inspire the agent to MAYBE rebalance)

    def DEADrequest(self, args):
        client, sizehint = args[:2]
        self.logger.debug("looking for an underserved file")
        files = self.underserved_for(client)
        self.logger.debug(files)

        # underserved files
        self.logger.debug(f"underserved files for {client}")
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


    # tries to return qty items from list(data)
    # https://stackoverflow.com/questions/6482889/get-random-sample-from-list-while-maintaining-ordering-of-items
    def random_subset(self, data, qty):
        # pathological cases
        if len(data) == 0:
            return []
        if qty >= len(data):
            return data
        return [ data[i] for i in 
                    sorted(random.sample(range(len(data)), qty)) ]


    # return a file which needs a backup, but is NOT held
    # by client
    # cf https://stackoverflow.com/questions/4113307/pythonic-way-to-select-list-elements-with-different-probability
    def underserved(self, args):
        client = args[0]
        self.logger.debug(f"Underserved for {client}?")
        # self.audit()
        files = []
        for filename in self.files:
            if client not in self.files[filename] and \
                len(self.files[filename]) < self.copies:
                    # weighted random... TODO make this ~O(1)
                    files.append(filename)
                    # return filename
        if len(files) > 0:
            # TODO: return a subset of the list
            return self.random_subset(files, 20)
            # file = random.choice(files)
            # self.logger.debug(f"returning {file} and moar")
            return files
        self.logger.debug("I got nothin'")
        return Communique(None)


    # should return the most-overserved file which is held
    # by the named client
    def overserved(self, args):
        client = args[0]
        files = {}
        for filename in self.files:
            # self.logger.debug(f"{filename}: {len(self.files[filename])}/{self.copies} {self.files[filename]}")
            if client in self.files[filename]: 
                # self.logger.debug(f"client match for {filename}")
                if len(self.files[filename]) > self.copies:
                    # self.logger.debug(f"overserved file: {filename}")
                    files[filename] = len(self.files[filename])
        if len(files.keys()) > 0:
            filenames = sorted(files.keys(), key=lambda x: files[x], reverse=True)
            return Communique(self.random_subset(filenames, 20))
        else:
            self.logger.debug("no overserved files :/")
            return Communique(None)


    """ return ANY of:
            underserved: at least one of my files needs coverage from you
            available  : all files are covered, but you could copy one
            overserved : all files are covered and you could drop one
            just right : you have all my files, none are overserved
    """
    def status(self, args):
        client = args[0]
        response = []
        if self.underserved(args):
            response.append("underserved")
        if self.request((client, 0)):
            response.append("available")
        if self.overserved(args):
            response.append("overserved")
        if response:
            return Communique(response)
        else:
            return Communique("just right")


    def heartbeep(self, args):
        client = args[0]
        self.logger.info(f"heartbeep from {client}: {args}")
        return Communique("ack")


    # handle a datagram request: returns a string
    def handle(self, action, args):
        if not self.handling:
            return "n/a"
        self.logger.debug(f"requested: {action} ({args})")
        actions = {"request":       self.request,
                    "claim":        self.claim,
                    "unclaim":      self.unclaim,
                    "drain":        self.drain,
                    "inventory":    self.inventory,
                    "overserved":   self.overserved,
                    "underserved":  self.underserved,
                    "status":       self.status,
                    "heartbeep":    self.heartbeep
                   }
        response = actions[action](args)
        self.logger.debug(f"responding: {action} {args} -> {response}")
        return str(response) # TODO: return Communique


class Server:
    def __init__(self, hostname):
        self.hostname = hostname
        self.config = config.Config.instance()
        self.logger = logging.getLogger(utils.logger_str(__class__))
        # self.logger.setLevel(logging.INFO)
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


    def heartbeep(self, client, args):
        self.logger.debug(f"heartbeep from {client}: {args}")


    def auditor(self):
        timer = elapsed.ElapsedTimer()
        while True:
            time.sleep(15)
            self.logger.info("Servlet status update: ")
            for context, servlet in self.servlets.items():
                servlet.audit()


    # client sends to a specific server context
    # action @@ server context @@ client context @@ arguments
    # action: context arg1, arg2
    def handle(self, data):
        request = Communique.build(data)
        action, server_context = request[:2]
        args = request[2:]
        if server_context not in self.servlets:
            return "__none__"
        self.logger.debug(f"acting: {server_context} => {action}({args})")
        response = self.servlets[server_context].handle(action, args)

        if not response:
            return "__none__"
        return str(response)


    # if data > 1000 bytes, prefix it with 16-byte header
    #   size: <bytes>
    # then the rest
    def sendall(self, conn, data):
        size = len(data)
        if size > 1000:
            self.logger.debug(f"returning {size} bytes: {data[:256]} ...")
            conn.send(bytes(f"size: {size:10d}", 'ascii'))
            conn.sendall(bytes(data, 'ascii'))
        else:
            self.logger.debug(f"returning {data}")
            conn.sendall(bytes(data, 'ascii'))
        

    def handler(self, conn, addr):
        BUFFER_SIZE = 1024 # datagrams (inbound) are very small
        while True:
            data = str(conn.recv(BUFFER_SIZE), 'ascii')
            if not data:
                break
            self.logger.debug(f"received {data}")
            response = str(self.handle(data))
            self.sendall(conn, response)
        conn.close()


    # https://wiki.python.org/moin/TcpCommunication
    def serve(self):
        ADDRESS = self.hostname
        PORT = int(self.config.get("global", "PORT", "5005"))
        BUFFER_SIZE = 1024 # datagrams (inbound) are very small


        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        s.bind((ADDRESS, PORT))
        s.listen(10)
        self.logger.info(f"Server starting, listening on {PORT}")
        _thread.start_new_thread(self.auditor, ())
        while True:
            conn, addr = s.accept()
            # self.logger.debug(f"Connecting address: {addr}")
            # TODO: dispatch a long-lived handler thread
            # with conn:
            #     data = str(conn.recv(BUFFER_SIZE), 'ascii')
            #     if data:
            #         self.logger.debug(f"received {data}")
            #         response = bytes(self.handle(data), 'ascii')
            #         self.logger.debug(f"returning {response}")
            #     conn.sendall(response)
            _thread.start_new_thread(self.handler, (conn, addr))



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
    if options["verbose"]:
        logger.setLevel(logging.DEBUG)
    assert os.path.exists(options["configfile"])
    assert type(options["hostname"]) is str
    cfg.init(options["configfile"], "source", "backup")
    s = Server(options["hostname"])
    s.run()


if __name__ == "__main__":
    main(sys.argv)
