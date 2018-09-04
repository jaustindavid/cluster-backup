#!/usr/bin/env python3

import sys, random, time, socket, logging, os, _thread
from threading import Thread
# from multiprocessing import Process as Thread

import config, elapsed, scanner, persistent_dict, utils, locker, lock
from datagram import *

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
* encourage clients to balance among themselves
* return multiple files (dict) with request() (requires client change)
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

        lazy_write = utils.str_to_duration(self.config.get(context, "LAZY WRITE", 5))
        # TODO: support expiration
        self.rescan = utils.str_to_duration(self.config.get(self.context, 
                                                            "rescan"))
        self.clients = persistent_dict.PersistentDict(
                f"/tmp/cb.s{context}.json.bz2", lazy_write=lazy_write, 
                cls=lock.Lock, expiry=self.rescan)
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
            self.config.load()
            self.scanner.scan()
            self.update_files()
            self.heartbeat()
            self.logger.info("Scanning complete; will re-scan in " \
                                + f"{utils.duration_to_str(self.rescan)}")
            time.sleep(self.rescan)

    
    # no longer required; DEAD
    def update_files(self):
        for filename, stuff in self.scanner.items():
            if not filename in self.clients:
                self.clients[filename] = None


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
        files = self.clients.keys()
        for filename in files:
            count = len(self.clients[filename])
            nblocks += count
            if count not in ncopies:
                ncopies[count] = 1
            else:
                ncopies[count] += 1
            if count >= self.copies:
                coverage += 1
            elif count > 0:
                coverage += count/self.copies
        if len(self.clients) > 0:
            # "coverage" shows the portion of dataset with "enough" copies
            # ... but > 100%, shows the amount of copies out there 
            #   :. coverage = 150% -> 1.5x requested number of copies
            #   ... not neccessarily "1.5 copies of everything"
            if coverage == len(self.clients): 
                # all files covered; show the *actual* coverge
                coverage = nblocks / (len(self.clients) * self.copies)
                message += f"coverage: {nblocks / (len(self.clients) * self.copies):5.1f}x"
            else:
                message += f"coverage: {100*coverage/len(self.clients):5.2f}%"
            message += f" of {self.copies}\n"
        self.logger.info(message)


    def audit(self):
        self.heartbeat()
        message = "\n"
        ncopies = {}
        clientelle = {}
        coverage = 0
        nblocks = 0
        for filename in self.clients:
            # self.logger.debug(f"{filename}: {len(self.clients[filename])} copies")
            clients = " + ".join(sorted(self.clients[filename]))
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
            message += f"{filename}:{len(self.clients[filename])} "
            i += 1
            if i > 20:
                message += "..."
                break
        self.logger.debug(message)


    # client claims filename (not a lock); returns a string
    # a few cases: 
    #   - not a file: drop it
    #   - not valid checksum because I haven't computed it: keep it
    #   - not valid checksum because it's wrong: update it
    #   - valid checksum (few cases): keep it
    def claim(self, args):
        client, filename, checksum = args[:3]
        if filename not in self.scanner:
            self.logger.warn("Client has a file, I don't; deleted?")
            return "drop"

        filestate = self.scanner[filename]
        self.logger.debug(f"{client} claims file {filename}")
        if filestate["checksum"] == "deferred":
            self.logger.debug("I have a deferred checksum; let it go (for now)")
            # fallthrough
        elif checksum != filestate["checksum"]:
            self.logger.warn(f"{client} has a different checksum ...")
            self.scanner.update(filename)
            filestate = self.scanner[filename]
            if checksum != filestate["checksum"]:
                self.logger.warn(f"{client} has the wrong checksum!\n" * 10)
                return "update"
            else:
                self.logger.warn(f"{client} was right; I'm straight now")
                return "keep"

#         if filename not in self.clients:
#             self.logger.warn(f"I'm learning about {filename}")
#             self.clients[filename] = [ client ]
#         elif client not in self.clients[filename]:
#                 self.clients[filename].append(client)
        self.clients[filename] = client
        self.stats['claims'] += 1
        self.release(filename)
        return "ack"


    # client releases their claim on filename
    def unclaim(self, args):
        client, filename = args[:2]
        # self.logger.debug(f"files: {self.clients}, client: {client}, filename: {filename}")
        if filename and len(self.clients) > 0 and filename in self.clients \
            and client in self.clients[filename]:
            # self.clients[filename].remove(client)
            del self.clients[filename][client]
            self.stats['drops'] += 1
        return "ack"


    # client wants to (gracefully) drop a file
    # ... just set a timer.  Client should re-ask if it's OK
    def drain(self, args):
        client, filename = args[:2]
        self.logger.debug(f"client: {client} requests drain {filename}")
        self.drains[f"{client}:{filename}"] = 1
        return "ack"


    # client asks for an inventory of the things I think he has
    # returns a list of filenames
    def inventory(self, args):
        client = args[0]
        self.logger.debug(f"{client} wants inventory")
        files = []
        client_files = self.clients.keys()
        for filename in client_files:
            if client in self.clients[filename]:
                # self.logger.debug(f"{client} has {filename}")
                files.append(filename)
        self.logger.debug(f"inventory: {len(files)}")
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


    # return nr_files least-served files smaller than sizehint
    def least_served(self, candidates, sizehint, nr_files):
        if self.scanner[candidates[0]]["size"] > sizehint:
            # they're all too big; pick one (TODO: some)
            filename = random.choice(candidates)
            return {filename: self.scanner[filename]["size"]}
        
        files = {}
        n = 0
        for filename in sorted(candidates, reverse=True, \
                        key = lambda f: self.scanner[f]["size"]):
            if self.scanner[filename]["size"] < int(sizehint):
                files[filename] = self.scanner[filename]["size"]
                n += 1
                if n >= nr_files:
                    break
        return files


    # serve a file request: the (if possible) largest, not-locked,
    # unowned-by-client file
    #  ANY underserved file is a candidate
    def request(self, args):
        client, sizehint = args[:2]
        self.update_files()  # TODO: fix the race condition behind this
        filelist = [ filename for filename in self.scanner.keys() \
                        if filename not in self.clients or \
                            ( client not in self.clients[filename] ) ] #\
                              #  and not self.held(filename, client) ) ]
        filelist = sorted(filelist, key=lambda f: self.scanner[f]["size"])
        filelist = sorted(filelist, key=lambda f: len(self.clients[f]))
        self.logger.debug(f"pulled a list for {client}:")
        self.dump_files(filelist)
        if not filelist: # no files for this client
            return None
        target = len(self.clients[filelist[0]]) + 1
        if target < self.copies:  # implies underserved; expand to 
            target = self.copies  # consider any underserved file
        candidates = [ filename for filename in filelist \
                        if len(self.clients[filename]) < target ]
        self.logger.debug(f"targeted list for {client}:")
        self.dump_files(candidates)
        files = self.least_served(candidates, int(sizehint), 20)
        self.logger.debug(f"least_served gives {len(files)} files")
        self.logger.debug(f"least_served gives {files}")
        if files:
            for filename in files.keys():
                self.hold(filename, client)
            return files
        else:
            return None


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
        for filename in self.clients:
            if client not in self.clients[filename] and \
                len(self.clients[filename]) < self.copies:
                    files.append(filename)
        if len(files) > 0:
            return self.random_subset(files, 20)
            # self.logger.debug(f"returning {file} and moar")
        self.logger.debug("I got nothin'")
        return None


    # should return the most-overserved file which is held
    # by the named client
    def overserved(self, args):
        client = args[0]
        files = {}
        for filename in self.clients:
            # self.logger.debug(f"{filename}: {len(self.clients[filename])}/{self.copies} {self.clients[filename]}")
            if client in self.clients[filename]: 
                # self.logger.debug(f"client match for {filename}")
                if len(self.clients[filename]) > self.copies:
                    # self.logger.debug(f"overserved file: {filename}")
                    files[filename] = len(self.clients[filename])
        if len(files.keys()) > 0:
            filenames = sorted(files.keys(), key=lambda x: files[x], reverse=True)
            return self.random_subset(filenames, 20)
        else:
            self.logger.debug("no overserved files :/")
            return None


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
        return response or ["just right"]


    def heartbeep(self, args):
        client = args[0]
        self.logger.info(f"heartbeep from {client}: {args}")
        return "ack"


    # handle a datagram request: returns a string
    def handle(self, action, args):
        if not self.handling:
            # return "n/a"
            return None
        # self.logger.debug(f"requested: {action} ({args})")
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
        # self.logger.debug(f"responding: {action} {args} -> {response}")
        return response


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
    # [ action, server context, client context, arguments ]
    def handle(self, request):
        action, server_context = request[:2]
        args = request[2:]
        if server_context not in self.servlets:
            return None
        # self.logger.debug(f"acting: {server_context} => {action}({args})")
        response = self.servlets[server_context].handle(action, args)

        return response


    def handler(self, datagram):
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


    def serve(self):
        ADDRESS = self.hostname
        PORT = int(self.config.get("global", "PORT", "5005"))
        dgserver = DatagramServer(ADDRESS, PORT)
        while True:
            datagram = dgserver.accept()
            _thread.start_new_thread(self.handler, (datagram,))


    # busy guy: all servlets should scan forever, and
    # server still has to serve forever
    def run(self):
        for context, servlet in self.servlets.items():
            servlet.start()
        _thread.start_new_thread(self.auditor, ())
        self.serve()


if __name__ == "__main__":
    sys.exit(1)
