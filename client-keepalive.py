#!/usr/bin/env python3

import sys, random, time, socket, logging, os
from threading import Thread
import config, scanner, file_state, utils, elapsed, comms
from utils import logger_str

"""
The backup client: a Client which just kicks off one-per-local-fs
Clientlets, and periodically asks them to audit (all at once, for
ease of finding in the logs)

The Clientlet owns a local filesystem.  It runs a Scanner which
actually watches files and checksums.  The Clientlet will request
files of sources, trusting that they will volunteer needy files.

Clientlet manages to a configured "size", or possibly can "reserve"
space for other use.  It will try to *always* leave "reserve" space
available, so if you use the FS for other purposes, the Clientlet's
consumption will grow/shrink actively.


BEHAVIOUR
see step()


TODO:
- seek to balance replicas for a server
- if I get overfull, is there a way to have another client shuffle so
    I can get straight?  Like, have an overserved client drop files
    then cover some of mine...  And is this a big deal?  (a sort of 
    "drain").  Maybe inform server (drain vs. unclaim) ?
    NB: just ~randomly dropping a file works, so a "drain" would work too

2018-08-23 04:34:13,407 [Scanner 9a1db81e:736ef407] scanning path ., ignoring ['.cb.736ef407.json']
2018-08-23 04:34:13,410 [Clientlet 9a1db81e] sending claim @@ 736ef407 @@ 9a1db81e @@ ./3-file3 @@ d21812cac840e5f7c1acadf3553169d44f1f40f230302d2afe74c61142917476
2018-08-23 04:34:13,410 [Clientlet 9a1db81e] received 'NACK'
2018-08-23 04:34:13,411 [Clientlet 9a1db81e] ret=NACK, of type <class 'comms.Communique'>
2018-08-23 04:34:13,411 [Clientlet 9a1db81e] 2329 NACK is <class 'str'>
2018-08-23 04:34:13,411 [Clientlet 9a1db81e] NACK is <class 'str'>
2018-08-23 04:34:13,411 [Clientlet 9a1db81e] NACK NACK NACK
2018-08-23 04:34:13,411 [Clientlet 9a1db81e] claim returned >NACK<
2018-08-23 04:34:13,411 [Clientlet 9a1db81e] dropping 736ef407:./3-file3
2018-08-23 04:34:13,411 [Clientlet 9a1db81e] sending unclaim @@ 736ef407 @@ 9a1db81e @@ ./3-file3
2018-08-23 04:34:13,412 [Clientlet 9a1db81e] received 'ack'
2018-08-23 04:34:13,412 [Clientlet 9a1db81e] ret=ack, of type <class 'comms.Communique'>
2018-08-23 04:34:13,412 [Clientlet 9a1db81e] 23235 response=ack, of type <class 'str'>
2018-08-23 04:34:13,412 [Scanner 9a1db81e:736ef407] dropping ./3-file3; before=2532835328
2018-08-23 04:34:13,421 [Scanner 9a1db81e:736ef407] scanning path ., ignoring ['.cb.736ef407.json']
2018-08-23 04:34:13,423 [Scanner 9a1db81e:736ef407] dropped ./3-file3; after=2400190464
2018-08-23 04:34:13,424 [Clientlet 9a1db81e] successfully dropped ./3-file3
Exception in thread Thread-1:
Traceback (most recent call last):
  File "/Library/Frameworks/Python.framework/Versions/3.6/lib/python3.6/threading.py", line 916, in _bootstrap_inner
    self.run()
  File "./client.py", line 528, in run
    self.inform()
  File "./client.py", line 140, in inform
    for filename in self.scanners[source_context].keys():
RuntimeError: dictionary changed size during iteration
"""



# a Clientlet can copy data from any/every source, but will only
# store in one backup location
#
# My job: for a specified backup location (context) I will copy
# data down from any server-context until I'm full
class Clientlet(Thread):
    def __init__(self, context):
        super().__init__()
        self.context = context
        self.config = config.Config.instance()
        self.logger = logging.getLogger(logger_str(__class__) + " " + context)
        self.logger.info(f"Creating clientlet {self.context}")

        self.path = config.path_for(self.config.get(self.context, "backup"))
        assert os.path.exists(self.path), f"{self.path} does not exist!"
            
        # ALL source contexts (we care a lot)
        self.sources = {}
        self.scanners = {}
        self.random_source_list = []
        self.build_sources()
        self.drops = 0  # count the number of times I drop a file

        self.update_allocation()
        self.bailing = False
        self.sockets = {}


    def build_sources(self):
        source_contexts = self.config.get_contexts_for_key("source")
        self.prune_sources(source_contexts)
        
        for source_context, source in source_contexts.items():
            self.sources[source_context] = config.host_for(source)
            path = f"{self.path}/{source_context}"
            self.scanners[source_context] = \
                scanner.Scanner(source_context, path, 
                                name=f"{self.context}:{source_context}")
            self.random_source_list.append(source_context)
        random.shuffle(self.random_source_list)


    # TODO: if I'm holding files for this source, remove them
    def prune_sources(self, source_contexts):
        # prune any sources I'm ignoring
        ignored_sources = self.config.get(self.context, "ignore source")
        if ignored_sources:
            if type(ignored_sources) is str:
                ignored_sources = [ ignored_sources ]
            banned_contexts = []
            for context, source in source_contexts.items():
                for ignored_source in ignored_sources:
                    if source.startswith(ignored_source):
                        banned_contexts.append(context)
            for context in banned_contexts:
                del source_contexts[context]
        return source_contexts


    # I (think I) have this file; claim it
    #   if my claim is invalid (checksum doesn't match), 
    #   remove it
    def claim(self, source_context, filename, counter=0, **kwargs):
        if "dropping" in kwargs:
            dropping = kwargs["dropping"]
        else:
            dropping = False
        self.logger.debug(f"claiming {source_context}:/{filename}")
        # scan should be cheap, so no per-file scan
        self.scanners[source_context].scan()
        filestate = self.scanners[source_context].get(filename)
        response = self.send(source_context, "claim", filename, \
                            filestate["checksum"])
        # self.logger.debug(f"2329 {response} is {type(response)}, bool({bool(response)})")
        if response: 
            if response in ("ack", "keep"):
                # self.files.append(filename)   # implied by ownership
                self.logger.debug(f"{self.context} claimed {filename} successfully")
            elif response in ("update", "invalid checksum"):
                if counter:
                    counter -= 1
                    self.logger.debug(f"failed, but retrying {counter} times")
                    self.retrieve(source_context, filename, counter)
                else:
                    self.logger.debug(f"Giving up, can't seem to copy {filename}")
                    self.drop(source_context, filename)
            else:
                self.logger.debug(f"I should drop {filename}")
                if dropping:
                    self.logger.debug(f"claim returned >{response}<")
                    self.drop(source_context, filename)
        else:
            self.logger.debug("not dropping... (non-response)")


    # inform (optionally a server, optionally of a filename)
    # that I have it
    def inform(self, source_context=None, filename=None):
        if source_context and filename: # DEAD
            self.claim(source_context, filename, dropping=False)
        elif source_context:            # DEAD
            for filename in self.scanners[source_context]:
                self.claim(source_context, filename, dropping=False)
        else:                           # NOT DEAD YET
            for source_context in self.random_source_list:
                for filename in list(self.scanners[source_context].keys()):
                    self.claim(source_context, filename, dropping=False)


    def unclaim(self, source_context, filename):
        response = self.send(source_context, "unclaim", filename)
        self.logger.debug(f"23235 response={response}, of type {type(response)}")
        return response
        

    # inverse of "inform": what files should I have?
    # for this list of files, either claim or unclaim them
    def inventory(self, source_context=None):
        if source_context:
            response = self.send(source_context, "inventory")
            if response:
                self.logger.debug(f"got {response} type={type(response)}")
                for filename in response: 
                    self.logger.debug(f"filename: {filename} type={type(filename)}")
                    if filename in self.scanners[source_context]:
                        self.claim(source_context, filename, dropping=True)
                    else:
                        self.logger.debug(f"unclaiming {filename}")
                        self.unclaim(source_context, filename)
        else:
            for source_context in self.random_source_list:
                self.inventory(source_context)


    # TODO: include a dirname + trailing slash on server
    # and call it 0 bytes (so everyone will get it)
    def makedirs(self, filename):
        path = os.path.dirname(filename)
        if not os.path.exists(path):
            self.logger.debug(f"making {path}")
            os.makedirs(path, exist_ok=True)



    # actually copying a file takes time
    # ... unless I already have it:
    #       because (for any reason) the server forgot, but it's
    #       in my backup folder (in which case, just send the checksum)
    def retrieve(self, source_context, filename, counter=0):
        self.logger.debug(f"retrieving {source_context}:{filename} to {self.path}/{source_context}/{filename}")
        # 0: do I have it?
        if self.scanners[source_context].contains_p(filename):
            self.logger.debug(f"I already have {filename}")
            # just send the one; inform() will handle the rest
            self.claim(source_context, filename, dropping=True)
            return

        # 1: build the filenames (full path) for source + dest
        source = self.config.get(source_context, "source") + "/" + filename
        src_host = config.host_for(source)
        hostname = config.host_for(self.config.get(self.context, "backup"))
        if src_host == hostname: # a local copy, just use path
            source = config.path_for(source)
        dest_path = f"{self.path}/{source_context}"
        dest = f"{dest_path}/{filename}"

        # 2: make the transfer
        self.logger.debug(f"rsync {source} {dest}")
        self.makedirs(dest)
        rsync_stat = file_state.rsync(source, dest)
        self.logger.debug(f"rsync returned {rsync_stat}")

        if rsync_stat == 0:
            # 3: record it
            self.claim(source_context, filename, counter, dropping=True)
        else:
            self.logger.error("Failed to rsync???")
            raise FileNotFoundError


    """
    usage: 
        if self.full_p():
            TODO: select a file
            if self.drain(source_context, filemame):
                self.drop(source_context, filename)
            
    for whatever reason I need to drop a file, but it's not
    an emergency.  I'll "drain" it, and give the server time
    to get it rebalanced
    """
    def drain(self, source_context, filename = None):
        pass


    def drop(self, source_context, filename):
        self.logger.debug(f"dropping {source_context}:{filename}")
        response = self.send(source_context, "unclaim", filename)[0] # TODO
        self.logger.debug(f"23235 response={response}, of type {type(response)}")
        if response:
            if self.scanners[source_context].contains_p(filename):
                self.scanners[source_context].drop(filename)
                self.logger.info(f"successfully dropped {filename}")
                self.drops += 1
                return True
            else:
                self.logger.warn(f"weird: I don't have {filename}")
                return False
        else:
            self.logger.warn(f"NACK?? not dropping {source_context}:{filename}")
            return False


    # returns a tuple -- (filename, size) (or a 2-element list)
    def request(self, source_context):
        response = self.send(source_context, "request", str(self.free()))
        self.logger.debug(f"request gets {response}")
        if response:
            self.logger.debug(f"873435 response: {type(response)}, {response.contents}")
            return response[0], response[1]
        return (None, 0)


    # calculate my consumed storage (based on the sum of sizes
    #   in each scanner) and compare to what I'm allowed to
    #   consume; return allotment - consumption
    def consumption(self):
        consumed = 0
        for source_context in self.scanners:
            consumed += self.scanners[source_context].consumption()
        return consumed


    def update_allocation(self):
        reserve = utils.str_to_bytes(self.config.get(self.context,
                                                        "reserve", "0"))
        size = utils.str_to_bytes(self.config.get(self.context, "size", "0"))
        if reserve:
            stat = os.statvfs(self.path)
            fs_free = stat.f_frsize * stat.f_bavail
            self.logger.debug(f"consumed: {self.consumption()/2**30:5.2f}, free: {fs_free/2**30:5.2f}, reserve: {reserve/2**30:5.2f}")
            self.allocation = max(0, self.consumption() + fs_free - reserve)
        elif size:
            self.allocation = size
        assert self.allocation, "Can't start; define 'size' or 'reserve'"

    def free(self):
        self.logger.debug(f"used {utils.bytes_to_str(self.consumption(), approximate=True)} of {utils.bytes_to_str(self.allocation)}")
        self.update_allocation()
        return self.allocation - self.consumption()


    def full_p(self):
        return self.free() <= 0



    # if needed, create a long-lived socket
    def get_socket(self, source_context):
        ADDRESS = self.sources[source_context]
        PORT = int(self.config.get("global", "PORT", "5005"))
        
        if source_context not in self.sockets:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect((ADDRESS, PORT))
            except (ConnectionResetError, ConnectionRefusedError):
                return None
            self.sockets[source_context] = s
        return self.sockets[source_context]


    def del_socket(self, source_context):
        sock = self.sockets[source_context]
        sock.close()
        del self.sockets[source_context]


    # tries to recv() all bytes from sock, 
    # when the first chunk is in data and has a
    # size: <bytes> hint (fixed @ 16 bytes wide)
    def recvall(self, sock, data):
        i = 0
        size = int(data[6:16])
        buf = data[16:]
        sock.settimeout(5)
        while i < size:
            try:
                data = sock.recv(10240)
            except socket.timeout:
                self.logger.warn("Socket timeout!")
                return None
            i += len(data)
            buf += str(data, 'ascii')
        self.logger.debug(f"I recv()d {i} bytes")
        return buf



    # send a command(args) to a source_context; 
    # if possible, re-use an existing socket
    def send(self, source_context, command, *args):
        BUFFER_SIZE = 1024 # 10*2**20 # 10MB
        # create a socket
        sock = self.get_socket(source_context)
        if not sock:
            return comms.Communique(None)

        # message = f"{command} @@ {source_context} @@ {self.context}"
        # if len(args) > 0:
        #     message += " @@ " + " @@ ".join(args)
        comm = comms.Communique(command, source_context, self.context)
        comm.append(*args)
        message = str(comm)

        self.logger.debug(f"sending '{message}'")
        try:
            sock.sendall(bytes(message, 'ascii'))
            data = str(sock.recv(BUFFER_SIZE), 'ascii')
            print(f"recv() got {data}")
            # data = data.decode()
        except BrokenPipeError:
            data = "__none__"
            self.logger.exception(f"{self.context} got that broken pipe")
            self.del_socket(source_context)

        if data.startswith("size: "):
            data = self.recvall(sock, data)

        self.logger.debug(f"received >{data}<")
        if not data or data == "n/a" or data == "__none__":
            data = None  # servlet not ready; treat like conn failure
            ret = comms.Communique(None)
        else:
            ret = comms.Communique.build(data, negatives=("__none__", "None"))
            self.logger.debug(f"ret={ret}, of type {type(ret)}")
        return ret


    # send "message" to host @ source_context; return the response
    # mssage: <command> @@ server @@ <client> @@ <args>
    # like "underserved @@ <server> @@ <client>" 
    #   or "claim @@ <server> @@ <client> @@ <filename>"
    #
    # server can defer reqeusts with "n/a"; if so, just keep trying
    def send_dgram(self, source_context, command, *args):
        ADDRESS = self.sources[source_context]
        PORT = int(self.config.get("global", "PORT", "5005"))
        BUFFER_SIZE = 1024 # TODO: this should probably be VERYBIG for inventory
        BUFFER_SIZE = 10*2**20 # 10MB
        # TODO
        message = f"{command} @@ {source_context} @@ {self.context}"
        if len(args) > 0:
            message += " @@ " + " @@ ".join(args)
        # self.logger.debug(f"{message} -> {ADDRESS}:{PORT}:/{source_context}")

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((ADDRESS, PORT))
        except (ConnectionResetError, ConnectionRefusedError):
            return comms.Communique(None)
            return (None,)

        self.logger.debug(f"sending {message}")
        try:
            s.send(message.encode())
            data = s.recv(BUFFER_SIZE)
            data = data.decode()
        except BrokenPipeError:
            data = "__none__"
            self.logger.exception(f"{self.context} got that broken pipe")

        s.close()
        self.logger.debug(f"received '{data}'")
        if data == "n/a":
            data = None  # servlet not ready; treat like conn failure
        ret = comms.Communique.build(data, negatives=("__none__", "None"))
        self.logger.debug(f"ret={ret}, of type {type(ret)}")
        return ret


    # TODO: clients are overfilling; why?
    def copy_from(self, source_context):
        response = self.send(source_context, "request", str(self.free()))
        if response:
            filename = response[0]
            size = response[1]
            if int(size) > self.free():
                # can't copy -- too big :(
                self.logger.debug(f"{filename} is too big; skipping")
                return False
            self.logger.debug(f"retrieving {source_context}:{filename}")
            self.retrieve(source_context, filename)
        return True


    # try to copy one file; prefer underserved (then available) hosts
    # if I fail to copy any files because of size, return False
    def try_to_copy(self):
        # answers = {}
        # for source_context in self.random_source_list:
        #     response = self.send(source_context, "status")
        #     if response:
        #         answers[source_context] = str(response)
        # self.logger.debug(answers)
        # if len(answers) == 0:
        #     return True    # no answers != no space
        # keys = list(answers.keys())
        # random.shuffle(keys)
        if "underserved" in self.server_statuses:
            source_context = random.choice(self.server_statuses['underserved'])
        # for source_context in keys:
        #     if answers[source_context] == "underserved":
            return self.copy_from(source_context)
        if "available" in self.server_statuses:
            source_context = random.choice(self.server_statuses['available'])
        # for source_context in keys:
        #     if answers[source_context] == "available":
            return self.copy_from(source_context)
        return True # fallthrough


    # hunt for ONE overserved file to drop, then drop it
    def try_to_drop(self):
        self.logger.debug("trying to drop:")
        self.logger.debug(f"statuses: {self.server_statuses}")
        if 'overserved' in self.server_statuses:
            source_context = random.choice(self.server_statuses['overserved'])
        # for source_context in self.random_source_list:
            response = self.send(source_context, 'overserved')
            if response:
                self.logger.debug(response)
                filename = response[0]
                self.logger.debug(f"overserved: {source_context}:{filename}")
                return self.drop(source_context, filename)
        else:
            self.logger.debug("couldn't find overserved")
        return False  # nothing dropped


    def check_on_servers(self):
        server_statuses = {}
        for source_context in self.random_source_list:
            response = self.send(source_context, "status")
            self.logger.debug(f"response is {type(response)}: >{response}<")
            if response:
                if response[0] in server_statuses:
                    server_statuses[response[0]].append(source_context)
                else:
                    server_statuses[response[0]] = [ source_context ]
        return server_statuses


    """
    * if *I* am full, 
        drop something & loop
    * if any of my servers are undeserved
        pull one file & loop
        * if I can't copy, try to drop & loop
    * if any files are available to copy
        * try to copy & loop
        * if I can't copy, sleep
    """
    def step(self):
        self.server_statuses = self.check_on_servers()
        self.logger.debug(f"statuses: {self.server_statuses}")
        if self.full_p():
            self.logger.debug("full; trying to drop")
            self.try_to_drop()
            return True

        if "underserved" in self.server_statuses:
            self.logger.debug("someone is underserved; copy")
            if not self.try_to_copy():
                self.try_to_drop()
            return True

        if "available" in self.server_statuses:
            self.logger.debug("maybe I can cover another file")
            return self.try_to_copy()
            

    def heartbeep(self):
        self.logger.debug(f"heartbeeping")
        infoes = f"used {utils.bytes_to_str(self.consumption(), approximate=True)} of {utils.bytes_to_str(self.allocation)}"
        for source_context in sorted(self.scanners):
            self.send(source_context, "heartbeep", infoes)
        self.logger.debug(infoes)


    def audit(self):
        self.logger.debug(f"auditing {self}: {self.drops} drops")
        for source_context in sorted(self.scanners):
            # nfiles = len(self.scanners[source_context].states.items())
            nfiles = len(self.scanners[source_context].items())
            self.logger.debug(f"{source_context}: {nfiles} files")


    # returns the min config'd internval under my or 
    # any of my server's contexts
    def get_interval(self, interval_name):
        interval = utils.str_to_duration( \
                        self.config.get(self.context, interval_name))
        for source_context in self.random_source_list:
            interval = min(interval,
                    utils.str_to_duration( \
                        self.config.get(source_context, interval_name)))
        return interval


    def run(self):
        self.bailing = False # future use, to kill the Thread
        timer = elapsed.ElapsedTimer()
        for source_context in self.scanners:
            self.scanners[source_context].scan()
        self.inventory()    # asks the server
        self.inform()       # tells the server
        while not self.bailing:
            # re-check this, in case config reloaded
            sleep_time = self.get_interval("rescan")//2
            self.logger.info(f"running")
            while self.step():
                time.sleep(3)  # hysteresis
                self.logger.debug(f"stepping again")
                if timer.once_every(sleep_time):
                    for source_context in self.scanners:
                        self.scanners[source_context].scan()
                    self.heartbeep()
                    self.inform()
                    self.inventory()
            self.audit()
            self.logger.info(f"sleeping {utils.duration_to_str(sleep_time)}")
            time.sleep(sleep_time)


    def __str__(self):
        return f"{self.context}: {utils.bytes_to_str(self.consumption())}/{utils.bytes_to_str(self.allocation)}"


# A Client represents this machine, and cares about ALL
# backup relevant to this machine.  Server interaction is 
# handled in the Clientlet
#
# My job is to run a bunch of per-backup Clientlets
class Client:
    def __init__(self, hostname):
        self.hostname = hostname
        self.config = config.Config.instance()
        self.logger = logging.getLogger(logger_str(__class__))
        # ONLY my / relevant backup contexts
        self.backup_contexts = \
            self.config.get_contexts_for_key_and_target("backup", hostname)
        self.clientlets = {}
        self.build_clientlets()


    def build_clientlets(self):
        for context in self.backup_contexts:
            self.clientlets[context] = Clientlet(context)


    def run(self):
        self.logger.info("Client running...")
        for context, clientlet in self.clientlets.items():
            clientlet.start()
            # time.sleep(15)
        self.logger.info("Clientlets started")
        while True:
            self.logger.info("looping")
            for context, clientlet in self.clientlets.items():
                clientlet.audit()
            time.sleep(30)



################################
#
#          M A I N
#
################################

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
    cfg.init(options["configfile"], "source", "backup")
    c = Client(options["hostname"])
    c.run()


if __name__ == "__main__":
    main(sys.argv)
