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

BEHAVIOUR
* if possible, back up a file (strategy = "acquire")
    * prefer "underserved" hosts
    * if none, take an "available" host
    * overserved & "just right" hosts are of no interest
* if I tried to copy a file and couldn't find any because of storage,
    change stratetgy (strategy = "drop")
    * hunt for (one) overserved file, and drop it; 
        change strategy to "acquire"

Source selection:
- prefer a source which reports a file being "underserved"
  - NO prioritization among files or sources; any is a candidate
    TODO: try again if the offered file would exhaust my free space
- randomly select a source which has *any* files
  - this means a backup-with-storage will (randomly) slurp more
    content down from a source; the source will see over-coverage 
    for some files
  - sources should therefore prefer handing out files in reverse
    order of coverage (job of the server)
- if no sources have any files, maybe rebalance

Once a source is chosen, it's "sticky"; the Clientlet will keep
copying from this source until the source is covered

Note that a source won't return a file to a client if the client
already has the file.

REBALANCE
If a Clientlet is full it will try to rebalance:
    if a server has an underserved file, 
    drop one of my overserved files
    (implied: next pass, I'll find the underserved file)

TODO:
- make a "are we good" message, for the server to have the opportunity
  to ask the client for an update (in the case where a server has no 
  state); possibly as a response to "overserved"
    * done in the periodic "inform" ?
- on startup, have the client ask the server IF I should be holding files 
  like an "inform 0"
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


# TODO: if a server volunteers a file I already have, 
# I should give him my inventory (and prune as needed)
# TODO: I should periodically tell a server about all my files



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
        assert os.path.exists(self.path)
        self.allocation = 2*2**30 # GB; TODO automate this
        self.allocation = utils.str_to_bytes(
                            self.config.get(self.context, "size", 0))
        assert self.allocation > 0
        self.bailing = False

        # ALL source contexts (we care a lot)
        self.source_contexts = self.config.get_contexts_for_key("source")
        self.sources = {}
        self.scanners = {}
        self.random_source_list = []
        self.build_sources()
        self.favorite_source = None
        self.strategy = "acquire" # could also be "drop"; will change over time
        self.drops = 0  # count the number of times I drop a file


    def build_sources(self):
        for source_context, source in self.source_contexts.items():
            self.sources[source_context] = config.host_for(source)
            path = f"{self.path}/{source_context}"
            self.scanners[source_context] = \
                scanner.Scanner(source_context, path, 
                                name=f"{self.context}:{source_context}")
            self.random_source_list.append(source_context)
        random.shuffle(self.random_source_list)


    # I (think I) have this file; claim it
    #   if my claim is invalid (checksum doesn't match), 
    #   remove it
    def claim(self, source_context, filename):
        self.logger.debug(f"claiming {source_context}:/{filename}")
        # scan should be cheap, so no per-file scan
        self.scanners[source_context].scan()
        filestate = self.scanners[source_context].get(filename)
        # result = self.send(source_context, "claim", filename, \
        #                     filestate["checksum"])[0]
        response = self.send(source_context, "claim", filename, \
                            filestate["checksum"])[0]
        self.logger.debug(f"2329 {response} is {type(response)}")
        if response and response != "NACK":
            # self.files.append(filename)   # implied by ownership
            self.logger.debug(f"{response} is {type(response)}")
            self.logger.debug(f"{self.context} claimed {filename} successfully")
        else:
            self.logger.debug(f"{response} is {type(response)}")
            self.logger.debug(f"NACK NACK NACK")
            self.logger.debug(f"claim returned >{response}<")
            self.drop(source_context, filename)


    def inform(self, source_context=None, filename=None):
        if source_context and filename:
            self.claim(source_context, filename)
        elif source_context:
            for filename in self.scanners[source_context]:
                self.claim(source_context, filename)
        else:
            for source_context in self.scanners:
                for filename in self.scanners[source_context].keys():
                    self.claim(source_context, filename)



    # actually copying a file takes time
    # ... unless I already have it:
    #       because (for any reason) the server forgot, but it's
    #       in my backup folder (in which case, just send the checksum)
    def retrieve(self, source_context, filename):
        self.logger.debug(f"retrieving {source_context}:{filename} to {self.path}/{source_context}/{filename}")
        # 0: do I have it?
        if self.scanners[source_context].contains_p(filename):
            self.logger.debug(f"I already have {filename}")
            # stomp on filename -- give him everything
            # TODO: send this as one big list (maybe NBD)
            for filename in self.scanners[source_context].keys():
                self.claim(source_context, filename)
            return

        # 1: build the filenames (full path) for source + dest
        source = config.path_for(self.config.get(source_context, "source")) + \
                                    "/" + filename
        dest_path = f"{self.path}/{source_context}"
        dest = f"{dest_path}/{filename}"

        # 2: make the transfer
        self.logger.debug(f"rsync {source} {dest}")
        rsync_stat = file_state.rsync(source, dest)
        self.logger.debug(f"rsync returned {rsync_stat}")

        if rsync_stat == 0:
            # 3: record it
            self.claim(source_context, filename)
        else:
            self.logger.error("Failed to rsync???")
            raise FileNotFoundError


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




    # choose a source; if I already have a favorite, use it
    # else cycle through sources and look for underserved
    #     (no prioritization -- any underserved is OK)
    # else just pick one (implied: all sources are candidates)
    def DEADselect_source(self):
        # do I already have a favorite?  be sticky (I'll forget it later)
        if self.favorite_source is not None:
            self.logger.debug(f"favorite: {self.favorite_source}")
            return

        # is any source underserved?
        for source_context in self.random_source_list:
            self.logger.debug(f"considering {source_context}")
            response = self.send(source_context, "underserved")
            if response: # if response[0] is not None:
                self.logger.debug(f"got {response}\n")
                self.favorite_source = source_context
                self.logger.debug(f"new favorite: {self.favorite_source} (underserved)")
                return

        self.logger.debug("no underserved hosts; looking for any")
        # Does any source have files?
        for source_context in self.random_source_list:
            self.logger.debug(f"considering {source_context}")
            # if self.send(source_context, "request", str(self.free())):
            response = self.request(source_context)
            if response:   # TODO: abbreviate
                self.favorite_source = source_context
                self.logger.debug(f"new favorite: {self.favorite_source} (satisfied)")
                return

        # no sticky, no underserved, all are satisfied: no sources
        self.favorite_source = None


    # returns a tuple -- (filename, size) (or a 2-element list)
    def request(self, source_context):
        # filename, *rest = self.send(source_context, "request", \
        #                                str(self.free()))[:2]
        # self.logger.debug(f"request gets {filename}, {rest}")
        # if filename is None:
        #     return (None, 0)
        # else:
        #     return (filename, rest[0])
        response = self.send(source_context, "request", str(self.free()))
        self.logger.debug(f"request gets {response}")
        if response:
            self.logger.debug(f"873435 response: {type(response)}, {response.contents}")
            return response[0], response[1]
        return (None, 0)


    # calculate my consumed storage (based on the sum of sizes
    #   in each scanner) and compare to what I'm allowed to
    #   consume; return allotment - consumption
    def free(self):
        self.logger.debug(f"used {utils.bytes_to_str(self.consumption(), approximate=True)} of {utils.bytes_to_str(self.allocation)}")
        return self.allocation - self.consumption()

    def full_p(self):
        return self.free() <= 0


    def consumption(self):
        consumed = 0
        for source_context in self.scanners:
            consumed += self.scanners[source_context].consumption()
        return consumed


    # send "message" to host @ source_context; return the response
    # mssage: <command> @@ server @@ <client> @@ <args>
    # like "underserved @@ <server> @@ <client>" 
    #   or "claim @@ <server> @@ <client> @@ <filename>"
    def send(self, source_context, command, *args):
        ADDRESS = self.sources[source_context]
        PORT = int(self.config.get("global", "PORT", "5005"))
        BUFFER_SIZE = 1024
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
            self.logger.debug(f"{self.context} got that broken pipe")

        s.close()
        self.logger.debug(f"received '{data}'")
        # if data == "__none__":
        #     return (None,)
        # return data.split(" @@ ")
        ret = comms.Communique.build(data, negatives=("NACK", "__none__", "None"))
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
        answers = {}
        for source_context in self.random_source_list:
            response = self.send(source_context, "status")
            if response:
                answers[source_context] = str(response)
        self.logger.debug(answers)
        if len(answers) == 0:
            return True    # no answers != no space
        keys = list(answers.keys())
        random.shuffle(keys)
        for source_context in keys:
            if answers[source_context] == "underserved":
                return self.copy_from(source_context)
        for source_context in keys:
            if answers[source_context] == "available":
                return self.copy_from(source_context)
        return True # fallthrough


    # hunt for ONE file to drop, then drop it
    def try_to_drop(self):
        for source_context in self.random_source_list:
            response = self.send(source_context, "overserved")
            self.logger.debug(f"2323 response: {response} of type {type(response)}")
            if response:
                self.logger.debug(response)
                filename = response[0]
                self.logger.debug(f"overserved: {source_context}:{filename}")
                return self.drop(source_context, filename)
        return False  # nothing dropped


    def check_on_servers(self):
        server_statuses = {}
        for source_context in self.random_source_list:
            response = self.send(source_context, "status")
            if response:
                if str(response) in server_statuses:
                    server_statuses[str(response)].append(source_context)
                else:
                    server_statuses[str(response)] = [ source_context ]
        return server_statuses


    """
    BEHAVIOUR
    * if possible, back up a file (strategy = "acquire")
        * prefer "underserved" hosts
        * if none, take an "available" host
        * overserved & "just right" hosts are of no interest
    * if I tried to copy a file and couldn't find any because of storage,
        change stratetgy (strategy = "drop")
        * hunt for (one) overserved file, and drop it; 
            change strategy to "acquire"
        * don't hunt forever (there might be smaller files I could pick up)

    ZOMG
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
        server_statuses = self.check_on_servers()
        if self.full_p():
            self.logger.debug("full; trying to drop")
            self.try_to_drop()
            return True

        if "underserved" in server_statuses:
            self.logger.debug("someone is underserved; copy")
            if not self.try_to_copy():
                self.try_to_drop()
            return True

        if "available" in server_statuses:
            self.logger.debug("maybe I can cover another file")
            return self.try_to_copy()
            

    def step2(self):
        if self.full_p():
            self.logger.debug("full; trying to drop")
            return self.try_to_drop()
        elif self.strategy == "acquire":
            # source_context = self.select_source("acquire")
            self.logger.debug("trying to copy")
            if not self.try_to_copy():
                self.strategy = "drop"
        else:
            self.logger.debug("not acquiring; trying to drop")
            self.strategy = "acquire"
            return self.try_to_drop()
        return True


    # "step" through one cycle (find/request/copy/record)
    # pulls one (list of) files from one server
    # returns True if I did something (implying there's probably 
    # more to do).  If I'm full I'll make a rebalance() pass, but
    # I'll return False
    def OLDstep(self): 
        # definitely rebalance if I'm full
        if self.full_p():
            return self.rebalance()

        self.logger.debug("selecting source")
        self.favorite_source = self.select_source()
        # if I get a "favorite" it's because files need replicatin'
        if self.favorite_source is not None:
            source_context = self.favorite_source
            self.logger.debug(f"got {source_context}")
            # filename, *rest = self.send(source_context, "request", \
            #                         str(self.free()))[:2]
            filename, size = self.request(source_context)
            if filename is not None:
                self.logger.debug(f"retrieving {source_context}:{filename}")
                self.retrieve(source_context, filename)
                return True
            else:
                # my favorite returned no files, so I've 
                # likely exhausted all sources; take a rest?
                # TODO: check this
                self.favorite_source = None
                return False
        else:
            # no favorite: I have all the files (yay!)
            # TODO "if someone is underserved, rebalance"
            return self.rebalance()
            return False


    def audit(self):
        self.logger.debug(f"auditing {self}: {self.drops} drops")
        for source_context in sorted(self.scanners):
            # nfiles = len(self.scanners[source_context].states.items())
            nfiles = len(self.scanners[source_context].items())
            self.logger.debug(f"{source_context}: {nfiles} files")


    def run(self):
        self.bailing = False # future use, to kill the Thread
        timer = elapsed.ElapsedTimer()
        for source_context in self.scanners:
            self.scanners[source_context].scan()
        while not self.bailing:
            self.logger.info(f"running")
            while self.step():
                time.sleep(1)
                self.logger.debug(f"stepping again")
                if timer.once_every(60):
                    for source_context in self.scanners:
                        self.scanners[source_context].scan()
                    self.inform()
            self.audit()
            self.logger.info(f"sleeping")
            time.sleep(30)


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
    cfg.init(options["configfile"], "source", "backup")
    c = Client(options["hostname"])
    c.run()


if __name__ == "__main__":
    main(sys.argv)
