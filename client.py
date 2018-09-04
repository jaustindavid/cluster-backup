#!/usr/bin/env python3

import sys, random, time, socket, logging, os
from threading import Thread
import config, scanner, file_state, utils, elapsed, stats
from utils import logger_str
from datagram import Datagram
from persistent_dict import PersistentDict

"""
The backup client: a Client which just kicks off one-per-local-fs
Clientlets, and periodically asks them to audit (all at once, for
ease of finding in the logs)

The Clientlet owns a local filesystem.  It runs a Scanner which
actually watches files and checksums.  The Clientlet will request
files from sources, trusting that they will volunteer needy files.

Clientlet manages to a configured "size" (static), or possibly can 
"reserve" space dynamically for other use.  It will try to *always* 
leave "reserve" space available, so if you use the FS for other
purposes, the Clientlet's consumption will grow/shrink actively.


BEHAVIOUR
see step()


TODO:
- a global "status" engine to report what I'm doing
- timeout requests
- retry claims (esp on inform)
- seek to balance replicas for a server
- if I get overfull, is there a way to have another client shuffle so
    I can get straight?  Like, have an overserved client drop files
    then cover some of mine...  And is this a big deal?  (a sort of 
    "drain").  Maybe inform server (drain vs. unclaim) ?
    NB: just ~randomly dropping a file works, so a "drain" would work too

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

        lazy_write = utils.str_to_duration(self.config.get(context, "LAZY WRITE", 5))
        # TODO: my cache of claims should expire in rescan/2
        self.rescan = self.get_interval("rescan")//2
        self.claims = PersistentDict(f"/tmp/cb.c{context}.json.bz2", 
                                    lazy_write=5, expiry=self.rescan)
        self.drops = 0  # count the number of times I drop a file
        self.stats = stats.Stats()

        self.update_allocation()
        self.bailing = False
        self.datagrams = {}


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


    #    # ##### # #      # ##### #   #
    #    #   #   # #      #   #    # #
    #    #   #   # #      #   #     #
    #    #   #   # #      #   #     #
    #    #   #   # #      #   #     #
     ####    #   # ###### #   #     #


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


    def unclaim(self, source_context, filename):
        response = self.send(source_context, "unclaim", filename)
        key = f"{source_context}:{filename}"
        if key in self.claims:
            del self.claims[key]
        # self.logger.debug(f"23235 response={response}, of type {type(response)}")
        return response



    """
    TODO: write 'drain'
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







    # if needed, create a long-lived datagram
    def get_datagram(self, source_context):
        ADDRESS = self.sources[source_context]
        PORT = int(self.config.get("global", "PORT", "5005"))
        
        if source_context not in self.datagrams:
            self.logger.debug(f"building a datagram for {source_context}")
            self.datagrams[source_context] = \
                        Datagram("Bogus", server=ADDRESS, port=PORT)
            self.datagrams[source_context].ping()
        return self.datagrams[source_context]


    # TODO: DEAD?
    def del_datagram(self, source_context):
        datagram = self.datagrams[source_context]
        datagram.close()
        del self.datagrams[source_context]



    # send a command(args) to a source_context; 
    # if possible, re-use an existing datagram
    #
    # this always returns a datagram; use .connected()
    def send(self, source_context, command, *args):
        datagram = self.get_datagram(source_context)

        if not datagram:
            self.logger.debug("whoa, no datagram")
            # try once:
            self.del_datagram(source_context)
            datagram = self.get_datagram(source_context)
            if not datagram:
                self.logger.debug("still didn't work?")
            return datagram

        self.stats['requests'].incr(1)
        commandlist = [ command, source_context, self.context ]
        for arg in args:
            commandlist.append(arg)
        datagram.set(commandlist)

        if datagram.send():
            datagram.receive()
        return datagram



     ####   ####  #####  #   #
    #    # #    # #    #  # #
    #      #    # #    #   #
    #      #    # #####    #
    #    # #    # #        #
     ####   ####  #        #


    def claim_is_valid(self, source_context, filename):
        rescan = self.get_interval("rescan")//2
        key = f"{source_context}:{filename}"
        return key in self.claims and \
            self.claims[key] + rescan > time.time()


    # I (think I) have this file; claim it
    #   if my claim is invalid (checksum doesn't match), 
    #   remove it
    def claim(self, source_context, filename, **kwargs):
        self.stats['claims'].incr(1)
        self.logger.debug(f"claiming {source_context}:{filename}")
        self.scanners[source_context].update(filename)
        filestate = self.scanners[source_context][filename]
        response = self.send(source_context, "claim", filename, \
                            filestate["checksum"])
        if response in ("ack", "keep"):
            self.claims[f"{source_context}:{filename}"] = time.time()
        # self.logger.debug(f"2329 {response} is {type(response)}, bool({bool(response)})")
        return response


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
    def retrieve(self, source_context, filename):
        self.logger.debug(f"retrieving {source_context}:{filename}" + \
                            f" to {self.path}/{source_context}/{filename}")
        # 0: do I have it?
        if self.scanners[source_context].contains_p(filename):
            claimed = self.claim(source_context, filename, dropping=True)
            self.logger.debug(f"I already have {filename}; claimed = {claimed}")
            if claimed in ("ack", "keep"):
                return claimed
            else:
                self.logger.debug(f"Something's wrong, trying again")

        # 1: build the filenames (full path) for source + dest
        source = self.config.get(source_context, "source") + "/" + filename
        src_host = config.host_for(source)
        hostname = config.host_for(self.config.get(self.context, "backup"))
        if src_host == hostname: # a local copy, just use path
            source = config.path_for(source)
        dest = f"{self.path}/{source_context}/{filename}"

        # 2: make the transfer
        self.logger.debug(f"rsync {source} {dest}")
        self.makedirs(dest)
        rsync_stat = file_state.rsync(source, dest)
        self.logger.debug(f"rsync returned {rsync_stat}")

        if rsync_stat == 0:
            # 3: record it
            self.claim(source_context, filename, dropping=True)
        else:
            self.logger.error("Failed to rsync???")
            raise FileNotFoundError


    # should return False if I wasn't able to copy because I'm out of space
    def copy_from(self, source_context):
        response = self.send(source_context, "request", str(self.free()))
        if response:
            for filename, size in response.value().items():
                if int(size) > self.free():
                    # can't copy -- too big :(
                    self.logger.debug(f"{filename} is too big; skipping")
                    self.last_copy = "not enough space"
                    return False
                else:
                    self.logger.debug(f"retrieving {source_context}:{filename}")
                    # TODO: insert retries here
                    if self.retrieve(source_context, filename):
                        self.last_copy = "success"
        return True


    # try to copy one file; prefer underserved (then available) hosts
    # if I fail to copy any files because of size, return False
    def try_to_copy(self):
        if "underserved" in self.server_statuses:
            source_context = random.choice(self.server_statuses['underserved'])
        elif "available" in self.server_statuses:
            source_context = random.choice(self.server_statuses['available'])
        if (source_context):
            return self.copy_from(source_context)
        return True # fallthrough


#####  #####   ####  #####
#    # #    # #    # #    #
#    # #    # #    # #    #
#    # #####  #    # #####
#    # #   #  #    # #
#####  #    #  ####  #


    def drop(self, source_context, filename):
        self.stats['drops'].incr(1)
        self.logger.debug(f"dropping {source_context}:{filename}")
        response = self.send(source_context, "unclaim", filename)
        self.logger.debug(f"response={response}")
        if response:
            if filename in self.scanners[source_context]:
                self.scanners[source_context].drop(filename)
                self.logger.info(f"successfully dropped {filename}")
                self.drops += 1  # TODO: stats pack
                return True
            else:
                self.logger.warn(f"weird: I don't have {filename}")
                return False
        else:
            self.logger.warn(f"no response, not dropping {source_context}:{filename}")
            return False


    # hunt for ONE overserved file to drop, then drop it
    def try_to_drop(self):
        self.logger.debug("trying to drop:")
        self.logger.debug(f"statuses: {self.server_statuses}")
        if 'overserved' in self.server_statuses:
            source_context = random.choice(self.server_statuses['overserved'])
            response = self.send(source_context, "overserved")
            if response:
                self.logger.debug(response)
                filename = response[0]
                self.logger.debug(f"overserved: {source_context}:{filename}")
                return self.drop(source_context, filename)
        else:
            self.logger.debug("couldn't find any overserved hosts")
        return False  # nothing dropped


     ####  #####   ##   ##### #    #  ####
    #        #    #  #    #   #    # #
     ####    #   #    #   #   #    #  ####
         #   #   ######   #   #    #      #
    #    #   #   #    #   #   #    # #    #
     ####    #   #    #   #    ####   ####


    def check_on_servers(self):
        server_statuses = {}
        self.logger.debug("checking status")
        for source_context in self.random_source_list:
            response = self.send(source_context, "status")
            # self.logger.debug(f"response: >{response}<")
            if response:
                for status in response:
                    # self.logger.debug(f"\tstatus: >{status}<")
                    if status in server_statuses:
                        server_statuses[status].append(source_context)
                    else:
                        server_statuses[status] = [ source_context ]
        return server_statuses


    def claim_from_list(self, source_context, filenames):
        for filename in filenames:
            if filename in self.scanners[source_context]:
                counted[filename] = 1
                if not self.claim_is_valid(source_context, filename):
                    claimed = self.claim(source_context, filename, dropping=True)
                    # TODO: handle a failed claim
                    if claimed not in ("ack", "keep"):
                        self.logger.debug(f"claim returns {claimed}")
                        if claimed.value() is None:
                            break # they're not listening RN
            else:
                self.unclaim(source_context, filename)


    def reinventory_source(self, source_context):
        inventory = self.send(source_context, "inventory")
        if not inventory or inventory.value() is None:
            self.logger.debug(f"Didn't get an inventory from {source_context}")
            return False

        scanner = self.scanners[source_context]
        self.logger.debug(f"re-inventory got {len(inventory)} files from {source_context}")
        self.logger.debug(f"I hold {len(scanner.keys())} files from {source_context}")
        # claim (or disclaim) the server's list
        self.claim_from_list(inventory)

        # claim (or disclaim) things I have but serve doesn't know about
        orphans = [ filename for filename in scanner \
                        if filename not in inventory ]
        self.claim_from_list(orphans)





    # aggressively re-inventory my contents:
    # 1) get a (complete) list of what the server thinks I have,
    #   reclaim (or unclaim) each
    # 2) claim any files the server doesn't know about 
    #   this is common for a server restart (client state gets reset)
    # TODO: multi-claim
    def reinventory(self):
        for source_context in self.random_source_list:
            if self.send(source_context, "heartbeep"):
                rescan = self.get_interval("rescan")//2
                response = self.send(source_context, "inventory")
                scanner = self.scanners[source_context]
                counted = {}
                if response and response.value() is not None:
                    self.logger.debug(f"re-inventory got {len(response)} files from {source_context}")
                    self.logger.debug(f"{response}")
                    self.logger.debug(f"I hold {len(scanner)} files from {source_context}")
                    # claim (or disclaim) the server's list
                    for filename in response:
                        if filename in scanner:
                            counted[filename] = 1
                            if not self.claim_is_valid(source_context, filename):
                                claimed = self.claim(source_context, filename, dropping=True)
                                # TODO: handle a failed claim
                                if claimed not in ("ack", "keep"):
                                    self.logger.debug(f"claim returns {claimed}")
                                    if claimed.value() is None:
                                        break # they're not listening RN
                        else:
                            self.unclaim(source_context, filename)
                # claim any I have but not in his list
                for filename in scanner:
                    if filename not in counted:
                        claimed = self.claim(source_context, filename, dropping=True)
                        # TODO: handle a failed claim
                        if claimed not in ("ack", "keep"):
                            self.logger.debug(f"claim returns {claimed}")
                            if claimed.value() is None:
                                break # they're not listening RN


    def heartbeep(self):
        self.logger.debug(f"heartbeeping")
        infoes = f"used {utils.bytes_to_str(self.consumption(), approximate=True)} of {utils.bytes_to_str(self.allocation)}"
        for source_context in sorted(self.scanners):
            self.send(source_context, "heartbeep", infoes)
        self.logger.debug(infoes)


    def audit(self):
        self.logger.debug(f"auditing {self}: {self.drops} drops")
        for stat in self.stats:
            self.logger.debug(f"{stat}: {self.stats[stat].qps()}")
        for source_context in sorted(self.scanners):
            # nfiles = len(self.scanners[source_context].states.items())
            nfiles = len(self.scanners[source_context].items())
            self.logger.debug(f"{source_context}: {nfiles} files")


    # calculate my consumed storage (based on the sum of sizes
    #   in each scanner)
    def consumption(self):
        consumed = 0
        for source_context in self.scanners:
            consumed += self.scanners[source_context].consumption()
        return consumed


    # update my allocation of storage: dynamic if I "reserve" space
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

    
    # number of bytes I could copy before going over
    def free(self):
        self.logger.debug(f"used {utils.bytes_to_str(self.consumption(), approximate=True)} of {utils.bytes_to_str(self.allocation)}")
        self.update_allocation()
        return self.allocation - self.consumption()


    def full_p(self):
        return self.free() <= 0


    def __str__(self):
        hostname = config.host_for(self.config.get(self.context, "backup"))
        return f"{hostname}: {utils.bytes_to_str(self.consumption())}/{utils.bytes_to_str(self.allocation)}"



    #####  #    # #    # #    # # #    #  ####  
    #    # #    # ##   # ##   # # ##   # #    # 
    #    # #    # # #  # # #  # # # #  # #      
    #####  #    # #  # # #  # # # #  # # #  ### 
    #   #  #    # #   ## #   ## # #   ## #    # 
    #    #  ####  #    # #    # # #    #  ####  


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
            

    def run_all_scanners_once(self):
        for source_context in self.scanners:
            self.scanners[source_context].scan()
            nfiles = len(self.scanners[source_context])
            self.logger.debug(f"scan complete, {nfiles} files")


    def stop(self):
        self.bailing = True


    def run(self):
        self.bailing = False # future use, to kill the Thread
        self.last_copy = "unknown"
        timer = elapsed.ElapsedTimer()
        while not self.bailing:
            self.audit()
            # TODO: honor different rescans per source
            if timer.once_every(self.rescan):
                self.run_all_scanners_once()
                self.reinventory()

            self.server_statuses = self.check_on_servers()
            if self.full_p(): 
                self.logger.debug("I'm full (?) trying to drop")
                self.try_to_drop()
            elif "underserved" in self.server_statuses:
                self.try_to_copy()
            elif "available" in self.server_statuses and \
                self.last_copy != "not enough space":
                self.try_to_copy()
            else:
                sleep_time = self.rescan - timer.elapsed()
                sleep_msg = utils.duration_to_str(sleep_time)
                self.logger.info(f"sleeping {sleep_msg} til next rescan")
                time.sleep(sleep_time)
            time.sleep(5)


    def DEADrun(self):
        self.bailing = False # future use, to kill the Thread
        timer = elapsed.ElapsedTimer()
        for source_context in self.scanners:
            self.scanners[source_context].scan()
            self.logger.debug(f"scan complete, {len(self.scanners[source_context].keys())} files")
        self.reinventory()
        while not self.bailing:
            self.config.load()
            # re-check this, in case config reloaded
            sleep_time = self.get_interval("rescan")//2
            hysteresis = self.get_interval("hysteresis")
            self.logger.info(f"running")
            while self.step():
                if hysteresis: time.sleep(hysteresis)
                self.logger.debug(f"stepping again")
                if timer.once_every(sleep_time):
                    for source_context in self.scanners:
                        self.scanners[source_context].scan()
                    self.heartbeep()
                    self.reinventory()
            self.audit()
            self.logger.info(f"sleeping {utils.duration_to_str(sleep_time)}")
            time.sleep(sleep_time)




# A Client represents this machine, and cares about ALL
# backup relevant to this machine.  Per-source interaction is 
# handled in the Clientlet[s]
#
# My job is to start a bunch of per-backup Clientlets, and
# periodically inspire them to print a status
class Client(Thread):
    def __init__(self, hostname):
        super().__init__()
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
        self.logger.info("Clientlets started")
        while True:
            self.logger.info("looping")
            for context, clientlet in self.clientlets.items():
                clientlet.audit()
            time.sleep(60)


if __name__ == "__main__":
    sys,exit(1)
