#!/usr/bin/env python3

import random, time
from threading import Thread
import config, scanner, utils, elapsed, stats
from utils import *
from datagram import Datagram
from persistent_dict import PersistentDict


"""
Server just has a dict of { filename: [ size, nclients ], }

Clientlet will:
    randomly pull from that list to build an inventory
        { source_context: { filename: [ size, nclients ], }, }
    copy that inventory down from the source
    randomly walk inventories, updating my local copy list
        { filename: (source, size, nclients) }
        build the copy list into a per-source rsync task list
        execute the rsync (confirm success)
    push a set of (renewed) claims to the source(s)
        run the scanner, reconcile what-I-want vs. what-I-have
    iterate

    Clientlet will prefer least-served files, and may drop
    overserved files to copy less-served
"""

# it's a struct, basically
class URI:
    def __init__(self, source_context, filename, size, have, need):
        self.source_context = source_context
        self.filename = filename
        self.size = size
        self.have = have
        self.need = need
        self.ratio = have/need

    def __str__(self):
        size = bytes_to_str(self.size)
        return f"{self.source_context}:{self.filename}, {size}, {self.have}/{self.need}"


 #####
#     # #      # ###### #    # ##### #      ###### #####
#       #      # #      ##   #   #   #      #        #
#       #      # #####  # #  #   #   #      #####    #
#       #      # #      #  # #   #   #      #        #
#     # #      # #      #   ##   #   #      #        #
 #####  ###### # ###### #    #   #   ###### ######   #


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

        # creates per-source scanners, random_source_list, claims
        self.build_sources() 

        self.stats = stats.Stats()
        self.bailing = False
        self.datagrams = {}
        self.current_state = "startup"
        self.state_timer = elapsed.ElapsedTimer()
        self.states = {'startup': 0}
        self.efficiency = {}


    # todo: use a small number of types-of-filelists
    def build_sources(self):
        self.sources = {}       # { source_context: hostname:source_path, ... }
        self.paths = {}         # { source_context: local_path, ... }
        self.inventory = {}     # { source_context: source:inventory() }
                                # { filename: (size, ncopies), }
        self.backups = {}       # local backups (intended or actual)
                                #  { source_context: {filename: size,}, }
        self.scanners = {}      # my local storage (actual)
        self.claims = {}        # { source_context: { filename : time() }, }
        self.random_source_list = []   # [ list, of, sources ]
        self.datagrams = {}     # internal storage of connections
        self.metadata = {}      # internal storage of server metadata

        lazy_write = get_interval(self.config, "LAZY WRITE", (self.context,))
        source_contexts = self.config.get_contexts_for_key("source")
        self.prune_sources(source_contexts)

        for source_context, source in source_contexts.items():
            self.sources[source_context] = source
            path = f"{self.path}/{source_context}"
            self.paths[source_context] = path
            self.scanners[source_context] = \
                scanner.ScannerLite(source_context, path,
                               name=f"{self.context}:{source_context}")
            claims = f"{self.path}/{self.context}:{source_context}.bz2"
            self.claims[source_context] = PersistentDict(claims,
                                                    lazy_write=lazy_write)
            self.backups[source_context] = {}
            self.random_source_list.append(source_context)
        random.shuffle(self.random_source_list)


    # TODO: if I'm holding files for a pruned source, remove them
    def prune_sources(self, source_contexts):
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


     ####  #####  ####  #####    ##    ####  ###### 
    #        #   #    # #    #  #  #  #    # #      
     ####    #   #    # #    # #    # #      #####  
         #   #   #    # #####  ###### #  ### #      
    #    #   #   #    # #   #  #    # #    # #      
     ####    #    ####  #    # #    #  ####  ###### 



    # calculate my *actual* consumed storage 
    # (based on the sum of sizes in each scanner)
    def consumption(self):
        consumed = 0
        for source_context in self.scanners:
            consumed += self.scanners[source_context].consumption()
        return consumed


    # calculate my *probable* consumed storage 
    # (if I actually copy all the files in self.backups)
    def probable_consumption(self):
        consumed = 0
        for source_context in self.backups:
            for filename in self.backups[source_context]:
                consumed += self.backups[source_context][filename]
        return consumed



    # update my allocation of storage: dynamic if I "reserve" space
    def update_allocation(self):
        reserve = str_to_bytes(self.config.get(self.context, "reserve", "0"))
        size = str_to_bytes(self.config.get(self.context, "size", "0"))
        if reserve:
            stat = os.statvfs(self.path)
            fs_free = stat.f_frsize * stat.f_bavail
            self.allocation = max(0, self.consumption() + fs_free - reserve)
        elif size:
            self.allocation = size
        assert self.allocation > 0, "Can't run; allocation is 0"


    # number of bytes I could copy before going over
    def free(self):
        self.logger.debug(f"used {bytes_to_str(self.consumption(), approximate=True)} of {bytes_to_str(self.allocation)}")
        self.update_allocation()
        return self.allocation - self.consumption()


    def claim_is_valid(self, source_context, filename):
        if filename not in self.claims[source_context]:
            return False
        claim_time = self.claims[source_context][filename]
        rescan = self.metadata[source_context]['rescan']
        return claim_time + rescan >= time.time()


    # update (or create) the claim timestamp for files in source_context
    def renew_claims(self, source_context):
        stamp = time.time()
        for filename in self.backups[source_context]:
            self.claims[source_context][filename] = stamp


    def get_metadata(self):
        for source_context in self.random_source_list:
            self.metadata[source_context] = {}
            rescan = get_interval(self.config, 'rescan', (source_context,))
            self.metadata[source_context]['rescan'] = rescan
            copies = int(self.config.get(source_context, "copies"))
            self.metadata[source_context]['copies'] = copies

                    
            # response = self.send(source_context, "metadata")
            # if response:
            #     self.metadata[source_context] = response.value()


    def get_inventories(self):
        for source_context in self.random_source_list:
            ##  self.logger.debug(f"getting list from {source_context}")
            response = self.send(source_context, "list")
            if response:
                self.logger.debug(f"got {len(response)} files from {source_context}")
                if type(response.value()) is not dict:
                    self.logger.debug(f"non-dict response?  {str(response.value())[:200]}...\n" * 10)
                else:
                    self.inventory[source_context] = response.value()


    # re-populates self.backups based on reality
    def build_backups(self):
        for source_context in self.scanners:
            self.backups[source_context] = self.scanners[source_context].data
        

    # fake-copy one URI
    def pseudo_copy_uri(self, uri):
        self.backups[uri.source_context][uri.filename] = uri.size
        uri.have += 1
        if not uri.filename in self.efficiency:
            self.efficiency[uri.filename] = 0
        self.efficiency[uri.filename] += 1


    def is_owned(self, uri):
        return uri.source_context in self.backups \
            and uri.filename in self.backups[uri.source_context]


    # "drop" one file
    def pseudo_drop_uri(self, uri):
        self.logger.debug(f"dropping {uri.filename}")
        self.scanners[uri.source_context].drop(uri.filename)
        if uri.filename in self.backups[uri.source_context]:
            del self.backups[uri.source_context][uri.filename]
            uri.have -= 1
            assert uri.have >= 0
        if self.claim_is_valid(uri.source_context, uri.filename):
            del self.claims[uri.source_context][uri.filename]
        self.stats['drops'].incr(1)


    # if given files = { source_context: [ filename, ], }
    #    as generated by overserved / underserved
    # ... copy from it in priority order;
    def pseudo_copy(self, priority_list):
        self.restate("selecting files")
        self.logger.debug(f"pseudo_copying from {len(priority_list)} files")
        free = self.allocation - self.probable_consumption()
        self.logger.debug(f"pseudo_copy has {bytes_to_str(free)} free")
        # while (i have space) and (i have files):
        for file_uri in priority_list:
            if free <= 0:
                break
            if file_uri.size < free:
                free -= file_uri.size
                self.pseudo_copy_uri(file_uri)


    # scans the universe for files, generates a YUUUUGE sorted
    #   list of URIs
    #       where "have" == number of actual copies
    #             "need" == number of desired copies
    #  (sorted by have/need); any < 1.0 == underserved
    def generate_priority_list(self, inventory):
        # self.logger.debug(f"Generating a list")
        priority_list = []
        for source_context in inventory:
            if inventory[source_context]:
                for filename in inventory[source_context]:
                    size = inventory[source_context][filename][0]
                    have = inventory[source_context][filename][1]
                    need = self.metadata[source_context]['copies']
                    uri = URI(source_context, filename, size, have, need)
                    priority_list.append(uri)
        priority_list = sorted(priority_list, key=lambda uri: uri.size)
        priority_list = sorted(priority_list, key=lambda uri: uri.have/uri.need)
        return priority_list


    # read-only scan of priority list to find size_target bytes
    def scan_overserved(self, priority_list, size_target, ratio_target):
        bytes_found = 0
        index = len(priority_list) - 1
        candidates = []
        self.logger.debug(f"Trying to find {bytes_to_str(size_target)}")
        while index >= 0 and bytes_found < size_target:
            uri = priority_list[index]
            index -= 1
            if uri.ratio < ratio_target:   # sorted list; never drop underserved
                break
            if self.is_owned(uri) and uri.ratio > ratio_target:
                self.logger.debug(f"found candidate: {uri.filename}:{uri.ratio}, {uri.size}")
                bytes_found += uri.size
                candidates.append(uri)
        if bytes_found >= size_target:
            return candidates, bytes_found
        else:
            found = bytes_to_str(bytes_found)
            self.logger.debug(f"Only found {found}, it's not enough")
        return None, 0



    # potential optimization: use a running list of oversized
    def pseudo_drop_overserved(self, priority_list, size_target, ratio_target):
        candidates, reclaimed = self.scan_overserved(priority_list,
                                                    size_target, ratio_target)
        if not candidates:
            self.logger.debug(f"No space to be reclaimed")
            return 0
        drops = {}
        for uri in candidates:
            self.pseudo_drop_uri(uri)
            if uri.source_context not in drops:
                drops[uri.source_context] = []
            drops[uri.source_context].append(uri.filename)
        self.unclaim(drops)
        self.logger.debug(f"dropped {len(candidates)} files, {bytes_to_str(reclaimed)}")
        return reclaimed


    # "if I were remove [an] overserved file[s], would I have
    # room for any underserved?
    #
    # algo: step through priority_list, and for every underserved
    # (prio order) try to drop enough overserved to make it.
    # stop when out of underserved or overserved
    def pseudo_rebalance(self, priority_list):
        self.restate("rebalancing files")
        if not priority_list:
            self.logger.warn("can't rebalance: no files given")
            return
        free = self.allocation - self.probable_consumption()
        self.logger.debug(f"rebalancing; starting with {bytes_to_str(free)} to spare")
        if free < 0:               # would happen with reserved storage
            needed = -1*free
            ratio_target = 1.0     # drop any overserved
            self.logger.warn(f"underwater, need {bytes_to_str(needed)}")
            reclaimed = self.pseudo_drop_overserved(priority_list, needed,
                                                    ratio_target)
            if not reclaimed:
                self.logger.debug(":( no space reclaimed (underwater)")
                return
            self.logger.debug(f":) pseudo-reclaimed {bytes_to_str(reclaimed)}")
            free += reclaimed
            assert free == self.allocation - self.probable_consumption()
            self.logger.debug(f"pseudo-rebalancing with {bytes_to_str(free)}")
        top_served = priority_list[-1]
        least_served = priority_list[0]
        self.logger.debug(f"Least served: {least_served}, top {top_served}")
        for uri in priority_list:
            # if uri.ratio >= 1.0:
            if uri.ratio > 1.0 and uri.ratio + 2 > top_served.ratio:
                break
            if self.is_owned(uri):
                continue
            if uri.size > free:
                space_needed = uri.size - free
                if uri.ratio < 1.0:
                    ratio_target = 1.0  # if i'm looking at undersreved 
                else:
                    ratio_target = uri.ratio + 2
                self.logger.debug(f"trying to make space for {uri.filename}:{uri.ratio}, need {bytes_to_str(space_needed)}")
                reclaimed = self.pseudo_drop_overserved(priority_list, space_needed, ratio_target)
                if not reclaimed:
                    self.logger.debug(":( no space reclaimed; giving up")
                    return
                self.logger.debug(f":) pseudo-reclaimed {bytes_to_str(reclaimed)}")
                free += reclaimed
                self.logger.debug(f"now have {bytes_to_str(free)} free")
            if uri.size <= free:
                self.pseudo_copy_uri(uri)
                free -= uri.size
                self.logger.debug(f"pseudo-copied {uri.filename}; {bytes_to_str(free)} free")


    # TODO: queue this if a source isn't available
    def unclaim_all(self):
        self.logger.debug("unclaiming all")
        for source_context in self.random_source_list:
            self.send(source_context, "unclaim all")


    # TODO: queue this if a source isn't available
    def unclaim(self, unclaims):
        self.logger.debug("unclaiming a bunch")
        for source_context in unclaims:
            response = self.send(source_context, "unclaim", unclaims[source_context])


    # this tracks local state (the claim) so we don't have to queue it
    def claim_everything(self):
        for source_context in self.backups:
            claim = list(self.backups[source_context].keys())
            if claim:
                response = self.send(source_context, "claim", claim)
                if response and response.value() == "ack":
                    self.logger.debug(f"renewing claims for {source_context}")
                    self.renew_claims(source_context)
                else:
                    self.logger.warn(f"send failed, not reclaiming for {source_context}")
            

    # returns (nfiles, total_size) of backups[source_context]
    def sizeof(self, source_context):
        if source_context not in self.backups:
            return (0, 0)
        nfiles = total_size = 0
        for filename in self.backups[source_context].keys():
            nfiles += 1
            total_size += self.backups[source_context][filename]
        return (nfiles, total_size)


    # returns the name of a file full of rsync-able sources
    # rsync --files-from=FILE
    def generate_rsync_list(self, source_context):
        if source_context not in self.backups:
            self.logger.debug(f"No files for {source_context}; not generating rsync list")
            return None
        (n, size) = self.sizeof(source_context)
        size = bytes_to_str(size)
        self.logger.debug(f"rsync {source_context}: {n} files, {size}")
        rsync_filename = f"{self.path}/{source_context}.rsync.txt"
        nfiles = total_size = 0
        with open(rsync_filename, "w") as rsync_file:
            for filename in self.backups[source_context].keys():
                rsync_file.write(f"{filename}\n")
        return rsync_filename


    def rsync_from_list(self, source_context, filename):
        self.logger.debug(f"rsync {source_context}: {filename}")
        timer = elapsed.ElapsedTimer()
        (n, size) = self.sizeof(source_context)
        source = self.sources[source_context]
        src_host = config.host_for(source)
        hostname = config.host_for(self.config.get(self.context, "backup"))
        if src_host == hostname: # a local copy, just use path
            source = config.path_for(source)
        dest = f"{self.paths[source_context]}/"
        filesfrom = f"--files-from={filename}"
        # self.logger.debug(f"rsync --delete {source} {dest} --files-from={filename}")
        prefix = f"{self.context}:{source_context}"
        rsync_exit = rsync(source, dest, (filesfrom,), prefix=prefix, stfu=False)
        bps = size/timer.elapsed()
        self.logger.debug(f"rsync returned {rsync_exit}: {bytes_to_str(bps)}B/s effective")
        return rsync_exit


    def rsync_everything(self):
        for source_context in self.backups:
            rsync_list = self.generate_rsync_list(source_context)
            if rsync_list:
                self.rsync_from_list(source_context, rsync_list)


    # virtually "crawl" the inventories of servers, ~randomly
    # selecting files which need copies, until I'm full
    # 
    # actually: 
    #   if sizeof(all overserved files) > smallest(underserved files):
    #       * drop the overserved file(s)
    #       * copy the underserved file(s)
    def crawl(self):
        self.stats.reset()
        # learn about sources
        self.get_metadata()
        self.get_inventories()

        self.logger.debug("generating internal state")
        # learn about me: generate pseudo-state
        self.build_backups() # copy state from disk to pseudo-state
        self.logger.debug(f"built backups: {str(self.backups)[:140]}...")
        alloc = bytes_to_str(self.allocation)
        pc = bytes_to_str(self.probable_consumption())
        ac = bytes_to_str(self.consumption())
        self.logger.debug(f"consumption: probable: {pc}, actual: {ac}, of {alloc}") 

        self.logger.debug("pseudo-copying")
        # priority fake-copy a much as I can
        priority_list = self.generate_priority_list(self.inventory)
        self.pseudo_copy(priority_list)

        pc = bytes_to_str(self.probable_consumption())
        ac = bytes_to_str(self.consumption())
        self.logger.debug(f"post pseudo-copy: probable: {pc}, actual: {ac} of {alloc}")

        self.logger.debug("pseudo-balancing")
        # rebalance, if needed
        self.pseudo_rebalance(priority_list)
        pc = bytes_to_str(self.probable_consumption())
        ac = bytes_to_str(self.consumption())
        self.logger.debug(f"post pseudo-rebalance: probable: {pc}, actual: {ac} of {alloc}")

        self.logger.debug("executing copies & claims")
        self.restate("copying & claiming")
        # execute the copies: 
        self.claim_everything()             # claim everything
        self.rsync_everything()             # copy everything
        # self.logger.warn("CHECK IT NOW\n" * 10)
        # time.sleep(9999)
        self.run_all_scanners_once()        # scan everything
        self.build_backups() # re-copy state from disk to pseudo-state
        self.logger.debug(f"re-built backups: {str(self.backups)[:140]}...")
        self.claim_everything()             # re-claim everything
        pc = bytes_to_str(self.probable_consumption())
        ac = bytes_to_str(self.consumption())
        self.logger.debug(f"post copy: probable: {pc}, actual: {ac} of {alloc}")
        self.restate("resting")


    #    # ###### ##### #    #  ####  #####  #    #
    ##   # #        #   #    # #    # #    # #   #
    # #  # #####    #   #    # #    # #    # ####
    #  # # #        #   # ## # #    # #####  #  #
    #   ## #        #   ##  ## #    # #   #  #   #
    #    # ######   #   #    #  ####  #    # #    #


    def get_datagram(self, source_context):
        ADDRESS = config.host_for(self.sources[source_context])
        PORT = int(self.config.get("global", "PORT", "5005"))

        if source_context not in self.datagrams:
            # self.logger.debug(f"building a datagram for {source_context}")
            name = f"Datagram {self.context}"
            self.datagrams[source_context] = \
                        Datagram("Bogus", server=ADDRESS, port=PORT, 
                                    name=name, compress=True)
            self.datagrams[source_context].ping()
        return self.datagrams[source_context]


    def del_datagram(self, source_context):
        datagram = self.datagrams[source_context]
        datagram.close()
        del self.datagrams[source_context]


    def DEADping(self, source_context):
        datagram = self.get_datagram(source_context)
        self.logger.debug("doing a ping")
        return datagram.ping()


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

        commandlist = [ command, source_context, self.context ]
        for arg in args:
            commandlist.append(arg)
        datagram.set(commandlist)

        if datagram.send():
            datagram.receive()
        return datagram


    def audit(self):
        self.update_allocation()
        nfiles = 0
        for source_context in self.scanners:
            nfiles += len(self.scanners[source_context])
        consumed = bytes_to_str(self.consumption())
        allocated = bytes_to_str(self.allocation)
        self.logger.info(f"currently {self.current_state}; used {consumed} of {allocated} in {nfiles} files")
        for stat in self.stats:
            self.logger.debug(f"{stat}/s: {self.stats[stat].qps()}")
        self.show_states()
        self.logger.debug(f"Efficiency: {self.compute_efficiency()}")


    def restate(self, new_state):
        if self.current_state != new_state:
            self.states[self.current_state] += self.state_timer.elapsed()
            self.current_state = new_state
            self.state_timer.reset()
            if new_state not in self.states:
                self.states[new_state] = 0
            # self.audit()


    def show_states(self):
        total = self.state_timer.elapsed()
        for state in self.states:
            total += self.states[state]
        for state in sorted(self.states.keys(), key=lambda x: self.states[x], 
                            reverse=True):
            time_in_state = self.states[state]
            if self.current_state == state:
                time_in_state += self.state_timer.elapsed()
            pct = 100*time_in_state / total
            self.logger.debug(f"\t{state}: {pct:5.2f}% | {time_in_state:1.1f}s")


    def compute_efficiency(self):
        ncopies = 0
        for filename in self.efficiency:
            ncopies += self.efficiency[filename]
        if ncopies > 0:
            l = len(self.efficiency)
            return f"{100 * l/ncopies:3.0f}% ({l}/{ncopies})"
        else:
            return "n/a"


    def __str__(self):
        hostname = config.host_for(self.config.get(self.context, "backup"))
        consumption = bytes_to_str(self.consumption())
        allocation = bytes_to_str(self.allocation)
        return f"{hostname}: {consumption}/{allocation}"


    def run_all_scanners_once(self):
        self.restate("scanning")
        for source_context in self.scanners:
            self.scanners[source_context].scan()
            nfiles = len(self.scanners[source_context])
            self.logger.debug(f"scan complete, {nfiles} files")
    

    def run(self):
        # startup tasks
        self.run_all_scanners_once()
        self.build_backups()
        self.unclaim_all()    # just once, on startup
        self.claim_everything()
        while not self.bailing:
            timer = elapsed.ElapsedTimer()
            self.update_allocation()
            self.run_all_scanners_once()
            self.crawl()
            self.audit()

            # TODO: per-source rescan intervals?
            rescan = get_interval(self.config, "rescan", self.context)
            # randomly run 2-4x per rescan interval
            rescan = random.randrange(rescan//4, rescan//2)
            sleep_time = max(rescan - timer.elapsed(), 10)
            sleep_msg = duration_to_str(sleep_time)
            self.logger.info(f"sleeping {sleep_msg} til next rescan")
            time.sleep(sleep_time)



 #####
#     # #      # ###### #    # #####
#       #      # #      ##   #   #
#       #      # #####  # #  #   #
#       #      # #      #  # #   #
#     # #      # #      #   ##   #
 #####  ###### # ###### #    #   #   

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
            time.sleep(30) # stagger multi-client startups
        self.logger.info("Clientlets started")
        while True:
            for context, clientlet in self.clientlets.items():
                clientlet.audit()
            time.sleep(60)



#    #   ##   # #    # 
##  ##  #  #  # ##   # 
# ## # #    # # # #  # 
#    # ###### # #  # # 
#    # #    # # #   ## 
#    # #    # # #    # 


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

    c = Client(options['hostname'])
    c.start()

    try:
        # wait for them to finish (if ever)
        c.join()
    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv)
