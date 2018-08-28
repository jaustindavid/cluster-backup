#!/usr/bin/env python3

import hashlib


# <class '__main__.GhettoCluster'> -> GhettoCluster
def logger_str(classname):
    string = str(classname)
    return string[string.index('.')+1:string.index("'>")]


# 1024 -> "1.000k" 
def bytes_to_str(bytes, **kwargs):
    def helper(bytes, string, magnitude, suffix):
        if bytes > magnitude:
            string = f"{string}{bytes // magnitude}{suffix}"
            bytes = bytes % magnitude
        return (bytes, string)

    string = ""

    if bytes > 2**40:
        return f"{bytes/2**40:5.3f}T"
    if bytes > 2**30:
        return f"{bytes/2**30:5.3f}G"
    if bytes > 2**20:
        return f"{bytes/2**20:5.3f}M"
    if bytes > 2**10:
        return f"{bytes/2**20:5.3f}K"
    return f"{bytes}B"
        

# "99gb" -> 99*2**30, "1.5t" -> 1.5*2**40
# suffices: t, g, m, k (trailing b is fine / ignored)
def str_to_bytes(data):

    data = data.lower()
    i = 0
    while i < len(data) and (data[i].isdigit() or data[i] == "."): 
        i += 1
    if not i: 
        return 0
    nr = float(data[:i])
    if len(data) > i:
        if data[i] is "k":
            return int(nr * 2**10)
        if data[i] is "m":
            return int(nr * 2**20)
        if data[i] is "g":
            return int(nr * 2**30)
        if data[i] is "t":
            return int(nr * 2**40)
        # FALLTHROUGH
    return int(nr)
        

# 64 -> "1m4s"
def duration_to_str(seconds):
    string = ""
    if seconds > 24*60*60:
        days = int(seconds / (24*60*60))
        seconds = seconds % (24*60*60)
        string += f"{days}d"
    if seconds > 60*60:
        hours = int(seconds / (60*60))
        seconds = seconds % (60*60)
        string += f"{hours}h"
    if seconds > 60:
        minutes = int(seconds / 60)
        seconds = seconds % 60
        string += f"{minutes}m"
    if seconds > 0:
        string += f"{int(seconds)}s"
    return string


# "6h1s" -> 21601
# method is really simplistic: ##d -> ## * (24 hrs); ##s -> ##
#   ... iteratively walk the string, scaling ## by d/h/m/s
# side effect: other characters are ignored, 1z6 -> 16 (seconds)
def str_to_duration(string):
    nr = 0
    total = 0
    string = string.lower()
    while string is not "":
        if string[0].isdigit():
            nr = nr*10 + int(string[0])
        elif string[0] == "d":
            total += 24*3600 * nr
            nr = 0
        elif string[0] == "h":
            total += 3600 * nr
            nr = 0
        elif string[0] == "m":
            total += 60 * nr
            nr = 0
        elif string[0] == "s":
            total += nr
            nr = 0
        string = string[1:]
    total += nr
    return total



def NOPErsync(source, dest, options = [], **kwargs):
    cfg = config.Config.instance()
    dryrun = cfg.getConfig("global", "dryrun", False)

    RSYNC = "rsync"
	# rsync is silly about escaping spaces -- remote locations ONLY
    if ":" in source: 
        source = re.sub(r' ', '\ ', source)
    if ":" in dest:
        dest = re.sub(r' ', '\ ', dest)
    RSYNC_TIMEOUT = str(cfg.getConfig("global", "RSYNC TIMEOUT", 180))
    command = [ RSYNC, "-av", "--inplace", "--partial", \
                "--timeout", RSYNC_TIMEOUT, "--progress", \
                source, dest ]
    if len(options) > 0:
        command += options
    if "verbose" in kwargs and kwargs["verbose"]:
        command += ["-v", "--progress"]
    logger = logging.getLogger("rsync")
    logger.debug(command)

    if dryrun:
        logger.info("> " + " ".join(command))
    else:
        # https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging#comment33261012_21953835
        from subprocess import Popen, PIPE, STDOUT

        if "stfu" in kwargs and kwargs["stfu"]:
            loghole = logger.debug
        else:
            loghole = logger.info

        try:
            process = Popen(command, stdout=PIPE, stderr=STDOUT)
            with process.stdout:
                for line in iter(process.stdout.readline, b''):
                    # returns bytestrings, decode those
                    loghole("> %s", line.decode().strip())
                exitcode = process.wait()
        except BaseException:
            process.terminate()
            logger.exception("Caught ...something")
            sys.exit()
        return exitcode


def hash(string):
    h = hashlib.sha256()
    h.update(string.encode())
    return h.hexdigest()[-8:]
