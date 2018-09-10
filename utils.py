#!/usr/bin/env python3

import hashlib, config, re, logging


# <class '__main__.GhettoCluster'> -> GhettoCluster
def logger_str(classname):
    string = str(classname)
    return string[string.index('.')+1:string.index("'>")]


def get_interval(cfg, interval_name, contexts = []):
    interval = cfg.get("global", interval_name)
    for context in contexts:
        new_interval = cfg.get(context, interval_name)
        if new_interval:
            interval = new_interval
    if interval:
        return str_to_duration(interval)
    return 0


# 1024 -> "1.000k" 
def bytes_to_str(bytes, **kwargs):
    def DEADhelper(bytes, string, magnitude, suffix):
        if bytes > magnitude:
            string = f"{string}{bytes // magnitude}{suffix}"
            bytes = bytes % magnitude
        return (bytes, string)

    prefix = ""
    if bytes < 0:
        bytes *= -1
        prefix = "-"

    if bytes > 2**40:
        return f"{prefix}{bytes/2**40:5.3f}T"
    if bytes > 2**30:
        return f"{prefix}{bytes/2**30:5.3f}G"
    if bytes > 2**20:
        return f"{prefix}{bytes/2**20:5.3f}M"
    if bytes > 2**10:
        return f"{prefix}{bytes/2**20:5.3f}K"
    return f"{prefix}{bytes}B"
        

# "99gb" -> 99*2**30, "1.5t" -> 1.5*2**40
# suffices: t, g, m, k (trailing b is fine / ignored)
def str_to_bytes(data):
    if not data: return 0
    data = str(data).lower()
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
    if not string: return 0
    nr = 0
    total = 0
    string = str(string).lower()
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


def hash(string):
    h = hashlib.sha256()
    h.update(string.encode())
    return h.hexdigest()[-8:]





def escape_special_chars(string):
    new_string = re.sub(r"'", r"\'", string)
    new_string = re.sub(r'"', r'\"', new_string)
    new_string = re.sub(r' ', r'\ ', new_string)
    new_string = re.sub(r'\(', r'\(', new_string)
    new_string = re.sub(r'\)', r'\)', new_string)
    new_string = re.sub(r'&', r'\&', new_string)
    return new_string


def looks_remote(string):
    return re.match(r'^[^/]+:', string)



# returns a Unix exit code: 0 == good, !0 == bad
# TODO: move options into kwargs
def rsync(source, dest, options = [], **kwargs):
    cfg = config.Config.instance()
    verbose = cfg.get("global", "verbose", False)
    dryrun = cfg.get("global", "dryrun", False)
    if 'prefix' in kwargs:
        logger_str = kwargs['prefix'] + " rsync"
    else:
        logger_str = "rsync"

    RSYNC = "rsync"
	# rsync is silly about escaping spaces -- remote locations ONLY
    if looks_remote(source):
         source = escape_special_chars(source)
    if looks_remote(dest):
        dest = escape_special_chars(dest)
    # print(source, dest)
    RSYNC_TIMEOUT = str(cfg.get("global", "RSYNC TIMEOUT", 180))
    RSYNC_BWLIMIT = str(cfg.get("global", "RSYNC BWLIMIT", 0))
    command = [ RSYNC, "-a", "--inplace", "--partial", \
                "--timeout", RSYNC_TIMEOUT, source, dest ]
    if len(options) > 0:
        command += options
    # if len(ignorals) > 0:
    #     command += [ f"--exclude={item}" for item in ignorals ] 
    if verbose:
        command += ["-v", "--progress"]
    if RSYNC_BWLIMIT != "0":
        command += ["--bwlimit", RSYNC_BWLIMIT]
    logger = logging.getLogger(logger_str)
    if "stfu" in kwargs and kwargs["stfu"]:
        logger.setLevel(logging.INFO)
    # logger.debug(command)
    logger.debug(f"executing: {' '.join(command)}")
    if dryrun:
        logger.info("> " + " ".join(command))
        return 0
    else:
        # https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging#comment33261012_21953835
        from subprocess import Popen, PIPE, STDOUT

        if "stfu" in kwargs and kwargs["stfu"]:
            loghole = logger.debug
        else:
            loghole = logger.info

        command = [ s.encode() for s in command ]

        process = Popen(command, stdout=PIPE, stderr=STDOUT)
        try:
            with process.stdout:
                for line in iter(process.stdout.readline, b''):
                    # b'\n'-separated lines
                    loghole("> %s", line.decode().strip())
                exitcode = process.wait()
            return exitcode
        except BaseException:
            process.terminate()
            logger.exception("Caught ...something")
            sys.exit()
        return None


if __name__ == "__main__":
    fs = FileState("file_state.py")
    print(fs)
