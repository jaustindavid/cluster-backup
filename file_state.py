#!/usr/bin/env python3.6

import logging, os, json, time, hashlib, random, subprocess, re
import config
from utils import logger_str, str_to_bytes


# { 'name' : filename, 
#   'size': int, 
#   'checksum' : sha256, 
#   'checksum_time' : time_t,
#   'ctime' : time_t,
#   'mtime' : time_t }
class FileState:
    def __init__(self, filename, genChecksums = True, **kwargs):
        self.data = {'filename' : filename}
        if "prefix" in kwargs:
            self.prefix = kwargs["prefix"]
        else:
            self.prefix = None
        self.logger = logging.getLogger(logger_str(__class__) + " " + \
                                os.path.basename(filename))
        self.update(genChecksums)


    def update(self, genChecksums = True):
        cfg = config.Config.instance()
        BLOCKSIZE = str_to_bytes(cfg.get("global", "BLOCKSIZE", "1MB"))
        NBLOCKS = int(cfg.get("global", "NBLOCKS", 0))
        IO_RATELIMIT = str_to_bytes(cfg.get("global", "IO_RATELIMIT", "0"))
        if self.prefix is not None:
            filename = f"{self.prefix}/{self.data['filename']}"
        else:
            filename = self.data['filename']
        
        if genChecksums:
            self.data['checksum'] = \
                sum_sha256(filename, BLOCKSIZE, NBLOCKS, IO_RATELIMIT)
        else:
            self.data['checksum'] = 'deferred'
        self.data['checksum_time'] = time.time()
        filestat = os.lstat(filename)
        self.data['size'] = filestat.st_size
        self.data['ctime'] = filestat.st_ctime
        self.data['mtime'] = filestat.st_mtime


    def from_dict(self, data):
        self.data = data


    def to_dict(self):
        return self.data


    def maybechanged(self, filestate_data):
        return self.data['ctime'] != filestate_data['ctime'] \
            or self.data['mtime'] != filestate_data['mtime'] \
            or self.data['size'] != filestate_data['size']
            

    def changed(self, filestate_data):
        if self.data['checksum'] == filestate_data['checksum']:
            return False
        return True


    def __str__(self):
        return str(self.data)
        return f"{self.filename}[{self.size}b]: "\
               f"{self.checksum[0-2]}..{self.checksum[-2:]}"\
               f"@{self.checksum_time} c:{self.ctime} m:{self.mtime}"



# https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
# https://gist.github.com/aunyks/042c2798383f016939c40aa1be4f4aaf
#
# NBLOCKS & BLOCKSIZE > 0: sampling
#  randomly (seeded on filesize, so consistent) sample
#  NBLOCKS of BLOCKSIZE in the file.  This should make
#  for ~ constant time hashing of very large files
#  sent NBLOCKS or BLOCKSIZE to 0 to disable
#
# For tuning to an FS, set NBLOCKS to 0 (no sampling) and
#  BLOCKSIZE to an integer multiple of the FS chunk size
def sum_sha256(fname, BLOCKSIZE = 2**20, NBLOCKS = 0, IO_RATELIMIT = 0):
    if not os.path.isfile(fname):
        return None

    # print(f"{NBLOCKS} blocks @ {BLOCKSIZE}, limit {IO_RATELIMIT}")
    def figure_ratelimiter(IO_RATELIMIT, BLOCKSIZE):
        if not IO_RATELIMIT:
            return 0
        # N mbps -> 1/(<blocksize>*ratelimit)
        # assumes IO is instantaneous
        delay = BLOCKSIZE/IO_RATELIMIT
        # print(f"{BLOCKSIZE} / {IO_RATELIMIT} == {delay}")
        return delay

    ratelimit_time = figure_ratelimiter(IO_RATELIMIT, BLOCKSIZE)
    hash_sha256 = hashlib.sha256()
    filestat = os.lstat(fname)
    with open(fname, "rb") as f:
        # for chunk in iter(lambda: f.read(BLOCKSIZE), b""):
        #    hash_sha256.update(chunk)
        if NBLOCKS*BLOCKSIZE == 0 or filestat.st_size < NBLOCKS*BLOCKSIZE:
            # "small" files, 10MB or less
            if BLOCKSIZE == 0:
                BLOCKSIZE = 2**20 # 1MB
            file_buffer = f.read(BLOCKSIZE)
            while len(file_buffer) > 0:
                hash_sha256.update(file_buffer)
                file_buffer = f.read(BLOCKSIZE)
                if ratelimit_time: time.sleep(ratelimit_time)
        else:
            # "large" files > 10MB; randomly sample (up to) 10 blocks
            file_buffer = f.read(BLOCKSIZE)
            random.seed(filestat.st_size)
            count = 0
            step = int(filestat.st_size/NBLOCKS)
            jump = random.randrange(step)
            f.seek(jump)
            # print(f"size: {filestat.st_size} step: {step} jump: {jump}")
            while len(file_buffer) > 0: # and count < NBLOCKS:
                # print(f"So far @ {count}:{f.tell()}: {hash_sha256.hexdigest()}")
                hash_sha256.update(file_buffer)
                file_buffer = f.read(BLOCKSIZE)
                if ratelimit_time: time.sleep(ratelimit_time)
                f.seek(step, 1)
                count += 1
    return hash_sha256.hexdigest()


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


"""
2018-08-30 17:20:49,964 [Clientlet 16bef420] retrieving 64b241a6:animated/Pixar
Short Films Collection \u2013 Volume 1.m4v
2018-08-30 17:20:49,964 [Clientlet 16bef420] retrieving 64b241a6:animated/Pixar
Short Films Collection \u2013 Volume 1.m4v to /mnt/data/austin/cluster-backups/6
4b241a6/animated/Pixar Short Films Collection \u2013 Volume 1.m4v
2018-08-30 17:20:49,965 [Clientlet 16bef420] rsync mini:/Volumes/Media_ZFS/Movie
s/animated/Pixar Short Films Collection \u2013 Volume 1.m4v /mnt/data/austin/clu
ster-backups/64b241a6/animated/Pixar Short Films Collection \u2013 Volume 1.m4v
2018-08-30 17:20:49,968 [rsync] ['rsync', '-a', '--inplace', '--partial', '--tim
eout', '180', 'mini:/Volumes/Media_ZFS/Movies/animated/Pixar\\ Short\\ Films\\ C
ollection\\ \u2013\\ Volume\\ 1.m4v', '/mnt/data/austin/cluster-backups/64b241a6
/animated/Pixar Short Films Collection \u2013 Volume 1.m4v', '-v', '--progress']
Exception in thread Thread-1:
Traceback (most recent call last):
  File "/home/austin/src/cluster-backup/file_state.py", line 198, in rsync
    process = Popen(command, stdout=PIPE, stderr=STDOUT)
  File "/usr/lib/python3.6/subprocess.py", line 709, in __init__
    restore_signals, start_new_session)
  File "/usr/lib/python3.6/subprocess.py", line 1275, in _execute_child
    restore_signals, start_new_session, preexec_fn)
UnicodeEncodeError: 'ascii' codec can't encode character '\u2013' in position 73
: ordinal not in range(128)
"""


# returns a Unix exit code: 0 == good, !0 == bad
# TODO: move options into kwargs
def rsync(source, dest, options = [], **kwargs):
    cfg = config.Config.instance()
    verbose = cfg.get("global", "verbose", False)
    dryrun = cfg.get("global", "dryrun", False)

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
    if True or verbose:
        command += ["-v", "--progress"]
    if RSYNC_BWLIMIT != "0":
        command += ["--bwlimit", RSYNC_BWLIMIT]
    logger = logging.getLogger("rsync")
    logger.debug(command)
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
