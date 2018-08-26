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
        BLOCKSIZE = int(str_to_bytes(cfg.get("global", 
                                            "BLOCKSIZE", "1MB")))
        NBLOCKS = int(cfg.get("global", "NBLOCKS", 0))
        if self.prefix is not None:
            filename = f"{self.prefix}/{self.data['filename']}"
        else:
            filename = self.data['filename']
        
        if genChecksums:
            self.data['checksum'] = \
                sum_sha256(filename, BLOCKSIZE, NBLOCKS)
        else:
            self.data['checksum'] = 'deferred'
        self.data['checksum_time'] = time.time()
        try:
            filestat = os.lstat(filename)
            self.data['size'] = filestat.st_size
            self.data['ctime'] = filestat.st_ctime
            self.data['mtime'] = filestat.st_mtime
        except FileNotFoundError:
            self.logger.exception("??")
            self.data['checksum'] = 'n/a'
            self.data['size'] = 0
            self.data['ctime'] = 0
            self.data['mtime'] = 0


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
def sum_sha256(fname, BLOCKSIZE = 2**20, NBLOCKS = 0):
    if not os.path.isfile(fname):
        return None
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
                f.seek(step, 1)
                count += 1
    return hash_sha256.hexdigest()


def escape_special_chars(string):
    new_string = re.sub(r"'", r"\'", string)
    new_string = re.sub(r'"', r'\"', new_string)
    new_string = re.sub(r' ', r'\ ', new_string)
    new_string = re.sub(r'(', r'\(', new_string)
    new_string = re.sub(r')', r'\)', new_string)
    return new_string


# returns an exit code: 0 == good, !0 == bad
def rsync(source, dest, options = [], **kwargs):
    cfg = config.Config.instance()
    verbose = cfg.get("global", "verbose", False)
    dryrun = cfg.get("global", "dryrun", False)

    RSYNC = "rsync"
	# rsync is silly about escaping spaces -- remote locations ONLY
    if ":" in source:
        source = escape_special_chars(source)
    if ":" in dest:
        dest = escape_special_chars(dest)
    print(source, dest)
    RSYNC_TIMEOUT = str(cfg.get("global", "RSYNC TIMEOUT", 180))
    command = [ RSYNC, "-a", "--inplace", "--partial", \
                "--timeout", RSYNC_TIMEOUT, source, dest ]
    if len(options) > 0:
        command += options
    # if len(ignorals) > 0:
    #     command += [ f"--exclude={item}" for item in ignorals ] 
    if True or verbose:
        command += ["-v", "--progress"]
    logger = logging.getLogger("rsync")
    logger.debug(command)
    if dryrun:
        logger.info("> " + " ".join(command))
        return 0
    else:
        # subprocess.call(command)
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
