#! python3.x

import platform, os, re, logging, sys
import utils
from singleton import Singleton

"""
Usage:
  cfg = config.Config.instance()
   ... ONCE on startup: cfg.init(filename)
  periodically:
    cfg.load()

A Config reads from a text file, builds a special dict,
and can load itself from a (defined) "master" config file

Configuration lines are key: value pairs.  
Lines with primary keys (one or more must be defined in init())
will generate a unique "context", which can be used to retrieve
following key:value pairs.  Non-primary key:value pairs found
before the first primary key are of "global" context

non-primary keys can be comma-separated lists, or when multiple
are found, a list will be created:
  key: value
  key: value2, value3
becomes { key: [ value, value2, value3 ] }

Structure:
    self.data[context]{ key: value, key: value ... }

"""


@Singleton
class Config:
    def __init__(self):
        self.data = {}
        self.config = {}
        self.logger = logging.getLogger(utils.logger_str(__class__))
        self.logger.setLevel(logging.INFO)


    def init(self, filename, *primary_keys, **kwargs):
        # print("Config initializaing (ONCE?!???!!!?)")
        self.filename = filename
        self.primary_keys = primary_keys
        if "hostname" in kwargs:
            self.hostname = kwargs["hostname"]
        else:
            self.hostname = platform.node()
        if "testing" in kwargs:
            self.testing = kwargs["testing"]
        else:
            self.testing = False

        self.master_config = None
        # prime the config (can't pull yet)
        self.read_config()
        # load for realz
        self.load()


    def load(self):
        self.pull_master_config()
        self.read_config()


    # if testing, just copy the thing (rsync w/o hostnames)
    #   otherwise, rsync to the config filename
    def pull_master_config(self):
        if self.master_config is None:
            return
            self.logger.error(f"ERROR: {self.filename} MUST contain a " \
                             f"\"master config:\" line")
            sys.exit(1)
        host = host_for(self.master_config)
        if host == self.hostname:
            self.logger.debug("I am master, not pulling the config")
            return
        self.logger.debug(f"pulling config {self.master_config}" \
                         f" -> {self.filename}")
        if self.testing:
            self.logger.debug(self.master_config)
            master_config = path_for(self.master_config)
        else:
            master_config = self.master_config
        file_state.rsync(master_config, self.filename, stfu=True)


    def read_config(self):
        self.logger.debug("Loading the config")
        primary_key = None
        context = "global"
        self.logger.debug(f"hunting for {self.primary_keys}")
        try:
            with open(self.filename, "r") as file:
                for line in file:
                    # print(f"> {line.strip()}")
                    self.logger.debug(f"> {line.strip()}")
                    line = line.split("#", 1)[0].strip()
                    tokens = line.split(": ", 1)
                    if tokens[0] == "master config" and context == 0:
                        self.master_config = tokens[1]
                    elif tokens[0] in self.primary_keys:
                        self.logger.debug(f"got one: {tokens[0]} :: {tokens[1]}")
                        # context += 1
                        context = utils.hash(tokens[1])
                        self.data[context] = {}
                        self.data[context][tokens[0]] = tokens[1]
                        # print(self.data)
                    elif len(tokens) == 2:
                        # not a "master" or "slave", must be a config option
                        self.set(context, tokens[0], tokens[1])
            self.logger.debug(self.data)
        except BaseException:
            self.logger.exception("Fatal error reading config")
            self.logger.error(f"Confirm {self.filename} is readable")
            sys.exit(1)
        self.process_global_options()


    def process_global_options(self):
        options = self.get("global", "options")
        if options is None:
            return
        if type(options) is str:
            self.process_option("global", options)
        else:
            for option in options:
                self.process_option("global", option)


    # "LAZY WRITE: 10" -> key, value
    def process_option(self, context, option):
        self.logger.debug(f"parsing {option}")
        if ": " not in option:
            self.logger.error("Can't parse {option} -- invalid format")
            self.logger.error("format is 'key: value'")
            raise SyntaxError
        tokens = option.split(": ")
        self.set(context, tokens[0], tokens[1])
        self.logger.debug(f"option {context}:{tokens[0]} => {tokens[1]}")


    def get_dirs(self, hostname = None):
        masters = []
        slaves = {}
        if hostname == None:
            hostname = self.hostname
        for key, values in self.data.items():
            if key.startswith(hostname):
                masters.append(key)
            else:
                for value in values:
                    if value.startswith(hostname):
                        slaves[value] = key
        return masters, slaves


    def set(self, context, key, value):
        # print(f"setting {key} => {value}")
        if context not in self.data:
            self.data[context] = {}
        if ", " in value:
            self.data[context][key] = value.split(", ")
        else:
            self.data[context][key] = value

            
    def get(self, context, key, default=None):
        if context in self.data:
            if key in self.data[context]:
                return self.data[context][key]
        if "global" in self.data and key in self.data["global"]:
            return self.data["global"][key]
        return default



    def get_contexts_for_key(self, key):
        result = {}
        for context, datum in self.data.items():
            if key in self.data[context]:
                result[context] = self.data[context][key]
        return result

        result = [ context for context in self.data \
            if key in self.data[context] ]
        self.logger.debug(self.data)
        self.logger.debug(result)
        return result


    # get all the contexts for a named key where
    # the keyed value has a prefix
    # contexts[context] = { key: value }
    #   returns { context: value } where value.startwith("target")
    # dict comprehension might work here BUT it's harder to debug 
    # and wouldn't be much shorter
    def get_contexts_for_key_and_target(self, key, target):
        result = {}
        contexts = self.get_contexts_for_key(key)
        for context in contexts:
            value = self.data[context][key]
            if value.startswith(target):
                result[context] = value
        return result



def host_for(host_path):
    return host_path.split(":")[0]

def path_for(host_path):
    return os.path.expanduser(host_path.split(":")[1])
