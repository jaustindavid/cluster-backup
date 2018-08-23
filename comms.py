#!/usr/bin/env python3

import logging, itertools

"""
This should evaluate to True or False

it can be packed up and sent (serialized to str)
or received and unpacked (deserialized from str)

it can be a list or (magically) a str or int.
  Communique("1", "2")[0] == 1
  Communique("123")[0] == 123
  Communique("strings", "are", "weird")[0] == "strings"

it should not act like the silly list/string thing
  Communique("string")[0] --> "string"

spaced double-ampersands are special, by default.  Or kwargs
special to something else: special=" @@ "
"""
class Communique:
    def __init__(self, *contents, **kwargs):
        self.special = " @@ "
        self.negatives = None
        self.truthiness = None
        if "special" in kwargs:
            self.special = kwargs["special"]
        if "negatives" in kwargs:
            self.negatives = kwargs["negatives"]
        if "truthiness" in kwargs:
            self.truthiness = kwargs["truthiness"]

        # print(f"I am {contents}")
        if len(contents) > 1:
            self.contents = contents
        elif len(contents) == 1:
            # print(f"I am {contents[0]}: len={len(contents[0])}")
            # self.contents = self.unroll(contents)
            self.contents = contents[0]
        else:
            self.contents = None


    ''' append 0 or more args to my contents '''
    def append(self, *args):
        args = list(itertools.chain.from_iterable(args))
        if len(args) > 0:
            if type(self.contents) is not list:
                self.contents = list(self.contents)
            self.contents += args


    def __getitem__(self, key):
        if type(self.contents) is str and key == 0:
            value = self.contents
        else:
            value = self.contents[key]
        # print(f"{key}: {value}")
        if type(value) is str and value.isdigit():
            return int(value)
        return value


    def __bool__(self):
        # print(f"testing: {type(self.contents)}: {self.contents}")
        if self.truthiness is not None:
            return self.truthiness
        if not self.contents:
            return False
        if type(self.contents) is str:
            if self.negatives and self.contents in self.negatives:
                return False
            return self.contents and self.contents != ""
        if type(self.contents) is list or type(self.contents) is tuple:
            # print(f"testing a list: {self.contents}")
            return bool(self.contents[0])
        return True
        

    def __str__(self):
        if type(self.contents) is list or type(self.contents) is tuple:
            return self.special.join(map(str, self.contents))
        return str(self.contents)


    def __len__(self):
        if type(self.contents) is str:
            return 1
        return len(self.contents)


    @staticmethod
    def build(string, **kwargs):
        if "special" in kwargs:
            special = kwargs["special"]
        else:
            special = " @@ "
        if special in string:
            # print(f"splitting {string} with {special}")
            return Communique(string.split(special))
        return Communique(string, **kwargs)
