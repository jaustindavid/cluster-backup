#!/usr/bin/env python3

import logging, json

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

TODO:
    - Communique.data is whatever
    - Communique.build takes serialized json data and deserializes
    - Communique.__str__ returns serialized json
    - Communique.__getitem__ exposes data
"""
class Communique:
    def __init__(self, *contents, **kwargs):
        self.truthiness = None
        if "bool" in kwargs:
            self.truthiness = kwargs["bool"]
        self.negatives = None
        if "negatives" in kwargs:
            self.negatives = kwargs["negatives"]

        if len(contents) == 1:
            # print(f"Unpacking {contents}")
            self.contents = contents[0]
        else:
            # print(f"NOT unpacking {contents}")
            self.contents = contents


    # this is terrible, DEAD 
    ''' append 0 or more args to my contents '''
    def append(self, *args):
        # print(f"len({args}) = {len(args)}")
        if len(args) == 1 and not args[0]:
            return
        if not self.contents:
            self.contents = list(args)
        elif type(self.contents) is str:
            self.contents = [ self.contents ] + list(args)
        else:
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
        if self.truthiness is not None:
            return self.truthiness
        if type(self.contents) is str:
            if self.negatives and self.contents in self.negatives:
                return False
            return bool(self.contents and self.contents != "")
        return bool(self.contents)
        

    def __len__(self):
        if type(self.contents) is str:
            return 1
        return len(self.contents)


    def __eq__(self, item):
        if type(item) is __class__:
            return str(self) == str(item)
        return self.contents == item


    def __iter__(self):
        if len(self) == 1:
            return iter([ str(self.contents) ])
        else:
            return iter(self.contents)


    # a.k.a. "serialize"
    def __str__(self):
        return json.dumps(self.contents)
        if type(self.contents) is list or type(self.contents) is tuple:
            return self.special.join(map(str, self.contents))
        return str(self.contents)


    # a.k.a. "deserialize"
    @staticmethod
    def build(data, **kwargs):
        # print(f"building {type(data)}: >{data}<")
        if data is None:
            return Communique(None, **kwargs)
        # print(f"json says {json.loads(data)}")
        try:
            c = Communique(json.loads(data), **kwargs)
        except json.decoder.JSONDecodeError:
            logger.exception("Error decoding!")
            return Communique(None)
        # print(f"I survived with {c}")
        return c


if __name__ == "__main__":
    l = list(flatten("a", "list", ["of", "things"]))
    print(l)
