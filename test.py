#!/usr/bin/python

__author__="raltnoeder"
__date__ ="$Sep 12, 2013 11:24:57 AM$"

from drbdmanage.exceptions import *
from drbdmanage.server import *
from drbdmanage.drbd.drbdcore import *
from drbdmanage.storage.storagecore import *
import sys
import string

def main():
    inst = Test()
    inst.run()

class Test(object):

    _argv    = None
    _arg_idx = None
    _arg_max = None
    _tokens  = None
    
    _server  = None

    def __init__(self):
        self._argv    = sys.argv
        self._arg_max = len(self._argv)
        self._arg_idx = 1
        self._server = DrbdManageServer()
    
    def run(self):
        while True:
            sys.stdout.write("> ")
            sys.stdout.flush()
            cmdline = sys.stdin.readline()
            if len(cmdline) < 1:
                return
            if cmdline.endswith("\n"):
                cmdline = cmdline[:len(cmdline) - 1]
            self.set_tokens(cmdline)
            cmd = self.next_token()
            if cmd is None:
                continue
            if cmd == "exit":
                return
            if cmd == "vol":
                self.volume()
            elif cmd == "size":
                self.show_size()
            else:
                sys.stderr.write("what?\n")
    
    def volume(self):
        name = self.next_token()
        size_str = self.next_token()
        if name is None or size_str is None:
            sys.stderr.write("vol <name> <megabytes>\n")
            return
        try:
            size = int(size_str)
        except Exception:
            sys.stderr.write("<megabytes> must be a numeric value\n")
            return
        rc = self._server.create_volume(name, size)
        if rc != DM_SUCCESS:
            sys.stderr.write(dm_exc_text(rc) + "\n")
    
    def show_size(self):
        name = self.next_token()
        if name is None:
            sys.stderr.write("size <name>\n")
            return
        volume = self._server.get_volume(name)
        if volume is None:
            sys.stderr.write("No volume named '" + name + "'\n")
            return
        # bytes
        size = volume.get_size(volume.UNIT_B)
        sys.stdout.write(string.ljust("Bytes:", 8) \
          + string.rjust(str(size), 28) + "\n")
        # binary kilobytes
        size = volume.get_size(volume.UNIT_KiB)
        sys.stdout.write(string.ljust("kiB:", 8) \
          + string.rjust(str(size), 28) + "\n")
        # binary megabytes
        size = volume.get_size(volume.UNIT_MiB)
        sys.stdout.write(string.ljust("MiB:", 8) \
          + string.rjust(str(size), 28) + "\n")
        # binary gigabytes
        size = volume.get_size(volume.UNIT_GiB)
        sys.stdout.write(string.ljust("GiB:", 8) \
          + string.rjust(str(size), 28) + "\n")
        # binary terrabytes
        size = volume.get_size(volume.UNIT_TiB)
        sys.stdout.write(string.ljust("TiB:", 8) \
          + string.rjust(str(size), 28) + "\n")
        
        # decimal kilobytes
        size = volume.get_size(volume.UNIT_KB)
        sys.stdout.write(string.ljust("kB:", 8) \
          + string.rjust(str(size), 28) + "\n")
        # decimal megabytes
        size = volume.get_size(volume.UNIT_MB)
        sys.stdout.write(string.ljust("MB:", 8) \
          + string.rjust(str(size), 28) + "\n")
        # decimal gigabytes
        size = volume.get_size(volume.UNIT_GB)
        sys.stdout.write(string.ljust("GB:", 8) \
          + string.rjust(str(size), 28) + "\n")
        # decimal terrabytes
        size = volume.get_size(volume.UNIT_TB)
        sys.stdout.write(string.ljust("TB:", 8) \
          + string.rjust(str(size), 28) + "\n")
    
    def next_arg(self):
        cur_idx = self._arg_idx
        if self._arg_idx < self._arg_max:
            self._arg_idx += 1
            return self._argv[cur_idx]
        return None
    
    def set_tokens(self, tokens):
        self._tokens  = tokens
    
    def next_token(self):
        if self._tokens is not None:
            idx = self._tokens.find(" ")
            if idx != -1:
                cur_token = self._tokens[:idx]
                self._tokens = self._tokens[idx + 1:]
                return cur_token
            else:
                if len(self._tokens) > 0:
                    cur_token = self._tokens
                    self._tokens = None
                    return cur_token
        return None

if __name__ == "__main__":
    main()
