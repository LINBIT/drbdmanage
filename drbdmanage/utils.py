#!/usr/bin/python

__author__="raltnoeder"
__date__ ="$Sep 16, 2013 1:40:12 PM$"

import sys
import hashlib

COLOR_BLACK     = chr(0x1b) + "[0;30m"
COLOR_DARKRED   = chr(0x1b) + "[0;31m"
COLOR_DARKGREEN = chr(0x1b) + "[0;32m"
COLOR_BROWN     = chr(0x1b) + "[0;33m"
COLOR_DARKBLUE  = chr(0x1b) + "[0;34m"
COLOR_DARKPINK  = chr(0x1b) + "[0;35m"
COLOR_TEAL      = chr(0x1b) + "[0;36m"
COLOR_GRAY      = chr(0x1b) + "[0;37m"
COLOR_DARKGRAY  = chr(0x1b) + "[1;30m"
COLOR_RED       = chr(0x1b) + "[1;31m"
COLOR_GREEN     = chr(0x1b) + "[1;32m"
COLOR_YELLOW    = chr(0x1b) + "[1;33m"
COLOR_BLUE      = chr(0x1b) + "[1;34m"
COLOR_PINK      = chr(0x1b) + "[1;35m"
COLOR_TURQUOIS  = chr(0x1b) + "[1;36m"
COLOR_WHITE     = chr(0x1b) + "[1;37m"
COLOR_NONE      = chr(0x1b) + "[0m"

def long_to_bin(number):
    num64 = int(number) & 0xffffffffffffffff
    if num64 != number:
        raise OverflowError
    field = bytearray('0' * 8)
    idx = 0
    while idx < 8:
        field[idx] = chr((num64 >> (7 - idx) * 8) & 0xff)
        idx += 1
    return field

def long_from_bin(field):
    num64 = 0
    if len(field) != 8:
        raise ValueError
    idx = 0
    while idx < 8:
        num64 |= (ord(field[idx]) << ((7 - idx) * 8))
        idx += 1
    return num64


class DataHash(object):
    HASH_LEN = 32 # SHA-256
    _hash = None
    def __init__(self):
        self._hash = hashlib.sha256()
    
    def update(self, data):
        self._hash.update(data)
    
    def get_hash(self):
        return self._hash.digest()
    
    def get_hex_hash(self):
        return self._hash.hexdigest()
    
    def get_hash_len(self):
        return self.HASH_LEN
    
    def get_hex_hash_len(self):
        return self.HASH_LEN * 2

class ArgvReader(object):
    _argv = None
    _idx  = None
    _max  = None
    
    def __init__(self, argv):
        self._argv = argv
        self._idx  = 1
        self._max  = len(argv)
    
    def next_arg(self):
        cur_idx = self._idx
        arg = None
        while self._idx < self._max:
            self._idx += 1
            if self._argv[cur_idx] != "":
                arg = self._argv[cur_idx]
                break
        return arg
    
    def peek_arg(self):
        arg = None
        if self._idx < self._max:
            arg = self._argv[self._idx]
        return arg
    
    def next(self):
        if self._idx < self._max:
            self._idx += 1
    
    def reset(self):
        self._idx = 1


class CmdLineReader(object):
    _cmdline = None
    
    def __init__(self, cmdline):
        self._cmdline = cmdline
    
    def next_arg(self):
        arg = None
        if self._cmdline is not None:
            self._remove_space()
            idx = self._cmdline.find(" ")
            if idx != -1:
                arg = self._cmdline[:idx]
                self._cmdline = self._cmdline[idx + 1:]
            else:
                if len(self._cmdline) > 0:
                    arg = self._cmdline
                self._cmdline = None
        return arg
    
    def next(self):
        if self._cmdline is not None:
            self._remove_space()
            idx = self._cmdline.find(" ")
            if idx != -1:
                self._cmdline = self._cmdline[idx + 1:]
            else:
                self._cmdline = None
    
    def peek_arg(self):
        arg = None
        if self._cmdline is not None:
            self._remove_space()
            idx = self._cmdline.find(" ")
            if idx != -1:
                arg = self._cmdline[:idx]
            else:
                arg = self._cmdline
        return arg
    
    def _remove_space(self):
        cmdline_b = bytearray(self._cmdline)
        for idx in xrange(0, len(cmdline_b)):
            if cmdline_b[idx] != 0x20 and cmdline_b[idx] != 0x9:
                cmdline_b = cmdline_b[idx:]
                self._cmdline = str(cmdline_b)
                break


class CommandParser(object):
    def __init__(self):
        pass
    
    def parse(self, args, order, params, opt, optalias, flags, flagsalias):
        rc = 0
        olen = len(order)
        for ctr in xrange(0, olen):
            params[order[ctr]] = None
        ctr = 0
        arg = args.next_arg()
        while arg is not None:
            if arg.startswith("-"):
                key = self._get_key(arg, opt, optalias)
                if key is not None:
                    val = args.next_arg()
                    if val is None:
                        sys.stderr.write("Error: Missing argument for option" \
                          + " '" + arg + "'\n")
                        rc = 1
                        break
                    else:
                        opt[key] = val
                else:
                    key = self._get_key(arg, flags, flagsalias)
                    if key is not None:
                        flags[key] = True
                    else:
                        sys.stderr.write("Error: Unknown option name '" \
                          + arg + "'\n")
                        rc = 1
                        break
            else:
                if ctr < olen:
                    params[order[ctr]] = arg
                    ctr += 1
                else:
                    sys.stderr.write("Error: Unexpected extra argument '" \
                      + arg+ "'\n")
                    rc = 1
                    break
            arg = args.next_arg()
        if ctr < olen:
            sys.stderr.write("Error: Incomplete command line\n")
            rc = 1
        return rc
    
    def _get_key(self, in_key, tbl, tblalias):
        out_key = None
        if in_key in tbl:
            out_key = in_key
        elif in_key in tblalias:
            out_key = tblalias[in_key]
        return out_key


class SizeCalc(object):
    _base_2  = 0x0200
    _base_10 = 0x0A00
    
    UNIT_B   =  0 | _base_2
    UNIT_KiB = 10 | _base_2
    UNIT_MiB = 20 | _base_2
    UNIT_GiB = 30 | _base_2
    UNIT_TiB = 40 | _base_2
    UNIT_PiB = 50 | _base_2
    UNIT_EiB = 60 | _base_2
    UNIT_ZiB = 70 | _base_2
    UNIT_YiB = 80 | _base_2
    
    UNIT_KB =   3 | _base_10
    UNIT_MB =   6 | _base_10
    UNIT_GB =   9 | _base_10
    UNIT_TB =  12 | _base_10
    UNIT_PB =  15 | _base_10
    UNIT_EB =  18 | _base_10
    UNIT_ZB =  21 | _base_10
    UNIT_YB =  24 | _base_10
    
    @classmethod
    def convert(self, size, unit_in, unit_out):
        fac_in   = ((unit_in & 0xffffff00) >> 8) ** (unit_in & 0xff)
        div_out  = ((unit_out & 0xffffff00) >> 8) ** (unit_out & 0xff)
        return (size * fac_in // div_out)
    
    @classmethod
    def convert_round_up(self, size, unit_in, unit_out):
        fac_in   = ((unit_in & 0xffffff00) >> 8) ** (unit_in & 0xff)
        div_out  = ((unit_out & 0xffffff00) >> 8) ** (unit_out & 0xff)
        bytes    = size * fac_in
        if bytes % div_out != 0:
            result = (bytes / div_out) + 1
        else:
            result = bytes / div_out
        return result
