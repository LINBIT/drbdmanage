#!/usr/bin/python

__author__="raltnoeder"
__date__ ="$Sep 16, 2013 1:40:12 PM$"

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
        for idx in range(0, len(cmdline_b)):
            if cmdline_b[idx] != 0x20 and cmdline_b[idx] != 0x9:
                cmdline_b = cmdline_b[idx:]
                self._cmdline = str(cmdline_b)
                break

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
