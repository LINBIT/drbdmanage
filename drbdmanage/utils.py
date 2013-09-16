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
        if self._idx < self._max:
            self._idx += 1
            arg = self._argv[cur_idx]
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
