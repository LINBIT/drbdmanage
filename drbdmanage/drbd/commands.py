#!/usr/bin/python

__author__="raltnoeder"
__date__ ="$Oct 24, 2013 4:04:23 PM$"

import os
import sys
import subprocess

from drbdmanage.utils import *

class DrbdAdm(object):
    EXECUTABLE = "drbdadm"
    
    execpath = None
    
    def __init__(self, path):
        prefix = path
        if not prefix.endswith("/"):
            prefix += "/"
        self.execpath = prefix + self.EXECUTABLE


    def adjust(self, res_name):
        sys.stdout.write("%sDEBUG: DrbdAdm: adjust %s%s\n"
          % (COLOR_GREEN, res_name, COLOR_NONE))
        args = [self.EXECUTABLE, "-c", "-", "adjust", res_name]
        return self._run_drbdadm(args)
    
    
    def up(self, res_name):
        sys.stdout.write("%sDEBUG: DrbdAdm: up %s%s\n"
          % (COLOR_GREEN, res_name, COLOR_NONE))
        args = [self.EXECUTABLE, "-c", "-", "up", res_name]
        return self._run_drbdadm(args)
    
    
    def down(self, res_name):
        sys.stdout.write("%sDEBUG: DrbdAdm: down %s%s\n"
          % (COLOR_GREEN, res_name, COLOR_NONE))
        args = [self.EXECUTABLE, "-c", "-", "down", res_name]
        return self._run_drbdadm(args)
    
    
    def primary(self, res_name, force):
        if force:
            sys.stdout.write("%sDEBUG: DrbdAdm: primary %s --force%s\n"
              % (COLOR_GREEN, res_name, COLOR_NONE))
        else:
            sys.stdout.write("%sDEBUG: DrbdAdm: primary %s%s\n"
              % (COLOR_GREEN, res_name, COLOR_NONE))
        args = [self.EXECUTABLE, "-c", "-"]
        if force:
            args.append("--")
            args.append("--force")
        args.append("primary")
        args.append(res_name)
        return self._run_drbdadm(args)
    
    
    def secondary(self, res_name):
        sys.stdout.write("%sDEBUG: DrbdAdm: secondary %s%s\n"
          % (COLOR_GREEN, res_name, COLOR_NONE))
        args = [self.EXECUTABLE, "-c", "-", "secondary", res_name]
        return self._run_drbdadm(args)
    
    
    def connect(self, res_name, discard):
        if discard:
            sys.stdout.write("%sDEBUG: DrbdAdm: connect %s "
              "--discard-my-data%s\n"
              % (COLOR_GREEN, res_name, COLOR_NONE))
        else:
            sys.stdout.write("%sDEBUG: DrbdAdm: connect %s%s\n"
              % (COLOR_GREEN, res_name, COLOR_NONE))
        args = [self.EXECUTABLE, "-c", "-"]
        if discard:
            args.append("--")
            args.append("--discard-my-data")
        args.append("connect")
        args.append(res_name)
        return self._run_drbdadm(args)
    
    
    def disconnect(self, res_name):
        sys.stdout.write("%sDEBUG: DrbdAdm: disconnect %s%s\n"
          % (COLOR_GREEN, res_name, COLOR_NONE))
        args = [self.EXECUTABLE, "-c", "-", "disconnect", res_name]
        return self._run_drbdadm(args)
    
    
    def attach(self, res_name, vol_id):
        sys.stdout.write("%sDEBUG: DrbdAdm: attach %s %d%s\n"
          % (COLOR_GREEN, res_name, vol_id, COLOR_NONE))
        args = [self.EXECUTABLE, "-c", "-", "attach",
          res_name + "/" + str(vol_id)]
        return self._run_drbdadm(args)
    
    
    def detach(self, res_name, vol_id):
        sys.stdout.write("%sDEBUG: DrbdAdm: detach %s %d%s\n"
          % (COLOR_GREEN, res_name, vol_id, COLOR_NONE))
        args = [self.EXECUTABLE, "-c", "-", "detach", res_name,
          res_name + "/" + str(vol_id)]
        return self._run_drbdadm(args)
    
    
    def create_md(self, res_name, vol_id):
        sys.stdout.write("%sDEBUG: DrbdAdm: create-md %s %d%s\n"
          % (COLOR_GREEN, res_name, vol_id, COLOR_NONE))
        args = [self.EXECUTABLE, "-c", "-", "create-md",
          res_name + "/" + str(vol_id)]
        return self._run_drbdadm(args)
    
    
    def _run_drbdadm(self, args):
        drbd_proc = subprocess.Popen(args, 0, self.execpath,
          stdin=subprocess.PIPE, close_fds=True)
        return drbd_proc
    