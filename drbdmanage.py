#!/usr/bin/python

import sys
import dbus
from drbdmanage.utils import *
from drbdmanage.dbusserver import DBusServer

__author__="raltnoeder"
__date__ ="$Sep 16, 2013 1:11:20 PM$"

class DrbdManage(object):
    _server = None
    
    def __init__(self):
        self.dbus_init()
        
    def dbus_init(self):
        dbus_con = dbus.SystemBus()
        self._server = dbus_con.get_object(DBusServer.DBUS_DRBDMANAGED, \
          DBusServer.DBUS_SERVICE)
    
    def run(self):
        self._debug_tests()
        interactive = False
        args = ArgvReader(sys.argv)
        rc = 0
        while True:
            arg = args.peek_arg()
            if arg is None:
                break
            if not arg.startswith("-"):
                # begin of drbdmanage command
                rc = self.exec_cmd(args, False)
                break
            else:
                if arg == "-i":
                    interactive = True
                else:
                    sys.stderr.write("Error: Invalid option '" + arg + "'\n")
                    return 1
            args.next()
        if rc == 0:
            if interactive:
                exit(self.cli())
        exit(rc)
    
    def cli(self):
        while True:
            sys.stdout.write("drbdmanage> ")
            sys.stdout.flush()
            cmdline = sys.stdin.readline()
            if len(cmdline) == 0:
                # end of file
                return 0
            if cmdline.endswith("\n"):
                cmdline = cmdline[:len(cmdline) - 1]
            if cmdline.startswith("#"):
                # remarks line
                continue
            args = CmdLineReader(cmdline)
            arg = args.peek_arg()
            if arg is None:
                # empty line
                continue
            else:
                rc = self.exec_cmd(args, True)
            # TODO: if input is not a terminal, and return code is error,
            #       stop processing
    
    def exec_cmd(self, args, interactive):
        arg = args.next_arg()
        if arg == "list":
            rc = self.cmd_list(args)
        elif arg == "new-node":
            rc = self.cmd_new_node(args)
        elif arg == "new-volume":
            rc = self.cmd_new_volume(args)
        elif arg == "assign":
            rc = self.cmd_assign(args)
        elif arg == "unassign":
            rc = self.cmd_unassign(args)
        elif arg == "exit":
            exit(0)
        else:
            sys.stderr.write("Error: unknown command '" + arg + "'\n")
            # writing nonsense on the command line is considered an error
            return 1
        return rc
    
    def cmd_list(self, args):
        self.debug_args(args)
        return 0
    
    def cmd_new_node(self, args):
        self.debug_args(args)
        return 0
    
    def cmd_new_volume(self, args):
        self.debug_args(args)
        return 0
    
    def cmd_assign(self, args):
        self.debug_args(args)
        return 0
    
    def cmd_unassign(self, args):
        self.debug_args(args)
        return 0
    
    def debug_args(self, args):
        first = True
        sys.stdout.write("DEBUG: args(")
        while True:
            arg = args.next_arg()
            if arg is not None:
                if first:
                    first = False
                else:
                    sys.stdout.write(", ")
                sys.stdout.write(arg)
            else:
                break
        sys.stdout.write(")\n")
    
    def _debug_tests(self):
        rc = self._server.create_node("remus", "10.43.5.208", "ipv4")
        rc = self._server.create_node("romulus", "10.43.5.209", "ipv4")
        rc = self._server.create_volume("vol01", 2460700)
        rc = self._server.create_volume("vol02", 1050260)
        rc = self._server.create_volume("petapool", 12890760600, \
          signature="sx")
        rc = self._server.assign("remus", "vol01")
        rc = self._server.assign("romulus", "vol02")
        rc = self._server.assign("romulus", "petapool")
        rc = self._server.assign("remus", "petapool")
        rc = self._server.debug_cmd("list-nodes")
        rc = self._server.debug_cmd("list-volumes")
        rc = self._server.debug_cmd("list-assignments")

def main():
    drbdmanage = DrbdManage()
    drbdmanage.run()

if __name__ == "__main__":
    main()
