#!/usr/bin/python

import sys
import dbus
from drbdmanage.utils import *
from drbdmanage.dbusserver import DBusServer
from drbdmanage.storage.storagecore import MinorNr
from drbdmanage.exceptions import *

# TODO: add syntax description when drbdmanage is called without any
#       command line arguments

__author__="raltnoeder"
__date__ ="$Sep 16, 2013 1:11:20 PM$"

class DrbdManage(object):
    _server = None
    _interactive = False
    
    def __init__(self):
        self.dbus_init()
    
    def dbus_init(self):
        try:
            dbus_con = dbus.SystemBus()
            self._server = dbus_con.get_object(DBusServer.DBUS_DRBDMANAGED, \
              DBusServer.DBUS_SERVICE)
        except dbus.exceptions.DBusException as exc:
            sys.stderr.write("Error: Cannot connect to the drbdmanaged "
              + "process using DBus\n")
            sys.stderr.write("The DBus subsystem returned the following "
              + "error description:\n")
            sys.stderr.write(str(exc) + "\n")
            exit(1)
    
    def run(self):
        rc = 1
        try:
            self._debug_tests()
            args = ArgvReader(sys.argv)
            script = False
            while True:
                arg = args.peek_arg()
                if arg is None:
                    break
                if not arg.startswith("-"):
                    # begin of drbdmanage command
                    rc = self.exec_cmd(args, False)
                    if rc != 0:
                        sys.stderr.write(chr(0x1b) \
                          + "[0;91m  Operation failed" + chr(0x1b) + "[0m\n")
                    break
                else:
                    if arg == "-i":
                        self._interactive = True
                    elif arg == "-s":
                        script = True
                    else:
                        sys.stderr.write("Error: Invalid option '" + arg \
                          + "'\n")
                        exit(1)
                args.next()
            if self._interactive and script:
                sys.stderr.write("Error: Interactive mode " \
                  + "(--interactive, -i) and script mode (--script, -s) "
                  + "are mutually exclusive options\n")
                exit(1)
            if self._interactive:
                rc = self.cli()
            elif script:
                rc = self.cli()
        except dbus.exceptions.DBusException as exc:
            sys.stderr.write("Error: The DBus connection to the drbdmanaged "
              + "process failed.\n")
            sys.stderr.write("The DBus subsystem returned the following "
              + "error description:\n")
            sys.stderr.write(str(exc) + "\n")
        exit(rc)
    
    def cli(self):
        while True:
            if self._interactive:
                sys.stdout.write("drbdmanage> ")
                sys.stdout.flush()
            cmdline = sys.stdin.readline()
            if len(cmdline) == 0:
                # end of file
                if self._interactive:
                    sys.stdout.write("\n")
                break
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
                if rc != 0 and self._interactive:
                    sys.stderr.write(chr(0x1b) + "[0;91m  Operation failed" \
                      + chr(0x1b) + "[0m\n")
                if rc != 0 and not self._interactive:
                    return rc
        return 0   
    
    def exec_cmd(self, args, interactive):
        rc = 1
        arg = args.next_arg()
        if arg is None:
            arg = ""
        if arg == "list-volumes":
            rc = self.cmd_list_volumes(args)
        elif arg == "list-nodes":
            rc = self.cmd_list_nodes(args)
        elif arg == "new-node":
            rc = self.cmd_new_node(args)
        elif arg == "remove-node":
            rc = self.cmd_remove_node(args)
        elif arg == "new-volume":
            rc = self.cmd_new_volume(args)
        elif arg == "remove-volume":
            rc = self.cmd_remove_volume(args)
        elif arg == "assign":
            rc = self.cmd_assign(args)
        elif arg == "unassign":
            rc = self.cmd_unassign(args)
        elif arg == "reconfigure":
            rc = self.cmd_reconfigure()
        elif arg == "shutdown":
            rc = self.cmd_shutdown(args)
        elif arg == "debug":
            rc = self._server.debug_cmd("list-nodes")
            rc = self._server.debug_cmd("list-volumes")
            rc = self._server.debug_cmd("list-assignments")
        elif arg == "safe":
            rc = self._server.debug_cmd("safe")
        elif arg == "exit":
            exit(0)
        else:
            if arg == "":
                rc = 0
            else:
                # writing nonsense on the command line is considered an error
                sys.stderr.write("Error: unknown command '" + arg + "'\n")
        return rc
    
    def cmd_new_node(self, args):
        rc      = 1
        name    = None
        ip      = None
        af      = "ipv4"
        # TODO: ip type recognition
        arg = args.next_arg()
        while arg is not None:
            if arg == "--addrfmly" or arg == "-a":
                af = args.next_arg()
                if af is None:
                    ip = None
                    break
                # Server checks ip type
            else:
                if name is None:
                    name = arg
                elif ip is None:
                    ip = arg
                else:
                    self.syntax_new_node()
                    break
            arg = args.next_arg()
        if name is not None and ip is not None:
            server_rc = self._server.create_node(name, ip, af)
            if server_rc == 0:
                rc = 0
            else:
                self.error_msg_text(server_rc)
        else:
            self.syntax_new_node()
        return rc
    
    def syntax_new_node(self):
        sys.stderr.write("Syntax: new-node [ --ip-type <type> ] <name> <ip>\n")
        sys.stderr.write("  <type> = { ipv4 | ipv6 }\n")
    
    def cmd_new_volume(self, args):
        rc = 1
        name      = None
        size_str  = None
        size      = None
        unit_str  = None
        unit      = SizeCalc.UNIT_GiB
        minor_str = None
        minor     = MinorNr.MINOR_AUTO
        arg = args.next_arg()
        while arg is not None:
            if arg == "--unit" or arg == "-u":
                unit_str = args.next_arg()
                if unit_str is None:
                    size = None
                    break
            elif arg == "--minor" or arg == "-m":
                minor_str = args.next_arg()
                if minor_str is None:
                    minor = None
                    break
                else:
                    if minor_str == "auto":
                        minor = MinorNr.MINOR_AUTO
                    elif minor_str == "auto-drbd":
                        minor = MinorNr.MINOR_AUTODRBD
                    else:
                        try:
                            minor = int(minor_str)
                        except Exception as exc:
                            sys.stderr.write("Error: <minor> must be a number "
                              + "or \"auto\" or \"auto-drbd\"\n")
                            minor = None
                            break
            else:
                if name is None:
                    name = arg
                elif size_str is None:
                    size_str = arg
                else:
                    self.syntax_new_volume()
            arg = args.next_arg()
        try:
            size = int(size_str)
        except Exception as exc:
            sys.stderr.write("Error: <size> must be a number\n")
            size = None
        if unit_str is not None:
            if unit_str  == "MiB":
                unit = SizeCalc.UNIT_MiB
            elif unit_str == "GiB":
                unit = SizeCalc.UNIT_GiB
            elif unit_str == "TiB":
                unit = SizeCalc.UNIT_TiB
            elif unit_str == "PiB":
                unit = SizeCalc.UNIT_PiB
            elif unit_str == "MB":
                unit = SizeCalc.UNIT_MB
            elif unit_str == "GB":
                unit = SizeCalc.UNIT_GB
            elif unit_str == "TB":
                unit = SizeCalc.UNIT_TB
            elif unit_str == "PB":
                unit = SizeCalc.UNIT_PB
            else:
                size = None
        if unit is not None and size is not None:
            if unit != SizeCalc.UNIT_MiB:
                size = SizeCalc.convert_round_up(size, unit, \
                  SizeCalc.UNIT_MiB)
        if name is not None and size is not None and minor is not None:
            server_rc = self._server.create_volume(name, size, minor, \
              signature="sxi")
            if server_rc == 0:
                rc = 0
            else:
                self.error_msg_text(server_rc)
        else:
            self.syntax_new_volume()
        return rc
    
    def cmd_remove_node(self, args):
        rc = 1
        node_name = None
        quiet = False
        force = False
        arg = args.next_arg()
        while arg is not None:
            if arg.startswith("-"):
                if arg == "--quiet" or arg == "-q":
                    quiet = True
                elif arg == "--force" or arg == "-f":
                    force == True
                else:
                    node_name = None
                    break
            else:
                if node_name is None:
                    node_name = arg
                else:
                    node_name = None
                    break
            arg = args.next_arg()
        if node_name is not None:
            if not quiet:
                quiet = self.user_confirm("You are going to remove a node from " \
                  + "the cluster. This will remove all resources from the " \
                  + "node.\nPlease confirm:")
            if quiet:
                server_rc = self._server.remove_node(node_name, force, \
                  signature="sb")
                if server_rc == 0:
                    rc = 0
                else:
                    self.error_msg_text(server_rc)
            else:
                rc = 0
        else:
            self.syntax_remove_node()
        return rc
    
    def syntax_remove_node(self):
        sys.stderr.write("Syntax: remove-node [ --quiet | -q ] <name>\n")
    
    def cmd_remove_volume(self, args):
        rc = 1
        vol_name = None
        quiet = False
        force = False
        arg = args.next_arg()
        while arg is not None:
            if arg.startswith("-"):
                if arg == "--quiet" or arg == "-q":
                    quiet = True
                elif arg == "--force" or arg == "-f":
                    force = True
                else:
                    vol_name = None
                    break
            else:
                if vol_name is None:
                    vol_name = arg
                else:
                    vol_name = None
                    break
            arg = args.next_arg()
        if vol_name is not None:
            if not quiet:
                quiet = self.user_confirm("You are going to remove a volume from " \
                  + "all nodes of the cluster.\n" \
                  + "Please confirm:")
            if quiet:
                server_rc = self._server.remove_volume(vol_name, force, \
                  signature="sb")
                if server_rc == 0:
                    rc = 0
                else:
                    self.error_msg_text(server_rc)
            else:
                rc = 0
        else:
            self.syntax_remove_volume()
        return rc
    
    def syntax_remove_volume(self):
        sys.stderr.write("Syntax: remove-volume [ --quiet | -q ] <name>\n")
    
    def syntax_new_volume(self):
        sys.stderr.write("Syntax: new-volume [ options ] <name> <size>\n")
        sys.stderr.write("  Options:\n" \
          + "    --unit { MB | GB | TB | PB | MiB | GiB | TiB | PiB }\n" \
          + "    --minor <minor-number>\n" \
          + "The default size unit is GiB.\n")
    
    def cmd_assign(self, args):
        rc = 1
        node_name = None
        vol_name  = None
        state     = []
        client    = False
        overwrite = False
        discard   = False
        arg = args.next_arg()
        while arg is not None:
            if arg.startswith("-"):
                if arg == "--client":
                    if overwrite:
                        node_name = None
                        break
                    client = True
                elif arg == "--overwrite":
                    if client:
                        node_name = None
                        break
                    overwrite = True
                elif arg == "--discard":
                    if overwrite:
                        node_name = None
                        break
                    discard = True
                else:
                    node_name = None
                    break
            else:
                if node_name is None:
                    node_name = arg
                elif vol_name is None:
                    vol_name = arg
                else:
                    node_name = None
                    break
            arg = args.next_arg()
        if client:
            state.append("client")
        if overwrite:
            state.append("overwrite")
        if discard:
            state.append("discard")
        if node_name is not None and vol_name is not None:
            server_rc = self._server.assign(node_name, vol_name, state,\
              signature="ssas")
            if server_rc == 0:
                rc = 0
            else:
                self.error_msg_text(server_rc)
        else:
            self.syntax_assign()
        return rc
    
    def syntax_assign(self):
        sys.stderr.write("Syntax: assign [ options ] <node> <volume>\n")
        sys.stderr.write("  Options:\n" \
          + "    --client      make this node a DRBD client only\n" \
          + "    --overwrite   copy this node's data to all other nodes\n" \
          + "    --discard     discard this node's data upon connect\n")
        sys.stderr.write("The following options are mutually exclusive:\n" \
          + "  --client and --overwrite\n"
          + "  --overwrite and --discard\n")
    
    def cmd_reconfigure(self):
        rc = 1
        server_rc = self._server.reconfigure()
        if server_rc == 0:
            rc = 0
        else:
            self.error_msg_text(server_rc)
        return rc
    
    def cmd_unassign(self, args):
        rc = 1
        node_name = None
        vol_name  = None
        arg = args.next_arg()
        while arg is not None:
            if node_name is None:
                node_name = arg
            elif vol_name is None:
                vol_name = arg
            else:
                node_name = None
                break
            arg = args.next_arg()
        if node_name is not None and vol_name is not None:
            server_rc = self._server.unassign(node_name, vol_name)
            if server_rc == 0:
                rc = 0
            else:
                self.error_msg_text(server_rc)
        else:
            self.syntax_unassign()
        return rc

    def syntax_unassign(self):
        sys.stderr.write("Syntax: unassign [ options ] <node> <volume>\n")
        sys.stderr.write("  Options:\n" \
          + "    --quiet | -q  disable the safety question\n")
    
    def cmd_shutdown(self, args):
        quiet = False
        arg  = args.next_arg()
        if arg is not None:
            if arg == "--quiet" or arg == "-q":
                quiet = True
            else:
                sys.stderr.write("Syntax: shutdown [ --quiet | -q ]\n")
                return 1
        if not quiet:
            quiet = self.user_confirm("You are going to shut down the " \
              + "drbdmanaged server process on this node.\nPlease confirm:")
        if quiet:
            try:
                self._server.shutdown()
            except dbus.exceptions.DBusException:
                # An exception is expected here, as the server
                # probably will not answer
                pass
            # Continuing the client without a server does not make sense, so:
            exit(0)
        return 0
    
    def cmd_list_nodes(self, args):
        node_list = self._server.node_list()
        # TODO: DEBUG: this is debug code only
        for properties in node_list:
            for item in properties:
                sys.stdout.write(item + ", ")
            sys.stdout.write("\n")
        return 0
    
    def cmd_list_volumes(self, args):
        volume_list = self._server.volume_list()
        # TODO: DEBUG: this is debug code only
        for properties in volume_list:
            for item in properties:
                sys.stdout.write(item + ", ")
            sys.stdout.write("\n")
        return 0
    
    def cmd_list_assignments(self):
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
    
    def user_confirm(self, question):
        """
        Ask yes/no questions. Requires the user to answer either "yes" or "no".
        If the input stream closes, it defaults to "no".
        returns: True for "yes", False for "no"
        """
        sys.stdout.write(question + "\n")
        sys.stdout.write("  yes/no: ")
        sys.stdout.flush()
        rc = False
        while True:
            answer = sys.stdin.readline()
            if len(answer) != 0:
                if answer.endswith("\n"):
                    answer = answer[:len(answer) - 1]
                if answer == "yes":
                    rc = True
                    break
                elif answer == "no":
                    break
                else:
                    sys.stdout.write("Please answer \"yes\" or \"no\": ")
                    sys.stdout.flush()
            else:
                # end of stream, no more input
                sys.stdout.write("\n")
                break
        return rc
    
    def error_msg_text(self, error):
        sys.stderr.write("Error: " + dm_exc_text(error) + "\n")
    
    def _debug_tests(self):
        # rc = self._server.create_node("remus", "10.43.5.208", "ipv4")
        # rc = self._server.create_node("romulus", "10.43.5.209", "ipv4")
        # rc = self._server.create_volume("vol01", 2460700)
        # rc = self._server.create_volume("vol02", 1050260)
        # rc = self._server.create_volume("petapool", 12890760600, \
        #   signature="sx")
        # rc = self._server.assign("remus", "vol01")
        # rc = self._server.assign("romulus", "vol02")
        # rc = self._server.assign("romulus", "petapool")
        # rc = self._server.assign("remus", "petapool")
        # rc = self._server.debug_cmd("list-nodes")
        # rc = self._server.debug_cmd("list-volumes")
        # rc = self._server.debug_cmd("list-assignments")
        pass

def main():
    drbdmanage = DrbdManage()
    drbdmanage.run()

if __name__ == "__main__":
    main()
