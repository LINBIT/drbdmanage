#!/usr/bin/python

import sys
import dbus
import string
import drbdmanage.drbd.drbdcore
from drbdmanage.utils import *
from drbdmanage.dbusserver import DBusServer
from drbdmanage.storage.storagecore import MinorNr
from drbdmanage.exceptions import *
from drbdmanage.drbd.drbdcore import DrbdNodeView
from drbdmanage.drbd.drbdcore import DrbdVolumeView
from drbdmanage.drbd.drbdcore import AssignmentView

# TODO: add syntax description when drbdmanage is called without any
#       command line arguments

__author__="raltnoeder"
__date__ ="$Sep 16, 2013 1:11:20 PM$"


class DrbdManage(object):
    _server = None
    _interactive = False
    _noerr       = False
    _colors      = True
    
    
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
            sys.stderr.write("%s\n" % (str(exc)))
            exit(1)
    
    
    def run(self):
        color = self.color
        rc = 1
        cl_cmd = False
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
                    cl_cmd = True
                    rc = self.exec_cmd(args, False)
                    if rc != 0:
                        sys.stderr.write("  %sOperation failed%s\n"
                          % (color(COLOR_RED), color(COLOR_NONE)))
                    break
                else:
                    if arg == "-i" or arg == "--interactive":
                        self._interactive = True
                    elif arg == "-s" or arg == "--stdin":
                        script = True
                        self._colors = False
                    elif arg == "--no-error-stop":
                        self._noerr = True
                    elif arg == "--no-colors":
                        self._colors = False
                    else:
                        sys.stderr.write("Error: Invalid option '%s'\n"
                          % (arg))
                        exit(1)
                args.next()
            if self._interactive and script:
                sys.stderr.write("Error: Interactive mode " \
                  + "(--interactive, -i) and stdin mode (--stdin, -s)\n"
                  + "       are mutually exclusive options\n")
                exit(1)
            if self._interactive or script:
                rc = self.cli()
            else:
                if not cl_cmd:
                    # neither interactive nor script mode and no command
                    # in the argument list
                    self.syntax()
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
                    sys.stderr.write("  %sOperation failed%s\n"
                          % (color(COLOR_RED), color(COLOR_NONE)))
                if rc != 0 and not self._interactive and not self._noerr:
                    return rc
        return 0   
    
    
    def exec_cmd(self, args, interactive):
        rc = 1
        arg = args.next_arg()
        if arg is None:
            arg = ""
        if arg == "assignments":
            rc = self.cmd_list_assignments(args)
        elif arg == "volumes":
            rc = self.cmd_list_volumes(args)
        elif arg == "nodes":
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
        rc = 1
        # Command parser configuration
        order      = [ "name", "ip" ]
        params     = {}
        opt        = { "-a" : None }
        optalias   = { "--address-family" : "a" }
        flags      = {}
        flagsalias = {}
        if CommandParser().parse(args, order, params, opt, optalias, \
          flags, flagsalias) == 0:
            name = params["name"]
            ip   = params["ip"]
            af   = opt["-a"]
            if af is None:
                af = drbdmanage.drbd.drbdcore.DrbdNode.AF_IPV4_LABEL
            server_rc = self._server.create_node(name, ip, af)
            if server_rc == 0:
                rc = 0
            else:
                self.error_msg_text(server_rc)
        else:
            self.syntax_new_node()
        return rc
    
    
    def syntax_new_node(self):
        sys.stderr.write("Syntax: new-node [ options ] <name> <ip>\n")
        sys.stderr.write("  Options:\n")
        sys.stderr.write("    --address-family | -a : { ipv4 | ipv6 }\n")
    
    
    def cmd_new_volume(self, args):
        rc    = 1
        unit  = SizeCalc.UNIT_GiB
        size  = None
        minor = MinorNr.MINOR_AUTO
        # Command parser configuration
        order      = [ "name", "size" ]
        params     = {}
        opt        = { "-u" : None, "-m" : None }
        optalias   = { "--unit" : "-u", "--minor" : "-m" }
        flags      = {}
        flagsalias = {}
        try:
            if CommandParser().parse(args, order, params, opt, optalias, \
              flags, flagsalias) != 0:
                raise SyntaxException
            name      = params["name"]
            size_str  = params["size"]
            unit_str  = opt["-u"]
            minor_str = opt["-m"]
            if minor_str is not None:
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
                        raise SyntaxException
            try:
                size = int(size_str)
            except Exception as exc:
                sys.stderr.write("Error: <size> must be a number\n")
                raise SyntaxException
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
                    raise SyntaxException
            if unit != SizeCalc.UNIT_MiB:
                size = SizeCalc.convert_round_up(size, unit, \
                  SizeCalc.UNIT_MiB)
            server_rc = self._server.create_volume(name, size, minor, \
              signature="sxi")
            if server_rc == 0:
                rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_new_volume()
        return rc
    
    
    def cmd_remove_node(self, args):
        rc = 1
        # Command parser configuration
        order = [ "node" ]
        params = {}
        opt   = {}
        optalias = {}
        flags = { "-q" : False, "-f" : False }
        flagsalias = { "--quiet" : "-q", "--force" : "-f" }
        
        try:
            if CommandParser().parse(args, order, params, opt, optalias, \
              flags, flagsalias) != 0:
                raise SyntaxException
        
            node_name = params["node"]
            force     = flags["-f"]
            quiet     = flags["-q"]
            if not quiet:
                quiet = self.user_confirm("You are going to remove a node " \
                  + "from the cluster. This will remove all resources from " \
                  + "the node.\nPlease confirm:")
            if quiet:
                server_rc = self._server.remove_node(node_name, force, \
                  signature="sb")
                if server_rc == 0:
                    rc = 0
                else:
                    self.error_msg_text(server_rc)
            else:
                rc = 0
        except SyntaxException:
            self.syntax_remove_node()
        return rc
    
    
    def syntax_remove_node(self):
        sys.stderr.write("Syntax: remove-node [ --quiet | -q ] <name>\n")
    
    
    def cmd_remove_volume(self, args):
        rc = 1
        # Command parser configuration
        order = [ "volume" ]
        params = {}
        opt   = {}
        optalias = {}
        flags = { "-q" : False, "-f" : False }
        flagsalias = { "--quiet" : "-q", "--force" : "-f" }
        
        try:
            if CommandParser().parse(args, order, params, opt, optalias, \
              flags, flagsalias) != 0:
                raise SyntaxException
        
            vol_name = params["volume"]
            force    = flags["-f"]
            quiet    = flags["-q"]
            if not quiet:
                quiet = self.user_confirm("You are going to remove a volume "\
                  + "from all nodes of the cluster.\n" \
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
        except SyntaxException:
            self.syntax_remove_volume()
        return rc
    
    
    def syntax_remove_volume(self):
        sys.stderr.write("Syntax: remove-volume [ --quiet | -q ] <name>\n")
    
    
    def syntax_new_volume(self):
        sys.stderr.write("Syntax: new-volume [ options ] <name> <size>\n")
        sys.stderr.write("  Options:\n" \
          + "    --unit | -u  : { MB | GB | TB | PB | MiB | GiB | TiB " \
          + "| PiB }\n" \
          + "    --minor | -m : <minor-number>\n" \
          + "The default size unit is GiB.\n")
    
    
    def cmd_assign(self, args):
        rc    = 1
        state = []
        # Command parser configuration
        order    = [ "node", "vol" ]
        params   = {}
        opt      = {}
        optalias = {}
        flags    = { "--overwrite" : False, "--client" : False, \
          "--discard" : False }
        flagsalias = { }
        try:
            if CommandParser().parse(args, order, params, opt, optalias, \
              flags, flagsalias) != 0:
                raise SyntaxException

            node_name = params["node"]
            vol_name  = params["vol"]

            client    = flags["--client"]
            overwrite = flags["--overwrite"]
            discard   = flags["--discard"]
            
            if (overwrite and client):
                sys.stderr.write("Error: --overwrite and --client "
                  "are mutually exclusive options\n")
                raise SyntaxException
            if (overwrite and discard):
                sys.stderr.write("Error: --overwrite and --discard "
                "are mutually exclusive options\n")
                raise SyntaxException
            if client:
                state.append("client")
            if overwrite:
                state.append("overwrite")
            if discard:
                state.append("discard")
            server_rc = self._server.assign(node_name, vol_name, state,
              signature="ssas")
            if server_rc == 0:
                rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_assign()
        return rc
    
    
    def syntax_assign(self):
        sys.stderr.write("Syntax: assign [ options ] <node> <volume>\n")
        sys.stderr.write("  Options:\n"
          "    --client      make this node a DRBD client only\n"
          "    --overwrite   copy this node's data to all other nodes\n"
          "    --discard     discard this node's data upon connect\n")
        sys.stderr.write("The following options are mutually exclusive:\n"
          "  --overwrite and --client\n"
          "  --overwrite and --discard\n")
    
    
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
        # Command parser configuration
        order      = [ "node", "vol" ]
        params     = {}
        opt        = {}
        optalias   = {}
        flags      = { "-f" : False }
        flagsalias = { "--force" : "-f" }
        if CommandParser().parse(args, order, params, opt, optalias, \
          flags, flagsalias) == 0:
            node_name = params["node"]
            vol_name  = params["vol"]
            force     = flags["-f"]
            server_rc = self._server.unassign(node_name, vol_name, force)
            if server_rc == 0:
                rc = 0
            else:
                self.error_msg_text(server_rc)
        else:
            self.syntax_unassign()
        return rc


    def syntax_unassign(self):
        sys.stderr.write("Syntax: unassign [ options ] <node> <volume>\n")
        sys.stderr.write("  Options:\n"
          "    --quiet | -q  disable the safety question\n")
    
    
    def syntax(self):
        sys.stderr.write("Syntax: drbdmanage [ options ] command\n")
        sys.stderr.write("  Options:\n"
          "    --interactive | -i ... run in interactive mode\n"
          "    --stdin       | -s ... read commands from stdin "
          "(for scripts)\n")
    
    
    def cmd_shutdown(self, args):
        # Command parser configuration
        order      = []
        params     = {}
        opt        = {}
        optalias   = {}
        flags      = { "-q" : False }
        flagsalias = { "--quiet" : "-q" }
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException
            quiet = flags["-q"]
            if not quiet:
                quiet = self.user_confirm("You are going to shut down the "
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
        except SyntaxException:
            sys.stderr.write("Syntax: shutdown [ --quiet | -q ]\n")
        return 0
    
    
    def cmd_list_nodes(self, args):
        color = self.color
        # Command parser configuration
        order    = []
        params   = {}
        opt      = {}
        optalias = {}
        flags    = { "-m" : False }
        flagsalias = { "--machine-readable" : "-m" }
        if CommandParser().parse(args, order, params, opt, optalias,
          flags, flagsalias) != 0:
              self.syntax_list_nodes()
              return 1
        
        machine_readable = flags["-m"]
        
        node_list = self._server.node_list()
        if len(node_list) == 0:
            if not machine_readable:
                sys.stdout.write("No nodes defined\n")
            return 0
        
        if not machine_readable:
            sys.stdout.write(self.color(COLOR_GREEN)
              + string.ljust("Name", DrbdNodeView.get_name_maxlen())
              + " "
              + string.ljust("AF", 5)
              + " "
              + string.ljust("IP address", 20)
              + " "
              + string.rjust("Pool size", 12)
              + " "
              + string.rjust("Pool free", 12)
              + " "
              + string.rjust("state", 8)
              + self.color(COLOR_NONE) + "\n")
        for properties in node_list:
            try:
                view = DrbdNodeView(properties, machine_readable)
            except IncompatibleDataException:
                sys.stderr.write("Warning: incompatible table entry skipped\n")
                continue
            node_name = view.get_name()
            node_af   = view.get_af()
            node_ip   = view.get_ip()
            node_pool = view.get_poolsize()
            node_free = view.get_poolfree()
            node_st   = view.get_state()
            if machine_readable:
                sys.stdout.write(node_name + "," + node_af + "," + node_ip
                  + "," + node_pool + "," + node_free + "," + node_st + "\n")
            else:
                sys.stdout.write(
                  string.ljust(node_name, DrbdNodeView.get_name_maxlen())
                  + " "
                  + string.ljust(node_af, 5)
                  + " "
                  + string.ljust(node_ip, 20)
                  + " "
                  + string.rjust(node_pool, 12)
                  + " "
                  + string.rjust(node_free, 12)
                  + " "
                  + string.rjust(node_st, 8)
                  + "\n")
        return 0
    
    
    def syntax_list_nodes(self):
        sys.stderr.write("Syntax: nodes [ --machine-readable | -m ]\n")
    
    
    def cmd_list_volumes(self, args):
        # Command parser configuration
        order    = []
        params   = {}
        opt      = {}
        optalias = {}
        flags    = { "-m" : False }
        flagsalias = { "--machine-readable" : "-m" }
        if CommandParser().parse(args, order, params, opt, optalias,
          flags, flagsalias) != 0:
              self.syntax_list_volumes()
              return 1
        
        machine_readable = flags["-m"]
        
        volume_list = self._server.volume_list()
        if len(volume_list) == 0:
            if not machine_readable:
                sys.stdout.write("No volumes defined\n")
            return 0
        
        if not machine_readable:
            sys.stdout.write(self.color(COLOR_GREEN)
              + string.ljust("Name", DrbdVolumeView.get_name_maxlen())
              + " "
              + string.rjust("Size (MiB)", 12)
              + " "
              + string.rjust("Minor#", 7)
              + " "
              + string.rjust("flags", 8)
              + self.color(COLOR_NONE) + "\n")
        for properties in volume_list:
            try:
                view = DrbdVolumeView(properties, machine_readable)
            except IncompatibleDataException:
                sys.stderr.write("Warning: incompatible table entry skipped\n")
                continue
            vol_name  = view.get_name()
            vol_size  = view.get_size()
            vol_minor = view.get_minor()
            vol_state = view.get_state()
            if machine_readable:
                sys.stdout.write(vol_name + "," + vol_size + ","
                  + vol_minor + "," + vol_state + "\n")
            else:
                sys.stdout.write(
                  string.ljust(vol_name, DrbdVolumeView.get_name_maxlen())
                  + " "
                  + string.rjust(vol_size, 12)
                  + " "
                  + string.rjust(vol_minor, 7)
                  + " "
                  + string.rjust(vol_state, 8)
                  + "\n")
        return 0
    
    
    def syntax_list_volumes(self):
        sys.stderr.write("Syntax: volumes [ --machine-readable | -m ]\n")
    
    
    def cmd_list_assignments(self, args):
        # Command parser configuration
        order    = []
        params   = {}
        opt      = {}
        optalias = {}
        flags    = { "-m" : False }
        flagsalias = { "--machine-readable" : "-m" }
        if CommandParser().parse(args, order, params, opt, optalias,
          flags, flagsalias) != 0:
              self.syntax_list_volumes()
              return 1
        
        machine_readable = flags["-m"]
        
        assignment_list = self._server.assignment_list()
        if len(assignment_list) == 0:
            if not machine_readable:
                sys.stdout.write("No assignments defined\n")
            return 0
        
        if not machine_readable:
            sys.stdout.write(self.color(COLOR_GREEN)
              + string.ljust("Node", DrbdNodeView.get_name_maxlen())
              + " "
              + string.ljust("Volume", DrbdVolumeView.get_name_maxlen())
              + " "
              + string.ljust("Blockdevice", 32)
              + " "
              + string.rjust("Node id", 7)
              + " "
              + string.rjust("state", 8)
              + self.color(COLOR_NONE) + "\n")
        prev_node = ""
        for properties in assignment_list:
            try:
                view = AssignmentView(properties, machine_readable)
            except IncompatibleDataException:
                sys.stderr.write("Warning: incompatible table entry skipped\n")
                continue
            node = view.get_node()
            vol  = view.get_volume()
            bd   = view.get_blockdevice()
            id   = view.get_node_id()
            cst  = view.get_cstate()
            tst  = view.get_tstate()
            st   = view.get_state()
            if machine_readable:
                sys.stdout.write(node + "," + vol + "," + bd + "," + id
                  + "," + cst + "," + tst + "\n")
            else:
                if prev_node == node:
                    view_node = ""
                else:
                    view_node = node
                    prev_node = node
                sys.stdout.write(
                  string.ljust(view_node, DrbdNodeView.get_name_maxlen())
                  + " "
                  + string.ljust(vol, DrbdVolumeView.get_name_maxlen())
                  + " "
                  + string.ljust(bd, 32)
                  + " "
                  + string.rjust(id, 7)
                  + " "
                  + string.rjust(st, 8)
                  + "\n")
        return 0
    
    
    def syntax_list_assignments(self):
        sys.stderr.write("Syntax: assignments [ --machine-readable | -m ]\n")
    

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
    
    
    def color(self, col):
        if self._colors:
            return col
        else:
            return ""
    
    
def main():
    drbdmanage = DrbdManage()
    drbdmanage.run()

if __name__ == "__main__":
    main()
