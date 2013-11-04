#!/usr/bin/python

import sys
import dbus
import string
import drbdmanage.drbd.drbdcore
from drbdmanage.utils import *
from drbdmanage.dbusserver import DBusServer
from drbdmanage.storage.storagecore import MinorNr
from drbdmanage.drbd.drbdcore import DrbdResource
from drbdmanage.drbd.drbdcore import Assignment
from drbdmanage.exceptions import *
from drbdmanage.drbd.drbdcore import DrbdNodeView
from drbdmanage.drbd.drbdcore import DrbdResourceView
from drbdmanage.drbd.drbdcore import DrbdVolumeView
from drbdmanage.drbd.drbdcore import DrbdVolumeStateView
from drbdmanage.drbd.drbdcore import AssignmentView


__author__="raltnoeder"
__date__ ="$Sep 16, 2013 1:11:20 PM$"


class DrbdManage(object):
    _server = None
    _interactive = False
    _noerr       = False
    _colors      = True
    
    VIEW_SEPARATOR_LEN = 78
    
    def __init__(self):
        self.dbus_init()
    
    
    def dbus_init(self):
        try:
            dbus_con = dbus.SystemBus()
            self._server = dbus_con.get_object(DBusServer.DBUS_DRBDMANAGED,
              DBusServer.DBUS_SERVICE)
        except dbus.exceptions.DBusException as exc:
            sys.stderr.write("Error: Cannot connect to the drbdmanaged "
              "process using DBus\n")
            sys.stderr.write("The DBus subsystem returned the following "
              "error description:\n")
            sys.stderr.write("%s\n" % (str(exc)))
            exit(1)
    
    
    def run(self):
        color = self.color
        rc = 1
        cl_cmd = False
        try:
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
                sys.stderr.write("Error: Interactive mode "
                  "(--interactive, -i) and stdin mode (--stdin, -s)\n"
                  "       are mutually exclusive options\n")
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
        color = self.color
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
        elif arg == "resources" or arg == "volumes":
            rc = self.cmd_list_resources(args)
        elif arg == "nodes":
            rc = self.cmd_list_nodes(args)
        elif arg == "new-node":
            rc = self.cmd_new_node(args)
        elif arg == "remove-node":
            rc = self.cmd_remove_node(args)
        elif arg == "new-volume":
            rc = self.cmd_new_volume(args)
        elif arg == "new-resource":
            rc = self.cmd_new_resource(args)
        elif arg == "remove-volume":
            rc = self.cmd_remove_volume(args)
        elif arg == "remove-resource":
            rc = self.cmd_remove_resource(args)
        elif arg == "connect":
            rc = self.cmd_connect(args, False)
        elif arg == "reconnect":
            rc = self.cmd_connect(args, True)
        elif arg == "disconnect":
            rc = self.cmd_disconnect(args)
        elif arg == "flags":
            rc = self.cmd_flags(args)
        elif arg == "attach":
            rc = self.cmd_attach(args)
        elif arg == "detach":
            rc = self.cmd_detach(args)
        elif arg == "assign":
            rc = self.cmd_assign(args)
        elif arg == "unassign":
            rc = self.cmd_unassign(args)
        elif arg == "deploy":
            rc = self.cmd_deploy(args)
        elif arg == "reconfigure":
            rc = self.cmd_reconfigure()
        elif arg == "update-pool":
            rc = self.cmd_update_pool()
        elif arg == "save":
            rc = self.cmd_save()
        elif arg == "load":
            rc = self.cmd_load()
        elif arg == "shutdown":
            rc = self.cmd_shutdown(args)
        elif arg == "debug":
            rc = self._server.debug_cmd("list")
        elif arg == "export":
            rc = self.cmd_export_conf(args)
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
        if CommandParser().parse(args, order, params, opt, optalias,
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
    
    
    def cmd_new_resource(self, args):
        rc    = 1
        port  = DrbdResource.PORT_NR_AUTO
        # Command parser configuration
        order      = [ "name" ]
        params     = {}
        opt        = { "-p" : "auto" }
        optalias   = { "--port" : "-p" }
        flags      = {}
        flagsalias = {}
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException
            name      = params["name"]
            port_str  = opt["-p"]
            if port_str != "auto":
                try:
                    port = int(port_str)
                except ValueError:
                    raise SyntaxException

            server_rc = self._server.create_resource(dbus.String(name),
              port)
            if server_rc == 0:
                rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_new_resource()
        return rc
    
    
    def syntax_new_resource(self):
        sys.stderr.write("Syntax: new-resource [ options ] <name>\n")
        sys.stderr.write("  Options:\n"
          "    --port | -p : <port-number>\n")
    
    
    def cmd_new_volume(self, args):
        rc    = 1
        unit  = SizeCalc.UNIT_GiB
        size  = None
        minor = MinorNr.MINOR_NR_AUTO
        # Command parser configuration
        order      = [ "name", "size" ]
        params     = {}
        opt        = { "-u" : None, "-m" : None }
        optalias   = { "--unit" : "-u", "--minor" : "-m", }
        flags      = {}
        flagsalias = {}
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException
            name      = params["name"]
            size_str  = params["size"]
            unit_str  = opt["-u"]
            minor_str = opt["-m"]
            if minor_str is not None:
                if minor_str == "auto":
                    minor = MinorNr.MINOR_NR_AUTO
                elif minor_str == "auto-drbd":
                    minor = MinorNr.MINOR_NR_AUTODRBD
                else:
                    try:
                        minor = int(minor_str)
                    except Exception as exc:
                        sys.stderr.write("Error: <minor> must be a number "
                          "or \"auto\" or \"auto-drbd\"\n")
                        raise SyntaxException
            try:
                size = long(size_str)
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
                size = SizeCalc.convert_round_up(size, unit,
                  SizeCalc.UNIT_MiB)
            server_rc = self._server.create_resource(dbus.String(name),
              DrbdResource.PORT_NR_AUTO)
            if server_rc == 0 or server_rc == DM_EEXIST:
                server_rc = self._server.create_volume(dbus.String(name),
                  dbus.Int64(size), dbus.Int32(minor))
            if server_rc == 0:
                rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_new_volume()
        return rc
    
    
    def syntax_new_volume(self):
        sys.stderr.write("Syntax: new-volume [ options ] <name> <size>\n")
        sys.stderr.write("  Options:\n"
          "    --unit | -u  : { MB | GB | TB | PB | MiB | GiB | TiB "
          "| PiB }\n"
          "    --minor | -m : <minor-number>\n"
          "The default size unit is GiB.\n")
    
    
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
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException
        
            node_name = params["node"]
            force     = flags["-f"]
            quiet     = flags["-q"]
            if not quiet:
                quiet = self.user_confirm("You are going to remove a node "
                  "from the cluster. This will remove all resources from "
                  "the node.\nPlease confirm:")
            if quiet:
                server_rc = self._server.remove_node(dbus.String(node_name),
                  dbus.Boolean(force))
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
    
    
    def cmd_remove_resource(self, args):
        rc = 1
        # Command parser configuration
        order = [ "resource" ]
        params = {}
        opt   = {}
        optalias = {}
        flags = { "-q" : False, "-f" : False }
        flagsalias = { "--quiet" : "-q", "--force" : "-f" }
        
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException
        
            res_name = params["resource"]
            force    = flags["-f"]
            quiet    = flags["-q"]
            if not quiet:
                quiet = self.user_confirm("You are going to remove a resource "
                  "and all of its volumes from all nodes of the cluster.\n"
                  "Please confirm:")
            if quiet:
                server_rc = self._server.remove_resource(dbus.String(res_name),
                  dbus.Boolean(force))
                if server_rc == 0:
                    rc = 0
                else:
                    self.error_msg_text(server_rc)
            else:
                rc = 0
        except SyntaxException:
            self.syntax_remove_resource()
        return rc
    
    
    def syntax_remove_resource(self):
        sys.stderr.write("Syntax: remove-resource [ --quiet | -q ] <name>\n")
    
    
    def cmd_remove_volume(self, args):
        rc = 1
        # Command parser configuration
        order = [ "volume", "id" ]
        params = {}
        opt   = {}
        optalias = {}
        flags = { "-q" : False, "-f" : False }
        flagsalias = { "--quiet" : "-q", "--force" : "-f" }
        
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException
        
            vol_name = params["volume"]
            id_str   = params["id"]
            try:
                id   = int(id_str)
            except ValueError:
                raise SyntaxException
            force    = flags["-f"]
            quiet    = flags["-q"]
            if not quiet:
                quiet = self.user_confirm("You are going to remove a volume "
                  "from all nodes of the cluster.\n"
                  "Please confirm:")
            if quiet:
                server_rc = self._server.remove_volume(dbus.String(vol_name),
                  dbus.Int32(id), dbus.Boolean(force))
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
        sys.stderr.write("Syntax: remove-volume [ --quiet | -q ] <name> "
          " <id>\n")
    
    
    def cmd_connect(self, args, reconnect):
        rc    = 1
        state = []
        # Command parser configuration
        order    = [ "node", "res" ]
        params   = {}
        opt      = {}
        optalias = {}
        flags    = { }
        flagsalias = { }
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException

            node_name = params["node"]
            res_name  = params["res"]
            
            server_rc = self._server.connect(dbus.String(node_name),
              dbus.String(res_name), dbus.Boolean(reconnect))
            if server_rc == 0:
                rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_connect(reconnect)
        return rc
    
    
    def syntax_connect(self, reconnect):
        if reconnect:
            cmd = "reconnect"
        else:
            cmd = "connect"
        sys.stderr.write("Syntax: %s <node> <resource>\n" % (cmd))
    
    
    def cmd_disconnect(self, args):
        rc    = 1
        state = []
        # Command parser configuration
        order    = [ "node", "res" ]
        params   = {}
        opt      = {}
        optalias = {}
        flags    = { }
        flagsalias = { }
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException

            node_name = params["node"]
            res_name  = params["res"]

            server_rc = self._server.disconnect(dbus.String(node_name),
              dbus.String(res_name))
            if server_rc == 0:
                rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_connect()
        return rc
    
    
    def syntax_disconnect(self):
        sys.stderr.write("Syntax: disconnect <node> <resource>\n")
    
    
    def cmd_flags(self, args):
        rc         = 1
        clear_mask = 0
        set_mask   = 0
        node_name  = None
        res_name   = None
        # Command parser configuration
        try:
            crt_arg = args.next_arg()
            while crt_arg is not None:
                flag = 0
                if crt_arg.startswith("-"):
                    if crt_arg.startswith("--reconnect="):
                        flag = Assignment.FLAG_RECONNECT
                    elif crt_arg.startswith("--updcon="):
                        flag = Assignment.FLAG_UPD_CON
                    elif crt_arg.startswith("--overwrite="):
                        flag = Assignment.FLAG_OVERWRITE
                    elif crt_arg.startswith("--discard="):
                        flag = Assignment.FLAG_DISCARD
                    else:
                        raise SyntaxException
                    val = self._cmd_flags_val(crt_arg)
                    if val == "0":
                        clear_mask = clear_mask | flag
                    elif val == "1":
                        set_mask = set_mask | flag
                    else:
                        raise SyntaxException
                else:
                    if node_name is None:
                        node_name = crt_arg
                    elif res_name is None:
                        res_name = crt_arg
                    else:
                        raise SyntaxException
                
                crt_arg = args.next_arg()
            
            if node_name is None or res_name is None:
                raise SyntaxException
            
            server_rc = self._server.modify_state(dbus.String(node_name),
              dbus.String(res_name), dbus.Int64(0), dbus.Int64(0),
              dbus.Int64(clear_mask), dbus.Int64(set_mask))
            if server_rc == 0:
                rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_flags()
        return rc
    
    
    def syntax_flags(self):
        sys.stderr.write("Syntax: flags <node> <resource> [ flags ]\n"
          "  flags:\n"
          "          --reconnect={0|1}\n"
          "          --updcon={0|1}\n"
          "          --overwrite={0|1}\n"
          "          --discard={0|1}\n")
    
    
    def _cmd_flags_val(self, arg):
        val = ""
        idx = arg.find("=")
        if idx != -1:
            val = arg[idx + 1:]
        return val
    
    
    def cmd_attach(self, args):
        rc    = 1
        state = []
        # Command parser configuration
        order    = [ "node", "res", "id" ]
        params   = {}
        opt      = {}
        optalias = {}
        flags    = { }
        flagsalias = { }
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException

            node_name = params["node"]
            res_name  = params["res"]
            id_str    = params["id"]
            try:
                id   = int(id_str)
            except ValueError:
                raise SyntaxException

            server_rc = self._server.attach(dbus.String(node_name),
              dbus.String(res_name), dbus.Int32(id))
            if server_rc == 0:
                rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_connect()
        return rc
    
    
    def syntax_attach(self):
        sys.stderr.write("Syntax: attach <node> <resource> <id>\n")
    
    
    def cmd_detach(self, args):
        rc    = 1
        state = []
        # Command parser configuration
        order    = [ "node", "res", "id" ]
        params   = {}
        opt      = {}
        optalias = {}
        flags    = { }
        flagsalias = { }
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException

            node_name = params["node"]
            res_name  = params["res"]
            id_str    = params["id"]
            try:
                id   = int(id_str)
            except ValueError:
                raise SyntaxException

            server_rc = self._server.detach(dbus.String(node_name),
              dbus.String(res_name), dbus.Int32(id))
            if server_rc == 0:
                rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_connect()
        return rc
    
    
    def syntax_detach(self):
        sys.stderr.write("Syntax: detach <node> <resource> <id>\n")
    
    
    def cmd_assign(self, args):
        rc    = 1
        cstate = 0
        tstate = 0
        # Command parser configuration
        order    = [ "node", "vol" ]
        params   = {}
        opt      = {}
        optalias = {}
        flags    = { "--overwrite" : False, "--client" : False,
          "--discard" : False, "-c" : False }
        flagsalias = { "--connect" : "-c" }
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException

            node_name = params["node"]
            vol_name  = params["vol"]

            client    = flags["--client"]
            overwrite = flags["--overwrite"]
            discard   = flags["--discard"]
            connect   = flags["-c"]
            
            if (overwrite and client):
                sys.stderr.write("Error: --overwrite and --client "
                  "are mutually exclusive options\n")
                raise SyntaxException
            if (overwrite and discard):
                sys.stderr.write("Error: --overwrite and --discard "
                "are mutually exclusive options\n")
                raise SyntaxException
            if client:
                tstate = tstate | Assignment.FLAG_DISKLESS
            if overwrite:
                tstate = tstate | Assignment.FLAG_OVERWRITE
            if discard:
                tstate = tstate | Assignment.FLAG_DISCARD
            if connect:
                tstate = tstate | Assignment.FLAG_CONNECT
            server_rc = self._server.assign(dbus.String(node_name),
              dbus.String(vol_name), dbus.Int64(cstate), dbus.Int64(tstate))
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
          "    --client         make this node a DRBD client only\n"
          "    --overwrite      copy this node's data to all other nodes\n"
          "    --discard        discard this node's data upon connect\n"
          "    -c | --connect   connect to peer resources on other nodes\n")
        sys.stderr.write("The following options are mutually exclusive:\n"
          "  --overwrite and --client\n"
          "  --overwrite and --discard\n")
    
    
    def cmd_deploy(self, args):
        rc    = 1
        cstate = 0
        tstate = 0
        # Command parser configuration
        order    = [ "res", "count" ]
        params   = {}
        opt      = {}
        optalias = {}
        flags    = { }
        flagsalias = { }
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException

            res_name  = params["res"]
            count_str = params["count"]
            count = 0
            try:
                count = int(count_str)
            except ValueError:
                raise SyntaxException
            
            if count < 1:
                raise SyntaxException

            server_rc = self._server.deploy(dbus.String(res_name),
              dbus.Int32(count))
            if server_rc == 0:
                rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_deploy()
        return rc
    
    
    def syntax_deploy(self):
        sys.stderr.write("Syntax: deploy <resource> <redundancy-count>\n")
        sys.stderr.write("    The redundancy count specifies the number of\n"
          "    nodes to which the resource should be deployed. It must be at\n"
          "    least 1 and less than the maximum allowable number of nodes\n"
          "    in the cluster\n")
    
    def cmd_update_pool(self):
        rc = 1
        server_rc = self._server.update_pool()
        if server_rc == 0:
            rc = 0
        else:
            self.error_msg_text(server_rc)
        return rc
    
    
    def cmd_reconfigure(self):
        rc = 1
        server_rc = self._server.reconfigure()
        if server_rc == 0:
            rc = 0
        else:
            self.error_msg_text(server_rc)
        return rc
    
    
    def cmd_save(self):
        rc = 1
        server_rc = self._server.save_conf()
        if server_rc == 0:
            rc = 0
        else:
            self.error_msg_text(server_rc)
        return rc
    
    
    def cmd_load(self):
        rc = 1
        server_rc = self._server.load_conf()
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
        if CommandParser().parse(args, order, params, opt, optalias,
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
                  "drbdmanaged server process on this node.\nPlease confirm:")
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
        if (not machine_readable) and len(node_list) == 0:
            sys.stdout.write("No nodes defined\n")
            return 0
        
        if not machine_readable:
            sys.stdout.write("%s%-*s%s %-12s %-34s %s%s%s\n"
              % (color(COLOR_TEAL), DrbdNodeView.get_name_maxlen(), "Node",
                color(COLOR_NONE), "addr family", "Network address",
                color(COLOR_RED), "state", color(COLOR_NONE))
              )
            sys.stdout.write("  %s* pool size  %14s / free  %14s%s\n"
              % (color(COLOR_BROWN), "", "", color(COLOR_NONE))
              )
            sys.stdout.write((self.VIEW_SEPARATOR_LEN * '-') + "\n")
        
        for properties in node_list:
            try:
                view = DrbdNodeView(properties, machine_readable)
                if not machine_readable:
                    poolsize = view.get_poolsize()
                    poolfree = view.get_poolfree()
                    if poolsize >= 0:
                        poolsize_text = "%14d" % (poolsize)
                    else:
                        poolsize_text = "unknown"
                    if poolfree >= 0:
                        poolfree_text = "%14d" % (poolfree)
                    else:
                        poolfree_text = "unknown"
                    sys.stdout.write("%s%-*s%s %-12s %-34s %s%s%s\n"
                      % (color(COLOR_TEAL), view.get_name_maxlen(),
                        view.get_name(), color(COLOR_NONE), view.get_af(),
                        view.get_ip(), color(COLOR_RED), view.get_state(),
                        color(COLOR_NONE))
                      )
                    sys.stdout.write("  %s* pool size: %14s / free: %14s%s\n"
                      % (color(COLOR_BROWN),
                      poolsize_text, poolfree_text,
                      color(COLOR_NONE))
                      )
                else:
                    sys.stdout.write("%s,%s,%s,%d,%d,%s\n"
                      % (view.get_name(), view.get_af(),
                        view.get_ip(), view.get_poolsize(),
                        view.get_poolfree(), view.get_state())
                      )
            except IncompatibleDataException:
                sys.stderr.write("Warning: incompatible table entry skipped\n")
                continue
        return 0
    
    
    def syntax_list_nodes(self):
        sys.stderr.write("Syntax: nodes [ --machine-readable | -m ]\n")
    
    
    def cmd_list_resources(self, args):
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
              self.syntax_list_resources()
              return 1
        
        machine_readable = flags["-m"]
        
        resource_list = self._server.resource_list()
        if (not machine_readable) and len(resource_list) == 0:
            sys.stdout.write("No resources defined\n")
            return 0
        
        # Header/key for the table
        if not machine_readable:
            sys.stdout.write(
                "%s%-*s%s %7s        %s%s%s\n" % (color(COLOR_DARKGREEN),
                    DrbdResourceView.get_name_maxlen(), "Resource",
                    color(COLOR_NONE), "Port",
                    color(COLOR_RED), "state", color(COLOR_NONE))
                  )
            sys.stdout.write(
              "  %s*%s%6s%s %14s %7s  %s%s\n"
                % (color(COLOR_BROWN), color(COLOR_DARKPINK),
                "id#", color(COLOR_BROWN), "size (MiB)", "minor#", "state",
                color(COLOR_NONE))
              )
            sys.stdout.write((self.VIEW_SEPARATOR_LEN * '-') + "\n")
        
        for properties in resource_list:
            try:
                view = DrbdResourceView(properties, machine_readable)
                if not machine_readable:
                    sys.stdout.write(
                      "%s%-*s%s %7s         %s%s%s\n"
                        % (color(COLOR_DARKGREEN),
                        view.get_name_maxlen(), view.get_name(),
                        color(COLOR_NONE), view.get_port(),
                        color(COLOR_RED), view.get_state(), color(COLOR_NONE))
                      )
                volume_list = view.get_volumes()
                for vol_view in volume_list:
                    if not machine_readable:
                        sys.stdout.write(
                          "  %s*%s%6d%s %14d %7s %s%s\n"
                            % (color(COLOR_BROWN), color(COLOR_DARKPINK),
                            vol_view.get_id(), color(COLOR_BROWN),
                            vol_view.get_size_MiB(),
                            vol_view.get_minor(), vol_view.get_state(),
                            color(COLOR_NONE))
                          )
                    else:
                        sys.stdout.write(
                          "%s,%s,%d,%d,%s,%s,%s\n" % (view.get_name(),
                            view.get_state(), vol_view.get_id(),
                            vol_view.get_size_MiB(), view.get_port(),
                            vol_view.get_minor(), vol_view.get_state())
                          )
            except IncompatibleDataException:
                sys.stderr.write("Warning: incompatible table entry skipped\n")
                continue
        return 0
    
    
    def syntax_list_resources(self):
        sys.stderr.write("Syntax: resources [ --machine-readable | -m ]\n")
    
    
    def cmd_list_assignments(self, args):
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
              self.syntax_list_assignments()
              return 1
        
        machine_readable = flags["-m"]
        
        assignment_list = self._server.assignment_list()
        if (not machine_readable) and len(assignment_list) == 0:
            sys.stdout.write("No assignments defined\n")
            return 0
            
        if not machine_readable:
            sys.stdout.write("%s%-*s%s\n"
                % (color(COLOR_TEAL), DrbdNodeView.get_name_maxlen(),
                "Node", color(COLOR_NONE))
              )
            sys.stdout.write("  %s%-*s%s %5s %35s%s%s%s\n"
                % (color(COLOR_DARKGREEN),
                DrbdResourceView.get_name_maxlen(),
                "Resource", color(COLOR_NONE),
                "Node#", "", color(COLOR_RED),
                "state (crt -> tgt)",
                color(COLOR_NONE))
              )
            sys.stdout.write("  %s* %s%6s%s %-48s %s%s%s\n"
                % (color(COLOR_BROWN), color(COLOR_DARKPINK),
                "Vol#",  color(COLOR_BROWN),
                "Blockdevice path",
                color(COLOR_DARKRED), "state (crt -> tgt)",
                color(COLOR_NONE))
              )
            sys.stdout.write((self.VIEW_SEPARATOR_LEN * '-') + "\n")
        
        prev_node = ""
        for properties in assignment_list:
            try:
                view = AssignmentView(properties, machine_readable)
                vol_state_list = view.get_volume_states()
                if not machine_readable:
                    crt_node = view.get_node()
                    if crt_node != prev_node:
                        prev_node = crt_node
                        sys.stdout.write("%s%-*s%s\n"
                            % (color(COLOR_TEAL),
                            DrbdNodeView.get_name_maxlen(), crt_node,
                            color(COLOR_NONE))
                          )
                    sys.stdout.write("  %s%-*s%s %5d %35s%s%s -> %s%s\n"
                        % (color(COLOR_DARKGREEN),
                        DrbdResourceView.get_name_maxlen(),
                        view.get_resource(), color(COLOR_NONE),
                        view.get_node_id(), "", color(COLOR_RED),
                        view.get_cstate(), view.get_tstate(),
                        color(COLOR_NONE))
                      )
                    for vol_state in vol_state_list:
                        sys.stdout.write("  %s* %s%6d%s %-48s %s%s  -> %s%s\n"
                            % (color(COLOR_BROWN), color(COLOR_DARKPINK),
                            vol_state.get_id(),  color(COLOR_BROWN),
                            vol_state.get_bd_path(),
                            color(COLOR_DARKRED), vol_state.get_cstate(),
                            vol_state.get_tstate(), color(COLOR_NONE))
                          )
                else:
                    sys.stdout.write("%s,%s,%d,%s,%s\n"
                        % (view.get_node(), view.get_resource(),
                        view.get_node_id(), view.get_cstate(),
                        view.get_tstate())
                      )
            except IncompatibleDataException:
                sys.stderr.write("Warning: incompatible table entry skipped\n")
                continue
        return 0
    
    
    def syntax_list_assignments(self):
        sys.stderr.write("Syntax: assignments [ --machine-readable | -m ]\n")
    
    
    def cmd_export_conf(self, args):
        rc = 1
        # Command parser configuration
        order    = [ "res" ]
        params   = {}
        opt      = {}
        optalias = {}
        flags    = { }
        flagsalias = { }
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException
        
            res_name = params["res"]
            if res_name == "*":
                res_name = ""

            server_rc = self._server.export_conf(dbus.String(res_name))
            if server_rc == 0:
                rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_export_conf()
        return rc
        
    
    def syntax_export_conf(self):
        sys.stderr.write("Syntax: export { resource | * }\n")
    

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
