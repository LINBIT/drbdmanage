#!/usr/bin/python
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2013, 2014   LINBIT HA-Solutions GmbH
                               Author: R. Altnoeder

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

"""
drbdmanage command line interface (cli)

This drbdmanage client communicates with a
local drbdmanage server through D-Bus.
"""

import sys
import os
import dbus
import subprocess
import drbdmanage.drbd.drbdcore
import drbdmanage.drbd.persistence

from drbdmanage.consts import (DEFAULT_VG, DRBDCTRL_DEFAULT_PORT, DRBDCTRL_DEV,
    DRBDCTRL_RES_NAME, DRBDCTRL_RES_FILE, DRBDCTRL_RES_PATH, NODE_ADDR,
    NODE_AF, RES_PORT, VOL_MINOR)
from drbdmanage.utils import ArgvReader, CmdLineReader, CommandParser
from drbdmanage.utils import SizeCalc
from drbdmanage.utils import get_terminal_size, build_path
from drbdmanage.utils import (COLOR_NONE, COLOR_RED, COLOR_DARKRED,
    COLOR_DARKGREEN, COLOR_BROWN, COLOR_DARKPINK, COLOR_TEAL)
from drbdmanage.exceptions import AbortException
from drbdmanage.exceptions import IncompatibleDataException
from drbdmanage.exceptions import SyntaxException
from drbdmanage.exceptions import dm_exc_text
from drbdmanage.exceptions import DM_EEXIST
from drbdmanage.dbusserver import DBusServer
from drbdmanage.drbd.drbdcore import DrbdResource
from drbdmanage.drbd.drbdcore import Assignment
from drbdmanage.drbd.drbdcore import AssignmentView
from drbdmanage.drbd.drbdcore import DrbdNodeView
from drbdmanage.drbd.drbdcore import DrbdResource
from drbdmanage.drbd.drbdcore import DrbdResourceView
from drbdmanage.storage.storagecore import MinorNr


class DrbdManage(object):

    """
    drbdmanage dbus client, the CLI for controlling the drbdmanage server
    """

    _server = None
    _interactive = False
    _noerr       = False
    _colors      = True

    VIEW_SEPARATOR_LEN = 78

    DRBDCTRL_BLOCKDEV = "/dev/mapper/" + DEFAULT_VG + "-" + DRBDCTRL_RES_NAME
    UMHELPER_FILE     = "/sys/module/drbd/parameters/usermode_helper"
    UMHELPER_OVERRIDE = "/bin/true"

    def __init__(self):
        pass


    def dbus_init(self):
        try:
            if self._server is None:
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
        fn_rc = 1
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
                    fn_rc = self.exec_cmd(args, False)
                    if fn_rc != 0:
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
                    elif arg == "-D":
                        args.next()
                        exit(self.cmd_debug(args))
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
                fn_rc = self.cli()
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
        exit(fn_rc)


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
                fn_rc = self.exec_cmd(args, True)
                if fn_rc != 0 and self._interactive:
                    sys.stderr.write("  %sOperation failed%s\n"
                          % (color(COLOR_RED), color(COLOR_NONE)))
                if fn_rc != 0 and not self._interactive and not self._noerr:
                    return fn_rc
        return 0


    def exec_cmd(self, args, interactive):
        fn_rc = 1
        arg = args.next_arg()
        if arg is None:
            fn_rc = 0
        else:
            cmd_func = self.COMMANDS.get(arg)
            if cmd_func is not None:
                fn_rc = cmd_func(self, args)
            else:
                # writing nonsense on the command line is considered an error
                sys.stderr.write("Error: unknown command '" + arg + "'\n")
                sys.stdout.write("Note: Valid commands are:\n")
                self.print_sub_commands()
        return fn_rc


    def cmd_new_node(self, args):
        fn_rc = 1
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

            props = dbus.Dictionary(signature="ss")
            props[NODE_ADDR] = ip
            props[NODE_AF]   = af

            self.dbus_init()
            server_rc = self._server.create_node(name, props)
            if server_rc == 0:
                fn_rc = 0
                joinc = self._server.text_query("joinc %s" % name)
                sys.stdout.write("\nJoin command for node %s:\n"
                    "%s\n" % (name, joinc))
            else:
                self.error_msg_text(server_rc)
        else:
            self.syntax_new_node()
        return fn_rc


    def syntax_new_node(self):
        sys.stderr.write("Syntax: new-node [ options ] <name> <ip>\n")
        sys.stderr.write("  Options:\n")
        sys.stderr.write("    --address-family | -a : { ipv4 | ipv6 }\n")


    def cmd_new_resource(self, args):
        fn_rc    = 1
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

            props = dbus.Dictionary(signature="ss")
            props[RES_PORT] = str(port)

            self.dbus_init()
            server_rc = self._server.create_resource(dbus.String(name),
              props)
            if server_rc == 0:
                fn_rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_new_resource()
        return fn_rc


    def syntax_new_resource(self):
        sys.stderr.write("Syntax: new-resource [ options ] <name>\n")
        sys.stderr.write("  Options:\n"
          "    --port | -p : <port-number>\n")


    def cmd_new_volume(self, args):
        fn_rc    = 1
        unit  = SizeCalc.UNIT_GiB
        size  = None
        minor = MinorNr.MINOR_NR_AUTO
        # Command parser configuration
        order      = [ "name", "size" ]
        params     = {}
        opt        = { "-u" : None, "-m" : None, "-d" : None }
        optalias   = { "--unit" : "-u", "--minor" : "-m", "--deploy" : "-d" }
        flags      = {}
        flagsalias = {}
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException
            name       = params["name"]
            size_str   = params["size"]
            unit_str   = opt["-u"]
            minor_str  = opt["-m"]
            deploy_str = opt["-d"]
            if minor_str is not None:
                if minor_str == "auto":
                    minor = MinorNr.MINOR_NR_AUTO
                else:
                    try:
                        minor = int(minor_str)
                    except Exception:
                        sys.stderr.write("Error: <minor> must be a number "
                          "or \"auto\"\n")
                        raise SyntaxException
            deploy = None
            if deploy_str is not None:
                try:
                    deploy = int(deploy_str)
                except ValueError:
                    pass
            try:
                size = long(size_str)
            except Exception:
                sys.stderr.write("Error: <size> must be a number\n")
                raise SyntaxException
            if unit_str is not None:
                if unit_str  == "kiB":
                    unit = SizeCalc.UNIT_kiB
                elif unit_str  == "MiB":
                    unit = SizeCalc.UNIT_MiB
                elif unit_str == "GiB":
                    unit = SizeCalc.UNIT_GiB
                elif unit_str == "TiB":
                    unit = SizeCalc.UNIT_TiB
                elif unit_str == "PiB":
                    unit = SizeCalc.UNIT_PiB
                elif unit_str == "kB":
                    unit = SizeCalc.UNIT_kB
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
            if unit != SizeCalc.UNIT_kiB:
                size = SizeCalc.convert_round_up(size, unit,
                  SizeCalc.UNIT_kiB)

            props = dbus.Dictionary(signature="ss")

            self.dbus_init()
            server_rc = self._server.create_resource(dbus.String(name),
              props)
            if server_rc == 0 or server_rc == DM_EEXIST:
                props = dbus.Dictionary(signature="ss")
                props[VOL_MINOR] = str(minor)
                server_rc = self._server.create_volume(dbus.String(name),
                  dbus.Int64(size), props)
            if server_rc == 0 and deploy is not None:
                server_rc = self._server.auto_deploy(dbus.String(name),
                  dbus.Int32(deploy))
            if server_rc == 0:
                fn_rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_new_volume()
        return fn_rc


    def syntax_new_volume(self):
        sys.stderr.write("Syntax: new-volume [ options ] <name> <size>\n")
        sys.stderr.write("  Options:\n"
          "    --unit | -u  : { kB | MB | GB | TB | PB | kiB | MiB | GiB "
          "| TiB | PiB }\n"
          "    --minor | -m : <minor-number>\n"
          "The default size unit is GiB.\n")


    def cmd_modify_resource(self, args):
        fn_rc    = 1
        port  = DrbdResource.PORT_NR_AUTO
        # Command parser configuration
        order      = [ "name" ]
        params     = {}
        opt        = { "-p" : None }
        optalias   = { "--port" : "-p" }
        flags      = {}
        flagsalias = {}
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException
            name     = params["name"]
            port_str = opt["-p"]

            if port_str is not None:
                if not port_str == "auto":
                    try:
                        port = int(port_str)
                    except ValueError:
                        raise SyntaxException

            props = dbus.Dictionary(signature="ss")
            if port_str is not None:
                props[RES_PORT]   = str(port)

            self.dbus_init()
            server_rc = self._server.modify_resource(dbus.String(name), props)
            if server_rc == 0:
                fn_rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_modify_resource()
        return fn_rc


    def syntax_modify_resource(self):
        sys.stderr.write("Syntax: modify-resource [ options ] <name>\n")
        sys.stderr.write("  Options:\n"
          "    --port   | -p : <port-number>\n"
          "    --secret | -s : <shared-secret>\n")


    def cmd_remove_node(self, args):
        fn_rc = 1
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
                self.dbus_init()
                server_rc = self._server.remove_node(dbus.String(node_name),
                  dbus.Boolean(force))
                if server_rc == 0:
                    fn_rc = 0
                else:
                    self.error_msg_text(server_rc)
            else:
                fn_rc = 0
        except SyntaxException:
            self.syntax_remove_node()
        return fn_rc


    def syntax_remove_node(self):
        sys.stderr.write("Syntax: remove-node [ --quiet | -q ] <name>\n")


    def cmd_remove_resource(self, args):
        fn_rc = 1
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
                self.dbus_init()
                server_rc = self._server.remove_resource(dbus.String(res_name),
                  dbus.Boolean(force))
                if server_rc == 0:
                    fn_rc = 0
                else:
                    self.error_msg_text(server_rc)
            else:
                fn_rc = 0
        except SyntaxException:
            self.syntax_remove_resource()
        return fn_rc


    def syntax_remove_resource(self):
        sys.stderr.write("Syntax: remove-resource [ --quiet | -q ] <name>\n")


    def cmd_remove_volume(self, args):
        fn_rc = 1
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
                vol_id   = int(id_str)
            except ValueError:
                raise SyntaxException
            force    = flags["-f"]
            quiet    = flags["-q"]
            if not quiet:
                quiet = self.user_confirm("You are going to remove a volume "
                  "from all nodes of the cluster.\n"
                  "Please confirm:")
            if quiet:
                self.dbus_init()
                server_rc = self._server.remove_volume(dbus.String(vol_name),
                  dbus.Int32(vol_id), dbus.Boolean(force))
                if server_rc == 0:
                    fn_rc = 0
                else:
                    self.error_msg_text(server_rc)
            else:
                fn_rc = 0
        except SyntaxException:
            self.syntax_remove_volume()
        return fn_rc


    def syntax_remove_volume(self):
        sys.stderr.write("Syntax: remove-volume [ --quiet | -q ] <name> "
          " <id>\n")


    def cmd_connect(self, args):
        return self._connect(args, False)


    def cmd_reconnect(self, args):
        return self._connect(args, True)


    def _connect(self, args, reconnect):
        fn_rc    = 1
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

            self.dbus_init()
            server_rc = self._server.connect(dbus.String(node_name),
              dbus.String(res_name), dbus.Boolean(reconnect))
            if server_rc == 0:
                fn_rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_connect()
        return fn_rc


    def syntax_connect(self):
        sys.stderr.write("Syntax: connect <node> <resource>\n")


    def syntax_reconnect(self):
        sys.stderr.write("Syntax: reconnect <node> <resource>\n")


    def cmd_disconnect(self, args):
        fn_rc    = 1
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

            self.dbus_init()
            server_rc = self._server.disconnect(dbus.String(node_name),
              dbus.String(res_name))
            if server_rc == 0:
                fn_rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_disconnect()
        return fn_rc


    def syntax_disconnect(self):
        sys.stderr.write("Syntax: disconnect <node> <resource>\n")


    def cmd_flags(self, args):
        fn_rc         = 1
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

            self.dbus_init()
            server_rc = self._server.modify_state(dbus.String(node_name),
              dbus.String(res_name), dbus.Int64(0), dbus.Int64(0),
              dbus.Int64(clear_mask), dbus.Int64(set_mask))
            if server_rc == 0:
                fn_rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_flags()
        return fn_rc


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
        fn_rc    = 1
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
                vol_id   = int(id_str)
            except ValueError:
                raise SyntaxException

            self.dbus_init()
            server_rc = self._server.attach(dbus.String(node_name),
              dbus.String(res_name), dbus.Int32(vol_id))
            if server_rc == 0:
                fn_rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_attach()
        return fn_rc


    def syntax_attach(self):
        sys.stderr.write("Syntax: attach <node> <resource> <id>\n")


    def cmd_detach(self, args):
        fn_rc    = 1
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
                vol_id   = int(id_str)
            except ValueError:
                raise SyntaxException

            self.dbus_init()
            server_rc = self._server.detach(dbus.String(node_name),
              dbus.String(res_name), dbus.Int32(vol_id))
            if server_rc == 0:
                fn_rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_detach()
        return fn_rc


    def syntax_detach(self):
        sys.stderr.write("Syntax: detach <node> <resource> <id>\n")


    def cmd_assign(self, args):
        fn_rc    = 1
        cstate = 0
        tstate = 0
        # Command parser configuration
        order    = [ "node", "vol" ]
        params   = {}
        opt      = {}
        optalias = {}
        flags    = { "--overwrite" : False, "--client" : False,
          "--discard" : False }
        flagsalias = {}
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException

            node_name = params["node"]
            vol_name  = params["vol"]

            client    = flags["--client"]
            overwrite = flags["--overwrite"]
            discard   = flags["--discard"]
            # Turn on the connect flag by default; drbdadm adjust connects
            # anyway, so this flag does not make a lot of sense at this time,
            # but it may be useful in the future
            connect   = True
            # connect   = flags["-c"]

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

            self.dbus_init()
            server_rc = self._server.assign(dbus.String(node_name),
              dbus.String(vol_name), dbus.Int64(cstate), dbus.Int64(tstate))
            if server_rc == 0:
                fn_rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_assign()
        return fn_rc


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
        fn_rc    = 1
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

            self.dbus_init()
            server_rc = self._server.auto_deploy(dbus.String(res_name),
              dbus.Int32(count))
            if server_rc == 0:
                fn_rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_deploy()
        return fn_rc


    def syntax_deploy(self):
        sys.stderr.write("Syntax: deploy <resource> <redundancy-count>\n")
        sys.stderr.write("    The redundancy count specifies the number of\n"
          "    nodes to which the resource should be deployed. It must be at\n"
          "    least 1 and at most the number of nodes in the cluster\n")


    def cmd_extend(self, args):
        fn_rc    = 1
        rel_flag = False
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
            if count_str.startswith("+"):
                count_str = count_str[1:]
                rel_flag = True
            count = 0
            try:
                count = int(count_str)
            except ValueError:
                raise SyntaxException

            if count < 1:
                raise SyntaxException

            self.dbus_init()
            server_rc = self._server.auto_extend(dbus.String(res_name),
              dbus.Int32(count), dbus.Boolean(rel_flag))
            if server_rc == 0:
                fn_rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_extend()
        return fn_rc


    def syntax_extend(self):
        sys.stderr.write("Syntax: extend <resource> [+]<redundancy-count>\n")
        sys.stderr.write("    The redundancy count specifies the number of\n"
          "    nodes to which the resource should be deployed. It must be\n"
          "    greater than the number of nodes the resource is currently\n"
          "    assigned to and no more than the number of nodes in the\n"
          "    cluster.\n"
          "    If the redundancy count is prepended with a plus sign (+),\n"
          "    the resource is deployed to the specified number of nodes\n"
          "    in addition to those nodes where the resource is deployed\n"
          "    already.\n")


    def cmd_reduce(self, args):
        fn_rc    = 1
        rel_flag = False
        try:
            res_name  = None
            count_str = None
            while True:
                arg = args.next_arg()
                if arg is None:
                    break
                if res_name is None:
                    res_name = arg
                elif count_str is None:
                    count_str = arg
                else:
                    raise SyntaxException

            if res_name is None or count_str is None:
                raise SyntaxException

            if count_str.startswith("-"):
                count_str = count_str[1:]
                rel_flag = True
            count = 0
            try:
                count = int(count_str)
            except ValueError:
                raise SyntaxException

            if count < 1:
                raise SyntaxException

            self.dbus_init()
            server_rc = self._server.auto_reduce(dbus.String(res_name),
              dbus.Int32(count), dbus.Boolean(rel_flag))
            if server_rc == 0:
                fn_rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_reduce()
        return fn_rc


    def syntax_reduce(self):
        sys.stderr.write("Syntax: reduce <resource> [-]<redundancy-count>\n")
        sys.stderr.write("    The redundancy count specifies the number of\n"
          "    nodes to which the resource should be deployed. It must be\n"
          "    less than the number of nodes the resource is currently\n"
          "    assigned to and must be at least one.\n"
          "    If the redundancy count is prepended with a minus sign (-),\n"
          "    the resource is undeployed from the specified number\n"
          "    of nodes.\n")


    def cmd_undeploy(self, args):
        fn_rc = 1
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
                quiet = self.user_confirm("You are going to undeploy this "
                  "resource from all nodes of the cluster.\n"
                  "Please confirm:")
            if quiet:
                self.dbus_init()
                server_rc = self._server.auto_undeploy(dbus.String(res_name),
                  dbus.Boolean(force))
                if server_rc == 0:
                    fn_rc = 0
                else:
                    self.error_msg_text(server_rc)
            else:
                fn_rc = 0
        except SyntaxException:
            self.syntax_undeploy()
        return fn_rc


    def syntax_undeploy(self):
        sys.stderr.write("Syntax: undeploy [ --quiet | -q ] <resource>\n")


    def cmd_update_pool(self, args):
        fn_rc = 1
        self.dbus_init()
        server_rc = self._server.update_pool()
        if server_rc == 0:
            fn_rc = 0
        else:
            self.error_msg_text(server_rc)
        return fn_rc


    def cmd_reconfigure(self, args):
        fn_rc = 1
        self.dbus_init()
        server_rc = self._server.reconfigure()
        if server_rc == 0:
            fn_rc = 0
        else:
            self.error_msg_text(server_rc)
        return fn_rc


    def cmd_save(self, args):
        fn_rc = 1
        self.dbus_init()
        server_rc = self._server.save_conf()
        if server_rc == 0:
            fn_rc = 0
        else:
            self.error_msg_text(server_rc)
        return fn_rc


    def cmd_load(self, args):
        fn_rc = 1
        self.dbus_init()
        server_rc = self._server.load_conf()
        if server_rc == 0:
            fn_rc = 0
        else:
            self.error_msg_text(server_rc)
        return fn_rc


    def cmd_unassign(self, args):
        fn_rc = 1
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
            self.dbus_init()
            server_rc = self._server.unassign(node_name, vol_name, force)
            if server_rc == 0:
                fn_rc = 0
            else:
                self.error_msg_text(server_rc)
        else:
            self.syntax_unassign()
        return fn_rc


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
                    self.dbus_init()
                    self._server.shutdown()
                except dbus.exceptions.DBusException:
                    # An exception is expected here, as the server
                    # probably will not answer
                    pass
                # Continuing the client without a server
                # does not make sense, therefore exit
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

        self.dbus_init()

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
                    poolsize = SizeCalc.convert(view.get_poolsize(),
                      SizeCalc.UNIT_kiB, SizeCalc.UNIT_MiB)
                    poolfree = SizeCalc.convert(view.get_poolfree(),
                      SizeCalc.UNIT_kiB, SizeCalc.UNIT_MiB)
                    if poolsize >= 0:
                        if poolsize < 1:
                            poolsize_text = "< 1"
                        else:
                            poolsize_text = str(poolsize)
                    else:
                        poolsize_text = "unknown"
                    if poolfree >= 0:
                        if poolfree < 1:
                            poolfree_text = "< 1"
                        else:
                            poolfree_text = str(poolfree)
                    else:
                        poolfree_text = "unknown"
                    sys.stdout.write("%s%-*s%s %-12s %-34s %s%s%s\n"
                      % (color(COLOR_TEAL), view.get_name_maxlen(),
                        view.get_name(), color(COLOR_NONE), view.get_addrfam(),
                        view.get_addr(), color(COLOR_RED), view.get_state(),
                        color(COLOR_NONE))
                      )
                    sys.stdout.write("  %s* pool size: %14s / free: %14s%s\n"
                      % (color(COLOR_BROWN),
                      poolsize_text, poolfree_text,
                      color(COLOR_NONE))
                      )
                else:
                    sys.stdout.write("%s,%s,%s,%d,%d,%s\n"
                      % (view.get_name(), view.get_addrfam(),
                        view.get_addr(), view.get_poolsize(),
                        view.get_poolfree(), view.get_state())
                      )
            except IncompatibleDataException:
                sys.stderr.write("Warning: incompatible table entry skipped\n")
                continue
        return 0


    def syntax_list_nodes(self):
        sys.stderr.write("Syntax: nodes [ --machine-readable | -m ]\n")


    def cmd_list_resources(self, args):
        return self._list_resources(args, False)


    def cmd_list_volumes(self, args):
        return self._list_resources(args, True)


    def _list_resources(self, args, list_volumes):
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

        self.dbus_init()

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
            if list_volumes:
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
                if list_volumes:
                    volume_list = view.get_volumes()
                    for vol_view in volume_list:
                        if not machine_readable:
                            size_MiB = SizeCalc.convert(
                              vol_view.get_size_kiB(),
                              SizeCalc.UNIT_kiB, SizeCalc.UNIT_MiB)
                            if size_MiB < 1:
                                size_MiB_str = "< 1"
                            else:
                                size_MiB_str = str(size_MiB)
                            sys.stdout.write(
                              "  %s*%s%6d%s %14s %7s %s%s\n"
                                % (color(COLOR_BROWN), color(COLOR_DARKPINK),
                                vol_view.get_id(), color(COLOR_BROWN),
                                size_MiB_str,
                                vol_view.get_minor(), vol_view.get_state(),
                                color(COLOR_NONE))
                              )
                        else:
                            sys.stdout.write(
                              "%s,%s,%d,%d,%s,%s,%s\n" % (view.get_name(),
                                view.get_state(), vol_view.get_id(),
                                vol_view.get_size_kiB(), view.get_port(),
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

        self.dbus_init()

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
        fn_rc = 1
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

            self.dbus_init()
            server_rc = self._server.export_conf(dbus.String(res_name))
            if server_rc == 0:
                fn_rc = 0
            else:
                self.error_msg_text(server_rc)
        except SyntaxException:
            self.syntax_export_conf()
        return fn_rc


    def cmd_ping(self, args):
        fn_rc = 1
        try:
            self.dbus_init()
            server_rc = self._server.ping()
            if server_rc == 0:
                sys.stdout.write("pong\n")
                fn_rc = 0
        except dbus.exceptions.DBusException:
            sys.stderr.write("drbdmanage: cannot connect to the drbdmanage "
              "server through D-Bus.\n")
        return fn_rc


    def cmd_init(self, args):
        """
        Initializes a new drbdmanage cluster
        """
        fn_rc = 1
        order    = [ "address" ]
        params   = {}
        opt      = { "-p" : str(DRBDCTRL_DEFAULT_PORT), "-a" : None }
        optalias = { "--port" : "-p", "--address-family" : "-a" }
        flags    = { "-q" : False }
        flagsalias = { "--quiet" : "-q" }

        try:
            if CommandParser().parse(args, order, params, opt, optalias,
                flags, flagsalias) != 0:
                    raise SyntaxException

            # BEGIN Setup drbdctrl resource properties
            node_name = None
            try:
                uname = os.uname()
                if len(uname) >= 2:
                    node_name = uname[1]
            except OSError:
                pass
            if node_name is None:
                raise AbortException

            af   = opt["-a"]
            if af is None:
                af = drbdmanage.drbd.drbdcore.DrbdNode.AF_IPV4_LABEL
            address = params["address"]
            if address is None:
                raise SyntaxException

            port  = opt["-p"]
            try:
                port_nr = int(port)
                if port_nr < 1 or port_nr > 65535:
                    raise ValueError
            except ValueError:
                sys.stderr.write("Invalid port number\n")
                raise AbortException
            quiet = flags["-q"]
            # END Setup drbdctrl resource properties

            if not quiet:
                quiet = self.user_confirm(
                    "You are going to initalize a new drbdmanage cluster.\n"
                    "CAUTION! Note that:\n"
                    "  * Any previous drbdmanage cluster information may be "
                    "removed\n"
                    "  * Any remaining resources managed by a previous "
                    "drbdmanage installation\n"
                    "    that still exist on this system will no longer be "
                    "managed by drbdmanage\n"
                    "\n"
                    "Confirm:\n"
                )
            if quiet:
                # ========================================
                # Cleanup
                # ========================================
                self._init_join_cleanup()

                # ========================================
                # Initialize a new drbdmanage cluster
                # ========================================

                # Create the .drbdctrl LV
                self._ext_command(["lvcreate", "-n", DRBDCTRL_RES_NAME,
                    "-L", "4m", DEFAULT_VG])

                # Create meta-data
                self._ext_command(["drbdmeta", "--force", "0",
                    "v09", self.DRBDCTRL_BLOCKDEV, "internal",
                    "create-md", "31"])

                # Configure the .drbdctrl resource
                self._ext_command(["drbdsetup", "new-resource",
                    DRBDCTRL_RES_NAME, "0"])
                self._ext_command(["drbdsetup", "new-minor", DRBDCTRL_RES_NAME,
                    "0", "0"])
                self._ext_command(["drbdmeta", "0", "v09",
                    self.DRBDCTRL_BLOCKDEV, "internal", "apply-al"])
                self._ext_command(["drbdsetup", "attach", "0",
                    self.DRBDCTRL_BLOCKDEV,
                    self.DRBDCTRL_BLOCKDEV, "internal"])
                self._ext_command(["drbdsetup",
                    "primary", DRBDCTRL_RES_NAME, "--force"])
                init_rc = self._drbdctrl_init(DRBDCTRL_DEV)
                if init_rc != 0:
                    # an error message is printed by _drbdctrl_init()
                    raise AbortException
                self._ext_command(["drbdsetup", "secondary",
                    DRBDCTRL_RES_NAME])


                props = {}
                props[NODE_ADDR] = address
                props[NODE_AF]   = af
                # Startup the drbdmanage server and add the current node
                self.dbus_init()
                server_rc = self._server.init_node(
                    dbus.String(node_name), props,
                    dbus.String(self.DRBDCTRL_BLOCKDEV), str(port)
                )

                if server_rc == 0:
                    fn_rc = 0
                else:
                    fn_rc = 1
            else:
                fn_rc = 0
        except AbortException:
            sys.stderr.write("Initialization failed\n")
            self._init_join_rollback()
        except SyntaxException:
            sys.stderr.write("Syntax: ipaddress [ { -p | --port } port\n")
        return fn_rc


    def cmd_join(self, args):
        """
        Joins an existing drbdmanage cluster
        """
        fn_rc = 1
        order    = [ "local_ip", "local_node_id", "peer_ip", "peer_name",
            "peer_node_id", "secret" ]
        params   = {}
        opt      = { "-p" : str(DRBDCTRL_DEFAULT_PORT), "-a" : None }
        optalias = { "--port" : "-p", "--address-family" : "-a" }
        flags    = { "-q" : False }
        flagsalias = { "--quiet" : "-q" }

        bdev = self.DRBDCTRL_BLOCKDEV
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
                flags, flagsalias) != 0:
                    raise SyntaxException

            # BEGIN Setup drbdctrl resource properties
            node_name = None
            try:
                uname = os.uname()
                if len(uname) >= 2:
                    node_name = uname[1]
            except OSError:
                pass
            if node_name is None:
                raise AbortException
            af   = opt["-a"]
            if af is None:
                af = drbdmanage.drbd.drbdcore.DrbdNode.AF_IPV4_LABEL
            port  = opt["-p"]
            try:
                port_nr = int(port)
                if port_nr < 1 or port_nr > 65535:
                    raise ValueError
            except ValueError:
                sys.stderr.write("Invalid port number\n")
                raise AbortException
            quiet = flags["-q"]
            # END Setup drbdctrl resource properties

            if not quiet:
                quiet = self.user_confirm(
                    "You are going to join an existing drbdmanage cluster.\n"
                    "CAUTION! Note that:\n"
                    "  * Any previous drbdmanage cluster information may be "
                    "removed\n"
                    "  * Any remaining resources managed by a previous "
                    "drbdmanage installation\n"
                    "    that still exist on this system will no longer be "
                    "managed by drbdmanage\n"
                    "\n"
                    "Confirm:\n"
                )
            if quiet:
                # ========================================
                # Cleanup
                # ========================================
                self._init_join_cleanup()

                # ========================================
                # Join an existing drbdmanage cluster
                # ========================================

                # Create the .drbdctrl LV
                self._ext_command(["lvcreate", "-n",
                    DRBDCTRL_RES_NAME, "-L", "4m", DEFAULT_VG])

                # Create meta-data
                self._ext_command(["drbdmeta", "--force", "0",
                    "v09", self.DRBDCTRL_BLOCKDEV, "internal",
                    "create-md", "31"])

                l_addr    = params["local_ip"]
                p_addr    = params["peer_ip"]
                p_name    = params["peer_name"]
                l_node_id = params["local_node_id"]
                p_node_id = params["peer_node_id"]
                secret    = params["secret"]

                # Configure the .drbdctrl resource
                self._ext_command(["drbdsetup", "new-resource",
                    DRBDCTRL_RES_NAME, l_node_id])
                self._ext_command(["drbdsetup", "new-minor",
                    DRBDCTRL_RES_NAME, "0", "0"])
                self._ext_command(["drbdmeta", "0", "v09",
                    self.DRBDCTRL_BLOCKDEV, "internal", "apply-al"])
                self._ext_command(["drbdsetup", "attach", "0",
                    self.DRBDCTRL_BLOCKDEV, self.DRBDCTRL_BLOCKDEV, "internal"])

                umh_f = None
                umh   = None
                try:
                    umh_f = open(
                        self.UMHELPER_FILE, "r")
                    umh = umh_f.read(8192)
                    umh_f.close()
                    umh_f = None
                    umh_f = open(
                        self.UMHELPER_FILE, "w")
                    umh_f.write(self.UMHELPER_OVERRIDE)
                except (IOError, OSError) as err:
                    print(err)
                    raise AbortException
                finally:
                    if umh_f is not None:
                        try:
                            umh_f.close()
                        except (IOError, OSError):
                            pass

                proc_rc = self._ext_command(["drbdsetup", "connect",
                    DRBDCTRL_RES_NAME,
                    "ipv4:" + l_addr + ":" + str(port),
                    "ipv4:" + p_addr + ":" + str(port),
                    "--peer-node-id=" + p_node_id,
                    "--_name=" + p_name,
                    "--shared-secret=" + secret,
                    "--cram-hmac-alg=sha256",
                    "--ping-timeout=30",
                    "--protocol=C"])

                umh_f = None
                if umh is not None:
                    try:
                        umh_f = open(
                        self.UMHELPER_FILE, "w")
                        umh_f.write(umh)
                    except (IOError, OSError) as err:
                        print(err)
                        raise AbortException
                    finally:
                        if umh_f is not None:
                            try:
                                umh_f.close()
                            except (IOError, OSError):
                                pass

                # Startup the drbdmanage server and update the local .drbdctrl
                # resource configuration file
                self.dbus_init()
                # server_rc = self._server.update_res()
                server_rc = self._server.join_node(bdev, port, secret)
                #server_rc = self._server.debug_console(dbus.String(
                #    "gen drbdctrl " + secret + " " + port + " " + bdev
                #))
                if server_rc == 0:
                    fn_rc = 0
                else:
                    fn_rc = 1
            else:
                fn_rc = 0
        except AbortException:
            sys.stderr.write("Initialization failed\n")
            self._init_join_rollback()
        except SyntaxException:
            sys.stderr.write("Syntax: local_ip local_node_id peer_ip peer_name "
                "peer_node_id secret\n")
        return fn_rc


    def _init_join_cleanup(self):
        """
        Cleanup before init / join operations

        Notice: Caller should handle AbortException
        """
        # Shut down any existing drbdmanage control volume
        self._ext_command(["drbdsetup", "down", DRBDCTRL_RES_NAME])

        # Delete any existing .drbdctrl LV
        self._ext_command(["lvremove", "--force", DEFAULT_VG + "/"
            + DRBDCTRL_RES_NAME])

        # Delete any existing configuration file
        try:
            os.unlink(build_path(DRBDCTRL_RES_PATH, DRBDCTRL_RES_FILE))
        except OSError:
            pass


    def _init_join_rollback(self):
        """
        Attempts cleanup after a failed init or join operation
        """
        try:
            self._ext_command(["drbdsetup", "down",
                DRBDCTRL_RES_NAME])
        except AbortException:
            pass
        try:
            self._ext_command(["lvremove", "--force", DEFAULT_VG + "/" +
                DRBDCTRL_RES_NAME])
        except AbortException:
            pass


    def cmd_initcv(self, args):
        fn_rc = 1
        # Command parser configuration
        order    = [ "dev" ]
        params   = {}
        opt      = {}
        optalias = {}
        flags    = { "-q" : False }
        flagsalias = { "--quiet" : "-q" }
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException

            drbdctrl_file = params["dev"]
            quiet         = flags["-q"]

            if not quiet:
                quiet = self.user_confirm((
                    "You are going to initalize a new "
                    "drbdmanage control volume on:\n"
                    "  %s\n"
                    "CAUTION! Note that:\n"
                    "  * Any previous drbdmanage cluster information may be "
                    "removed\n"
                    "  * Any remaining resources managed by a previous "
                    "drbdmanage installation\n"
                    "    that still exist on this system will no longer be "
                    "managed by drbdmanage\n"
                    "\n"
                    "Confirm:\n"
                    % drbdctrl_file))
            if quiet:
                fn_rc = self._drbdctrl_init(drbdctrl_file)
            else:
                fn_rc = 0
        except SyntaxException:
            self.syntax_init()
        return fn_rc


    def _ext_command(self, args):
        """
        Run external commands in a subprocess
        """
        proc_rc = 127
        try:
            ext_proc = subprocess.Popen(args, 0, None, close_fds=True)
            proc_rc = ext_proc.wait()
        except OSError as oserr:
            if oserr.errno == errno.ENOENT:
                sys.stderr.write("Cannot find command: %s\n" % args[0])
            elif oserr.errno == errno.EACCES:
                sys.stderr.write("Cannot execute %s, "
                  "permission denied" % args[0])
            else:
                sys.stderr.write("Cannot execute %s, "
                  "error returned by the OS is: "
                  "%s\n" % (args[0], oserr.strerror))
            raise AbortException
        return proc_rc


    def print_sub_commands(self):
        (term_width, term_height) = get_terminal_size()
        columns = term_width / 20;
        i = 0

        for cmd_name in self.COMMANDS:
            if len(cmd_name) == 1:
                continue # ignore those one character aliases for now
            i = i + 1
            if i % columns:
                sys.stdout.write("  ")
            sys.stdout.write("{:<18}".format(cmd_name))
            if not (i % columns):
                sys.stdout.write("\n")

        if i % columns:
            sys.stdout.write("\n")
        return 0


    def cmd_usage(self, args):
        (term_width, term_height) = get_terminal_size()
        columns = term_width / 23;
        i = 0
        sys.stdout.write("Usage: drbdmanage [options...] command [args...]\n"
                         "\n"
                         "where command is one out of:\n")

        self.print_sub_commands()
        return 0


    def syntax_init(self):
        sys.stderr.write("Syntax: init [ -q | --quiet ] device\n")


    def cmd_exit(self, args):
        exit(0)


    def cmd_debug(self, args):
        fn_rc = 1
        command = ""
        first   = True
        while True:
            arg = args.next_arg()
            if arg is not None:
                if first:
                    first = False
                else:
                    command += " "
                command += arg
            else:
                break
        try:
            self.dbus_init()
            fn_rc = self._server.debug_console(dbus.String(command))
            sys.stderr.write("%s fn_rc=%d\n" % (command, fn_rc))
        except dbus.exceptions.DBusException:
            sys.stderr.write("drbdmanage: cannot connect to the drbdmanage "
              "server through D-Bus.\n")
        return fn_rc


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
        fn_rc = False
        while True:
            answer = sys.stdin.readline()
            if len(answer) != 0:
                if answer.endswith("\n"):
                    answer = answer[:len(answer) - 1]
                if answer == "yes":
                    fn_rc = True
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
        return fn_rc


    def error_msg_text(self, error):
        sys.stderr.write("Error: " + dm_exc_text(error) + "\n")


    def color(self, col):
        if self._colors:
            return col
        else:
            return ""


    def _drbdctrl_init(self, drbdctrl_file):
        fn_rc = 1

        init_blks = 4
        pers_impl = drbdmanage.drbd.persistence.PersistenceImpl
        blksz     = pers_impl.BLKSZ

        index_name = pers_impl.IDX_NAME
        index_off  = pers_impl.IDX_OFFSET
        hash_off   = pers_impl.HASH_OFFSET
        data_off   = pers_impl.DATA_OFFSET

        assg_len_name  = pers_impl.ASSG_LEN_NAME
        assg_off_name  = pers_impl.ASSG_OFF_NAME
        nodes_len_name = pers_impl.NODES_LEN_NAME
        nodes_off_name = pers_impl.NODES_OFF_NAME
        res_len_name   = pers_impl.RES_LEN_NAME
        res_off_name   = pers_impl.RES_OFF_NAME

        drbdctrl = None
        try:
            data_hash = drbdmanage.utils.DataHash()

            index_str = (
                    "{\n"
                    "    \"" + index_name + "\": {\n"
                    "        \"" + assg_len_name + "\": 3,\n"
                    "        \"" + assg_off_name + "\": "
                    + str(data_off) + ",\n"
                    "        \"" + nodes_len_name + "\": 3,\n"
                    "        \"" + nodes_off_name + "\": "
                    + str(data_off) + ",\n"
                    "        \"" + res_len_name + "\": 3,\n"
                    "        \"" + res_off_name + "\": "
                    + str(data_off) + "\n"
                    "    }\n"
                    "}\n"
            )
            data_str = "{}\n"

            pos = 0
            while pos < 3:
                data_hash.update(data_str)
                pos += 1

            drbdctrl = open(drbdctrl_file, "rb+")
            zeroblk  = bytearray('\0' * blksz)
            pos      = 0
            while pos < init_blks:
                drbdctrl.write(zeroblk)
                pos += 1
            drbdctrl.seek(index_off)
            drbdctrl.write(index_str)
            drbdctrl.seek(data_off)
            drbdctrl.write(data_str)
            drbdctrl.seek(hash_off)
            drbdctrl.write(
                "{\n"
                "    \"hash\": \"" + data_hash.get_hex_hash() + "\"\n"
                "}\n"
            )
            fn_rc = 0
        except IOError as ioexc:
            sys.stderr.write("Initialization of the control volume failed: "
                "%s\n" % str(ioexc))
        finally:
            if drbdctrl is not None:
                try:
                    drbdctrl.close()
                except IOError:
                    pass
        sys.stdout.write("empty drbdmanage control volume initialized.\n")

        return fn_rc


    COMMANDS = {
        "assignments"       : cmd_list_assignments,
        "a"                 : cmd_list_assignments,
        "resources"         : cmd_list_resources,
        "r"                 : cmd_list_resources,
        "volumes"           : cmd_list_volumes,
        "v"                 : cmd_list_volumes,
        "nodes"             : cmd_list_nodes,
        "n"                 : cmd_list_nodes,
        "new-node"          : cmd_new_node,
        "remove-node"       : cmd_remove_node,
        "new-volume"        : cmd_new_volume,
        "new-resource"      : cmd_new_resource,
        "modify-resource"   : cmd_modify_resource,
        "remove-volume"     : cmd_remove_volume,
        "remove-resource"   : cmd_remove_resource,
        "connect"           : cmd_connect,
        "reconnect"         : cmd_connect,
        "disconnect"        : cmd_disconnect,
        "flags"             : cmd_flags,
        "attach"            : cmd_attach,
        "detach"            : cmd_detach,
        "assign"            : cmd_assign,
        "unassign"          : cmd_unassign,
        "deploy"            : cmd_deploy,
        "extend"            : cmd_extend,
        "reduce"            : cmd_reduce,
        "undeploy"          : cmd_undeploy,
        "reconfigure"       : cmd_reconfigure,
        "update-pool"       : cmd_update_pool,
        "save"              : cmd_save,
        "load"              : cmd_load,
        "shutdown"          : cmd_shutdown,
        "export"            : cmd_export_conf,
        "ping"              : cmd_ping,
        "initcv"            : cmd_initcv,
        "exit"              : cmd_exit,
        "usage"             : cmd_usage,
        "init"              : cmd_init,
        "join"              : cmd_join
      }


def main():
    client = DrbdManage()
    client.run()

if __name__ == "__main__":
    main()
