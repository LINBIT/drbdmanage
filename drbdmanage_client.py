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
import errno
import dbus
import subprocess
import time
import traceback
import drbdmanage.drbd.drbdcore
import drbdmanage.drbd.persistence

from drbdmanage.consts import (SERVER_CONFFILE, KEY_DRBDCTRL_VG, DEFAULT_VG,
    DRBDCTRL_DEFAULT_PORT, DRBDCTRL_DEV, DRBDCTRL_RES_NAME, DRBDCTRL_RES_FILE,
    DRBDCTRL_RES_PATH, NODE_ADDR, NODE_AF, NODE_ID, NODE_POOLSIZE,
    NODE_POOLFREE, RES_PORT, VOL_MINOR, VOL_BDEV, RES_PORT_NR_AUTO,
    FLAG_DISKLESS, FLAG_OVERWRITE, FLAG_DISCARD, FLAG_CONNECT)
from drbdmanage.utils import ArgvReader, CmdLineReader, CommandParser
from drbdmanage.utils import SizeCalc
from drbdmanage.utils import (get_terminal_size, build_path, bool_to_string,
    map_val_or_dflt)
from drbdmanage.utils import (COLOR_NONE, COLOR_RED, COLOR_DARKRED,
    COLOR_DARKGREEN, COLOR_BROWN, COLOR_DARKPINK, COLOR_TEAL)
from drbdmanage.conf.conffile import ConfFile
from drbdmanage.exceptions import AbortException
from drbdmanage.exceptions import IncompatibleDataException
from drbdmanage.exceptions import SyntaxException
from drbdmanage.exceptions import dm_exc_text
from drbdmanage.exceptions import (DM_SUCCESS, DM_EEXIST)
from drbdmanage.dbusserver import DBusServer
from drbdmanage.drbd.drbdcore import DrbdResource
from drbdmanage.drbd.drbdcore import Assignment
from drbdmanage.drbd.drbdcore import DrbdResource
from drbdmanage.drbd.views import AssignmentView
from drbdmanage.drbd.views import DrbdNodeView
from drbdmanage.drbd.views import DrbdResourceView
from drbdmanage.drbd.views import DrbdVolumeView
from drbdmanage.drbd.views import DrbdVolumeStateView
from drbdmanage.storage.storagecore import MinorNr
from drbdmanage.defaultip import default_ip


class DrbdManage(object):

    """
    drbdmanage dbus client, the CLI for controlling the drbdmanage server
    """

    _server = None
    _interactive = False
    _noerr       = False
    _colors      = True

    VIEW_SEPARATOR_LEN = 78

    UMHELPER_FILE      = "/sys/module/drbd/parameters/usermode_helper"
    UMHELPER_OVERRIDE  = "/bin/true"
    UMHELPER_WAIT_TIME = 5.0

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
            # ignore remarks lines
            if not cmdline.startswith("#"):
                if cmdline.endswith("\n"):
                    cmdline = cmdline[:len(cmdline) - 1]
                args = CmdLineReader(cmdline)
                arg = args.peek_arg()
                # ignore empty lines
                if arg is not None:
                    fn_rc = self.exec_cmd(args, True)
                    if fn_rc != 0 and self._interactive:
                        sys.stderr.write("  %sOperation failed%s\n"
                              % (color(COLOR_RED), color(COLOR_NONE)))
                    if (fn_rc != 0 and
                        not self._interactive and
                        not self._noerr):
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


    def cmd_poke(self, args):
        fn_rc = 1
        self.dbus_init()
        server_rc = self._server.poke()
        fn_rc = self._list_rc_entries(server_rc)
        return fn_rc


    def cmd_new_node(self, args):
        fn_rc = 1
        # Command parser configuration
        order      = [ "name", "ip" ]
        params     = {}
        opt        = { "-a" : None }
        optalias   = { "--address-family" : "a" }
        flags      = { "-q" : False }
        flagsalias = { "--quiet" : "-q" }
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
            fn_rc = self._list_rc_entries(server_rc)

            if fn_rc == 0:
                server_rc, joinc = self._server.text_query(["joinc", name])
                joinc_text = str(" ".join(joinc))

                # Text queries do not return error codes, so check whether the
                # string returned by the server looks like a join command or
                # like an error message
                if joinc_text.startswith("Error:"):
                    sys.stderr.write(joinc_text + "\n")
                else:
                    try:
                        sshc = ["ssh", "-oBatchMode=yes", "-oConnectTimeout=2",
                                "root@" + ip]
                        if subprocess.call(sshc + ["true"]) == 0:
                            sys.stdout.write("\nExecuting join command on "
                                             "%s using ssh.\n" % (name))
                            ssh_joinc = (sshc + joinc +
                                         ([ "-q" ] if flags["-q"] else []))
                            subprocess.check_call(ssh_joinc)
                        else:
                            sys.stdout.write("\nJoin command for node %s:\n"
                                             "%s\n" % (name, joinc_text))
                    except subprocess.CalledProcessError:
                        sys.stderr.write("Error: Attempt to execute the " +
                                         "join command remotely failed\n")

            fn_rc = self._list_rc_entries(server_rc)

        else:
            self.syntax_new_node()
        return fn_rc


    def syntax_new_node(self):
        sys.stderr.write("Syntax: new-node [ options ] <name> <ip>\n")
        sys.stderr.write("  Options:\n")
        sys.stderr.write("    --address-family | -a : { ipv4 | ipv6 }\n")


    def cmd_new_resource(self, args):
        fn_rc    = 1
        port  = RES_PORT_NR_AUTO
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
            fn_rc = self._list_rc_entries(server_rc)
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
            (size_digits, unit_suffix) = self.split_number_unit(size_str)
            try:
                size = long(size_digits)
            except Exception:
                sys.stderr.write("Error: <size> must be a number\n")
                raise SyntaxException

            if unit_suffix is not None:
                try:
                    unit_suffix_sel = self.UNITS_MAP[unit_suffix.lower()]
                except KeyError:
                    raise SyntaxException
            if unit_str is not None:
                try:
                    unit_str_sel = self.UNITS_MAP[unit_str.lower()]
                except KeyError:
                    raise SyntaxException

            if unit_str is None:
                if unit_suffix is None:
                    # no unit selected, default to GiB
                    unit = SizeCalc.UNIT_GiB
                else:
                    # no unit parameter, but unit suffix present
                    # use unit suffix
                    unit = unit_suffix_sel
            else:
                if unit_suffix is None:
                    # unit parameter set, but no unit suffix present
                    # use unit parameter
                    unit = unit_str_sel
                else:
                    # unit parameter set AND unit suffix present
                    if unit_str_sel != unit_suffix_sel:
                        # unit parameter and unit suffix disagree about the
                        # selected unit, abort
                        sys.stderr.write("Error: unit parameter and size "
                            "suffix mismatch\n")
                        raise SyntaxException
                    else:
                        # unit parameter and unit suffix agree about the
                        # selected unit
                        unit = unit_str_sel

            if unit != SizeCalc.UNIT_kiB:
                size = SizeCalc.convert_round_up(size, unit,
                  SizeCalc.UNIT_kiB)

            props = dbus.Dictionary(signature="ss")

            self.dbus_init()
            server_rc = self._server.create_resource(dbus.String(name),
              props)
            for rc_entry in server_rc:
                try:
                    rc_num, rc_fmt, rc_args = rc_entry
                    if rc_num == 0 or rc_num == DM_EEXIST:
                        fn_rc = 0
                    else:
                        sys.stderr.write("%s\n" % dm_exc_text(rc_num))
                except (TypeError, ValueError):
                    pass

            if fn_rc == 0:
                props = dbus.Dictionary(signature="ss")
                props[VOL_MINOR] = str(minor)
                server_rc = self._server.create_volume(
                    dbus.String(name),
                    dbus.Int64(size), props
                )
                fn_rc = self._list_rc_entries(server_rc)

                if fn_rc == 0 and deploy is not None:
                    server_rc = self._server.auto_deploy(
                        dbus.String(name),
                        dbus.Int32(deploy), dbus.Int32(0),
                        dbus.Boolean(False)
                    )
                    fn_rc = self._list_rc_entries(server_rc)
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
        port  = RES_PORT_NR_AUTO
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
            fn_rc = self._list_rc_entries(server_rc)
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
                fn_rc = self._list_rc_entries(server_rc)
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
                fn_rc = self._list_rc_entries(server_rc)
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
                fn_rc = self._list_rc_entries(server_rc)
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
            fn_rc = self._list_rc_entries(server_rc)
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
              dbus.String(res_name), dbus.Boolean(False))
            fn_rc = self._list_rc_entries(server_rc)
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
              dbus.String(res_name), dbus.UInt64(0), dbus.UInt64(0),
              dbus.UInt64(clear_mask), dbus.UInt64(set_mask))
            fn_rc = self._list_rc_entries(server_rc)
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
            fn_rc = self._list_rc_entries(server_rc)
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
            fn_rc = self._list_rc_entries(server_rc)
        except SyntaxException:
            self.syntax_detach()
        return fn_rc


    def syntax_detach(self):
        sys.stderr.write("Syntax: detach <node> <resource> <id>\n")


    def cmd_assign(self, args):
        fn_rc  = 1
        cstate = 0
        tstate = 0
        # Command parser configuration
        order    = [ "node", "res" ]
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
            res_name  = params["res"]

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

            props = {}
            props[FLAG_DISKLESS]  = bool_to_string(client)
            props[FLAG_OVERWRITE] = bool_to_string(overwrite)
            props[FLAG_DISCARD]   = bool_to_string(discard)
            props[FLAG_CONNECT]   = bool_to_string(connect)

            self.dbus_init()
            server_rc = self._server.assign(dbus.String(node_name),
              dbus.String(res_name), props)
            fn_rc = self._list_rc_entries(server_rc)
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


    def cmd_free_space(self, args):
        fn_rc    = 1
        # Command parser configuration
        order    = [ "redundancy" ]
        params   = {}
        opt      = {}
        optalias = {}
        flags    = { "-m" : False }
        flagsalias = { "--machine-readable" : "-m" }
        try:
            if CommandParser().parse(args, order, params, opt, optalias,
              flags, flagsalias) != 0:
                raise SyntaxException

            redundancy_str = params["redundancy"]
            redundancy = 0
            try:
                redundancy = int(redundancy_str)
            except ValueError:
                raise SyntaxException

            if redundancy < 1:
                raise SyntaxException

            self.dbus_init()
            server_rc, free_space = (
                self._server.cluster_free_query(dbus.Int32(redundancy))
            )

            successful = self._is_rc_successful(server_rc)
            if successful:
                machine_readable = flags["-m"]
                if machine_readable:
                    sys.stdout.write("%lu\n" % (free_space))
                else:
                    sys.stdout.write(
                        "The maximum size for a %dx redundant "
                        "volume is %lu kB\n"
                        % (redundancy, free_space)
                    )
            fn_rc = self._list_rc_entries(server_rc)
        except SyntaxException:
            self.syntax_free_space()
        return fn_rc


    def syntax_free_space(self):
        sys.stderr.write(
            "Syntax: free-space [ --machine-readable | -m ] "
            "<redundancy-count>\n"
        )
        sys.stderr.write("    Queries the maximum size of a volume that "
          "could be\n    deployed with the specified level of redundancy\n")


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
              dbus.Int32(count), dbus.Int32(0), dbus.Boolean(False))
            fn_rc = self._list_rc_entries(server_rc)
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
            num_str = params["count"]
            if num_str.startswith("+"):
                num_str = num_str[1:]
                rel_flag = True
            num = 0
            try:
                num = int(num_str)
            except ValueError:
                raise SyntaxException

            if num < 1:
                raise SyntaxException

            if rel_flag:
                count = 0
                delta = num
            else:
                count = num
                delta = 0

            self.dbus_init()
            server_rc = self._server.auto_deploy(dbus.String(res_name),
              dbus.Int32(count), dbus.Int32(delta), dbus.Boolean(False))
            fn_rc = self._list_rc_entries(server_rc)
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
        # FIXME: illegal statement somewhere in here
        fn_rc    = 1
        try:
            res_name  = None
            num_str = None
            while True:
                arg = args.next_arg()
                if arg is None:
                    break
                if res_name is None:
                    res_name = arg
                elif num_str is None:
                    num_str = arg
                else:
                    raise SyntaxException

            if res_name is None or num_str is None:
                raise SyntaxException

            num = 0
            try:
                num = int(num_str)
            except ValueError:
                raise SyntaxException

            if num == 0:
                raise SyntaxException

            if num < 0:
                count = 0
                delta = num
            else:
                count = num
                delta = 0

            self.dbus_init()
            server_rc = self._server.auto_deploy(dbus.String(res_name),
              dbus.Int32(count), dbus.Int32(delta), dbus.Boolean(False))
            fn_rc = self._list_rc_entries(server_rc)
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
                fn_rc = self._list_rc_entries(server_rc)
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
        server_rc = self._server.update_pool(dbus.Array([], signature="s"))
        fn_rc = self._list_rc_entries(server_rc)
        return fn_rc


    def cmd_reconfigure(self, args):
        fn_rc = 1
        self.dbus_init()
        server_rc = self._server.reconfigure()
        fn_rc = self._list_rc_entries(server_rc)
        return fn_rc


    def cmd_save(self, args):
        fn_rc = 1
        self.dbus_init()
        server_rc = self._server.save_conf()
        fn_rc = self._list_rc_entries(server_rc)
        return fn_rc


    def cmd_load(self, args):
        fn_rc = 1
        self.dbus_init()
        server_rc = self._server.load_conf()
        fn_rc = self._list_rc_entries(server_rc)
        return fn_rc


    def cmd_unassign(self, args):
        fn_rc = 1
        # Command parser configuration
        order      = [ "node", "res" ]
        params     = {}
        opt        = {}
        optalias   = {}
        flags      = { "-f" : False }
        flagsalias = { "--force" : "-f" }
        if CommandParser().parse(args, order, params, opt, optalias,
          flags, flagsalias) == 0:
            node_name = params["node"]
            res_name  = params["res"]
            force     = flags["-f"]
            self.dbus_init()
            server_rc = self._server.unassign(node_name, res_name, force)
            fn_rc = self._list_rc_entries(server_rc)
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

        server_rc, node_list = self._server.list_nodes(
            dbus.Array([], signature="s"),
            0,
            dbus.Dictionary({}, signature="ss"),
            dbus.Array([], signature="s")
        )

        if (not machine_readable) and (node_list is None
            or len(node_list) == 0):
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

        for node_entry in node_list:
            try:
                node_name, properties = node_entry
                view   = DrbdNodeView(properties, machine_readable)
                v_af   = self._property_text(view.get_property(NODE_AF))
                v_addr = self._property_text(view.get_property(NODE_ADDR))
                if not machine_readable:
                    prop_str = view.get_property(NODE_POOLSIZE)
                    try:
                        poolsize_kiB = int(prop_str)
                        poolsize = SizeCalc.convert(poolsize_kiB,
                            SizeCalc.UNIT_kiB, SizeCalc.UNIT_MiB)
                        if poolsize >= 0:
                            if poolsize_kiB > 0 and poolsize < 1:
                                # less than a megabyte but more than zero kiB
                                poolsize_text = "< 1"
                            else:
                                poolsize_text = str(poolsize)
                        else:
                            poolsize_text = "unknown"
                    except ValueError:
                        poolsize = "n/a"

                    prop_str = view.get_property(NODE_POOLFREE)
                    try:
                        poolfree_kiB = int(prop_str)
                        poolfree = SizeCalc.convert(poolfree_kiB,
                            SizeCalc.UNIT_kiB, SizeCalc.UNIT_MiB)
                        if poolfree >= 0:
                            if poolfree_kiB > 0 and poolfree < 1:
                                # less than a megabyte but more than zero kiB
                                poolfree_text = "< 1"
                            else:
                                poolfree_text = str(poolfree)
                        else:
                            poolfree_text = "unknown"
                    except:
                        poolfree = "n/a"

                    sys.stdout.write("%s%-*s%s %-12s %-34s %s%s%s\n"
                      % (color(COLOR_TEAL), view.get_name_maxlen(),
                        node_name, color(COLOR_NONE), v_af,
                        v_addr, color(COLOR_RED), view.get_state(),
                        color(COLOR_NONE))
                    )
                    sys.stdout.write("  %s* pool size: %14s / free: %14s%s\n"
                      % (color(COLOR_BROWN),
                      poolsize_text, poolfree_text,
                      color(COLOR_NONE))
                    )
                else:
                    v_psize = self._property_text(
                        view.get_property(NODE_POOLSIZE))
                    v_pfree = self._property_text(
                        view.get_property(NODE_POOLFREE))

                    sys.stdout.write("%s,%s,%s,%s,%s,%s\n"
                      % (node_name, v_af,
                        v_addr, v_psize,
                        v_pfree, view.get_state())
                    )
            except IncompatibleDataException:
                sys.stderr.write("Warning: incompatible table entry skipped\n")
        return 0


    def syntax_list_nodes(self):
        sys.stderr.write("Syntax: nodes [ --machine-readable | -m ]\n")


    def cmd_list_resources(self, args):
        return self._list_resources(args, False)


    def cmd_list_volumes(self, args):
        return self._list_resources(args, True)


    def _list_resources(self, args, list_volumes):
        """
        Outputs human- or machine-readable lists of resources or volumes

        For machine readable lists, if a resource list is requested, one line
        per resource is generated; if a volume list is requested, multiple
        lines per resource, containing one line for each volume of the
        resource, are generated.
        For human readable lists, if a resource list is requested, then only
        resources are listed; if a volume list is requested, every resource
        description is followed by a description of all volumes of the
        respective resource.
        """
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

        if list_volumes:
            server_rc, res_list = self._server.list_volumes(
                dbus.Array([], signature="s"),
                0,
                dbus.Dictionary({}, signature="ss"),
                dbus.Array([], signature="s")
            )
        else:
            server_rc, res_list = self._server.list_resources(
                dbus.Array([], signature="s"),
                0,
                dbus.Dictionary({}, signature="ss"),
                dbus.Array([], signature="s")
            )
        if (not machine_readable) and (res_list is None
            or len(res_list) == 0):
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

        for res_entry in res_list:
            try:
                if list_volumes:
                    res_name, properties, vol_list = res_entry
                else:
                    res_name, properties = res_entry
                res_view = DrbdResourceView(properties, machine_readable)
                v_port  = self._property_text(res_view.get_property(RES_PORT))
                if not machine_readable:
                    # Human readable output of the resource description
                    sys.stdout.write(
                        "%s%-*s%s %7s         %s%s%s\n"
                        % (color(COLOR_DARKGREEN),
                        res_view.get_name_maxlen(), res_name,
                        color(COLOR_NONE), v_port,
                        color(COLOR_RED), res_view.get_state(),
                        color(COLOR_NONE))
                    )
                if list_volumes:
                    for vol_entry in vol_list:
                        vol_id, vol_properties = vol_entry
                        vol_view = DrbdVolumeView(vol_properties,
                            machine_readable)
                        v_minor = self._property_text(
                            vol_view.get_property(VOL_MINOR)
                        )
                        if not machine_readable:
                            # human readable output of the volume description
                            size_MiB = SizeCalc.convert(
                                vol_view.get_size_kiB(),
                                SizeCalc.UNIT_kiB, SizeCalc.UNIT_MiB
                            )
                            if size_MiB < 1:
                                size_MiB_str = "< 1"
                            else:
                                size_MiB_str = str(size_MiB)
                            sys.stdout.write(
                                "  %s*%s%6s%s %14s %7s  %s%s\n"
                                % (color(COLOR_BROWN), color(COLOR_DARKPINK),
                                str(vol_view.get_id()), color(COLOR_BROWN),
                                size_MiB_str,
                                v_minor, vol_view.get_state(),
                                color(COLOR_NONE))
                            )
                        else:
                            # machine readable output of the volume description
                            sys.stdout.write(
                                "%s,%s,%s,%d,%s,%s,%s\n"
                                % (res_name, res_view.get_state(),
                                str(vol_view.get_id()),
                                vol_view.get_size_kiB(), v_port,
                                v_minor, vol_view.get_state())
                            )
                elif machine_readable:
                    # machine readable output of the resource description
                    sys.stdout.write(
                        "%s,%s,%s\n"
                        % (res_name, v_port, res_view.get_state())
                    )
            except IncompatibleDataException:
                sys.stderr.write("Warning: incompatible table entry skipped\n")
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

        server_rc, assg_list = self._server.list_assignments(
            dbus.Array([], signature="s"),
            dbus.Array([], signature="s"),
            0,
            dbus.Dictionary({}, signature="ss"),
            dbus.Array([], signature="s")
        )
        if (not machine_readable) and (assg_list is None
            or len(assg_list) == 0):
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
        for assg_entry in assg_list:
            try:
                node_name, res_name, properties, vol_state_list = assg_entry
                view = AssignmentView(properties, machine_readable)
                v_node_id = self._property_text(view.get_property(NODE_ID))
                v_cstate  = view.get_cstate()
                v_tstate  = view.get_tstate()
                if not machine_readable:
                    if node_name != prev_node:
                        prev_node = node_name
                        sys.stdout.write("%s%-*s%s\n"
                            % (color(COLOR_TEAL),
                            DrbdNodeView.get_name_maxlen(), node_name,
                            color(COLOR_NONE))
                          )
                    sys.stdout.write("  %s%-*s%s %5s %35s%s%s -> %s%s\n"
                        % (color(COLOR_DARKGREEN),
                        DrbdResourceView.get_name_maxlen(),
                        res_name, color(COLOR_NONE),
                        v_node_id, "", color(COLOR_RED),
                        v_cstate, v_tstate,
                        color(COLOR_NONE))
                      )

                    for vol_state in vol_state_list:
                        vol_id, properties = vol_state
                        vol_view = DrbdVolumeStateView(properties,
                            machine_readable)
                        v_bdev = self._property_text(
                            vol_view.get_property(VOL_BDEV))

                        sys.stdout.write("  %s* %s%6s%s %-48s %s%s  -> %s%s\n"
                            % (color(COLOR_BROWN), color(COLOR_DARKPINK),
                            vol_id,  color(COLOR_BROWN),
                            v_bdev,
                            color(COLOR_DARKRED), vol_view.get_cstate(),
                            vol_view.get_tstate(), color(COLOR_NONE))
                          )
                else:
                    sys.stdout.write("%s,%s,%s,%s,%s\n"
                        % (node_name, res_name, v_node_id, v_cstate, v_tstate)
                      )
            except IncompatibleDataException:
                sys.stderr.write("Warning: incompatible table entry skipped\n")
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
            fn_rc = self._list_rc_entries(server_rc)
        except SyntaxException:
            self.syntax_export_conf()
        return fn_rc


    def cmd_howto_join(self, args):
        """
        Queries the command line to join a node from the server
        """
        fn_rc = 1
        # Command parser configuration
        order    = [ "node" ]
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
            self.dbus_init()
            server_rc, joinc = self._server.text_query(["joinc", node_name])
            sys.stdout.write("%s\n" % " ".join(joinc))
            fn_rc = self._list_rc_entries(server_rc)
        except SyntaxException:
            self.syntax_howto_join()
        return fn_rc


    def syntax_howto_join(self):
        sys.stderr.write("Syntax: howto-join <node>\n")


    def cmd_query_conf(self, args):
        """
        Retrieves the configuration file for a resource on a specified node
        """
        fn_rc = 1
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
            server_rc, res_config = self._server.text_query(
                [
                    "export_conf",
                    node_name, res_name
                ]
            )

            # Server generated error messages do not end with newline,
            # but the configuration file does, so compensate for that
            # to avoid a superfluous empty line at the end of the
            # configuration output
            if res_config[0].endswith("\n"):
                format = "%s"
            else:
                format = "%s\n"

            sys.stdout.write(format % res_config[0])
            fn_rc = self._list_rc_entries(server_rc)
        except SyntaxException:
            self.syntax_query_conf()
        return fn_rc


    def syntax_query_conf(self):
        sys.stderr.write("Syntax: query-conf <node> <resource>\n")


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


    def cmd_startup(self, args):
        fn_rc = 1
        try:
            sys.stdout.write(
                "Attempting to startup the server through "
                "D-Bus activation...\n"
            )
            self.dbus_init()
            server_rc = self._server.ping()
            if server_rc == 0:
                sys.stdout.write(
                    "D-Bus connection successful, "
                    "server is running and reachable\n"
                )
                fn_rc = 0
        except dbus.exceptions.DBusException:
            sys.stderr.write(
                "D-Bus connection FAILED -- the D-Bus server may have "
                "been unable to activate\nthe drbdmanage service.\n"
                "Review the syslog for error messages logged by the "
                "D-Bus server\nor the drbdmanage server\n"
            )
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
            params["address"] = default_ip();

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
                # Load the configuration
                # (WITHOUT default values; only values
                #  from the configuration file will
                #  be loaded)
                # ========================================
                server_conf = self.load_server_conf()

                # ========================================
                # Set up the path to the drbdctrl LV
                # ========================================
                if server_conf is not None:
                    drbdctrl_vg = map_val_or_dflt(
                        server_conf, KEY_DRBDCTRL_VG, DEFAULT_VG
                    )
                else:
                    drbdctrl_vg = DEFAULT_VG
                drbdctrl_blockdev = (
                    "/dev/" + drbdctrl_vg + "/" + DRBDCTRL_RES_NAME
                )

                # ========================================
                # Cleanup
                # ========================================
                self._init_join_cleanup(drbdctrl_vg)

                # ========================================
                # Initialize a new drbdmanage cluster
                # ========================================

                # Create the .drbdctrl LV
                self._ext_command(
                    [
                        "lvcreate", "-n", DRBDCTRL_RES_NAME, "-L", "4m",
                        drbdctrl_vg
                    ]
                )

                # Create meta-data
                self._ext_command(
                    [
                        "drbdmeta", "--force", "0", "v09", drbdctrl_blockdev,
                        "internal", "create-md", "31"
                    ]
                )

                # Configure the .drbdctrl resource
                self._ext_command(
                    ["drbdsetup", "new-resource", DRBDCTRL_RES_NAME, "0"]
                )
                self._ext_command(
                    ["drbdsetup", "new-minor", DRBDCTRL_RES_NAME, "0", "0"]
                )
                self._ext_command(
                    [
                        "drbdmeta", "0", "v09",
                        drbdctrl_blockdev, "internal", "apply-al"
                    ]
                )
                self._ext_command(
                    [
                        "drbdsetup", "attach", "0", drbdctrl_blockdev,
                        drbdctrl_blockdev, "internal"
                    ]
                )
                self._ext_command(
                    ["drbdsetup", "primary", DRBDCTRL_RES_NAME, "--force"]
                )
                init_rc = self._drbdctrl_init(DRBDCTRL_DEV)

                # FIXME: return codes broken atm because of new API, turn
                #        this back on after it has been changed to the
                #        new api
                #if init_rc != 0:
                    # an error message is printed by _drbdctrl_init()
                #    raise AbortException
                self._ext_command(
                    ["drbdsetup", "secondary", DRBDCTRL_RES_NAME]
                )


                props = {}
                props[NODE_ADDR] = address
                props[NODE_AF]   = af
                # Startup the drbdmanage server and add the current node
                self.dbus_init()
                server_rc = self._server.init_node(
                    dbus.String(node_name), props,
                    dbus.String(drbdctrl_blockdev), str(port)
                )

                fn_rc = self._list_rc_entries(server_rc)
            else:
                fn_rc = 0
        except AbortException:
            sys.stderr.write("Initialization failed\n")
            self._init_join_rollback(drbdctrl_vg)
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
                # Load the configuration
                # (WITHOUT default values; only values
                #  from the configuration file will
                #  be loaded)
                # ========================================
                server_conf = self.load_server_conf()

                # ========================================
                # Set up the path to the drbdctrl LV
                # ========================================
                if server_conf is not None:
                    drbdctrl_vg = map_val_or_dflt(
                        server_conf, KEY_DRBDCTRL_VG, DEFAULT_VG
                    )
                else:
                    drbdctrl_vg = DEFAULT_VG
                drbdctrl_blockdev = (
                    "/dev/" + drbdctrl_vg + "/" + DRBDCTRL_RES_NAME
                )

                # ========================================
                # Cleanup
                # ========================================
                self._init_join_cleanup(drbdctrl_vg)

                # ========================================
                # Join an existing drbdmanage cluster
                # ========================================

                # Create the .drbdctrl LV
                self._ext_command(
                    [
                        "lvcreate", "-n", DRBDCTRL_RES_NAME, "-L", "4m",
                        drbdctrl_vg
                    ]
                )

                # Create meta-data
                self._ext_command(
                    [
                        "drbdmeta", "--force", "0", "v09",
                        drbdctrl_blockdev, "internal",
                        "create-md", "31"
                    ]
                )

                l_addr    = params["local_ip"]
                p_addr    = params["peer_ip"]
                p_name    = params["peer_name"]
                l_node_id = params["local_node_id"]
                p_node_id = params["peer_node_id"]
                secret    = params["secret"]

                # Configure the .drbdctrl resource
                self._ext_command(
                    ["drbdsetup", "new-resource", DRBDCTRL_RES_NAME, l_node_id]
                )
                self._ext_command(
                    ["drbdsetup", "new-minor", DRBDCTRL_RES_NAME, "0", "0"]
                )
                self._ext_command(
                    [
                        "drbdmeta", "0", "v09",
                        drbdctrl_blockdev, "internal", "apply-al"
                    ]
                )
                self._ext_command(
                    [
                        "drbdsetup", "attach", "0",
                        drbdctrl_blockdev, drbdctrl_blockdev, "internal"
                    ]
                )

                umh_f = None
                umh   = None
                try:
                    umh_f = open(self.UMHELPER_FILE, "r")
                    umh = umh_f.read(8192)
                    # The kernel adds a newline character when the file
                    # is read, but does not remove any newline characters
                    # when the same content is written back.
                    # For this reason, a trailing newline character will be
                    # removed to avoid filling up the file with trailing
                    # newlines upon multiple runs of the join procedure
                    if umh.endswith("\n"):
                        umh = umh[:-1]
                    umh_f.close()
                    umh_f = None
                    umh_f = open(self.UMHELPER_FILE, "w")
                    umh_f.write(self.UMHELPER_OVERRIDE)
                except (IOError, OSError) as err:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    exc_lines = traceback.format_exception_only(
                        exc_type, exc_obj
                    )
                    exc_text = exc_lines[0]
                    sys.stderr.write(
                        "Warning: Reading the current usermode helper "
                        "failed:\n%s"
                        % (exc_text)
                    )
                    raise AbortException
                finally:
                    if umh_f is not None:
                        try:
                            umh_f.close()
                        except (IOError, OSError):
                            pass

                proc_rc = self._ext_command(
                    [
                        "drbdsetup", "connect", DRBDCTRL_RES_NAME,
                        "ipv4:" + l_addr + ":" + str(port),
                        "ipv4:" + p_addr + ":" + str(port),
                        "--peer-node-id=" + p_node_id,
                        "--_name=" + p_name,
                        "--shared-secret=" + secret,
                        "--cram-hmac-alg=sha256",
                        "--protocol=C"
                    ]
                )

                # FIXME: wait here -- otherwise, restoring the user mode
                #        helper will probably race with establishing the
                #        network connection
                time.sleep(self.UMHELPER_WAIT_TIME)

                umh_f = None
                if umh is not None:
                    try:
                        umh_f = open(self.UMHELPER_FILE, "w")
                        umh_f.write(umh)
                    except (IOError, OSError) as err:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        exc_lines = traceback.format_exception_only(
                            exc_type, exc_obj
                        )
                        exc_text = exc_lines[0]
                        sys.stderr.write(
                            "Warning: Resetting the usermode helper failed:\n"
                            "%s"
                            % (exc_text)
                        )
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
                server_rc = self._server.join_node(
                    drbdctrl_blockdev, port, secret
                )
                #server_rc = self._server.debug_console(dbus.String(
                #    "gen drbdctrl " + secret + " " + port + " " + bdev
                #))
                fn_rc = self._list_rc_entries(server_rc)
            else:
                fn_rc = 0
        except AbortException:
            sys.stderr.write("Initialization failed\n")
            self._init_join_rollback(drbdctrl_vg)
        except SyntaxException:
            sys.stderr.write(
                "Syntax: local_ip local_node_id peer_ip peer_name "
                "peer_node_id secret\n"
            )
        return fn_rc


    def _init_join_cleanup(self, drbdctrl_vg):
        """
        Cleanup before init / join operations

        Notice: Caller should handle AbortException
        """
        # Shut down any existing drbdmanage control volume
        self._ext_command(
            ["drbdsetup", "down", DRBDCTRL_RES_NAME]
        )

        # Delete any existing .drbdctrl LV
        self._ext_command(
            ["lvremove", "--force", drbdctrl_vg + "/" + DRBDCTRL_RES_NAME]
        )

        # Delete any existing configuration file
        try:
            os.unlink(build_path(DRBDCTRL_RES_PATH, DRBDCTRL_RES_FILE))
        except OSError:
            pass


    def _init_join_rollback(self, drbdctrl_vg):
        """
        Attempts cleanup after a failed init or join operation
        """
        try:
            self._ext_command(
                ["drbdsetup", "down", DRBDCTRL_RES_NAME]
            )
        except AbortException:
            pass
        try:
            self._ext_command(
                ["lvremove", "--force", drbdctrl_vg + "/" + DRBDCTRL_RES_NAME]
            )
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
                quiet = self.user_confirm(
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
                    % (drbdctrl_file)
                )
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
        col_width = 20
        (term_width, term_height) = get_terminal_size()
        columns = term_width / col_width if term_width >= col_width else 1

        items = 0
        for cmd_name in self.COMMANDS:

            # ignore shortcut aliases (one and two characters) for now
            if len(cmd_name) > 2:
                items += 1
                if items % columns != 0:
                    sys.stdout.write("  ")
                sys.stdout.write("%-18s" % (cmd_name))
                if items % columns == 0:
                    sys.stdout.write("\n")

        if items % columns != 0:
            sys.stdout.write("\n")
        return 0


    def cmd_usage(self, args):
        sys.stdout.write("Usage: drbdmanage [options...] command [args...]\n"
                         "\n"
                         "where command is one out of:\n")
        self.print_sub_commands()
        return 0


    def syntax_init(self):
        sys.stderr.write("Syntax: init [ -q | --quiet ] device\n")


    def cmd_exit(self, args):
        exit(0)


    def _list_rc_entries(self, server_rc):
        """
        Lists default error messages for a list of server return codes
        """
        return self._process_rc_entries(server_rc, True)


    def _is_rc_successful(self, server_rc):
        """
        Indicates whether server return codes contain a success message
        """
        successful = (True if self._process_rc_entries(server_rc, False) == 0
                      else False)
        return successful


    def _process_rc_entries(self, server_rc, output):
        """
        Processes a list of server return codes

        * Indicates whether the return codes contain a success message
        * If the output flag is set, prints the default error message for
          each return code
        """
        fn_rc = 1
        try:
            for rc_entry in server_rc:
                try:
                    rc_num, rc_fmt, rc_args = rc_entry
                    if rc_num == DM_SUCCESS:
                        fn_rc = 0
                    if output:
                        self.error_msg_text(rc_num)
                except (TypeError, ValueError):
                    sys.stderr.write("WARNING: unparseable return code "
                        "omitted\n")
        except (TypeError, ValueError):
            sys.stderr.write("WARNING: cannot parse server return codes\n")
        return fn_rc


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
            sys.stderr.write("fn_rc=%d, %s\n" % (fn_rc, command))
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
        if error == 0:
            prefix = ""
        else:
            prefix = "Error: "
        sys.stderr.write("%s%s\n" % (prefix, dm_exc_text(error)))


    def color(self, col):
        if self._colors:
            return col
        else:
            return ""


    def split_number_unit(self, input):
        split_idx = 0
        for in_char in input:
            if not (in_char >= '0'and in_char <= '9'):
                break
            split_idx += 1
        number = input[:split_idx]
        unit   = input[split_idx:]
        if len(number) == 0:
            number = None
        if len(unit) == 0:
            unit = None
        return (number, unit)


    def _property_text(self, text):
        if text is None:
            return "N/A"
        else:
            return text


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
        cconf_len_name = pers_impl.CCONF_LEN_NAME
        cconf_off_name = pers_impl.CCONF_OFF_NAME

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
                    + str(data_off) + ",\n"
                    "        \"" + cconf_len_name + "\": 3,\n"
                    "        \"" + cconf_off_name + "\": "
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


    def load_server_conf(self):
        in_file     = None
        conf_loaded = None
        try:
            in_file = open(SERVER_CONFFILE, "r")
            conffile = ConfFile(in_file)
            conf_loaded = conffile.get_conf()
        except IOError as ioerr:
            sys.stderr.write("No server configuration file loaded:\n")
            if ioerr.errno == errno.EACCES:
                sys.stderr.write("Cannot open configuration file '%s', "
                  "permission denied\n" % SERVER_CONFFILE)
            elif ioerr.errno != errno.ENOENT:
                sys.stderr.write("Cannot open configuration file '%s', "
                  "error returned by the OS is: %s\n"
                  % (SERVER_CONFFILE, ioerr.strerror))
        finally:
            if in_file is not None:
                in_file.close()
        return conf_loaded

    """
    Unit names are lower-case; functions using the lookup table should
    convert the unit name to lower-case to look it up in this table
    """
    UNITS_MAP = {
        "k"   : SizeCalc.UNIT_kiB,
        "m"   : SizeCalc.UNIT_MiB,
        "g"   : SizeCalc.UNIT_GiB,
        "t"   : SizeCalc.UNIT_TiB,
        "p"   : SizeCalc.UNIT_PiB,
        "kib" : SizeCalc.UNIT_kiB,
        "mib" : SizeCalc.UNIT_MiB,
        "gib" : SizeCalc.UNIT_GiB,
        "tib" : SizeCalc.UNIT_TiB,
        "pib" : SizeCalc.UNIT_PiB,
        "kb"  : SizeCalc.UNIT_kB,
        "mb"  : SizeCalc.UNIT_MB,
        "gb"  : SizeCalc.UNIT_GB,
        "tb"  : SizeCalc.UNIT_TB,
        "pb"  : SizeCalc.UNIT_PB,
    }


    COMMANDS = {
        "assignments"       : cmd_list_assignments,
        "a"                 : cmd_list_assignments,
        "resources"         : cmd_list_resources,
        "r"                 : cmd_list_resources,
        "volumes"           : cmd_list_volumes,
        "v"                 : cmd_list_volumes,
        "nodes"             : cmd_list_nodes,
        "n"                 : cmd_list_nodes,
        "poke"              : cmd_poke,
        "p"                 : cmd_poke,
        "new-node"          : cmd_new_node,
        "add-node"          : cmd_new_node,
        "nn"                : cmd_new_node,
        "an"                : cmd_new_node,
        "remove-node"       : cmd_remove_node,
        "delete-node"       : cmd_remove_node,
        "dn"                : cmd_remove_node,
        "rn"                : cmd_remove_node,
        "new-volume"        : cmd_new_volume,
        "add-volume"        : cmd_new_volume,
        "nv"                : cmd_new_volume,
        "av"                : cmd_new_volume,
        "new-resource"      : cmd_new_resource,
        "add-resource"      : cmd_new_resource,
        "nr"                : cmd_new_resource,
        "ar"                : cmd_new_resource,
        "modify-resource"   : cmd_modify_resource,
        "remove-volume"     : cmd_remove_volume,
        "delete-volume"     : cmd_remove_volume,
        "dv"                : cmd_remove_volume,
        "rv"                : cmd_remove_volume,
        "remove-resource"   : cmd_remove_resource,
        "delete-resource"   : cmd_remove_resource,
        "dr"                : cmd_remove_resource,
        "rr"                : cmd_remove_resource,
        "connect"           : cmd_connect,
        "reconnect"         : cmd_connect,
        "disconnect"        : cmd_disconnect,
        "flags"             : cmd_flags,
        "attach"            : cmd_attach,
        "detach"            : cmd_detach,
        "free-space"        : cmd_free_space,
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
        "export"            : cmd_export_conf,
        "ping"              : cmd_ping,
        "startup"           : cmd_startup,
        "shutdown"          : cmd_shutdown,
        "initcv"            : cmd_initcv,
        "exit"              : cmd_exit,
        "usage"             : cmd_usage,
        "init"              : cmd_init,
        "join"              : cmd_join,
        "howto-join"        : cmd_howto_join,
        "query-conf"        : cmd_query_conf
      }


def main():
    client = DrbdManage()
    client.run()

if __name__ == "__main__":
    main()
