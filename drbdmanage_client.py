#!/usr/bin/env python2
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2013 - 2017  LINBIT HA-Solutions GmbH
                               Author: R. Altnoeder, Roland Kammerer

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
import dbus.mainloop.glib
import json
import re
import subprocess
import time
import traceback
import locale
import drbdmanage.drbd.drbdcore
import drbdmanage.drbd.persistence
import drbdmanage.argparse.argparse as argparse
import drbdmanage.argcomplete as argcomplete

from drbdmanage.consts import (
    KEY_DRBDCTRL_VG, KEY_CUR_MINOR_NR, DEFAULT_VG, DRBDCTRL_DEFAULT_PORT, DBUS_DRBDMANAGED, DBUS_SERVICE,
    DRBDCTRL_RES_NAME, DRBDCTRL_RES_FILE, DRBDCTRL_RES_PATH,
    NODE_ADDR, NODE_AF, NODE_ID, NODE_POOLSIZE, NODE_POOLFREE, RES_PORT,
    VOL_MINOR, VOL_BDEV, RES_PORT_NR_AUTO, FLAG_DISKLESS, FLAG_OVERWRITE,
    FLAG_DRBDCTRL, FLAG_STORAGE, FLAG_EXTERNAL, FLAG_DISCARD, FLAG_CONNECT, FLAG_QIGNORE, FLAG_FORCEWIN,
    KEY_DRBD_CONFPATH, DEFAULT_DRBD_CONFPATH, DM_VERSION, DM_GITHASH,
    CONF_NODE, CONF_GLOBAL, KEY_SITE, BOOL_TRUE, BOOL_FALSE, FILE_GLOBAL_COMMON_CONF, KEY_VG_NAME,
    NODE_SITE, NODE_VOL_0, NODE_VOL_1, NODE_PORT, NODE_SECRET,
    DRBDCTRL_LV_NAME_0, DRBDCTRL_LV_NAME_1, DRBDCTRL_DEV_0, DRBDCTRL_DEV_1,
    KEY_S_CMD_SHUTDOWN,
    KEY_COLORS, KEY_UTF8, NODE_NAME, RES_NAME, SNAPS_NAME, KEY_SHUTDOWN_RES, MANAGED
)
from drbdmanage.utils import SizeCalc
from drbdmanage.utils import Table
from drbdmanage.utils import DrbdSetupOpts
from drbdmanage.utils import ExternalCommandBuffer
from drbdmanage.utils import (
    build_path, bool_to_string, string_to_bool, rangecheck, namecheck, ssh_exec,
    load_server_conf_file, filter_prohibited, get_uname, approximate_size_string
)
from drbdmanage.utils import (
    COLOR_NONE, COLOR_RED, COLOR_DARKRED, COLOR_DARKGREEN, COLOR_BROWN,
    COLOR_DARKPINK, COLOR_TEAL, COLOR_GREEN, COLOR_YELLOW
)
from drbdmanage.exceptions import AbortException
from drbdmanage.exceptions import IncompatibleDataException
from drbdmanage.exceptions import SyntaxException
from drbdmanage.exceptions import dm_exc_text
from drbdmanage.exceptions import (
    DM_SUCCESS, DM_EEXIST, DM_ENOENT, DM_ENOTREADY, DM_ENOTREADY_STARTUP, DM_ENOTREADY_REQCTRL
)
from drbdmanage.dbusserver import DBusServer
from drbdmanage.drbd.drbdcore import Assignment
from drbdmanage.drbd.views import AssignmentView
from drbdmanage.drbd.views import DrbdNodeView
from drbdmanage.drbd.views import DrbdResourceView
from drbdmanage.drbd.views import DrbdVolumeView
from drbdmanage.drbd.views import DrbdVolumeStateView
from drbdmanage.drbd.views import GenericView
from drbdmanage.snapshots.views import DrbdSnapshotAssignmentView
from drbdmanage.storage.storagecore import MinorNr
from drbdmanage.storage.header import gen_header
from drbdmanage.defaultip import default_ip

from drbdmanage.propscontainer import Props


class DrbdManage(object):

    """
    drbdmanage dbus client, the CLI for controlling the drbdmanage server
    """

    _dbus = None
    _gmainloop = None
    _server = None
    _noerr = False
    _colors = True
    _utf8 = False
    _all_commands = None

    UMHELPER_FILE = "/sys/module/drbd/parameters/usermode_helper"
    UMHELPER_OVERRIDE = "/bin/true"
    UMHELPER_WAIT_TIME = 5.0

    def __init__(self):
        try:
            locale.setlocale(locale.LC_ALL, '')
        except:
            pass
        try:
            self._parser = self.setup_parser()
        except dbus.exceptions.DBusException as exc:
            self._print_dbus_exception(exc)
            exit(1)
        self._all_commands = self.parser_cmds()
        self._config = load_server_conf_file(localonly=True)
        if KEY_COLORS in self._config:
            self._colors = True if self._config[KEY_COLORS].strip().lower() == 'yes' else False
        if KEY_UTF8 in self._config:
            self._utf8 = True if self._config[KEY_UTF8].strip().lower() == 'yes' else False

    def _print_dbus_exception(self, exc):
        sys.stderr.write(
            "\nError: Cannot connect to the drbdmanaged process using DBus\n"
        )
        sys.stderr.write(
            "The DBus subsystem returned the following "
            "error description:\n"
        )
        sys.stderr.write("%s\n" % (str(exc)))

    def dbus_init(self):
        try:
            if self._server is None:
                dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
                self._dbus = dbus.SystemBus()
                self._server = self._dbus.get_object(
                    DBUS_DRBDMANAGED, DBUS_SERVICE
                )
        except dbus.exceptions.DBusException as exc:
            self._print_dbus_exception(exc)
            exit(1)

    def dsc(self, fn, *args, **kwargs):
        tries, retries_max = 0, 15
        while tries <= retries_max:
            if tries > 0:
                if tries == 1:
                    sys.stdout.write('Waiting for server: ')
                sys.stdout.write('.')
                sys.stdout.flush()
            server_rc = fn(*args, **kwargs)
            try:
                # single int return codes like ping
                server_rc = int(server_rc)
                return server_rc
            except:
                pass
            if len(server_rc) > 1:
                chk = server_rc[0]
                if not isinstance(chk[0], dbus.Struct):
                    chk = dbus.Array([dbus.Struct(server_rc[0])])
            else:
                chk = server_rc
            if self._is_rc_retry(chk):
                tries += 1
                time.sleep(2)
                continue
            else:
                break
        if tries > 0:
            sys.stdout.write('\n')
        if tries == retries_max + 1:
            self._process_rc_entries(chk, True)

        return server_rc

    def setup_parser(self):
        parser = argparse.ArgumentParser(prog='drbdmanage')
        parser.add_argument('--version', '-v', action='version',
                            version='%(prog)s ' + DM_VERSION + '; ' + DM_GITHASH)
        subp = parser.add_subparsers(title='subcommands',
                                     description='valid subcommands',
                                     help='Use the list command to print a '
                                     'nicer looking overview of all valid '
                                     'commands')

        # interactive mode
        parser_ia = subp.add_parser('interactive',
                                    description='Start interactive mode')
        parser_ia.set_defaults(func=self.cmd_interactive)

        # help
        p_help = subp.add_parser('help',
                                 description='Print help for a command')
        p_help.add_argument('command')
        p_help.set_defaults(func=self.cmd_help)

        # list
        p_list = subp.add_parser('list', aliases=['commands'],
                                 description='List available commands')
        p_list.set_defaults(func=self.cmd_list)

        # exit
        p_exit = subp.add_parser('exit', aliases=['quit'],
                                 description='Only useful in interactive mode')
        p_exit.set_defaults(func=self.cmd_exit)

        # poke
        p_poke = subp.add_parser('poke')
        p_poke.set_defaults(func=self.cmd_poke)

        # new-node
        def ip_completer(where):
            def completer(prefix, parsed_args, **kwargs):
                import socket
                opt = where
                if opt == "name":
                    name = parsed_args.name
                elif opt == "peer_ip":
                    name = parsed_args.peer_ip
                else:
                    return ""

                ip = socket.gethostbyname(name)
                ip = [ip]
                return ip
            return completer

        def node_completer(prefix, **kwargs):
            server_rc, node_list = self._get_nodes()
            possible = set()
            for n in node_list:
                name, _ = n
                possible.add(name)

            return possible

        # type checkers (generate them only once)
        check_node_name = namecheck(NODE_NAME)
        check_res_name = namecheck(RES_NAME)
        check_snaps_name = namecheck(SNAPS_NAME)

        p_new_node = subp.add_parser('add-node',
                                     aliases=['nn', 'new-node', 'an'],
                                     description='Creates a node entry for a node that participates in the '
                                     'drbdmanage cluster. Nodename must match the name that is used as the '
                                     'node name of the drbdmanage server on the new participating node. '
                                     'Commonly, nodename is set to match the output of "uname -n" '
                                     'on the new participating node. Unless specified otherwise, address is '
                                     'expected to be an IPv4 ip address, and the address family field of the '
                                     'node entry is implicitly set to ipv4.')
        p_new_node.add_argument('-a', '--address-family', metavar="FAMILY",
                                default='ipv4', choices=['ipv4', 'ipv6'],
                                help='FAMILY: "ipv4" (default) or "ipv6"')
        p_new_node.add_argument('-q', '--quiet', action="store_true")
        p_new_node.add_argument('-l', '--satellite',
                                action="store_true",
                                help='This node does not have a copy of the control volume'
                                ' in persistent storage. It is always used as a satellite node')
        p_new_node.add_argument('-e', '--external', action="store_true",
                                help='External node that is neither a control node nor a satellite')
        p_new_node.add_argument('-s', '--no-storage', action="store_true")
        p_new_node.add_argument('-j', '--no-autojoin', action="store_true")
        p_new_node.add_argument('name', help='Name of the new node', type=check_node_name)
        p_new_node.add_argument('ip',
                                help='IP address of the new node').completer = ip_completer("name")
        p_new_node.set_defaults(func=self.cmd_new_node)

        # modify-node
        p_mod_node_command = 'modify-node'
        p_mod_node = subp.add_parser(p_mod_node_command,
                                     aliases=['mn'],
                                     description='Modifies a drbdmanage node.')
        p_mod_node.add_argument('-a', '--address-family', metavar="FAMILY",
                                choices=['ipv4', 'ipv6'],
                                help='FAMILY: "ipv4" (default) or "ipv6"')
        p_mod_node.add_argument('-s', '--storage')
        p_mod_node.add_argument('name', type=check_node_name,
                                help='Name of the node').completer = node_completer
        p_mod_node.add_argument('--address',
                                help='Network address of the node').completer = ip_completer("name")
        p_mod_node.set_defaults(func=self.cmd_modify_node)
        p_mod_node.set_defaults(command=p_mod_node_command)

        # remove-node
        p_rm_node = subp.add_parser('remove-node',
                                    aliases=['rn', 'delete-node', 'dn'],
                                    description='Removes a node from the drbdmanage cluster. '
                                    'All drbdmanage resources that are still deployed on the specified '
                                    'node are marked for undeployment, and the node entry is marked for '
                                    "removal from drbdmanage's data tables. The specified node is "
                                    'expected to undeploy all resources. As soon as all resources have been '
                                    'undeployed from the node, the node entry is removed from '
                                    "drbdmanage's data tables.")
        p_rm_node.add_argument('-q', '--quiet', action="store_true",
                               help='Unless this option is used, drbdmanage will issue a safety question '
                               'that must be answered with yes, otherwise the operation is canceled.')
        p_rm_node.add_argument('-f', '--force', action="store_true",
                               help='The node entry and all associated assignment entries are removed from '
                               "drbdmanage's data tables immediately, without taking any action on the "
                               'cluster node that the node entry refers to.')
        p_rm_node.add_argument('name', nargs="+",
                               help='Name of the node to remove').completer = node_completer
        p_rm_node.set_defaults(func=self.cmd_remove_node)

        # Quorum control, completion of the action parameter
        quorum_completer_possible = ('ignore', 'unignore')

        def quorum_action_completer(prefix, **kwargs):
            possible = list(quorum_completer_possible)
            if prefix is not None and prefix != "":
                possible = [item for item in possible if item.startswith(prefix)]
            return possible

        p_quorum = subp.add_parser("quorum-control",
                                   aliases=["qc"],
                                   description="Sets quorum parameters on drbdmanage cluster nodes")
        p_quorum.add_argument('-o', '--override', action="store_true",
                              help="Override change protection in a partition without quorum")
        p_quorum.add_argument(
            "action", choices=quorum_completer_possible, help="The action to perform on the affected nodes"
        ).completer = quorum_action_completer
        p_quorum.add_argument(
            "name", nargs="+", type=check_node_name, help="Name of the affected node or nodes"
        ).completer = node_completer
        p_quorum.set_defaults(func=self.cmd_quorum_control)

        # new-resource
        p_new_res = subp.add_parser('add-resource',
                                    aliases=['nr', 'new-resource', 'ar'],
                                    description='Defines a DRBD resource for use with drbdmanage. '
                                    'Unless a specific IP port-number is supplied, the port-number is '
                                    'automatically selected by the drbdmanage server on the current node. ')
        p_new_res.add_argument('-p', '--port', type=rangecheck(1, 65535))
        p_new_res.add_argument('name', type=check_res_name, help='Name of the new resource')
        p_new_res.set_defaults(func=self.cmd_new_resource)

        # modify-resource
        def res_completer(prefix, **kwargs):
            server_rc, res_list = self.__list_resources(False)
            possible = set()
            for r in res_list:
                name, _ = r
                possible.add(name)

            if not prefix or prefix == '':
                return possible
            else:
                return [res for res in possible if res.startswith(prefix)]

            return possible

        # modify-resource
        p_mod_res_command = 'modify-resource'
        p_mod_res = subp.add_parser(p_mod_res_command,
                                    aliases=['mr'],
                                    description='Modifies a DRBD resource.')
        p_mod_res.add_argument('-p', '--port', type=rangecheck(1, 65535))
        p_mod_res.add_argument('-m', '--managed', choices=(BOOL_TRUE, BOOL_FALSE))
        p_mod_res.add_argument('name', type=check_res_name,
                               help='Name of the resource').completer = res_completer
        p_mod_res.set_defaults(func=self.cmd_modify_resource)
        p_mod_res.set_defaults(command=p_mod_res_command)

        # remove-resource
        p_rm_res = subp.add_parser('remove-resource',
                                   aliases=['rr', 'delete-resource', 'dr'],
                                   description=' Removes a resource and its associated resource definition '
                                   'from the drbdmanage cluster. The resource is undeployed from all nodes '
                                   "and the resource entry is marked for removal from drbdmanage's data "
                                   'tables. After all nodes have undeployed the resource, the resource '
                                   "entry is removed from drbdmanage's data tables.")
        p_rm_res.add_argument('-q', '--quiet', action="store_true",
                              help='Unless this option is used, drbdmanage will issue a safety question '
                              'that must be answered with yes, otherwise the operation is canceled.')
        p_rm_res.add_argument('-f', '--force', action="store_true",
                              help='If present, then the resource entry and all associated assignment '
                              "entries are removed from drbdmanage's data tables immediately, without "
                              'taking any action on the cluster nodes that have the resource deployed.')
        p_rm_res.add_argument('name',
                              nargs="+",
                              help='Name of the resource to delete').completer = res_completer
        p_rm_res.set_defaults(func=self.cmd_remove_resource)

        # new-volume
        def size_completer(prefix, **kwargs):
            choices = ('kB', 'MB', 'GB', 'TB', 'PB', 'kiB', 'MiB', 'GiB',
                       'TiB', 'PiB')
            m = re.match('(\d+)(\D*)', prefix)

            digits = m.group(1)
            unit = m.group(2)

            if unit and unit != "":
                p_units = [x for x in choices if x.startswith(unit)]
            else:
                p_units = choices

            return [digits + u for u in p_units]

        p_new_vol_command = 'add-volume'
        p_new_vol = subp.add_parser(p_new_vol_command,
                                    aliases=['nv', 'new-volume', 'av'],
                                    description='Defines a volume with a capacity of size for use with '
                                    'drbdmanage. If the resource resname exists already, a new volume is '
                                    'added to that resource, otherwise the resource is created automatically '
                                    'with default settings. Unless minornr is specified, a minor number for '
                                    "the volume's DRBD block device is assigned automatically by the "
                                    'drbdmanage server.')
        p_new_vol.add_argument('-m', '--minor', type=int)
        p_new_vol.add_argument('-d', '--deploy', type=int)
        p_new_vol.add_argument('-s', '--site', default='',
                               help="only consider nodes from this site")
        p_new_vol.add_argument('name', type=check_res_name,
                               help='Name of a new/existing resource').completer = res_completer
        p_new_vol.add_argument(
            'size',
            help='Size of the volume in resource. '
            'The default unit for size is GiB (size * (2 ^ 30) bytes). '
            'Another unit can be specified by using an according postfix. '
            "Drbdmanage's internal granularity for the capacity of volumes is one "
            'Kibibyte (2 ^ 10 bytes). All other unit specifications are implicitly '
            'converted to Kibibyte, so that the actual size value used by drbdmanage '
            'is the smallest natural number of Kibibytes that is large enough to '
            'accommodate a volume of the requested size in the specified size unit.'
        ).completer = size_completer
        p_new_vol.set_defaults(func=self.cmd_new_volume)
        p_new_vol.set_defaults(command=p_new_vol_command)

        def vol_completer(prefix, parsed_args, **kwargs):
            server_rc, res_list = self.__list_resources(True)
            possible = set()
            for r in res_list:
                name, _, vol_list = r
                if name == parsed_args.name:
                    vol_list.sort(key=lambda vol_entry: vol_entry[0])
                    for v in vol_list:
                        vol_id, _ = v
                        possible.add(str(vol_id))

            return possible

        # resize-volume
        p_resize_vol_command = 'resize-volume'
        p_resize_vol = subp.add_parser(p_resize_vol_command,
                                       aliases=['resize'],
                                       description='Resizes a volume to the specified size, which must be '
                                       'greater than the current size of the volume.')
        p_resize_vol.add_argument('name', type=check_res_name,
                                  help='Name of the resource').completer = res_completer
        p_resize_vol.add_argument('id', help='Volume ID', type=int).completer = vol_completer
        p_resize_vol.add_argument(
            'size',
            help='New size of the volume. '
            'The default unit for size is GiB (size * (2 ^ 30) bytes). '
            'Another unit can be specified by using an according postfix. '
            "Drbdmanage's internal granularity for the capacity of volumes is one "
            'Kibibyte (2 ^ 10 bytes). All other unit specifications are implicitly '
            'converted to Kibibyte, so that the actual size value used by drbdmanage '
            'is the smallest natural number of Kibibytes that is large enough to '
            'accommodate a volume of the requested size in the specified size unit.'
        ).completer = size_completer
        p_resize_vol.set_defaults(func=self.cmd_resize_volume)
        p_resize_vol.set_defaults(command=p_resize_vol_command)

        # modify-volume
        p_mod_vol_command = 'modify-volume'
        p_mod_vol = subp.add_parser(p_mod_vol_command,
                                    aliases=['mv'],
                                    description='Modifies a DRBD volume.')
        p_mod_vol.add_argument('name', type=check_res_name,
                               help='Name of the resource').completer = res_completer
        p_mod_vol.add_argument('id', help='Volume id', type=int).completer = vol_completer
        p_mod_vol.add_argument('-m', '--minor', type=rangecheck(0, 1048575))
        p_mod_vol.set_defaults(func=self.cmd_modify_volume)
        p_mod_vol.set_defaults(command=p_mod_vol_command)

        # modify-assignment
        p_mod_assg_command = 'modify-assignment'
        p_mod_assg = subp.add_parser(p_mod_assg_command,
                                     aliases=['ma'],
                                     description='Modifies a drbdmanage assignment.')
        p_mod_assg.add_argument('resource', type=check_res_name,
                                help='Name of the resource').completer = res_completer
        p_mod_assg.add_argument('node', help='Name of the node').completer = node_completer
        p_mod_assg.add_argument('-o', '--overwrite')
        p_mod_assg.add_argument('-d', '--discard')
        p_mod_assg.set_defaults(func=self.cmd_modify_assignment)
        p_mod_assg.set_defaults(command=p_mod_assg_command)

        # remove-volume
        p_rm_vol = subp.add_parser('remove-volume',
                                   aliases=['rv', 'delete-volume', 'dv'],
                                   description='Removes a volume from the drbdmanage cluster, and removes '
                                   'the volume definition from the resource definition. The volume is '
                                   'undeployed from all nodes and the volume entry is marked for removal '
                                   "from the resource definition in drbdmanage's data tables. After all "
                                   'nodes have undeployed the volume, the volume entry is removed from '
                                   'the resource definition.')
        p_rm_vol.add_argument('-q', '--quiet', action="store_true",
                              help='Unless this option is used, drbdmanage will issue a safety question '
                              'that must be answered with yes, otherwise the operation is canceled.')
        p_rm_vol.add_argument('-f', '--force', action="store_true",
                              help='If present, then the volume entry is removed from the resource '
                              'definition immediately, without taking any action on the cluster nodes '
                              'that have the volume deployed.')

        p_rm_vol.add_argument('name',
                              help='Name of the resource').completer = res_completer
        p_rm_vol.add_argument('vol_id', help='Volume ID', type=int).completer = vol_completer
        p_rm_vol.set_defaults(func=self.cmd_remove_volume)

        # connect
        p_conn = subp.add_parser('connect-resource', description='Connect resource on node',
                                 aliases=['connect'])
        p_conn.add_argument('resource', type=check_res_name).completer = res_completer
        p_conn.add_argument('node', type=check_node_name).completer = node_completer
        p_conn.set_defaults(func=self.cmd_connect)

        # reconnect
        p_reconn = subp.add_parser('reconnect-resource', description='Reconnect resource on node',
                                   aliases=['reconnect'])
        p_reconn.add_argument('resource', type=check_res_name).completer = res_completer
        p_reconn.add_argument('node', type=check_node_name).completer = node_completer
        p_reconn.set_defaults(func=self.cmd_reconnect)

        # disconnect
        p_disconn = subp.add_parser('disconnect-resource', description='Disconnect resource on node',
                                    aliases=['disconnect'])
        p_disconn.add_argument('resource', type=check_res_name).completer = res_completer
        p_disconn.add_argument('node', type=check_node_name).completer = node_completer
        p_disconn.set_defaults(func=self.cmd_disconnect)

        # flags
        p_flags = subp.add_parser('set-flags', description='Set flags of resource on node',
                                  aliases=['flags'])
        p_flags.add_argument('resource', type=check_res_name,
                             help='Name of the resource').completer = res_completer
        p_flags.add_argument('node', type=check_node_name,
                             help='Name of the node').completer = node_completer
        p_flags.add_argument('--reconnect', choices=(0, 1), type=int)
        p_flags.add_argument('--updcon', choices=(0, 1), type=int)
        p_flags.add_argument('--overwrite', choices=(0, 1), type=int)
        p_flags.add_argument('--discard', choices=(0, 1), type=int)
        p_flags.set_defaults(func=self.cmd_flags)

        # attach
        p_attach = subp.add_parser('attach-volume', description='Attach volume from node',
                                   aliases=['attach'])
        p_attach.add_argument('resource', type=check_res_name).completer = res_completer
        p_attach.add_argument('id', help='Volume ID', type=int).completer = vol_completer
        p_attach.add_argument('node', type=check_node_name).completer = node_completer
        p_attach.set_defaults(func=self.cmd_attach_detach, fname='attach')
        # detach
        p_detach = subp.add_parser('detach-volume', description='Detach volume from node',
                                   aliases=['detach'])
        p_detach.add_argument('resource', type=check_res_name).completer = res_completer
        p_detach.add_argument('id', help='Volume ID', type=int).completer = vol_completer
        p_detach.add_argument('node', type=check_node_name).completer = node_completer
        p_detach.set_defaults(func=self.cmd_attach_detach, fname='detach')

        # assign
        p_assign = subp.add_parser('assign-resource',
                                   aliases=['assign'],
                                   description='Creates an assignment for the deployment of the '
                                   'specified resource on the specified node.')
        p_assign.add_argument('--client', action="store_true")
        p_assign.add_argument('--overwrite', action="store_true",
                              help='If specified, drbdmanage will issue a "drbdmadm -- --force primary" '
                              'after the resource has been started.')
        p_assign.add_argument('--discard', action="store_true",
                              help='If specified, drbdmanage will issue a "drbdadm -- --discard-my-data" '
                              'connect after the resource has been started.')
        p_assign.add_argument('resource', type=check_res_name).completer = res_completer
        p_assign.add_argument('node', type=check_node_name, nargs="+").completer = node_completer
        p_assign.set_defaults(func=self.cmd_assign)

        # free space
        def redundancy_type(r):
            r = int(r)
            if r < 1:
                raise argparse.ArgumentTypeError('Minimum redundancy is 1')
            return r
        p_fspace = subp.add_parser('list-free-space',
                                   description='Queries the maximum size of a'
                                   ' volume that could be deployed with the'
                                   ' specified level of redundancy',
                                   aliases=['free-space'])
        p_fspace.add_argument('-m', '--machine-readable', action="store_true")
        p_fspace.add_argument('-s', '--site', default='',
                              help="only consider nodes from this site")
        p_fspace.add_argument('redundancy', type=redundancy_type,
                              help='Redundancy level (>=1)')
        p_fspace.set_defaults(func=self.cmd_free_space)

        # deploy
        p_deploy = subp.add_parser('deploy-resource',
                                   aliases=['deploy'],
                                   description='Deploys a resource on n automatically selected nodes '
                                   "of the drbdmanage cluster. Using the information in drbdmanage's data "
                                   'tables, the drbdmanage server tries to find n nodes that have enough '
                                   'free storage capacity to deploy the resource resname.')
        p_deploy.add_argument('resource', type=check_res_name).completer = res_completer
        p_deploy.add_argument('-i', '--increase', action="store_true",
                              help='Increase the redundancy count relative to'
                              ' the currently set value by a number of'
                              ' <redundancy_count>')
        p_deploy.add_argument('-d', '--decrease', action="store_true",
                              help='Decrease the redundancy count relative to'
                              ' the currently set value by a number of'
                              ' <redundancy_count>')
        p_deploy.add_argument('redundancy_count', type=redundancy_type,
                              help='The redundancy count specifies the number'
                              ' of nodes to which the resource should be'
                              ' deployed. It must be at least 1 and at most'
                              ' the number of nodes in the cluster')
        p_deploy.add_argument('--with-clients', action="store_true")
        p_deploy.add_argument('-s', '--site', default='',
                              help="only consider nodes from this site")
        p_deploy.set_defaults(func=self.cmd_deploy)

        # undeploy
        p_undeploy = subp.add_parser('undeploy-resource',
                                     aliases=['undeploy'],
                                     description='Undeploys a resource from all nodes. The resource '
                                     "definition is still kept in drbdmanage's data tables.")
        p_undeploy.add_argument('-q', '--quiet', action="store_true")
        p_undeploy.add_argument('-f', '--force', action="store_true")
        p_undeploy.add_argument('resource', type=check_res_name).completer = res_completer
        p_undeploy.set_defaults(func=self.cmd_undeploy)

        # update-pool
        p_upool = subp.add_parser('update-pool',
                                  description='Checks the storage pool total size and free space on '
                                  'the local node and updates the associated values in the data '
                                  'tables on the control volume.')
        p_upool.set_defaults(func=self.cmd_update_pool)

        # reconfigure
        p_reconfigure = subp.add_parser('reconfigure',
                                        description='Re-reads server configuration and'
                                        ' reloads storage plugin')
        p_reconfigure.set_defaults(func=self.cmd_reconfigure)

        # save
        p_save = subp.add_parser('save',
                                 description='Orders the drbdmanage server to save the current '
                                 "configuration of drbdmanage's resources to the data tables "
                                 'on the drbdmanaege control volume')
        p_save.set_defaults(func=self.cmd_save)

        # load
        p_save = subp.add_parser('load',
                                 description='Orders the drbdmanage server to reload the current '
                                 "configuration of drbdmanage's resources from the data tables on "
                                 'the drbdmanage control volume')
        p_save.set_defaults(func=self.cmd_load)

        # unassign
        p_unassign = subp.add_parser('unassign-resource',
                                     aliases=['unassign'],
                                     description='Undeploys the specified resource from the specified '
                                     "node and removes the assignment entry from drbdmanage's data "
                                     'tables after the node has finished undeploying the resource. '
                                     'If the resource had been assigned to a node, but that node has '
                                     'not deployed the resource yet, the assignment is canceled.')
        p_unassign.add_argument('-q', '--quiet', action="store_true",
                                help='Unless this option is used, drbdmanage will issue a safety question '
                                'that must be answered with yes, otherwise the operation is canceled.')
        p_unassign.add_argument('-f', '--force', action="store_true",
                                help="If present, the assignment entry will be removed from drbdmanage's "
                                'data tables immediately, without taking any action on the node where '
                                'the resource is been deployed.')
        p_unassign.add_argument('resource', type=check_res_name).completer = res_completer
        p_unassign.add_argument('node', type=check_node_name, nargs="+").completer = node_completer
        p_unassign.set_defaults(func=self.cmd_unassign)

        # new-snapshot
        p_nsnap = subp.add_parser('add-snapshot',
                                  aliases=['ns', 'create-snapshot', 'cs',
                                           'new-snapshot', 'as'],
                                  description='Create a LVM snapshot')
        p_nsnap.add_argument('snapshot', type=check_snaps_name, help='Name of the snapshot')
        p_nsnap.add_argument('resource', type=check_res_name,
                             help='Name of the resource').completer = res_completer
        p_nsnap.add_argument('nodes', type=check_node_name,
                             help='List of nodes', nargs='+').completer = node_completer
        p_nsnap.set_defaults(func=self.cmd_new_snapshot)

        # Snapshot commands:
        # These commands do not follow the usual option order:
        # For example remove-snapshot should have the snapshot name as first argument and the resource as
        # second argument. BUT: There are (potentially) more snapshots than resources, so specifying the
        # resource first and then completing only the snapshots for that resource makes more sense.

        # remove-snapshot
        def snaps_completer(prefix, parsed_args, **kwargs):
            server_rc, res_list = self._list_snapshots()
            possible = set()
            for r in res_list:
                res_name, snaps_list = r
                if res_name == parsed_args.resource:
                    for s in snaps_list:
                        snaps_name, _ = s
                        possible.add(snaps_name)

            return possible

        p_rmsnap = subp.add_parser('remove-snapshot',
                                   aliases=['delete-snapshot', 'ds'],
                                   description='Remove LVM snapshot of a resource')
        p_rmsnap.add_argument('-f', '--force', action="store_true")
        p_rmsnap.add_argument('resource', type=check_res_name,
                              help='Name of the resource').completer = res_completer
        p_rmsnap.add_argument('snapshot', type=check_snaps_name, nargs="+",
                              help='Name of the snapshot').completer = snaps_completer
        p_rmsnap.set_defaults(func=self.cmd_remove_snapshot)

        # remove-snapshot-assignment
        p_rmsnapas = subp.add_parser('remove-snapshot-assignment',
                                     aliases=['rsa',
                                              'delete-snapshot-assignment',
                                              'dsa'],
                                     description='Remove snapshot assignment')
        p_rmsnapas.add_argument('-f', '--force', action="store_true")
        p_rmsnapas.add_argument('resource', type=check_res_name,
                                help='Name of the resource').completer = res_completer
        p_rmsnapas.add_argument('snapshot', type=check_snaps_name,
                                help='Name of the snapshot').completer = snaps_completer
        p_rmsnapas.add_argument('node', type=check_node_name,
                                help='Name of the node').completer = node_completer
        p_rmsnapas.set_defaults(func=self.cmd_remove_snapshot_assignment)

        # restore-snapshot
        p_restsnap = subp.add_parser('restore-snapshot',
                                     aliases=['rs'],
                                     description='Restore snapshot')
        p_restsnap.add_argument('resource', type=check_res_name,
                                help='Name of the new resource that gets created from existing snapshot')
        p_restsnap.add_argument('snapshot_resource', type=check_res_name,
                                help='Name of the resource that was snapshoted').completer = res_completer
        p_restsnap.add_argument('snapshot', type=check_snaps_name,
                                help='Name of the snapshot').completer = snaps_completer
        p_restsnap.set_defaults(func=self.cmd_restore_snapshot)

        # resume-all
        p_resume_all = subp.add_parser('resume-all',
                                       description="Resumes all failed assignments")
        p_resume_all.set_defaults(func=self.cmd_resume_all)

        def shutdown_restart(command, description, func, aliases=False):
            if aliases:
                p_cmd = subp.add_parser(command, aliases=aliases, description=description)
            else:
                p_cmd = subp.add_parser(command, description=description)
            p_cmd.add_argument('-l', '--satellite', action="store_true",
                               help='If given, also send a shutdown command to connected satellites.',
                               default=False)
            p_cmd.add_argument('-q', '--quiet', action="store_true",
                               help='Unless this option is used, drbdmanage will issue a safety question '
                               'that must be answered with yes, otherwise the operation is canceled.')
            p_cmd.add_argument('-r', '--resources', action="store_true",
                               help='Shutdown all drbdmanage-controlled resources too',
                               default=False)
            p_cmd.set_defaults(func=func)

        # shutdown
        shutdown_restart('shutdown', description='Stops the local drbdmanage server process.',
                         func=self.cmd_shutdown)
        # restart
        shutdown_restart('restart', description='Restarts the local drbdmanage server process.',
                         func=self.cmd_restart)

        # nodes
        nodesverbose = ('Family', 'IP', 'Site')
        nodesgroupby = ('Name', 'Pool_Size', 'Pool_Free', 'Family', 'IP', 'State')

        def show_group_completer(lst, where):
            def completer(prefix, parsed_args, **kwargs):
                possible = lst
                opt = where
                if opt == "groupby":
                    opt = parsed_args.groupby
                elif opt == "show":
                    opt = parsed_args.show
                else:
                    return possible

                if opt:
                    possible = [i for i in lst if i not in opt]

                return possible
            return completer

        nodes_verbose_completer = show_group_completer(nodesverbose, "show")
        nodes_group_completer = show_group_completer(nodesgroupby, "groupby")
        p_lnodes = subp.add_parser('list-nodes', aliases=['n', 'nodes'],
                                   description='Prints a list of all cluster nodes known to drbdmanage. '
                                   'By default, the list is printed as a human readable table.')
        p_lnodes.add_argument('-m', '--machine-readable', action="store_true")
        p_lnodes.add_argument('-p', '--pastable', action="store_true", help='Generate pastable output')
        p_lnodes.add_argument('-s', '--show', nargs='+',
                              choices=nodesverbose).completer = nodes_verbose_completer
        p_lnodes.add_argument('-g', '--groupby', nargs='+',
                              choices=nodesgroupby).completer = nodes_group_completer
        p_lnodes.add_argument('-N', '--nodes', nargs='+', type=check_node_name,
                              help='Filter by list of nodes').completer = node_completer
        p_lnodes.add_argument('--separators', action="store_true")
        p_lnodes.set_defaults(func=self.cmd_list_nodes)

        # resources
        resverbose = ('Port',)
        resgroupby = ('Name', 'Port', 'State')
        res_verbose_completer = show_group_completer(resverbose, "show")
        res_group_completer = show_group_completer(resgroupby, "groupby")

        p_lreses = subp.add_parser('list-resources', aliases=['r', 'resources'],
                                   description='Prints a list of all resource definitions known to '
                                   'drbdmanage. By default, the list is printed as a human readable table.')
        p_lreses.add_argument('-m', '--machine-readable', action="store_true")
        p_lreses.add_argument('-p', '--pastable', action="store_true", help='Generate pastable output')
        p_lreses.add_argument('-s', '--show', nargs='+',
                              choices=resverbose).completer = res_verbose_completer
        p_lreses.add_argument('-g', '--groupby', nargs='+',
                              choices=resgroupby).completer = res_group_completer
        p_lreses.add_argument('-R', '--resources', nargs='+', type=check_res_name,
                              help='Filter by list of resources').completer = res_completer
        p_lreses.add_argument('--separators', action="store_true")
        p_lreses.set_defaults(func=self.cmd_list_resources)

        # volumes
        volgroupby = resgroupby + ('Vol_ID', 'Size', 'Minor')
        vol_group_completer = show_group_completer(volgroupby, 'groupby')

        p_lvols = subp.add_parser('list-volumes', aliases=['v', 'volumes'],
                                  description=' Prints a list of all volume definitions known to drbdmanage. '
                                  'By default, the list is printed as a human readable table.')
        p_lvols.add_argument('-m', '--machine-readable', action="store_true")
        p_lvols.add_argument('-p', '--pastable', action="store_true", help='Generate pastable output')
        p_lvols.add_argument('-s', '--show', nargs='+',
                             choices=resverbose).completer = res_verbose_completer
        p_lvols.add_argument('-g', '--groupby', nargs='+',
                             choices=volgroupby).completer = vol_group_completer
        p_lvols.add_argument('--separators', action="store_true")
        p_lvols.add_argument('-R', '--resources', nargs='+', type=check_res_name,
                             help='Filter by list of resources').completer = res_completer
        p_lvols.set_defaults(func=self.cmd_list_volumes)

        # snapshots
        snapgroupby = ("Resource", "Name", "State")
        snap_group_completer = show_group_completer(snapgroupby, "groupby")

        p_lsnaps = subp.add_parser('list-snapshots', aliases=['s', 'snapshots'],
                                   description='List available snapshots')
        p_lsnaps.add_argument('-m', '--machine-readable', action="store_true")
        p_lsnaps.add_argument('-p', '--pastable', action="store_true", help='Generate pastable output')
        p_lsnaps.add_argument('-g', '--groupby', nargs='+',
                              choices=snapgroupby).completer = snap_group_completer
        p_lsnaps.add_argument('--separators', action="store_true")
        p_lsnaps.add_argument('-R', '--resources', nargs='+', type=check_res_name,
                              help='Filter by list of resources').completer = res_completer
        p_lsnaps.set_defaults(func=self.cmd_list_snapshots)

        # snapshot-assignments
        snapasgroupby = ("Resource", "Name", "Node", "State")

        snapas_group_completer = show_group_completer(snapasgroupby, "groupby")

        p_lsnapas = subp.add_parser('list-snapshot-assignments', aliases=['sa', 'snapshot-assignments'],
                                    description='List snapshot assignments')
        p_lsnapas.add_argument('-m', '--machine-readable', action="store_true")
        p_lsnapas.add_argument('-p', '--pastable', action="store_true", help='Generate pastable output')
        p_lsnapas.add_argument('-g', '--groupby', nargs='+',
                               choices=snapasgroupby).completer = snapas_group_completer
        p_lsnapas.add_argument('--separators', action="store_true")
        p_lsnapas.add_argument('-N', '--nodes', nargs='+', type=check_node_name,
                               help='Filter by list of nodes').completer = node_completer
        p_lsnapas.add_argument('-R', '--resources', nargs='+', type=check_res_name,
                               help='Filter by list of resources').completer = res_completer
        p_lsnapas.set_defaults(func=self.cmd_list_snapshot_assignments)

        # assignments
        assignverbose = ('Blockdevice', 'Node_ID')
        assigngroupby = ('Node', 'Resource', 'Vol_ID', 'Blockdevice',
                         'Node_ID', 'State')

        ass_verbose_completer = show_group_completer(assignverbose, "show")
        ass_group_completer = show_group_completer(assigngroupby, "groupby")

        p_assignments = subp.add_parser('list-assignments', aliases=['a', 'assignments'],
                                        description="Prints a list of each node's assigned resources."
                                        "Nodes that do not have any resources assigned do not appear in the "
                                        "list. By default, the list is printed as a human readable table.")
        p_assignments.add_argument('-m', '--machine-readable',
                                   action="store_true")
        p_assignments.add_argument('-p', '--pastable', action="store_true", help='Generate pastable output')
        p_assignments.add_argument('-s', '--show', nargs='+',
                                   choices=assignverbose).completer = ass_verbose_completer
        p_assignments.add_argument('-g', '--groupby', nargs='+',
                                   choices=assigngroupby).completer = ass_group_completer
        p_assignments.add_argument('--separators', action="store_true")
        p_assignments.add_argument('-N', '--nodes', nargs='+', type=check_node_name,
                                   help='Filter by list of nodes').completer = node_completer
        p_assignments.add_argument('-R', '--resources', nargs='+', type=check_res_name,
                                   help='Filter by list of resources').completer = res_completer
        p_assignments.set_defaults(func=self.cmd_list_assignments)

        # export
        def exportnamecheck(name):
            if name == '*':
                return name
            return check_res_name(name)

        p_export = subp.add_parser('export-res', aliases=['export'],
                                   description='Exports the configuration files of the specified '
                                   'drbdmanage resource for use with drbdadm. If "*" is used as '
                                   'resource name, the configuration files of all drbdmanage resources '
                                   'deployed on the local node are exported. The configuration files will '
                                   'be created (or updated) in the drbdmanage directory for temporary '
                                   'configuration files, typically /var/lib/drbd.d.')
        p_export.add_argument('resource', nargs="+", type=exportnamecheck,
                              help='Name of the resource').completer = res_completer
        p_export.set_defaults(func=self.cmd_export_conf)

        # howto-join
        p_howtojoin = subp.add_parser('howto-join',
                                      description='Print the command to'
                                      ' execute on the given node in order to'
                                      ' join the cluster')
        p_howtojoin.add_argument('node', type=check_node_name,
                                 help='Name of the node to join').completer = node_completer
        p_howtojoin.add_argument('-q', '--quiet', action="store_true",
                                 help="If the --quiet option is used, the join command is printed "
                                      "with a --quiet option")
        p_howtojoin.set_defaults(func=self.cmd_howto_join)

        def ll_debug_cmd_completer(prefix, **kwargs):
            self.dbus_init()
            # needed to wait for completion
            self._server.Introspect()
            fns = []
            expected = DBUS_DRBDMANAGED + "."
            expected_len = len(expected)
            for fn in self._server._introspect_method_map.iterkeys():
                if not fn.startswith(expected):
                    continue
                fn_short = fn[expected_len:]
                if fn_short.startswith(prefix):
                    fns.append(fn_short)
            return fns

        p_lowlevel_debug = subp.add_parser("lowlevel-debug", description="JSON-to-DBus debug interface")
        p_lowlevel_debug.add_argument("cmd",
                                      help="DBusServer function to call").completer = ll_debug_cmd_completer

        def ll_debug_json_completer(prefix, parsed_args=None, **kwargs):
            self.dbus_init()
            fn = getattr(self._server, parsed_args.cmd)
            if not fn:
                return []

            # TODO: introspect fn, to see whether array/dict/etc. is wanted..
            if prefix == '':
                return ['[]', '{}']
            return []
        p_lowlevel_debug.add_argument("json",
                                      help="JSON to deserialize",
                                      nargs="*").completer = ll_debug_json_completer
        p_lowlevel_debug.set_defaults(func=self.cmd_lowlevel_debug)

        # server-version
        p_server_version = subp.add_parser('server-version',
                                           description='Queries version information from the '
                                           'drbdmanage server')
        p_server_version.set_defaults(func=self.cmd_server_version)

        # message-log
        p_message_log = subp.add_parser('list-message-log', aliases=['message-log', 'list-ml', 'ml'],
                                        description='Queries the server\'s message log')
        p_message_log.set_defaults(func=self.cmd_message_log)

        # clear-message-log
        p_message_log = subp.add_parser('clear-message-log', aliases=['clear-ml', 'cml'],
                                        description='Queries the server\'s message log')
        p_message_log.set_defaults(func=self.cmd_clear_message_log)

        # query-conf
        p_queryconf = subp.add_parser('query-conf',
                                      description='Print the DRBD'
                                      ' configuration file for a given'
                                      ' resource on a given node')
        p_queryconf.add_argument('node', type=check_node_name,
                                 help='Name of the node').completer = node_completer
        p_queryconf.add_argument('resource', type=check_res_name,
                                 help='Name of the resource').completer = res_completer
        p_queryconf.set_defaults(func=self.cmd_query_conf)

        # ping
        p_ping = subp.add_parser('ping', description='Pings the server. The '
                                 'server should answer with a "pong"')
        p_ping.set_defaults(func=self.cmd_ping)

        # wait-for-startup
        p_ping = subp.add_parser('wait-for-startup', description='Wait until server is started up')
        p_ping.set_defaults(func=self.cmd_wait_for_startup)

        # startup
        p_startup = subp.add_parser('startup',
                                    description='Start the server via D-Bus')
        p_startup.set_defaults(func=self.cmd_startup)

        class IPAddressCheck(object):

            def __init__(self):
                pass

            # used for "in" via "choices":
            def __contains__(self, key):
                import socket
                try:
                    ips = socket.getaddrinfo(key, 0)
                except socket.gaierror:
                    return None
                if len(ips) == 0:
                    return None
                return ips[0][4][0]

            def __iter__(self):
                return iter([])  # gives no sane text
                return iter(["any valid IP address"])  # completes this text

        # init
        p_init = subp.add_parser('init', description='Initialize the cluster'
                                 ' (including the control volume)')
        p_init.add_argument('-a', '--address-family', metavar="FAMILY",
                            default='ipv4', choices=['ipv4', 'ipv6'],
                            help='FAMILY: "ipv4" (default) or "ipv6"')
        p_init.add_argument('-p', '--port', type=rangecheck(1, 65535),
                            default=DRBDCTRL_DEFAULT_PORT)
        p_init.add_argument('-q', '--quiet', action="store_true")
        p_init.add_argument('-s', '--no-storage', action="store_true")
        p_init.add_argument('ip', nargs='?',
                            default=default_ip(),
                            help="IP address of the machine",
                            choices=IPAddressCheck())
        p_init.set_defaults(func=self.cmd_init)

        # uninit
        p_uninit = subp.add_parser('uninit', description='Delete the control'
                                   ' volume of a node')
        p_uninit.add_argument('-q', '--quiet', action="store_true")
        p_uninit.add_argument('-s', '--shutdown', action="store_true")
        p_uninit.set_defaults(func=self.cmd_uninit)

        # join
        p_join = subp.add_parser('join-cluster',
                                 description='Join an existing cluster',
                                 aliases=['join'])
        p_join.add_argument('-a', '--address-family', metavar="FAMILY",
                            default='ipv4', choices=['ipv4', 'ipv6'],
                            help='FAMILY: "ipv4" (default) or "ipv6"')
        p_join.add_argument('-p', '--port', type=rangecheck(1, 65535),
                            default=DRBDCTRL_DEFAULT_PORT)
        p_join.add_argument('-q', '--quiet', action="store_true")
        p_join.add_argument('local_ip')
        p_join.add_argument('local_node_id')
        p_join.add_argument('peer_name', type=check_node_name)
        p_join.add_argument('peer_ip').completer = ip_completer("peer_ip")
        p_join.add_argument('peer_node_id')
        p_join.add_argument('secret')
        p_join.set_defaults(func=self.cmd_join)

        # initcv
        p_join = subp.add_parser('initcv',
                                 description='Initialize control volume')
        p_join.add_argument('-q', '--quiet', action="store_true")
        p_join.add_argument('dev', help='Path to the control volume')
        p_join.set_defaults(func=self.cmd_initcv)

        # debug
        p_debug = subp.add_parser('debug')
        p_debug.add_argument('cmd')
        p_debug.set_defaults(func=self.cmd_debug)

        def res_vol_completer(prefix, parsed_args, **kwargs):
            server_rc, res_list = self.__list_resources(True)
            possible = set()
            for r in res_list:
                name, _, vol_list = r
                vol_list.sort(key=lambda vol_entry: vol_entry[0])
                for v in vol_list:
                    vol_id, _ = v
                    possible.add("%s/%d" % (name, vol_id))

            return possible

        # disk-options
        do = DrbdSetupOpts('disk-options')
        if do.ok:
            p_do = do.genArgParseSubcommand(subp)
            p_do.add_argument('--common', action="store_true")
            p_do.add_argument('--resource', type=check_res_name,
                              help='Name of the resource to modify').completer = res_completer
            p_do.add_argument('--volume',
                              help='Name of the volume to modify').completer = res_vol_completer
            p_do.set_defaults(optsobj=do)
            p_do.set_defaults(type="disko")
            p_do.set_defaults(func=self.cmd_disk_options)

        # peer-device-options (shares func with disk-options)
        pdo = DrbdSetupOpts('peer-device-options')
        if pdo.ok:
            p_pdo = pdo.genArgParseSubcommand(subp)
            p_pdo.add_argument('--common', action="store_true")
            p_pdo.add_argument('--resource', type=check_res_name,
                               help='Name of the resource to modify').completer = res_completer
            p_pdo.add_argument('--volume',
                               help='Name of the volume to modify').completer = res_vol_completer
            p_pdo.set_defaults(optsobj=pdo)
            p_pdo.set_defaults(type="peerdisko")
            p_pdo.set_defaults(func=self.cmd_disk_options)

        # resource-options
        ro = DrbdSetupOpts('resource-options')
        if ro.ok:
            p_ro = ro.genArgParseSubcommand(subp)
            p_ro.add_argument('--common', action="store_true")
            p_ro.add_argument('--resource', type=check_res_name,
                              help='Name of the resource to modify').completer = res_completer
            p_ro.set_defaults(optsobj=ro)
            p_ro.set_defaults(type="reso")
            p_ro.set_defaults(func=self.cmd_res_options)

        # net-options
        # TODO: not allowed to set per connection, drbdmanage currently has no notion of a
        # connection in its object model.
        #
        no = DrbdSetupOpts('new-peer', 'net-options')
        if no.ok:
            p_no = no.genArgParseSubcommand(subp)
            p_no.add_argument('--common', action="store_true")
            p_no.add_argument('--resource', type=check_res_name,
                              help='Name of the resource to modify').completer = res_completer
            p_no.add_argument('--sites',
                              help='Set net options between sites (SiteA:SiteB)')
            p_no.set_defaults(optsobj=no)
            p_no.set_defaults(func=self.cmd_net_options)

        # handlers
        # currently we do not parse the xml-output because drbd-utils are not ready for it
        # number and handler names are very static, so use a list for now and add this feature to
        # drbd-utils later
        handlers = (
            'after-resync-target',  'before-resync-target', 'fence-peer', 'initial-split-brain',
            'local-io-error', 'pri-lost', 'pri-lost-after-sb', 'pri-on-incon-degr', 'split-brain',
        )
        p_handlers = subp.add_parser('handlers',
                                     description='Set or unset event handlers.')
        p_handlers.add_argument('--common', action="store_true")
        p_handlers.add_argument('--resource', type=check_res_name,
                                help='Name of the resource to modify').completer = res_completer
        for handler in handlers:
            p_handlers.add_argument('--' + handler, help='Please refer to drbd.conf(5)', metavar='cmd')
            p_handlers.add_argument('--unset-' + handler, action='store_true')
        p_handlers.set_defaults(func=self.cmd_handlers)

        # list-options
        p_listopts = subp.add_parser('list-options',
                                     description='List drbd options set',
                                     aliases=['show-options'])
        p_listopts.add_argument('resource', type=check_res_name,
                                help='Name of the resource to show').completer = res_completer
        p_listopts.set_defaults(func=self.cmd_list_options)
        p_listopts.set_defaults(doobj=do)
        p_listopts.set_defaults(noobj=no)
        p_listopts.set_defaults(roobj=ro)
        p_listopts.set_defaults(pdoobj=pdo)

        # edit config
        p_editconf = subp.add_parser('modify-config',
                                     description='Modify drbdmanage configuration',
                                     aliases=['edit-config'])
        # p_editconf.add_argument('config', choices=('drbdmanage',))
        p_editconf.add_argument('--node', '-n', type=check_node_name,
                                help='Name of the node. This enables node specific options '
                                '(e.g. plugin settings)').completer = node_completer
        p_editconf.set_defaults(func=self.cmd_edit_config)
        p_editconf.set_defaults(type="edit")

        # export config
        p_exportconf = subp.add_parser('export-config',
                                       description='Export drbdmanage configuration',
                                       aliases=['cat-config'])
        p_exportconf.add_argument('--node', '-n', type=check_node_name,
                                  help='Name of the node.').completer = node_completer
        p_exportconf.add_argument('--file', '-f',
                                  help='File to save configuration')
        p_exportconf.set_defaults(func=self.cmd_edit_config)
        p_exportconf.set_defaults(type="export")

        # export ctrl-vol
        p_exportctrlvol = subp.add_parser('export-ctrlvol',
                                          description='Export drbdmanage control volume as json blob')
        p_exportctrlvol.add_argument('--file', '-f',
                                     help='File to save configuration json blob, if not given: stdout')
        p_exportctrlvol.set_defaults(func=self.cmd_export_ctrlvol)

        # import ctrl-vol
        p_importctrlvol = subp.add_parser('import-ctrlvol',
                                          description='Import drbdmanage control volume from json blob')
        p_importctrlvol.add_argument('-q', '--quiet', action="store_true",
                                     help='Unless this option is used, drbdmanage will issue a safety '
                                     'question that must be answered with yes, otherwise the operation '
                                     'is canceled.')
        p_importctrlvol.add_argument('--file', '-f',
                                     help='File to load configuration json blob, if not given: stdin')
        p_importctrlvol.set_defaults(func=self.cmd_import_ctrlvol)

        # role
        p_role = subp.add_parser('role',
                                 description='Show role of local drbdmanaged (controlnode/satellite/unknown)')
        p_role.set_defaults(func=self.cmd_role)

        # dbus-trace
        p_dbustrace_command = 'dbus-trace'
        p_dbustrace = subp.add_parser(p_dbustrace_command,
                                      description='Trace DBUS calls and generate python script')
        p_dbustrace_g = p_dbustrace.add_mutually_exclusive_group()
        p_dbustrace_g.add_argument('-s', '--start', action='store_true', help='Start a tracing')
        p_dbustrace_g.add_argument('-p', '--stop', action='store_true', help='Stop a tracing')
        p_dbustrace.add_argument('-m', '--maxlog', help='Maximum number of tracing entries', type=int)
        p_dbustrace.set_defaults(func=self.cmd_dbustrace)
        p_dbustrace.set_defaults(command=p_dbustrace_command)

        # reelect
        p_reelect = subp.add_parser('reelect', description='Reelect leader. DO NOT USE this command '
                                    'if you do not understand all implications!')
        p_reelect.add_argument('--force-win', action='store_true',
                               help='This is a last resort command to bring up a single leader '
                               'in order to get access to the control volume (e.g. remove node '
                               'in 2 node cluster)')
        p_reelect.set_defaults(func=self.cmd_reelect)

        argcomplete.autocomplete(parser)

        return parser

    def parse(self, pargs):
        args = self._parser.parse_args(pargs)
        args.func(args)

    def parser_cmds(self):
        # AFAIK there is no other way to get the subcommands out of argparse.
        # This avoids at least to manually keep track of subcommands

        cmds = dict()
        subparsers_actions = [
            action for action in self._parser._actions if isinstance(action,
                                                                     argparse._SubParsersAction)]
        for subparsers_action in subparsers_actions:
            for choice, subparser in subparsers_action.choices.items():
                parser_hash = subparser.__hash__
                if parser_hash not in cmds:
                    cmds[parser_hash] = list()
                cmds[parser_hash].append(choice)

        # sort subcommands and their aliases,
        # subcommand dictates sortorder, not its alias (assuming alias is
        # shorter than the subcommand itself)
        cmds_sorted = [sorted(cmd, key=len, reverse=True) for cmd in
                       cmds.values()]

        # "add" and "new" have the same length (as well as "delete" and
        # "remove), therefore prefer one of them to group commands for the
        # "list" command
        for cmds in cmds_sorted:
            idx = 0
            found = False
            for idx, cmd in enumerate(cmds):
                if cmd.startswith("add-") or cmd.startswith("remove-"):
                    found = True
                    break
            if found:
                cmds.insert(0, cmds.pop(idx))

        # sort subcommands themselves
        cmds_sorted.sort(lambda a, b: cmp(a[0], b[0]))
        return cmds_sorted

    def parser_cmds_description(self, all_commands):
        toplevel = [top[0] for top in all_commands]

        subparsers_actions = [
            action for action in self._parser._actions if isinstance(action,
                                                                     argparse._SubParsersAction)]
        description = {}
        for subparsers_action in subparsers_actions:
            for choice, subparser in subparsers_action.choices.items():
                if choice in toplevel:
                    description[choice] = subparser.description

        return description

    def cmd_list(self, args):
        sys.stdout.write('Use "help <command>" to get help for a specific command.\n\n')
        sys.stdout.write('Available commands:\n')
        # import pprint
        # pp = pprint.PrettyPrinter()
        # pp.pprint(self._all_commands)
        for cmd in self._all_commands:
            sys.stdout.write("- " + cmd[0])
            if len(cmd) > 1:
                sys.stdout.write("(%s)" % (", ".join(cmd[1:])))
            sys.stdout.write("\n")

    def cmd_interactive(self, args):
        all_cmds = [i for sl in self._all_commands for i in sl]

        # helper function
        def unknown(cmd):
            sys.stdout.write("\n" + "Command \"%s\" not known!\n" % (cmd))
            self.cmd_list(args)

        # helper function
        def parsecatch(cmds, stoprec=False):
            try:
                self.parse(cmds)
            except SystemExit:  # raised by argparse
                if stoprec:
                    return

                cmd = cmds[0]
                if cmd == "exit":
                    sys.exit(0)
                elif cmd == "help":
                    if len(cmds) == 1:
                        self.cmd_list(args)
                        return
                    else:
                        cmd = " ".join(cmds[1:])
                        if cmd not in all_cmds:
                            unknown(cmd)
                elif cmd in all_cmds:
                    if '-h' in cmds or '--help' in cmds:
                        return
                    sys.stdout.write("\nIncorrect syntax. Use the command as follows:\n")
                    parsecatch(["help", cmd], stoprec=True)
                else:
                    unknown(cmd)

        # main part of interactive mode:

        # try to load readline
        # if loaded, raw_input makes use of it
        try:
            import readline
            completer = argcomplete.CompletionFinder(self._parser)
            readline.set_completer_delims("")
            readline.set_completer(completer.rl_complete)
            readline.parse_and_bind("tab: complete")
        except:
            pass

        self.cmd_list(args)
        while True:
            try:
                sys.stdout.write("\n")
                cmds = raw_input('> ').strip()

                cmds = [cmd.strip() for cmd in cmds.split()]
                if not cmds:
                    self.cmd_list(args)
                else:
                    parsecatch(cmds)
            except (EOFError, KeyboardInterrupt):  # raised by ctrl-d, ctrl-c
                sys.stdout.write("\n")  # additional newline, makes shell prompt happy
                return

    def cmd_help(self, args):
        self.parse([args.command, "-h"])

    def cmd_exit(self, _):
        exit(0)

    def run(self):
        try:
            self.parse(sys.argv[1:])
        except dbus.exceptions.DBusException as exc:
            self._print_dbus_exception(exc)
            exit(1)

    def cmd_poke(self, args):
        fn_rc = 1
        self.dbus_init()
        server_rc = self.dsc(self._server.poke)
        fn_rc = self._list_rc_entries(server_rc)
        return fn_rc

    def cmd_new_node(self, args):
        fn_rc = 1
        name = args.name
        satellite = args.satellite
        ip = args.ip
        af = args.address_family
        if af is None:
            af = drbdmanage.drbd.drbdcore.DrbdNode.AF_IPV4_LABEL
        flag_storage = not args.no_storage
        flag_external = args.external
        flag_drbdctrl = not (flag_external or satellite)
        flag_autojoin = not args.no_autojoin

        if flag_external:
            # currently not implemented
            # start rm
            sys.stderr.write('Currently not implemented\n')
            sys.exit(1)
            # end rm
            if satellite:
                sys.stderr.write('Not allowed to mix --external with --satellite\n')
                sys.exit(1)
            flag_storage = False
            flag_drbdctrl = False

        props = dbus.Dictionary(signature="ss")
        props[NODE_ADDR] = ip
        props[NODE_AF] = af
        if not flag_drbdctrl:
            props[FLAG_DRBDCTRL] = bool_to_string(flag_drbdctrl)
        if not flag_storage:
            props[FLAG_STORAGE] = bool_to_string(flag_storage)
        if flag_external:
            props[FLAG_EXTERNAL] = bool_to_string(flag_external)

        self.dbus_init()
        server_rc = self.dsc(self._server.create_node, name, props)
        fn_rc = self._list_rc_entries(server_rc)

        if fn_rc == 0:  # join node
            server_rc, joinc = self.dsc(self._server.text_query, ["joinc", name])
            joinc_text = str(" ".join(joinc))

            fn_rc = self._list_rc_entries(server_rc)

            # Text queries do not return error codes, so check whether the
            # string returned by the server looks like a join command or
            # like an error message
            if joinc_text.startswith("Error:"):
                sys.stderr.write(joinc_text + "\n")
            else:
                join_performed = False
                if flag_autojoin:
                    # ssh_exec("wait-for-startup", ip, name, ['drbdmanage', 'wait-for-startup'],
                    #          False, False)
                    # THINK: the following is racy:
                    # leader may not have contacted satellite before calling "join" (which wants persistence)
                    # self._server.poke()
                    join_performed = ssh_exec("join", ip, name, joinc,
                                              args.quiet or satellite, satellite)
                if not join_performed:
                    sys.stdout.write("\nJoin command for node %s:\n"
                                     "%s\n" % (name, joinc_text))
        return fn_rc

    def cmd_new_resource(self, args):
        fn_rc = 1

        name = args.name
        port = args.port if args.port else RES_PORT_NR_AUTO

        props = dbus.Dictionary(signature="ss")
        props[RES_PORT] = str(port)

        self.dbus_init()
        server_rc = self.dsc(self._server.create_resource, dbus.String(name), props)
        fn_rc = self._list_rc_entries(server_rc)
        return fn_rc

    def cmd_new_volume(self, args):
        fn_rc = 1

        minor = MinorNr.MINOR_NR_AUTO
        if args.minor is not None:
            minor = args.minor
        name = args.name
        deploy = args.deploy

        try:
            size = self._get_volume_size_arg(args)

            props = dbus.Dictionary(signature="ss")

            self.dbus_init()
            server_rc = self.dsc(self._server.create_resource, dbus.String(name), props)
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
                server_rc = self.dsc(self._server.create_volume,
                                     dbus.String(name),
                                     dbus.Int64(size), props)
                fn_rc = self._list_rc_entries(server_rc)

                if fn_rc == 0 and deploy is not None:
                    server_rc = self.dsc(self._server.auto_deploy_site,
                                         dbus.String(name), dbus.Int32(deploy), dbus.Int32(0),
                                         dbus.Boolean(False), dbus.String(args.site))
                    fn_rc = self._list_rc_entries(server_rc)
        except SyntaxException:
            self.cmd_help(args)

        return fn_rc

    def cmd_resize_volume(self, args):
        fn_rc = 1
        try:
            name   = args.name
            vol_id = args.id
            size   = self._get_volume_size_arg(args)

            self.dbus_init()
            server_rc = self.dsc(self._server.resize_volume,
                                 dbus.String(name), dbus.Int32(vol_id),
                                 dbus.Int64(0), dbus.Int64(size), dbus.Int64(0))
            fn_rc = self._list_rc_entries(server_rc)
        except SyntaxException:
            self.cmd_help(args)
        return fn_rc

    def _get_volume_size_arg(self, args):
        m = re.match('(\d+)(\D*)', args.size)

        size = 0
        try:
            size = int(m.group(1))
        except AttributeError:
            sys.stderr.write('Size is not a valid number\n')
            raise SyntaxException

        unit_str = m.group(2)
        if unit_str == "":
            unit_str = "GiB"
        try:
            unit = self.UNITS_MAP[unit_str.lower()]
        except KeyError:
            sys.stderr.write('"%s" is not a valid unit!\n' % (unit_str))
            sys.stderr.write('Valid units: %s\n' % (','.join(self.UNITS_MAP.keys())))
            raise SyntaxException

        unit = self.UNITS_MAP[unit_str.lower()]

        if unit != SizeCalc.UNIT_kiB:
            size = SizeCalc.convert_round_up(size, unit,
                                             SizeCalc.UNIT_kiB)

        return size

    def cmd_modify_node(self, args):
        fn_rc = 1

        try:
            props = dbus.Dictionary(signature="ss")
            if args.address_family is not None:
                props[NODE_AF] = args.address_family
            if args.address is not None:
                props[NODE_ADDR] = args.address
            if args.storage is not None:
                props[FLAG_STORAGE] = args.storage

            if len(props) == 0:
                raise SyntaxException

            self.dbus_init()
            server_rc = self.dsc(self._server.modify_node,
                                 dbus.String(args.name), 0, props)
            fn_rc = self._list_rc_entries(server_rc)
        except SyntaxException:
            self.cmd_help(args)

        return fn_rc

    def cmd_modify_resource(self, args):
        fn_rc = 1

        try:
            props = dbus.Dictionary(signature="ss")
            if args.port is not None:
                try:
                    props[RES_PORT] = str(args.port)
                except ValueError:
                    raise SyntaxException

            if args.managed is not None:
                props[MANAGED] = args.managed

            if len(props) == 0:
                raise SyntaxException

            self.dbus_init()
            server_rc = self.dsc(self._server.modify_resource,
                                 dbus.String(args.name), 0, props)
            fn_rc = self._list_rc_entries(server_rc)
        except SyntaxException:
            self.cmd_help(args)
        return fn_rc

    def cmd_modify_volume(self, args):
        fn_rc = 1
        try:
            props = dbus.Dictionary(signature="ss")
            if args.minor is not None:
                try:
                    props[VOL_MINOR] = str(args.minor)
                except ValueError:
                    raise SyntaxException

            if len(props) == 0:
                raise SyntaxException

            self.dbus_init()
            server_rc = self.dsc(self._server.modify_volume,
                                 dbus.String(args.name), dbus.Int32(args.id), 0, props)
            fn_rc = self._list_rc_entries(server_rc)
        except SyntaxException:
            self.cmd_help(args)
        return fn_rc

    def cmd_modify_assignment(self, args):
        fn_rc = 1

        try:
            props = dbus.Dictionary(signature="ss")
            if args.overwrite is not None:
                props[FLAG_OVERWRITE] = self._args_bool_to_string(args.overwrite)
            if args.discard is not None:
                props[FLAG_DISCARD] = self._args_bool_to_string(args.discard)

            if len(props) == 0:
                raise SyntaxException

            self.dbus_init()
            server_rc = self.dsc(self._server.modify_assignment,
                                 dbus.String(args.resource), dbus.String(args.node), 0, props)
            fn_rc = self._list_rc_entries(server_rc)
        except SyntaxException:
            self.cmd_help(args)
        return fn_rc

    def cmd_remove_node(self, args):
        fn_rc = 0

        force = args.force
        quiet = args.quiet
        confirmed = True
        self.dbus_init()
        display_names = (len(args.name) > 1)
        for node_name in args.name:
            if not quiet:
                confirmed = self.user_confirm(
                    "You are going to remove the node '%s' from the cluster. "
                    "This will remove all resources from the node.\n"
                    "Please confirm:"
                    % (node_name)
                )
            if confirmed:
                self.dbus_init()
                server_rc = self.dsc(self._server.remove_node,
                                     dbus.String(node_name), dbus.Boolean(force))
                if display_names:
                    sys.stdout.write("Removing node '%s':\n" % (node_name))
                item_rc = self._list_rc_entries(server_rc)
                if display_names:
                    sys.stdout.write("\n")
                if item_rc != 0:
                    fn_rc = 1

        return fn_rc

    def cmd_remove_resource(self, args):
        fn_rc = 0

        force = args.force
        quiet = args.quiet
        confirmed = True
        self.dbus_init()
        display_names = (len(args.name) > 1)
        for res_name in args.name:
            if not quiet:
                confirmed = self.user_confirm(
                    "You are going to remove the resource '%s' and all of its "
                    "volumes from all nodes of the cluster.\n"
                    "Please confirm:"
                    % (res_name)
                )
            if confirmed:
                server_rc = self.dsc(self._server.remove_resource,
                                     dbus.String(res_name), dbus.Boolean(force))
                if display_names:
                    sys.stdout.write("Removing resource '%s':\n" % (res_name))
                item_rc = self._list_rc_entries(server_rc)
                if display_names:
                    sys.stdout.write("\n")
                if item_rc != 0:
                    fn_rc = 1

        return fn_rc

    def cmd_remove_volume(self, args):
        fn_rc = 1

        vol_name = args.name
        vol_id = args.vol_id
        force = args.force
        quiet = args.quiet
        if not quiet:
            quiet = self.user_confirm(
                "You are going to remove a volume from all nodes of "
                "the cluster.\n"
                "Please confirm:"
            )
        if quiet:
            self.dbus_init()
            server_rc = self.dsc(self._server.remove_volume,
                                 dbus.String(vol_name), dbus.Int32(vol_id), dbus.Boolean(force))
            fn_rc = self._list_rc_entries(server_rc)
        else:
            fn_rc = 0

        return fn_rc

    def cmd_connect(self, args):
        return self._connect(args, False)

    def cmd_reconnect(self, args):
        return self._connect(args, True)

    def _connect(self, args, reconnect):
        fn_rc = 1

        node_name = args.node
        res_name = args.resource

        self.dbus_init()
        server_rc = self.dsc(self._server.connect,
                             dbus.String(node_name), dbus.String(res_name),
                             dbus.Boolean(reconnect))
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_disconnect(self, args):
        fn_rc = 1

        node_name = args.node
        res_name = args.resource

        self.dbus_init()
        server_rc = self.dsc(self._server.disconnect,
                             dbus.String(node_name), dbus.String(res_name),
                             dbus.Boolean(False))
        fn_rc = self._list_rc_entries(server_rc)
        return fn_rc

    def cmd_flags(self, args):
        fn_rc = 1
        clear_mask = 0
        set_mask = 0

        res_name = args.resource
        node_name = args.node

        if args.reconnect is not None:
            if args.reconnect == 1:
                set_mask |= Assignment.FLAG_RECONNECT
            else:
                clear_mask |= Assignment.FLAG_RECONNECT

        if args.updcon is not None:
            if args.updcon == 1:
                set_mask |= Assignment.FLAG_UPD_CON
            else:
                clear_mask |= Assignment.FLAG_UPD_CON

        if args.overwrite is not None:
            if args.overwrite == 1:
                set_mask |= Assignment.FLAG_OVERWRITE
            else:
                clear_mask |= Assignment.FLAG_OVERWRITE

        if args.discard is not None:
            if args.discard == 1:
                set_mask |= Assignment.FLAG_DISCARD
            else:
                clear_mask |= Assignment.FLAG_DISCARD

        self.dbus_init()
        server_rc = self.dsc(self._server.modify_state,
                             dbus.String(node_name), dbus.String(res_name), dbus.UInt64(0),
                             dbus.UInt64(0), dbus.UInt64(clear_mask), dbus.UInt64(set_mask))
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_attach_detach(self, args):
        fn_rc = 1

        node_name = args.node
        res_name = args.resource
        vol_id = args.id

        self.dbus_init()

        if args.fname == "attach":
            func = self._server.attach
        elif args.fname == "detach":
            func = self._server.detach
        else:
            sys.stderr.write("Wether attach nor detach\n")
            exit(1)
        server_rc = self.dsc(func,
                             dbus.String(node_name), dbus.String(res_name),
                             dbus.Int32(vol_id))
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_assign(self, args):
        fn_rc = 0

        res_name = args.resource

        client = args.client
        overwrite = args.overwrite
        discard = args.discard
        # Turn on the connect flag by default; drbdadm adjust connects
        # anyway, so this flag does not make a lot of sense at this time,
        # but it may be useful in the future
        connect = True
        # connect   = flags["-c"]

        # we cannot handle this complex "double mutually exclusive" situation
        # with argparse and its add_mutually_exclusive_group() :/
        if (overwrite and client):
            sys.stderr.write(
                "Error: --overwrite and --client are mutually "
                "exclusive options\n"
            )
            exit(1)
        if (overwrite and discard):
            sys.stderr.write(
                "Error: --overwrite and --discard are mutually "
                "exclusive options\n"
            )
            exit(1)

        props = {}
        props[FLAG_DISKLESS] = bool_to_string(client)
        props[FLAG_OVERWRITE] = bool_to_string(overwrite)
        props[FLAG_DISCARD] = bool_to_string(discard)
        props[FLAG_CONNECT] = bool_to_string(connect)

        self.dbus_init()

        display_names = (len(args.node) > 1)
        for node_name in args.node:
            if display_names:
                sys.stdout.write("Assigning to node '%s':\n" % (node_name))
            server_rc = self.dsc(self._server.assign,
                                 dbus.String(node_name), dbus.String(res_name), props)
            item_rc = self._list_rc_entries(server_rc)
            if display_names:
                sys.stdout.write("\n")
            if item_rc != 0:
                fn_rc = 1

        return fn_rc

    def cmd_free_space(self, args):
        fn_rc = 1

        redundancy = args.redundancy

        self.dbus_init()
        server_rc, free_space, total_space = (
            self.dsc(self._server.cluster_free_query_site,
                     dbus.Int32(redundancy), dbus.String(args.site))
        )

        successful = self._is_rc_successful(server_rc)
        if successful:
            machine_readable = args.machine_readable
            if machine_readable:
                sys.stdout.write("%lu,%lu\n" % (free_space, total_space))
            else:
                sys.stdout.write(
                    "The maximum size for a %dx redundant "
                    "volume is %lu kiB\n"
                    "(Aggregate cluster storage size: %lu kiB)\n"
                    % (redundancy, free_space, total_space)
                )
        fn_rc = self._list_rc_entries(server_rc)
        return fn_rc

    def cmd_deploy(self, args):
        fn_rc = 1
        res_name = args.resource
        count = args.redundancy_count
        delta = 0
        site_clients = args.with_clients

        if args.decrease:
            count *= -1

        if args.increase or args.decrease:
            count, delta = delta, count

        self.dbus_init()
        server_rc = self.dsc(self._server.auto_deploy_site,
                             dbus.String(res_name), dbus.Int32(count),
                             dbus.Int32(delta), dbus.Boolean(site_clients), dbus.String(args.site))
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_undeploy(self, args):
        fn_rc = 1

        res_name = args.resource
        force = args.force
        quiet = args.quiet
        if not quiet:
            quiet = self.user_confirm(
                "You are going to undeploy this resource from all nodes "
                "of the cluster.\n"
                "Please confirm:"
            )
        if quiet:
            self.dbus_init()
            server_rc = self.dsc(self._server.auto_undeploy,
                                 dbus.String(res_name), dbus.Boolean(force))
            fn_rc = self._list_rc_entries(server_rc)
        else:
            fn_rc = 0

        return fn_rc

    def cmd_update_pool(self, args):
        fn_rc = 1
        self.dbus_init()
        server_rc = self.dsc(self._server.update_pool, dbus.Array([], signature="s"))
        fn_rc = self._list_rc_entries(server_rc)
        return fn_rc

    def cmd_reconfigure(self, args):
        fn_rc = 1
        self.dbus_init()
        server_rc = self.dsc(self._server.reconfigure)
        fn_rc = self._list_rc_entries(server_rc)
        return fn_rc

    def cmd_save(self, args):
        fn_rc = 1
        self.dbus_init()
        server_rc = self.dsc(self._server.save_conf)
        fn_rc = self._list_rc_entries(server_rc)
        return fn_rc

    def cmd_load(self, args):
        fn_rc = 1
        self.dbus_init()
        server_rc = self.dsc(self._server.load_conf)
        fn_rc = self._list_rc_entries(server_rc)
        return fn_rc

    def cmd_unassign(self, args):
        fn_rc = 0

        res_name = args.resource
        force = args.force
        # quiet = args.quiet
        # TODO: implement quiet

        self.dbus_init()

        display_names = (len(args.node) > 1)
        for node_name in args.node:
            if display_names:
                sys.stdout.write("Unassigning from node '%s':\n" % (node_name))
            server_rc = self.dsc(self._server.unassign, node_name, res_name, force)
            item_rc = self._list_rc_entries(server_rc)
            if display_names:
                sys.stdout.write("\n")
            if item_rc != 0:
                fn_rc = 1

        return fn_rc

    def cmd_quorum_control(self, args):
        fn_rc = 0

        override = args.override
        action   = args.action
        display_names = (len(args.name))

        # TODO: handle invalid actions
        qignore_field = BOOL_FALSE
        if action == "ignore":
            qignore_field = BOOL_TRUE

        props = dbus.Dictionary(signature="ss")
        props[FLAG_QIGNORE] = qignore_field

        self.dbus_init()
        for node_name in args.name:
            if display_names:
                sys.stdout.write("Modifying quorum state of node '%s':\n" % (node_name))
            server_rc = self.dsc(self._server.quorum_control,
                dbus.String(node_name), props, dbus.Boolean(override)
            )
            item_rc = self._list_rc_entries(server_rc)
            if item_rc != 0:
                fn_rc = 1
            if display_names:
                sys.stdout.write("\n")

        return fn_rc

    def cmd_new_snapshot(self, args):
        fn_rc = 1

        res_name = args.resource
        snaps_name = args.snapshot
        node_list = args.nodes

        props = dbus.Dictionary(signature="ss")

        self.dbus_init()
        server_rc = self.dsc(self._server.create_snapshot,
            dbus.String(res_name), dbus.String(snaps_name),
            dbus.Array(node_list, signature="s"), props
        )
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_remove_snapshot(self, args):
        fn_rc = 0

        res_name = args.resource
        force = args.force

        self.dbus_init()
        display_names = (len(args.snapshot) > 1)
        for snaps_name in args.snapshot:
            if display_names:
                sys.stdout.write("Removing snapshot '%s':\n" % (snaps_name))
            server_rc = self.dsc(self._server.remove_snapshot,
                dbus.String(res_name), dbus.String(snaps_name), force
            )
            item_rc = self._list_rc_entries(server_rc)
            if display_names:
                sys.stdout.write("\n")
            if item_rc != 0:
                fn_rc = 1

        return fn_rc

    def cmd_remove_snapshot_assignment(self, args):
        fn_rc = 1

        res_name = args.resource
        snaps_name = args.snapshot
        node_name = args.node
        force = args.force

        self.dbus_init()
        server_rc = self.dsc(self._server.remove_snapshot_assignment,
            dbus.String(res_name), dbus.String(snaps_name),
            dbus.String(node_name), force
        )
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_restore_snapshot(self, args):
        fn_rc = 1

        res_name = args.resource
        snaps_res_name = args.snapshot_resource
        snaps_name = args.snapshot

        res_props = dbus.Dictionary(signature="ss")
        vols_props = dbus.Dictionary(signature="ss")

        self.dbus_init()
        server_rc = self.dsc(self._server.restore_snapshot,
            dbus.String(res_name), dbus.String(snaps_res_name),
            dbus.String(snaps_name), res_props, vols_props
        )
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_resume_all(self, args):
        self.dbus_init()
        server_rc = self.dsc(self._server.resume_all)
        fn_rc = self._list_rc_entries(server_rc)
        return fn_rc

    def cmd_shutdown(self, args, doexit=True):
        quiet = args.quiet
        satellites = args.satellite
        resources = args.resources
        props = dbus.Dictionary(signature="ss")
        props[KEY_S_CMD_SHUTDOWN] = bool_to_string(satellites)
        props[KEY_SHUTDOWN_RES] = bool_to_string(resources)
        if not quiet:
            quiet = self.user_confirm(
                "You are going to shut down the drbdmanaged server "
                "process on this node.\nPlease confirm:"
            )
        if quiet:
            try:
                self.dbus_init()
                self.dsc(self._server.shutdown, props)
            except dbus.exceptions.DBusException:
                # An exception is expected here, as the server
                # probably will not answer
                pass
            # make sure the service is no longer available
            if self._dbus:
                try:
                    dbo = self._dbus.get_object('org.freedesktop.DBus',
                                                '/org/freedesktop/DBus')

                    dbus_iface = dbus.Interface(dbo, 'org.freedesktop.DBus')
                    tries, retries_max = 0, 15
                    while tries <= retries_max:
                        services = dbus_iface.ListNames()
                        if DBUS_DRBDMANAGED not in services:
                            break
                        time.sleep(1)
                        tries += 1
                except:
                    pass

            # Continuing the client without a server
            # does not make sense, therefore exit
            if doexit:
                exit(0)
        return 0

    def cmd_restart(self, args):
        self.cmd_shutdown(args, doexit=False)
        self._server = None
        self.cmd_startup(None)

    def _get_nodes(self, sort=False, node_filter=[]):
        self.dbus_init()

        server_rc, node_list = self.dsc(self._server.list_nodes,
                                        dbus.Array(node_filter, signature="s"),
                                        0,
                                        dbus.Dictionary({}, signature="ss"),
                                        dbus.Array([], signature="s"))

        if sort:
            node_list.sort(key=lambda node_entry: node_entry[0])

        return (server_rc, node_list)

    def cmd_list_nodes(self, args):
        color = self.color

        machine_readable = args.machine_readable
        pastable = args.pastable
        if pastable:
            self._colors = False

        node_filter_arg = [] if args.nodes is None else args.nodes

        server_rc, node_list = self._get_nodes(sort=True,
                                               node_filter=node_filter_arg)

        if (not machine_readable) and (node_list is None or len(node_list) == 0):
            sys.stdout.write("No nodes defined\n")
            return 0

        t = Table(colors=self._colors, utf8=self._utf8, pastable=pastable)
        if not args.groupby:
            groupby = ["Name"]
        else:
            groupby = args.groupby

        t.add_column("Name", color=color(COLOR_TEAL))
        t.add_column("Pool_Size", color=color(COLOR_BROWN), just_txt='>')
        t.add_column("Pool_Free", color=color(COLOR_BROWN), just_txt='>')
        t.add_column("Site", color=color(COLOR_BROWN), just_txt='>')
        t.add_column("Family", just_txt='>')
        t.add_column("IP", just_txt='>')
        t.add_column("State", color=color(COLOR_DARKGREEN), just_txt='>', just_col='>')

        # fixed ones we always show
        tview = ["Name", "Pool_Size", "Pool_Free", "State"]
        if args.show:
            tview += args.show
        t.set_view(tview)

        t.set_groupby(groupby)

        for node_entry in node_list:
            try:
                node_name, properties = node_entry
                view = DrbdNodeView(properties, machine_readable)
                v_af = self._property_text(view.get_property(NODE_AF))
                v_addr = self._property_text(view.get_property(NODE_ADDR))
                ns = Props.NAMESPACES[Props.KEY_DMCONFIG]
                v_site = self._property_text(view.get_property(ns + NODE_SITE))
                if not machine_readable:
                    prop_str = view.get_property(NODE_POOLSIZE)
                    try:
                        poolsize_kiB = int(prop_str)
                        poolsize = SizeCalc.convert(
                            poolsize_kiB, SizeCalc.UNIT_kiB, SizeCalc.UNIT_MiB
                        )
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
                        poolfree = SizeCalc.convert(
                            poolfree_kiB, SizeCalc.UNIT_kiB, SizeCalc.UNIT_MiB
                        )
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

                    level, state_text = view.state_info()
                    level_color = self._level_color(level)
                    row_data = [
                        node_name, poolsize_text, poolfree_text, v_site,
                        "ipv" + v_af, v_addr, (level_color, state_text)
                    ]
                    t.add_row(row_data)
                else:
                    v_psize = self._property_text(
                        view.get_property(NODE_POOLSIZE))
                    v_pfree = self._property_text(
                        view.get_property(NODE_POOLFREE))

                    sys.stdout.write(
                        "%s,%s,%s,%s,%s,%s,%s\n"
                        % (node_name, v_af,
                           v_addr, v_psize,
                           v_pfree, view.get_state(), v_site)
                    )
            except IncompatibleDataException:
                sys.stderr.write("Warning: incompatible table entry skipped\n")

        if not machine_readable:
            t.set_show_separators(args.separators)
            t.show()
        return 0

    def cmd_list_resources(self, args):
        return self._list_resources(args, False)

    def cmd_list_volumes(self, args):
        return self._list_resources(args, True)

    def __list_resources(self, list_volumes, resource_filter=[]):
        self.dbus_init()

        if list_volumes:
            server_rc, res_list = self.dsc(self._server.list_volumes,
                                           dbus.Array(resource_filter, signature="s"),
                                           0,
                                           dbus.Dictionary({}, signature="ss"),
                                           dbus.Array([], signature="s"))
        else:
            server_rc, res_list = self.dsc(self._server.list_resources,
                                           dbus.Array(resource_filter, signature="s"),
                                           0,
                                           dbus.Dictionary({}, signature="ss"),
                                           dbus.Array([], signature="s"))

        # sort the resource list by resource name
        res_list.sort(key=lambda res_entry: res_entry[0])

        return (server_rc, res_list)

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

        machine_readable = args.machine_readable
        pastable = args.pastable
        if pastable:
            self._colors = False

        resource_filter_arg = [] if args.resources is None else args.resources

        server_rc, res_list = self.__list_resources(
            list_volumes, resource_filter=resource_filter_arg
        )

        if (not machine_readable) and (res_list is None or len(res_list) == 0):
                sys.stdout.write("No resources defined\n")
                return 0

        t = Table(colors=self._colors, utf8=self._utf8, pastable=pastable)

        if not args.groupby:
            groupby = ["Name"]
        else:
            groupby = args.groupby

        t.add_column("Name", color=color(COLOR_TEAL))
        if list_volumes:
            t.add_column("Vol_ID", color=color(COLOR_BROWN), just_txt='>')
            t.add_column("Size", color=color(COLOR_BROWN), just_txt='>')
            t.add_column("Minor", color=color(COLOR_BROWN), just_txt='>')
        t.add_column("Port", just_txt='>')
        t.add_column("State", color=color(COLOR_DARKGREEN), just_txt='>', just_col='>')

        # fixed ones we always show
        tview = ["Name", "State"]

        if list_volumes:
            tview += ["Vol_ID", "Size", "Minor"]

        if args.show:
            tview += args.show
        t.set_view(tview)

        t.set_groupby(groupby)

        for res_entry in res_list:
            try:
                if list_volumes:
                    res_name, properties, vol_list = res_entry
                else:
                    res_name, properties = res_entry
                res_view = DrbdResourceView(properties, machine_readable)
                v_port = self._property_text(res_view.get_property(RES_PORT))
                if not machine_readable and not list_volumes:
                    # Human readable output of the resource description
                    level, state_text = res_view.state_info()
                    level_color = self._level_color(level)
                    row_data = [
                        res_name, v_port, state_text
                    ]
                    if level == GenericView.STATE_NORM:
                        t.add_row(row_data)
                    else:
                        t.add_row(row_data, color=color(level_color))
                if list_volumes:
                    # sort volume list by volume id
                    vol_list.sort(key=lambda vol_entry: vol_entry[0])
                    if len(vol_list) > 0:
                        for vol_entry in vol_list:
                            vol_id, vol_properties = vol_entry
                            vol_view = DrbdVolumeView(vol_properties,
                                                      machine_readable)
                            v_minor = self._property_text(
                                vol_view.get_property(VOL_MINOR)
                            )
                            if not machine_readable:
                                # human readable output of the volume description
                                size_info = approximate_size_string(vol_view.get_size_kiB())
                                level, state_text = vol_view.state_info()
                                level_color = self._level_color(level)
                                row_data = [
                                    res_name, str(vol_view.get_id()),
                                    size_info, v_minor, v_port, (level_color, state_text)
                                ]
                                t.add_row(row_data)
                            else:
                                # machine readable output of the volume description
                                sys.stdout.write(
                                    "%s,%s,%s,%d,%s,%s,%s\n"
                                    % (res_name, res_view.get_state(),
                                       str(vol_view.get_id()),
                                       vol_view.get_size_kiB(), v_port,
                                       v_minor, vol_view.get_state())
                                )
                    else:
                        row_data = [
                            res_name, "*", "*", "*", v_port, "*"
                        ]
                        t.add_row(row_data)
                elif machine_readable:
                    # machine readable output of the resource description
                    sys.stdout.write(
                        "%s,%s,%s\n"
                        % (res_name, v_port, res_view.get_state())
                    )
            except IncompatibleDataException:
                sys.stderr.write("Warning: incompatible table entry skipped\n")

        if not machine_readable:
            t.set_show_separators(args.separators)
            # t.show(overwrite=list_volumes)
            t.show()
        return 0

    def _list_snapshots(self, resource_filter=[]):
        self.dbus_init()

        server_rc, res_list = self.dsc(self._server.list_snapshots,
                                       dbus.Array(resource_filter, signature="s"),
                                       dbus.Array([], signature="s"),
                                       0,
                                       dbus.Dictionary({}, signature="ss"),
                                       dbus.Array([], signature="s"))

        # sort the list by resource name
        res_list.sort(key=lambda res_entry: res_entry[0])

        return (server_rc, res_list)

    def cmd_list_snapshots(self, args):
        color = self.color

        machine_readable = args.machine_readable
        pastable = args.pastable
        if pastable:
            self._colors = False

        resource_filter_arg = [] if args.resources is None else args.resources

        server_rc, res_list = self._list_snapshots(
            resource_filter=resource_filter_arg
        )

        if (not machine_readable) and (res_list is None or len(res_list) == 0):
            sys.stdout.write("Snapshot list is empty\n")
            return 0

        t = Table(colors=self._colors, utf8=self._utf8, pastable=pastable)
        if not args.groupby:
            groupby = ["Resource"]
        else:
            groupby = args.groupby

        t.add_column("Resource", color=color(COLOR_DARKGREEN))
        t.add_column("Name", color=color(COLOR_DARKPINK))
        t.add_column("State", color=color(COLOR_DARKGREEN), just_txt='>', just_col='>')

        t.set_groupby(groupby)

        for res_entry in res_list:
            res_name, snaps_list = res_entry
            # sort the list by snapshot name
            snaps_list.sort(key=lambda snaps_entry: snaps_entry[0])
            for snaps_entry in snaps_list:
                snaps_name, snaps_props = snaps_entry
                if machine_readable:
                    sys.stdout.write("%s,%s\n" % (res_name, snaps_name))
                else:
                    t.add_row([res_name, snaps_name, "n/a"])

        if not machine_readable:
            t.set_show_separators(args.separators)
            t.show()
        return 0

    def cmd_list_snapshot_assignments(self, args):
        color = self.color

        self.dbus_init()

        machine_readable = args.machine_readable
        pastable = args.pastable
        if pastable:
            self._colors = False

        node_filter_arg = [] if args.nodes is None else args.nodes
        resource_filter_arg = [] if args.resources is None else args.resources

        server_rc, assg_list = self.dsc(self._server.list_snapshot_assignments,
                                        dbus.Array(resource_filter_arg, signature="s"),
                                        dbus.Array([], signature="s"),
                                        dbus.Array(node_filter_arg, signature="s"),
                                        0,
                                        dbus.Dictionary({}, signature="ss"),
                                        dbus.Array([], signature="s"))

        if (not machine_readable) and (assg_list is None or len(assg_list) == 0):
            sys.stdout.write("Snapshot assignment list is empty\n")
            return 0

        t = Table(colors=self._colors, utf8=self._utf8, pastable=pastable)
        if not args.groupby:
            groupby = ["Resource", "Name"]
        else:
            groupby = args.groupby

        t.add_column("Resource", color=color(COLOR_DARKGREEN))
        t.add_column("Name", color=color(COLOR_DARKPINK))
        t.add_column("Node", color=color(COLOR_TEAL))
        t.add_column("State", color=color(COLOR_DARKGREEN), just_txt='>', just_col='>')

        t.set_groupby(groupby)

        for assg_list_entry in assg_list:
            res_name, snaps_name, snaps_assg_list = assg_list_entry
            for snaps_assg_entry in snaps_assg_list:
                node_name, snaps_assg_props = snaps_assg_entry
                snaps_assg = DrbdSnapshotAssignmentView(
                    snaps_assg_props, machine_readable
                )

                if machine_readable:
                    sys.stdout.write(
                        "%s,%s,%s,%s,%s\n"
                        % (res_name, snaps_name, node_name,
                           snaps_assg.get_cstate(), snaps_assg.get_tstate())
                    )
                else:
                    level, state_text = snaps_assg.state_info()
                    level_color = self._level_color(level)
                    row_data = [
                        res_name, snaps_name, node_name, (level_color, state_text)
                    ]
                    t.add_row(row_data)

        if not machine_readable:
            t.set_show_separators(args.separators)
            t.show()
        return 0

    def cmd_list_assignments(self, args):
        color = self.color

        self.dbus_init()

        machine_readable = args.machine_readable
        pastable = args.pastable
        if pastable:
            self._colors = False

        node_filter_arg = [] if args.nodes is None else args.nodes
        resource_filter_arg = [] if args.resources is None else args.resources

        server_rc, assg_list = self.dsc(self._server.list_assignments,
                                        dbus.Array(node_filter_arg, signature="s"),
                                        dbus.Array(resource_filter_arg, signature="s"),
                                        0,
                                        dbus.Dictionary({}, signature="ss"),
                                        dbus.Array([], signature="s"))
        if (not machine_readable) and (assg_list is None or len(assg_list) == 0):
            sys.stdout.write("No assignments defined\n")
            return 0

        t = Table(colors=self._colors, utf8=self._utf8, pastable=pastable)

        if not args.groupby:
            groupby = ["Node", "Resource"]
        else:
            groupby = args.groupby

        t.add_column("Node", color=color(COLOR_TEAL))
        t.add_column("Resource", color=color(COLOR_DARKGREEN))
        t.add_column("Vol_ID", color=color(COLOR_DARKPINK), just_txt='>')
        t.add_column("Blockdevice")
        t.add_column("Node_ID", just_txt='>')
        t.add_column("State", color=color(COLOR_DARKGREEN), just_txt='>', just_col='>')

        # fixed ones we always show
        tview = ["Node", "Resource", "Vol_ID", "State"]

        if args.show:
            tview += args.show
        t.set_view(tview)

        t.set_groupby(groupby)

        for assg_entry in assg_list:
            try:
                node_name, res_name, properties, vol_state_list = assg_entry
                view = AssignmentView(properties, machine_readable)
                v_node_id = self._property_text(view.get_property(NODE_ID))
                v_cstate  = view.get_cstate()
                v_tstate  = view.get_tstate()
                if not machine_readable:
                    level, state_text = view.state_info()
                    level_color = self._level_color(level)
                    row_data = [
                        node_name, res_name, "*", "*", "*", (level_color, state_text)
                    ]
                    t.add_row(row_data)

                    for vol_state in vol_state_list:
                        vol_id, properties = vol_state
                        vol_view = DrbdVolumeStateView(properties,
                                                       machine_readable)

                        v_level, v_state_text = vol_view.state_info()
                        v_level_color = self._level_color(v_level)

                        if v_level != GenericView.STATE_NORM:
                            v_bdev = self._property_text(
                                vol_view.get_property(VOL_BDEV)
                            )
                            v_row_data = [
                                node_name, res_name, vol_id,
                                v_bdev, v_node_id, (v_level_color, v_state_text)
                            ]
                            t.add_row(v_row_data)
                else:
                    sys.stdout.write(
                        "%s,%s,%s,%s,%s\n"
                        % (node_name, res_name, v_node_id, v_cstate, v_tstate)
                    )
            except IncompatibleDataException:
                sys.stderr.write("Warning: incompatible table entry skipped\n")

        if not machine_readable:
            t.set_show_separators(args.separators)
            # t.show(overwrite=True)
            t.show()
        return 0

    def cmd_export_conf(self, args):
        fn_rc = 0

        self.dbus_init()
        display_names = (len(args.resource) > 1)
        for res_name in args.resource:
            server_rc = self.dsc(self._server.export_conf, dbus.String(res_name))
            if display_names:
                sys.stdout.write("Exporting resource '%s':\n" % (res_name))
            item_rc = self._list_rc_entries(server_rc)
            if display_names:
                sys.stdout.write("\n")
            if item_rc != 0:
                fn_rc = 1

        return fn_rc

    def cmd_howto_join(self, args):
        """
        Queries the command line to join a node from the server
        """
        fn_rc = 1

        node_name = args.node
        quiet = args.quiet
        format = "%s"
        if (quiet is not None) and quiet:
            format += " --quiet"
        format += "\n"
        self.dbus_init()
        server_rc, joinc = self.dsc(self._server.text_query, ["joinc", node_name])
        if not quiet:
            sys.stderr.write('IMPORTANT: Execute the following command only on node %s!\n' % (node_name))
        sys.stdout.write(format % " ".join(joinc))
        if not quiet and 'restart' in joinc:  # here we should query if it is a satellite node; the easy path
            sys.stderr.write('IMPORTANT: If your satellite node is systemd socket activated, '
                             'do not do anything\n')
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_server_version(self, args):
        """
        Queries version information from the drbdmanage server
        """
        fn_rc = 1

        self.dbus_init()
        query = ["version"]
        server_rc, version_info = self.dsc(self._server.text_query, query)
        for entry in version_info:
            sys.stdout.write("%s\n" % entry)

        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_message_log(self, args):
        """
        Displays the server's message log
        """
        fn_rc = 1

        self.dbus_init()
        query = ["message_log"]
        server_rc, messages = self.dsc(self._server.text_query, query)
        if len(messages) == 0:
            sys.stdout.write("Message log is empty.\n")
        else:
            for line in messages:
                sys.stdout.write("%s\n" % (line))
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_clear_message_log(self, args):
        """
        Clears the server's message log
        """
        fn_rc = 1

        self.dbus_init()
        query = ["clear_message_log"]
        server_rc, messages = self.dsc(self._server.text_query, query)
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_lowlevel_debug(self, args):
        cmd = args.cmd

        params = []
        for s in args.json:
            # Empty instances need to have the type annotated manually.
            st = s.strip()
            if not st:
                params.append('')
            elif st == "[]":
                params.append(dbus.Array([], signature="s"))
            elif st == "{}":
                params.append(dbus.Dictionary({}, signature="ss"))
            elif st[0] in "[{":
                params.append(json.loads(st))
            else:
                params.append(st)

        self.dbus_init()
        fn = getattr(self._server, cmd)
        if not fn:
            raise "No such function"

        try:
            res = fn(*params)
            sys.stdout.write(json.dumps(res,
                             sort_keys=True,
                             indent=4,
                             separators=(',', ': ')) + "\n")
        except dbus.DBusException as e:
            if e._dbus_error_name == 'org.freedesktop.DBus.Python.TypeError':
                if sys.hexversion >= 0x02070000:
                    msg = re.sub(r'.*\n(TypeError:)', '\\1',
                                 e.message,
                                 flags=re.DOTALL + re.MULTILINE)
                else:
                    msg = e.message
                sys.stderr.write(msg)
                return 1
            else:
                raise
        except:
            raise

        return 0

    def cmd_query_conf(self, args):
        """
        Retrieves the configuration file for a resource on a specified node
        """
        fn_rc = 1

        node_name = args.node
        res_name = args.resource

        self.dbus_init()
        server_rc, res_config = self.dsc(self._server.text_query,
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

        return fn_rc

    def cmd_wait_for_startup(self, args):
        """
        Retrieves the configuration file for a resource on a specified node
        """
        fn_rc = 1

        self.cmd_ping({})
        self.dbus_init()
        server_rc = self.dsc(self._server.wait_for_startup)
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_ping(self, args):
        fn_rc = 1
        try:
            self.dbus_init()
            server_rc = self.dsc(self._server.ping)
            if server_rc == 0:
                sys.stdout.write("pong\n")
                fn_rc = 0
        except dbus.exceptions.DBusException:
            sys.stderr.write(
                "drbdmanage: cannot connect to the drbdmanage "
                "server through D-Bus.\n"
            )
        return fn_rc

    def cmd_startup(self, args):
        fn_rc = 1
        # if we start using args, check for 'None', because
        # restart might call us with 'None' args
        try:
            sys.stdout.write(
                "Attempting to startup the server through "
                "D-Bus activation...\n"
            )
            self.dbus_init()
            # no dsc, just returns 0, THINK could/should be changed
            server_rc = self._server.ping()
            if server_rc == 0:
                sys.stdout.write(
                    "D-Bus connection successful, "
                    "server is running and reachable\n"
                )
                fn_rc = 0
        except dbus.exceptions.DBusException:
            sys.stderr.write("""
D-Bus connection FAILED -- the D-Bus server may have been unable to activate
the drbdmanage service.
Review the syslog for error messages logged by the D-Bus server
or the drbdmanage server.
""")
        return fn_rc

    def cmd_init(self, args):
        """
        Initializes a new drbdmanage cluster
        """
        fn_rc = 1

        drbdctrl_vg = self._get_drbdctrl_vg(self._config)

        try:
            # BEGIN Setup drbdctrl resource properties
            node_name = get_uname()
            if node_name is None:
                sys.stderr.write('Hostname not valid\n')
                raise AbortException

            af = args.address_family
            address = args.ip
            port = args.port
            quiet = args.quiet
            flag_storage = not args.no_storage
            # END Setup drbdctrl resource properties

            if not quiet:
                quiet = self.user_confirm("""
You are going to initialize a new drbdmanage cluster.
CAUTION! Note that:
  * Any previous drbdmanage cluster information may be removed
  * Any remaining resources managed by a previous drbdmanage installation
    that still exist on this system will no longer be managed by drbdmanage

Confirm:
""")
            if quiet:
                drbdctrl_blockdev_0, drbdctrl_blockdev_1 = self._create_drbdctrl(
                    "0", self._config, DRBDCTRL_LV_NAME_0, DRBDCTRL_LV_NAME_1
                )
                self._ext_command(
                    ["drbdsetup", "primary", DRBDCTRL_RES_NAME, "--force"]
                )

                # error messages are printed by _drbdctrl_init()
                if self._drbdctrl_init(DRBDCTRL_DEV_0) != 0:
                    raise AbortException
                if self._drbdctrl_init(DRBDCTRL_DEV_1) != 0:
                    raise AbortException

                self._ext_command(
                    ["drbdsetup", "secondary", DRBDCTRL_RES_NAME]
                )

                # Shutdown a running drbdmanaged server process before continuing
                self.dbus_init()
                props = dbus.Dictionary(signature="ss")
                props[KEY_S_CMD_SHUTDOWN] = BOOL_TRUE
                try:
                    self.dsc(self._server.shutdown, props)
                except dbus.exceptions.DBusException:
                    # Shutdown always causes an exception,
                    # because the server does not answer
                    pass

                props = {}
                props[NODE_ADDR] = address
                props[NODE_AF] = af
                props[NODE_VOL_0] = drbdctrl_blockdev_0
                props[NODE_VOL_1] = drbdctrl_blockdev_1
                props[NODE_PORT] = str(port)
                if not flag_storage:
                    props[FLAG_STORAGE] = bool_to_string(flag_storage)
                # Startup the drbdmanage server and add the current node
                # Previous DBus connection is gone after shutdown,
                # must run dbus_init() again
                self._server = self._dbus.get_object(
                    DBUS_DRBDMANAGED, DBUS_SERVICE
                )
                server_rc = self.dsc(self._server.init_node,
                                     dbus.String(node_name), props)

                fn_rc = self._list_rc_entries(server_rc)
            else:
                fn_rc = 0
        except AbortException:
            self._init_join_rollback(drbdctrl_vg)
            sys.stderr.write("%sInitialization failed%s\n"
                             % (self.color(COLOR_RED), self.color(COLOR_NONE)))
        return fn_rc

    def cmd_uninit(self, args):
        fn_rc = 1

        quiet = args.quiet
        shutdown = args.shutdown

        if not quiet:
            quiet = self.user_confirm("""
You are going to remove the drbdmanage server from this node.
CAUTION! Note that:
  * All temporary configuration files for resources managed by drbdmanage
    will be removed
  * Any remaining resources managed by this drbdmanage installation
    that still exist on this system will no longer be managed by drbdmanage

Confirm:
""")
        if quiet:
            if shutdown:
                try:
                    self.dbus_init()
                    props = dbus.Dictionary(signature="ss")
                    self.dsc(self._server.shutdown, props)
                except dbus.exceptions.DBusException:
                    # The server does not answer after a shutdown,
                    # or it might not have been running in the first place,
                    # both is not considered an error here
                    pass
            try:
                drbdctrl_vg = self._get_drbdctrl_vg(self._config)
                conf_path = self._get_conf_path(self._config)
                self._init_join_cleanup(drbdctrl_vg, conf_path)
                fn_rc = 0
            except:
                fn_rc = 1
        else:
            fn_rc = 0

        return fn_rc

    def cmd_join(self, args):
        """
        Joins an existing drbdmanage cluster
        """
        fn_rc = 1

        drbdctrl_vg = self._get_drbdctrl_vg(self._config)

        # Initialization of the usermode helper restore delay
        delay_flag = False
        time_set = False
        begin_time = 0
        end_time = 0

        # DRBD usermode helper file, usermode helper setting
        umh_f = None
        umh = None
        try:
            # BEGIN Setup drbdctrl resource properties
            node_name = get_uname()
            if node_name is None:
                raise AbortException
            af = args.address_family
            port = args.port
            quiet = args.quiet
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
                # Enable the usermode helper restore delay
                delay_flag = True
                l_addr = args.local_ip
                p_addr = args.peer_ip
                p_name = args.peer_name
                l_node_id = args.local_node_id
                p_node_id = args.peer_node_id
                secret = args.secret

                drbdctrl_blockdev_0, drbdctrl_blockdev_1 = self._create_drbdctrl(
                    l_node_id, self._config, DRBDCTRL_LV_NAME_0, DRBDCTRL_LV_NAME_1
                )

                begin_time = time.time()
                # change the usermode helper temporarily
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
                except (IOError, OSError):
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

                self._ext_command(
                    [
                        "drbdsetup", "new-peer",
                        DRBDCTRL_RES_NAME, p_node_id,
                        "--_name=" + p_name,
                        "--shared-secret=" + secret,
                        "--cram-hmac-alg=sha256",
                        "--protocol=C"
                    ]
                )

                self._ext_command(
                    [
                        "drbdsetup", "new-path",
                        DRBDCTRL_RES_NAME, p_node_id,
                        af + ':' + l_addr + ":" + str(port),
                        af + ':' + p_addr + ":" + str(port)
                    ]
                )

                self._ext_command(
                    [
                        "drbdsetup", "connect",
                        DRBDCTRL_RES_NAME, p_node_id
                    ]
                )

                # Startup the drbdmanage server and update the local .drbdctrl
                # resource configuration file
                self.dbus_init()
                props = {}
                props[NODE_PORT] = str(port)
                props[NODE_VOL_0] = drbdctrl_blockdev_0
                props[NODE_VOL_1] = drbdctrl_blockdev_1
                props[NODE_SECRET] = secret
                server_rc = self.dsc(self._server.join_node, props)
                end_time = time.time()
                time_set = True
                for rc_entry in server_rc:
                    (err_code, err_msg, err_args) = rc_entry
                    if err_code == DM_ENOENT:
                        sys.stderr.write(
                            "JOIN ERROR: This node has no node entry in the "
                            "drbdmanage configuration\n"
                        )
                        raise AbortException
                fn_rc = self._list_rc_entries(server_rc)
            else:
                fn_rc = 0
        except AbortException:
            # Disable the usermode helper restore delay
            delay_flag = False
            sys.stderr.write("Initialization failed\n")
            self._init_join_rollback(drbdctrl_vg)
        finally:
            if delay_flag:
                # undo the temporary change of the usermode helper
                # FIXME: wait here -- otherwise, restoring the user mode
                #        helper will probably race with establishing the
                #        network connection
                delay_time = self.UMHELPER_WAIT_TIME
                if time_set:
                    if begin_time >= 0 and end_time >= 0:
                        diff_time = end_time - begin_time
                        if not diff_time < 0:
                            if diff_time <= self.UMHELPER_WAIT_TIME:
                                # set the remaining delay time
                                delay_time = self.UMHELPER_WAIT_TIME - diff_time
                            else:
                                delay_time = 0

                if delay_time > 0:
                    time.sleep(delay_time)

            # Restore the usermode helper
            umh_f = None
            if umh is not None:
                try:
                    umh_f = open(self.UMHELPER_FILE, "w")
                    umh_f.write(umh)
                except (IOError, OSError):
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
        return fn_rc

    def _init_join_cleanup(self, drbdctrl_vg, conf_path):
        """
        Cleanup before init / join operations

        Notice: Caller should handle AbortException
        """
        # Shut down any existing drbdmanage control volume
        try:
            self._ext_command(
                ["drbdsetup", "down", DRBDCTRL_RES_NAME]
            )
        except AbortException:
            pass

        # Delete existing .drbdctrl LV 0
        try:
            self._ext_command(
                ["lvremove", "--force", drbdctrl_vg + "/" + DRBDCTRL_LV_NAME_0],
                ignore_error=True
            )
        except AbortException:
            pass
        # Delete existing .drbdctrl LV 1
        try:
            self._ext_command(
                ["lvremove", "--force", drbdctrl_vg + "/" + DRBDCTRL_LV_NAME_1],
                ignore_error=True
            )
        except AbortException:
            pass

        # Delete any existing configuration file
        try:
            [os.unlink(os.path.join(conf_path, f))
             for f in os.listdir(conf_path) if f.endswith(".res")]
        except OSError:
            pass

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
                ["lvremove", "--force", drbdctrl_vg + "/" + DRBDCTRL_LV_NAME_0],
                ignore_error=True
            )
        except AbortException:
            pass
        try:
            self._ext_command(
                ["lvremove", "--force", drbdctrl_vg + "/" + DRBDCTRL_LV_NAME_1],
                ignore_error=True
            )
        except AbortException:
            pass

    def cmd_initcv(self, args):
        fn_rc = 1

        drbdctrl_file = args.dev
        quiet = args.quiet

        if not quiet:
            quiet = self.user_confirm(
                "You are going to initialize a new "
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
        return fn_rc

    def _ext_command(self, exec_args, ignore_error=False):
        """
        Run external commands in a subprocess
        """
        exit_code = 127
        try:
            cmd_exec = ExternalCommandBuffer(self.__class__.__name__, exec_args)
            exit_code = cmd_exec.run()
            if exit_code != 0 and not ignore_error:
                sys.stderr.write("%sError: External command failed:%s\n"
                                 % (self.color(COLOR_RED), self.color(COLOR_NONE)))
                sys.stderr.write("%s%s%s\n"
                                 % (self.color(COLOR_RED), " ".join(exec_args), self.color(COLOR_NONE)))
                sys.stderr.write("%sCommand output:%s\n"
                                 % (self.color(COLOR_RED), self.color(COLOR_NONE)))
                for line in cmd_exec.get_stdout():
                    sys.stderr.write("  %s(stdout) %s%s"
                                     % (self.color(COLOR_RED), self.color(COLOR_NONE), line))
                for line in cmd_exec.get_stderr():
                    sys.stderr.write("  %s(stderr) %s%s"
                                      % (self.color(COLOR_RED), self.color(COLOR_NONE), line))
                sys.stderr.write("%sCommand exited with exit_code %d%s\n\n"
                                 % (self.color(COLOR_RED), exit_code, self.color(COLOR_NONE)))
                raise AbortException
        except OSError as oserr:
            if oserr.errno == errno.ENOENT:
                sys.stderr.write("Cannot find command '%s'\n" % exec_args[0])
            elif oserr.errno == errno.EACCES:
                sys.stderr.write(
                    "Cannot execute '%s', permission denied\n"
                    % (exec_args[0])
                )
            else:
                sys.stderr.write(
                    "Cannot execute '%s', error returned by the OS is: %s\n"
                    % (exec_args[0], oserr.strerror)
                )
            raise AbortException
        return exit_code

    def _create_drbdctrl(self, node_id, server_conf, drbdctrl_lv_0, drbdctrl_lv_1):
        drbdctrl_vg = self._get_drbdctrl_vg(server_conf)
        conf_path = self._get_conf_path(server_conf)

        drbdctrl_blockdev_0 = ("/dev/" + drbdctrl_vg + "/" + drbdctrl_lv_0)
        drbdctrl_blockdev_1 = ("/dev/" + drbdctrl_vg + "/" + drbdctrl_lv_1)

        # ========================================
        # Cleanup
        # ========================================
        self._init_join_cleanup(drbdctrl_vg, conf_path)

        # ========================================
        # Join an existing drbdmanage cluster
        # ========================================

        # Create the .drbdctrl LV
        self._ext_command(
            ["lvcreate", "-n", drbdctrl_lv_0, "-L", "4m", drbdctrl_vg]
        )
        try:
            subprocess.call(["wipefs", "-a", "-q", drbdctrl_blockdev_0])
        except:
            sys.stderr.write("Could not wipefs %s\n" % drbdctrl_blockdev_0)

        self._ext_command(
            ["lvcreate", "-n", drbdctrl_lv_1, "-L", "4m", drbdctrl_vg]
        )
        try:
            subprocess.call(["wipefs", "-a", "-q", drbdctrl_blockdev_1])
        except:
            sys.stderr.write("Could not wipefs %s\n" % drbdctrl_blockdev_1)

        # Create meta-data
        self._ext_command(
            [
                "drbdmeta", "--force", "0", "v09", drbdctrl_blockdev_0,
                "internal", "create-md", "31"
            ]
        )
        self._ext_command(
            [
                "drbdmeta", "--force", "1", "v09", drbdctrl_blockdev_1,
                "internal", "create-md", "31"
            ]
        )

        # Configure the .drbdctrl resource
        self._ext_command(
            ["drbdsetup", "new-resource", DRBDCTRL_RES_NAME, node_id]
        )
        # Note: Syntax: drbdsetup new-minor minor-nr volume-nr
        self._ext_command(
            ["drbdsetup", "new-minor", DRBDCTRL_RES_NAME, "0", "0"]
        )
        self._ext_command(
            ["drbdsetup", "new-minor", DRBDCTRL_RES_NAME, "1", "1"]
        )
        retry_counter = 3
        while retry_counter > 0:
            rc_0 = self._ext_command(
                [
                    "drbdmeta", "0", "v09",
                    drbdctrl_blockdev_0, "internal", "apply-al"
                ]
            )
            rc_1 = self._ext_command(
                [
                    "drbdmeta", "1", "v09",
                    drbdctrl_blockdev_1, "internal", "apply-al"
                ]
            )
            if rc_0 == 0 and rc_1 == 0:
                break
            retry_counter -= 1
        self._ext_command(
            [
                "drbdsetup", "attach", "0",
                drbdctrl_blockdev_0, drbdctrl_blockdev_0, "internal"
            ]
        )
        self._ext_command(
            [
                "drbdsetup", "attach", "1",
                drbdctrl_blockdev_1, drbdctrl_blockdev_1, "internal"
            ]
        )
        return drbdctrl_blockdev_0, drbdctrl_blockdev_1

    def _get_drbdctrl_vg(self, server_conf):
        # ========================================
        # Set up the path to the drbdctrl LV
        # ========================================
        return server_conf.get(KEY_DRBDCTRL_VG, DEFAULT_VG)

    def _get_conf_path(self, server_conf):
        return server_conf.get(KEY_DRBD_CONFPATH, DEFAULT_DRBD_CONFPATH)

    def _list_rc_entries(self, server_rc):
        """
        Lists default error messages for a list of server return codes
        """
        return self._process_rc_entries(server_rc, True)

    def _is_rc_successful(self, server_rc):
        """
        Indicates whether server return codes contain a success message
        """
        successful = (self._process_rc_entries(server_rc, False) == 0)
        return successful

    def _is_rc_retry(self, server_rc):
        for rc_entry in server_rc:
            rc_num, _, _ = rc_entry
            if rc_num == DM_ENOTREADY or rc_num == DM_ENOTREADY_STARTUP or rc_num == DM_ENOTREADY_REQCTRL:
                return True
        return False

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
                    sys.stderr.write(
                        "WARNING: unparseable return code omitted\n"
                    )
        except (TypeError, ValueError):
            sys.stderr.write("WARNING: cannot parse server return codes\n")
        return fn_rc

    def _level_color(self, level):
        """
        Selects a color for a level returned by GenericView subclasses
        """
        level_color = COLOR_RED
        if level == GenericView.STATE_NORM:
            level_color = COLOR_DARKGREEN
        elif level == GenericView.STATE_WARN:
            level_color = COLOR_YELLOW
        return self.color(level_color)

    def cmd_debug(self, args):
        fn_rc = 1

        command = args.cmd
        try:
            self.dbus_init()
            fn_rc = self.dsc(self._server.debug_console, dbus.String(command))
            sys.stderr.write("fn_rc=%d, %s\n" % (fn_rc, command))
        except dbus.exceptions.DBusException:
            sys.stderr.write(
                "drbdmanage: cannot connect to the drbdmanage "
                "server through D-Bus.\n"
            )
        return fn_rc

    def _checkmutex(self, args, names):
        target = ""
        for o in names:
            if args.__dict__[o]:
                if target:
                    sys.stderr.write("--%s and --%s are mutually exclusive\n" % (o, target))
                    sys.exit(1)
                target = o

        if not target:
            sys.stderr.write("You have to specify (exactly) one of %s\n" % ('--' + ' --'.join(names)))
            sys.exit(1)

        return target

    def _set_drbdsetup_props(self, opts):
        fn_rc = 1
        try:
            self.dbus_init()
            fn_rc = self.dsc(self._server.set_drbdsetup_props, opts)
        except dbus.exceptions.DBusException:
            sys.stderr.write(
                "drbdmanage: cannot connect to the drbdmanage "
                "server through D-Bus.\n"
            )
        return fn_rc

    def cmd_res_options(self, args):
        fn_rc = 1
        target = self._checkmutex(args,
                                  ("common", "resource"))

        newopts = args.optsobj.filterNew(args)
        if not newopts:
            sys.stderr.write('No new options found\n')
            return fn_rc

        newopts["target"] = target
        newopts["type"] = args.type

        return self._set_drbdsetup_props(newopts)

    def cmd_disk_options(self, args):
        fn_rc = 1
        target = self._checkmutex(args,
                                  ("common", "resource", "volume"))

        newopts = args.optsobj.filterNew(args)
        if not newopts:
            sys.stderr.write('No new options found\n')
            return fn_rc
        if target == "volume" and newopts["volume"].find('/') == -1:
            sys.stderr.write('You have to specify the volume as: res/vol\n')
            return fn_rc

        newopts["target"] = target
        newopts["type"] = args.type

        return self._set_drbdsetup_props(newopts)

    def cmd_net_options(self, args):
        fn_rc = 1
        target = self._checkmutex(args, ("common", "resource", "sites"))

        newopts = args.optsobj.filterNew(args)
        if not newopts:
            sys.stderr.write('No new options found\n')
            return fn_rc

        newopts["target"] = target
        newopts["type"] = "neto"

        return self._set_drbdsetup_props(newopts)

    def cmd_handlers(self, args):
        fn_rc = 1
        target = self._checkmutex(args, ("common", "resource"))
        from drbdmanage.utils import filter_new_args
        newopts = filter_new_args('unset', args)
        if not newopts:
            sys.stderr.write('No new options found\n')
            return fn_rc

        newopts["target"] = target
        newopts["type"] = "handlers"

        return self._set_drbdsetup_props(newopts)

    def cmd_list_options(self, args):
        net_options = args.noobj.get_options()
        disk_options = args.doobj.get_options()
        peer_device_options = args.pdoobj.get_options()
        resource_options = args.roobj.get_options()

        # filter net-options drbdmanage sets unconditionally.
        net_options = filter_prohibited(net_options, ('shared-secret', 'cram-hmac-alg'))

        colors = {
            'net-options': self.color(COLOR_TEAL),
            'disk-options': self.color(COLOR_BROWN),
            'peer-device-options': self.color(COLOR_GREEN),
            'resource-options': self.color(COLOR_DARKPINK),
        }

        self.dbus_init()
        ret, conf = self.dsc(self._server.get_selected_config_values, [KEY_DRBD_CONFPATH])

        res_file = 'drbdmanage_' + args.resource + '.res'
        conf_path = self._get_conf_path(conf)
        res_file = os.path.join(conf_path, res_file)
        if not os.path.isfile(res_file):
            sys.stderr.write('Resource file "' + res_file + '" does not exist\n')
            sys.exit(1)

        common_file = os.path.join(conf_path, FILE_GLOBAL_COMMON_CONF)

        def highlight(option_type, color, found):
            if found:
                return True
            for o in option_type:
                if line.find(o) != -1:
                    sys.stdout.write(color + line.rstrip() + COLOR_NONE + "\n")
                    return True
            return False

        for res_f in (common_file, res_file):
            sys.stdout.write(res_f + ":\n")
            with open(res_f) as f:
                for line in f:
                    if line.find('{') != -1 or line.find('}') != -1:
                        sys.stdout.write(line)
                        continue

                    found = highlight(net_options, colors['net-options'], False)
                    found = highlight(disk_options, colors['disk-options'], found)
                    found = highlight(peer_device_options, colors['peer-device-options'], found)
                    found = highlight(resource_options, colors['resource-options'], found)
                    if not found:
                        sys.stdout.write(line)
            sys.stdout.write("\n")

        sys.stdout.write("Legend:\n")
        for k, v in colors.items():
            sys.stdout.write(v + k + COLOR_NONE + "\n")
        sys.stdout.write("\nNote: Do not directly edit these auto-generated"
                         " files as they will be overwritten.\n")
        sys.stdout.write("Use the according drbdmanage sub-commands to set/unset options.\n")

    def cmd_edit_config(self, args):
        import ConfigParser
        cfg = ConfigParser.RawConfigParser()

        self.dbus_init()

        if args.node:
            cfgtype = CONF_NODE
            server_rc, plugins = self.dsc(self._server.get_plugin_default_config)
            plugin_names = [p['name'] for p in plugins]
        else:
            cfgtype = CONF_GLOBAL

        # get all known config keys
        server_rc, config_keys = self.dsc(self._server.get_config_keys)

        # setting the drbdctrl-vg here is not allowed
        # setting the current minor number offset is not allowed here
        # only allowed via the config file
        prohibited = (KEY_DRBDCTRL_VG, KEY_CUR_MINOR_NR)
        config_keys = filter_prohibited(config_keys, prohibited)

        # get all config options that are set cluster wide (aka GLOBAL)
        server_rc, cluster_config = self.dsc(self._server.get_cluster_config)

        # get all site configuration
        server_rc, site_config = self.dsc(self._server.get_site_config)

        cfg.add_section('GLOBAL')
        for k, v in cluster_config.items():
            if k in config_keys:
                cfg.set('GLOBAL', k, v)

        for site in site_config:
            site_name = 'Site:' + site['name']
            cfg.add_section(site_name)
            for k, v in site.items():
                if k in config_keys:
                    cfg.set(site_name, k, v)

        server_rc, node_list = self._get_nodes()

        ns = Props.NAMESPACES[Props.KEY_DMCONFIG]
        pns = Props.NAMESPACES[Props.KEY_PLUGINS]

        node_names = []
        for node_entry in node_list:
            node_name, properties = node_entry
            if cfgtype == CONF_NODE and node_name != args.node:
                continue
            node_names.append(node_name)

            properties = Props(properties)
            cur_props = properties.get_all_props(ns)
            if len(cur_props) > 0 or cfgtype == CONF_NODE:
                secname = 'Node:' + node_name
                cfg.add_section(secname)
                for k, v in cur_props.items():
                    if k in config_keys.keys() + [KEY_SITE]:
                        cfg.set(secname, k, v)

            cur_props = properties.get_all_props(pns)
            if cfgtype == CONF_NODE and len(cur_props) > 0:
                # for simplicity in the for loop, because we access the nodes properties, but executed
                # exactly once.

                for plugin in plugins:
                    plugin_name = plugin['name']
                    current_pns = pns + plugin_name
                    plugin_props = properties.get_all_props(current_pns)
                    if len(plugin_props) > 0:
                        secname = 'Plugin:' + plugin_name
                        cfg.add_section(secname)
                        for k, v in plugin_props.items():
                            cfg.set(secname, k, v)

        import tempfile
        import os
        import shutil

        tmpf = tempfile.mkstemp(suffix='.cfg')[1]
        orig = tempfile.mkstemp(suffix='.cfg')[1]

        with open(tmpf, 'wb') as configfile:
            cfg.write(configfile)
            hdr = 'Options you can set with their default value in the GLOBAL section, per Site, or per Node:'
            configfile.write('# %s\n# %s\n' % (hdr, '~' * len(hdr)))
            for k in sorted(config_keys.keys()):
                configfile.write('# %s = %s\n' % (k, config_keys[k]))
            configfile.write('\n')

            if cfgtype == CONF_NODE:
                hdr = 'Plugin options you can set with their default value:'
                configfile.write('# %s\n# %s\n' % (hdr, '~' * len(hdr)))
                for plugin in plugins:
                    secname = '# [Plugin:' + plugin['name'] + ']'
                    configfile.write(secname + '\n')
                    for o in plugin:
                        if o == 'name':
                            continue
                        cfgstr = '# ' + o + ' = ' + plugin[o]
                        if o == KEY_VG_NAME:
                            cfgstr += '\n# ^^ or the value of %s if it is set and %s is unset' % (KEY_DRBDCTRL_VG,
                                                                                                  KEY_VG_NAME)
                        configfile.write(cfgstr + '\n')
                    configfile.write('\n')

            hdr = 'Nodes available in this view:'
            configfile.write('# %s\n# %s\n' % (hdr, '~' * len(hdr)))
            configfile.write('# %s\n' % (', '.join(node_names)))
            configfile.write('# You can also specify the keyword "%s" in node sections\n' % (KEY_SITE))
            example_node = node_names[0] if node_names else 'nodeA'
            configfile.write('# Example: [Node:%s]\n' % (example_node))
            configfile.write('\n')

            configfile.write('# For further information please refer to "Configuration" in drbdmanage(8)\n')

        shutil.copyfile(tmpf, orig)

        if args.type == 'export':
            prog = 'cat'
            if args.file:
                shutil.copyfile(tmpf, args.file)
                sys.exit(0)
        else:
            prog = os.getenv('EDITOR', 'vi')

        before = os.stat(tmpf).st_mtime
        try:
            subprocess.call([prog, tmpf])
        except:
            sys.stderr.write('Could not load %s, your changes will not be saved.\n' % (prog))
            sys.exit(1)

        if args.type == 'export':
            sys.exit(0)

        after = os.stat(tmpf).st_mtime
        if before == after:
            sys.stdout.write("Nothing to save, bye\n")
            sys.exit(0)

        try:
            # recreate the cfg object, otherwise read() reads old values, no
            # matter if you close the file or not
            cfg = ConfigParser.RawConfigParser()
            cfg.read(tmpf)
        except:
            sys.stderr.write('Could not parse configuration, your changes will not be saved.\n')
            sys.exit(1)

        # parse back to dict, while keeping it flat
        cfgdict = {'nodes': [], 'globals': [], 'sites': [],
                   'plugins': [], 'type': [{'type': cfgtype}]}

        for section in cfg.sections():
            if section.startswith('Node:'):
                name = section.split(':')[1]
                if name in node_names:
                    e = dict(cfg.items(section) + [('name', name)])
                    e = filter_prohibited(e, prohibited)
                    cfgdict['nodes'].append(e)
                else:
                    sys.stderr.write('%s is not a valid node name. '
                                     'Configuration for this node ignored\n' % (name))
            elif section.startswith('Site:'):
                name = section.split(':')[1]
                e = dict(cfg.items(section) + [('name', name)])
                e = filter_prohibited(e, prohibited)
                cfgdict['sites'].append(e)
            elif section.startswith('Plugin:') and cfgtype == CONF_NODE:
                name = section.split(':')[1]
                if name in plugin_names:
                    e = dict(cfg.items(section) + [('name', name)])
                    cfgdict['plugins'].append(e)
                else:
                    sys.stderr.write('%s is not a valid plugin name. '
                                     'Configuration for this plugin ignored\n' % (name))
            elif section.startswith('GLOBAL'):
                e = dict(cfg.items(section))
                e = filter_prohibited(e, prohibited)
                cfgdict['globals'].append(e)

        # set at least empty node sections, this might have happend if the user deleted the whole [Node:]
        # section
        if len(cfgdict['nodes']) == 0:
            cfgdict['nodes'] = [dict(('name', node_name) for node_name in node_names)]
        # print cfgdict['nodes'], len(cfgdict['nodes'])
        # print cfgdict['globals']
        # print cfgdict['plugins']
        # print cfgdict['sites']
        # print cfgdict['sites']

        server_rc = self.dsc(self._server.set_cluster_config, cfgdict)

        return server_rc

    def cmd_role(self, args):
        fn_rc = 1
        self.dbus_init()
        server_rc, role = self.dsc(self._server.role)
        fn_rc = self._list_rc_entries(server_rc)

        if fn_rc == 0:
            sys.stdout.write('%s\n' % (role))

        return fn_rc

    def cmd_reelect(self, args):
        fn_rc = 1
        props = {}
        props[FLAG_FORCEWIN] = bool_to_string(args.force_win)

        self.dbus_init()
        # no dsc. allow anytime
        server_rc, role = self._server.reelect(props)
        fn_rc = self._list_rc_entries(server_rc)

        if fn_rc == 0:
            sys.stdout.write('%s\n' % (role))

        return fn_rc

    def cmd_dbustrace(self, args):
        fn_rc = 1
        props = {}

        try:
            if args.start:
                props["start"] = True
                if args.maxlog:
                    props["maxlog"] = args.maxlog
            elif args.stop:
                props["stop"] = True
            else:
                sys.stderr.write('Specify one of -s or -p\n')
                raise SyntaxException

            self.dbus_init()
            server_rc, fname = self.dsc(self._server.dbus_tracer, props)
            fn_rc = self._list_rc_entries(server_rc)

            if fn_rc == 0 and args.stop:
                sys.stdout.write('Trace written (on server) to: %s\n' % (fname))
        except SyntaxException:
            self.cmd_help(args)

        return fn_rc

    def cmd_export_ctrlvol(self, args):
        fn_rc = 1
        outf = sys.stdout

        if args.file:
            outf = open(args.file, 'w')

        self.dbus_init()

        server_rc, jsonblob = self.dsc(self._server.get_ctrlvol)
        fn_rc = self._list_rc_entries(server_rc)
        if fn_rc == 0:
            outf.write(jsonblob)
        if outf != sys.stdout:
            outf.close()

        return fn_rc

    def cmd_import_ctrlvol(self, args):
        fn_rc = 0
        if not args.quiet and not self.user_confirm('Did you read the according section in the user guide? '
                                                    'Are you sure you understand the consequences?'):
            return fn_rc

        inf = sys.stdin
        if args.file:
            inf = open(args.file)

        jsonblob = inf.read()
        if inf != sys.stdin:
            inf.close()

        self.dbus_init()
        server_rc = self.dsc(self._server.set_ctrlvol, jsonblob)
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

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
                if answer.lower() == "yes":
                    fn_rc = True
                    break
                elif answer.lower() == "no":
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
        if error < 100:
            prefix = ""
        else:
            prefix = "Error: "
        if not (error > 0 and error < 100):
            sys.stderr.write("%s%s\n" % (prefix, dm_exc_text(error)))

    def color(self, col):
        if self._colors:
            return col
        else:
            return ""

    def _property_text(self, text):
        if text is None:
            return "N/A"
        else:
            return text

    def _args_bool_to_string(self, arg):
        arg = arg.lower()
        text = None
        if arg == BOOL_TRUE or arg == "yes" or arg == "on":
            text = BOOL_TRUE
        elif arg == BOOL_FALSE or arg == "no" or arg == "off":
            text = BOOL_FALSE
        else:
            raise SyntaxException
        return text

    def _drbdctrl_init(self, drbdctrl_file):
        fn_rc = 1

        init_blks = 4
        persist = drbdmanage.drbd.persistence.ServerDualPersistence
        blksz = persist.BLOCK_SIZE

        index_name = persist.INDEX_KEY
        index_off = persist.INDEX_OFFSET
        hash_off = persist.HASH_OFFSET
        data_off = persist.DATA_OFFSET
        cconf_off = persist.DATA_OFFSET + 4096

        assg_len_name = persist.ASSG_LEN_KEY
        assg_off_name = persist.ASSG_OFF_KEY
        nodes_len_name = persist.NODES_LEN_KEY
        nodes_off_name = persist.NODES_OFF_KEY
        res_len_name = persist.RES_LEN_KEY
        res_off_name = persist.RES_OFF_KEY
        cconf_len_name = persist.CCONF_LEN_KEY
        cconf_off_name = persist.CCONF_OFF_KEY
        common_len_name = persist.COMMON_LEN_KEY
        common_off_name = persist.COMMON_OFF_KEY

        drbdctrl = None
        try:
            data_hash = drbdmanage.utils.DataHash()

            data_str = "{}\n"
            cconf_str = "{\n    \"serial\": \"0\"\n}"
            cconf_len = len(cconf_str)
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
                "        \"" + cconf_len_name + "\": " + str(cconf_len) + ",\n"
                "        \"" + cconf_off_name + "\": "
                + str(cconf_off) + ",\n"
                "        \"" + common_len_name + "\": 3,\n"
                "        \"" + common_off_name + "\": "
                + str(data_off) + "\n"
                "    }\n"
                "}\n"
            )

            # One update of the data_hash for every section that has an
            # index entry
            pos = 0
            while pos < 3:
                data_hash.update(data_str)
                pos += 1
            data_hash.update(cconf_str)
            data_hash.update(data_str)

            drbdctrl = open(drbdctrl_file, "rb+")
            zeroblk = bytearray('\0' * blksz)
            pos = 0
            h = gen_header()
            drbdctrl.write(h)
            pos += 1
            while pos < init_blks:
                drbdctrl.write(zeroblk)
                pos += 1

            # Write the control volume magic number
            drbdctrl.seek(persist.MAGIC_OFFSET)
            drbdctrl.write(persist.PERSISTENCE_MAGIC)

            # Write the control volume version
            drbdctrl.seek(persist.VERSION_OFFSET)
            drbdctrl.write(persist.PERSISTENCE_VERSION)

            drbdctrl.seek(index_off)
            drbdctrl.write(index_str)

            drbdctrl.seek(data_off)
            drbdctrl.write(data_str)

            drbdctrl.seek(cconf_off)
            drbdctrl.write(cconf_str)

            drbdctrl.seek(hash_off)
            drbdctrl.write(
                "{\n"
                "    \"hash\": \"" + data_hash.get_hex_hash() + "\"\n"
                "}\n"
            )

            fn_rc = 0
        except IOError as ioexc:
            sys.stderr.write(
                "Initialization of the control volume failed: "
                "%s\n" % (str(ioexc))
            )
        finally:
            if drbdctrl is not None:
                try:
                    drbdctrl.close()
                except IOError:
                    pass
        sys.stdout.write("Empty drbdmanage control volume initialized on '%s'.\n" % (drbdctrl_file))

        return fn_rc

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


def main():
    rc = 0
    client = DrbdManage()
    try:
        client.run()
    except KeyboardInterrupt:
        sys.stderr.write("\ndrbdmanage: Client exiting (received SIGINT)\n")
        rc = 1
    return rc

if __name__ == "__main__":
    main()
