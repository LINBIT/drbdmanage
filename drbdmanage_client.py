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
import json
import re
import subprocess
import time
import traceback
import drbdmanage.drbd.drbdcore
import drbdmanage.drbd.persistence
import drbdmanage.argparse.argparse as argparse
import drbdmanage.argcomplete as argcomplete

from drbdmanage.consts import (
    SERVER_CONFFILE, KEY_DRBDCTRL_VG, DEFAULT_VG, DRBDCTRL_DEFAULT_PORT,
    DRBDCTRL_DEV, DRBDCTRL_RES_NAME, DRBDCTRL_RES_FILE, DRBDCTRL_RES_PATH,
    NODE_ADDR, NODE_AF, NODE_ID, NODE_POOLSIZE, NODE_POOLFREE, RES_PORT,
    VOL_MINOR, VOL_BDEV, RES_PORT_NR_AUTO, FLAG_DISKLESS, FLAG_OVERWRITE,
    FLAG_DRBDCTRL, FLAG_STORAGE, FLAG_DISCARD, FLAG_CONNECT,
    KEY_DRBD_CONFPATH, DEFAULT_DRBD_CONFPATH, DM_VERSION
)
from drbdmanage.utils import SizeCalc
from drbdmanage.utils import Table
from drbdmanage.utils import (
    build_path, bool_to_string, map_val_or_dflt
)
from drbdmanage.utils import (
    COLOR_NONE, COLOR_RED, COLOR_DARKRED, COLOR_DARKGREEN, COLOR_BROWN,
    COLOR_DARKPINK, COLOR_TEAL, COLOR_GREEN
)
from drbdmanage.conf.conffile import ConfFile
from drbdmanage.exceptions import AbortException
from drbdmanage.exceptions import IncompatibleDataException
from drbdmanage.exceptions import SyntaxException
from drbdmanage.exceptions import dm_exc_text
from drbdmanage.exceptions import DM_SUCCESS, DM_EEXIST, DM_ENOENT
from drbdmanage.dbusserver import DBusServer
from drbdmanage.drbd.drbdcore import Assignment
from drbdmanage.drbd.views import AssignmentView
from drbdmanage.drbd.views import DrbdNodeView
from drbdmanage.drbd.views import DrbdResourceView
from drbdmanage.drbd.views import DrbdVolumeView
from drbdmanage.drbd.views import DrbdVolumeStateView
from drbdmanage.snapshots.views import DrbdSnapshotAssignmentView
from drbdmanage.storage.storagecore import MinorNr
from drbdmanage.defaultip import default_ip


class DrbdManage(object):

    """
    drbdmanage dbus client, the CLI for controlling the drbdmanage server
    """

    _server = None
    _noerr = False
    _colors = True
    _all_commands = None

    VIEW_SEPARATOR_LEN = 78

    UMHELPER_FILE = "/sys/module/drbd/parameters/usermode_helper"
    UMHELPER_OVERRIDE = "/bin/true"
    UMHELPER_WAIT_TIME = 5.0

    def __init__(self):
        self._parser = self.setup_parser()
        self._all_commands = self.parser_cmds()

    def dbus_init(self):
        try:
            if self._server is None:
                dbus_con = dbus.SystemBus()
                self._server = dbus_con.get_object(
                    DBusServer.DBUS_DRBDMANAGED, DBusServer.DBUS_SERVICE
                )
        except dbus.exceptions.DBusException as exc:
            sys.stderr.write(
                "Error: Cannot connect to the drbdmanaged process using DBus\n"
            )
            sys.stderr.write(
                "The DBus subsystem returned the following "
                "error description:\n"
            )
            sys.stderr.write("%s\n" % (str(exc)))
            exit(1)

    def setup_parser(self):
        parser = argparse.ArgumentParser(prog='drbdmanage')
        parser.add_argument('--version', '-v', action='version',
                            version='%(prog)s ' + DM_VERSION)
        subp = parser.add_subparsers(title='subcommands',
                                     description='valid subcommands',
                                     help='Use the list command to print a '
                                     'nicer looking overview about all valid '
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
                                 description='Only useful in interacive mode')
        p_exit.set_defaults(func=self.cmd_exit)

        # poke
        p_poke = subp.add_parser('poke')
        p_poke.set_defaults(func=self.cmd_poke)

        # new-node
        def IPCompleter(prefix, parsed_args, **kwargs):
            import socket
            name = parsed_args.name
            ip = socket.gethostbyname(name)
            ip = [ip]
            return ip

        p_new_node = subp.add_parser('new-node',
                                     description='Names must match the output '
                                     'of "uname -n"',
                                     aliases=['nn', 'add-node', 'an'])
        p_new_node.add_argument('-a', '--address-family', metavar="FAMILY",
                                default='ipv4', choices=['ipv4', 'ipv6'],
                                help='FAMILY: "ipv4" (default) or "ipv6"')
        p_new_node.add_argument('-q', '--quiet', action="store_true")
        p_new_node.add_argument('-c', '--no-control-volume',
                                action="store_true")
        p_new_node.add_argument('-s', '--no-storage', action="store_true")
        p_new_node.add_argument('-j', '--no-autojoin', action="store_true")
        p_new_node.add_argument('name', help='Name of the new node')
        p_new_node.add_argument('ip',
                                help='IP address of the new node').completer = IPCompleter
        p_new_node.set_defaults(func=self.cmd_new_node)

        # remove-node
        def NodeCompleter(prefix, **kwargs):
            server_rc, node_list = self._get_nodes()
            possible = set()
            for n in node_list:
                name, _ = n
                possible.add(name)

            if not prefix or prefix == '':
                return possible
            else:
                return [node for node in possible if node.startswith(prefix)]

        p_rm_node = subp.add_parser('remove-node',
                                    description='Remove node',
                                    aliases=['rn', 'delete-node', 'dn'])
        p_rm_node.add_argument('-q', '--quiet', action="store_true")
        p_rm_node.add_argument('-f', '--force', action="store_true")
        p_rm_node.add_argument('name', help='Name of the node to remove').completer = NodeCompleter
        p_rm_node.set_defaults(func=self.cmd_remove_node)

        def port_type(p):
            p = int(p)
            if p < 1 or p > 65535:
                raise argparse.ArgumentTypeError('Port range: [1, 65535]')
            return p
        # new-resource
        p_new_res = subp.add_parser('new-resource',
                                    description='Add a new resource',
                                    aliases=['nr', 'add-resource', 'ar'])
        p_new_res.add_argument('-p', '--port', type=port_type)
        p_new_res.add_argument('name', help='Name of the new resource')
        p_new_res.set_defaults(func=self.cmd_new_resource)

        # modify-resource
        def ResourceCompleter(prefix, **kwargs):
            server_rc, res_list = self.__list_resources(False)
            possible = set()
            for r in res_list:
                name, _ = r
                possible.add(name)

            if not prefix or prefix == '':
                return possible
            else:
                return [res for res in possible if res.startswith(prefix)]

        p_mod_res = subp.add_parser('modify-resource',
                                    description='Modify a resource')
        p_mod_res.add_argument('-p', '--port', type=port_type)
        p_mod_res.add_argument('name',
                               help='Name of the resource to modify'). completer = ResourceCompleter
        p_mod_res.set_defaults(func=self.cmd_modify_resource)

        # remove-resource
        p_rm_res = subp.add_parser('remove-resource',
                                   description='Remove node',
                                   aliases=['rr', 'delete-resource', 'dr'])
        p_rm_res.add_argument('-q', '--quiet', action="store_true")
        p_rm_res.add_argument('-f', '--force', action="store_true")
        p_rm_res.add_argument('name',
                              help='Name of the resource to delete').completer = ResourceCompleter
        p_rm_res.set_defaults(func=self.cmd_remove_resource)

        # new-volume
        p_new_vol = subp.add_parser('new-volume',
                                    description='Add a new volume',
                                    aliases=['nv', 'add-volume', 'av'])
        p_new_vol.add_argument('-u', '--unit', default='GiB',
                               choices=['kB', 'MB', 'GB', 'TB', 'PB', 'kiB',
                                        'MiB', 'GiB'], help='Default: "GiB"')
        p_new_vol.add_argument('-m', '--minor', type=int)
        p_new_vol.add_argument('-d', '--deploy', type=int)
        p_new_vol.add_argument('name', help='Name of the new resource')
        p_new_vol.add_argument('size', help='Size of the new resource',
                               type=int)
        p_new_vol.set_defaults(func=self.cmd_new_volume)

        # remove-volume
        def VolumeCompleter(prefix, parsed_args, **kwargs):
            server_rc, res_list = self.__list_resources(True)
            possible = set()
            for r in res_list:
                name, _, vol_list = r
                if name == parsed_args.name:
                    vol_list.sort(key=lambda vol_entry: vol_entry[0])
                    for v in vol_list:
                        vol_id, _ = v
                        possible.add(str(vol_id))

            if not prefix or prefix == '':
                return possible
            else:
                return [res for res in possible if res.startswith(prefix)]

        p_mod_res = subp.add_parser('remove-volume',
                                    description='Remove volume',
                                    aliases=['rv', 'delete-volume', 'dv'])
        p_mod_res.add_argument('-q', '--quiet', action="store_true")
        p_mod_res.add_argument('-f', '--force', action="store_true")
        p_mod_res.add_argument('name', help='Name of the resource').completer = ResourceCompleter
        p_mod_res.add_argument('id', help='Volume ID', type=int).completer = VolumeCompleter
        p_mod_res.set_defaults(func=self.cmd_remove_volume)

        # connect
        p_conn = subp.add_parser('connect', description='Connect')
        p_conn.add_argument('node').completer = NodeCompleter
        p_conn.add_argument('resource').completer = ResourceCompleter
        p_conn.set_defaults(func=self.cmd_connect)

        # reconnect
        p_reconn = subp.add_parser('reconnect', description='Reonnect')
        p_reconn.add_argument('node').completer = NodeCompleter
        p_reconn.add_argument('resource').completer = ResourceCompleter
        p_reconn.set_defaults(func=self.cmd_reconnect)

        # disconnect
        p_disconn = subp.add_parser('disconnect', description='Disconnect')
        p_disconn.add_argument('node').completer = NodeCompleter
        p_disconn.add_argument('resource').completer = ResourceCompleter
        p_disconn.set_defaults(func=self.cmd_disconnect)

        # flags
        p_flags = subp.add_parser('flags', description='Set flags')
        p_flags.add_argument('node', help='Name of the node')
        p_flags.add_argument('resource', help='Name of the resource')
        p_flags.add_argument('--reconnect', choices=[0, 1], type=int)
        p_flags.add_argument('--updcon', choices=[0, 1], type=int)
        p_flags.add_argument('--overwrite', choices=[0, 1], type=int)
        p_flags.add_argument('--discard', choices=[0, 1], type=int)
        p_flags.set_defaults(func=self.cmd_flags)

        # attach
        p_attach = subp.add_parser('attach', description='Attach')
        p_attach.add_argument('node').completer = NodeCompleter
        p_attach.add_argument('resource').completer = ResourceCompleter
        p_attach.add_argument('id', help='Volume ID', type=int).completer = VolumeCompleter
        p_attach.set_defaults(func=self.cmd_attach_detach, fname='attach')
        # detach
        p_detach = subp.add_parser('detach', description='Detach')
        p_detach.add_argument('node').completer = NodeCompleter
        p_detach.add_argument('resource').completer = ResourceCompleter
        p_detach.add_argument('id', help='Volume ID', type=int).completer = VolumeCompleter
        p_detach.set_defaults(func=self.cmd_attach_detach, fname='detach')

        # assign
        p_assign = subp.add_parser('assign', description='Assign')
        p_assign.add_argument('--client', action="store_true")
        p_assign.add_argument('--overwrite', action="store_true")
        p_assign.add_argument('--discard', action="store_true")
        p_assign.add_argument('node').completer = NodeCompleter
        p_assign.add_argument('resource').completer = ResourceCompleter
        p_assign.set_defaults(func=self.cmd_assign)

        # free space
        def redundancy_type(r):
            r = int(r)
            if r < 1:
                raise argparse.ArgumentTypeError('Minimum redundancy is 1')
            return r
        p_fspace = subp.add_parser('free-space',
                                   description='Queries the maximum size of a '
                                   'volume that could be deployed with the '
                                   'specified level of redundancy')
        p_fspace.add_argument('-m', '--machine-readable', action="store_true")
        p_fspace.add_argument('redundancy', type=redundancy_type,
                              help='Redundancy level (>=1)')
        p_fspace.set_defaults(func=self.cmd_free_space)

        # deploy
        p_deploy = subp.add_parser('deploy', description='Deploy a resource')
        p_deploy.add_argument('resource').completer = ResourceCompleter
        p_deploy.add_argument('-i', '--increase', action="store_true",
                              help='Increase the redundancy count relative to '
                              'the currently set value by a number of '
                              '<redundancy_count>')
        p_deploy.add_argument('-d', '--decrease', action="store_true",
                              help='Decrease the redundancy count relative to '
                              'the currently set value by a number of '
                              '<redundancy_count>')
        p_deploy.add_argument('redundancy_count', type=redundancy_type,
                              help='The redundancy count specifies the number '
                              'of nodes to which the resource should be '
                              'deployed. It must be at least 1 and at most '
                              'the number of nodes in the cluster')
        p_deploy.set_defaults(func=self.cmd_deploy)

        # undeploy
        p_undeploy = subp.add_parser('undeploy',
                                     description='Undeploy a resource')
        p_undeploy.add_argument('-q', '--quiet', action="store_true")
        p_undeploy.add_argument('-f', '--force', action="store_true")
        p_undeploy.add_argument('resource').completer = ResourceCompleter
        p_undeploy.set_defaults(func=self.cmd_undeploy)

        # update-pool
        p_upool = subp.add_parser('update-pool',
                                  description='Update a pool')
        p_upool.set_defaults(func=self.cmd_update_pool)

        # reconfigure
        p_reconfigure = subp.add_parser('reconfigure',
                                        description='Reconfigure')
        p_reconfigure.set_defaults(func=self.cmd_reconfigure)

        # save
        p_save = subp.add_parser('save',
                                 description='Save')
        p_save.set_defaults(func=self.cmd_save)

        # load
        p_save = subp.add_parser('load',
                                 description='Load')
        p_save.set_defaults(func=self.cmd_load)

        # unassign
        p_unassign = subp.add_parser('unassign',
                                     description='Unassign a resource from a '
                                     'node')
        p_unassign.add_argument('-q', '--quiet', action="store_true")
        p_unassign.add_argument('-f', '--force', action="store_true")
        p_unassign.add_argument('node').completer = NodeCompleter
        p_unassign.add_argument('resource').completer = ResourceCompleter
        p_unassign.set_defaults(func=self.cmd_unassign)

        # new-snapshot
        p_nsnap = subp.add_parser('new-snapshot',
                                  aliases=['ns', 'create-snapshot', 'cs',
                                           'add-snapshot', 'as'],
                                  description='Create LVM snapshot')
        p_nsnap.add_argument('resource', help='Name of the resource').completer = ResourceCompleter
        p_nsnap.add_argument('snapshot', help='Name of the snapshot')
        p_nsnap.add_argument('nodes', help='List of nodes', nargs='+').completer = NodeCompleter
        p_nsnap.set_defaults(func=self.cmd_new_snapshot)

        # remove-snapshot
        def SnapsCompleter(prefix, parsed_args, **kwargs):
            server_rc, res_list = self._list_snapshots()
            possible = set()
            for r in res_list:
                res_name, snaps_list = r
                if res_name == parsed_args.resource:
                    for s in snaps_list:
                        snaps_name, _ = s
                        possible.add(snaps_name)

            if not prefix or prefix == '':
                return possible
            else:
                return [snap for snap in possible if snap.startswith(prefix)]

        p_rmsnap = subp.add_parser('remove-snapshot',
                                   aliases=['delete-snapshot', 'ds'],
                                   description='Remove LVM snapshot')
        p_rmsnap.add_argument('-f', '--force', action="store_true")
        p_rmsnap.add_argument('resource', help='Name of the resource').completer = ResourceCompleter
        p_rmsnap.add_argument('snapshot', help='Name of the snapshot').completer = SnapsCompleter
        p_rmsnap.set_defaults(func=self.cmd_remove_snapshot)

        # remove-snapshot-assignment
        p_rmsnapas = subp.add_parser('remove-snapshot-assignment',
                                     aliases=['rsa',
                                              'delete-snapshot-assignment',
                                              'dsa'],
                                     description='Remove snapshot assignment')
        p_rmsnapas.add_argument('-f', '--force', action="store_true")
        p_rmsnapas.add_argument('resource',
                                help='Name of the resource').completer = ResourceCompleter
        p_rmsnapas.add_argument('snapshot',
                                help='Name of the snapshot').completer = SnapsCompleter
        p_rmsnapas.add_argument('node', help='Name of the node').completer = NodeCompleter
        p_rmsnapas.set_defaults(func=self.cmd_remove_snapshot_assignment)

        # restore-snapshot
        p_restsnap = subp.add_parser('restore-snapshot',
                                     aliases=['rs'],
                                     description='Restore snapshot')
        p_restsnap.add_argument('resource', help='Name of the new resource')
        p_restsnap.add_argument('snapshot_resource',
                                help='Name of the snapshot resource')
        p_restsnap.add_argument('snapshot', help='Name of the snapshot')
        p_restsnap.set_defaults(func=self.cmd_restore_snapshot)

        # shutdown
        p_shutdown = subp.add_parser('shutdown',
                                     description='Shutdown')
        p_shutdown.add_argument('-q', '--quiet', action="store_true")
        p_shutdown.set_defaults(func=self.cmd_shutdown)

        # nodes
        nodesverbose = ['Family', 'IP']
        nodesgroupby = ['Name', 'Pool_Size', 'Pool_Free', 'Family', 'IP', 'State']

        def NodesVerboseCompleter(prefix, parsed_args, **kwargs):
            possible = nodesverbose
            if parsed_args.show:
                possible = [i for i in nodesverbose if i not in
                            parsed_args.show]

            if not prefix or prefix == '':
                return possible
            else:
                return [opt for opt in possible if opt.startswith(prefix)]

        def NodesGroupCompleter(prefix, parsed_args, **kwargs):
            possible = nodesgroupby
            if parsed_args.groupby:
                possible = [i for i in nodesgroupby if i not in
                            parsed_args.groupby]

            if not prefix or prefix == '':
                return possible
            else:
                return [opt for opt in possible if opt.startswith(prefix)]
        p_lnodes = subp.add_parser('nodes', aliases=['n'],
                                   description='List nodes')
        p_lnodes.add_argument('-m', '--machine-readable', action="store_true")
        p_lnodes.add_argument('-s', '--show', nargs='+',
                              choices=nodesverbose).completer = NodesVerboseCompleter
        p_lnodes.add_argument('-g', '--groupby', nargs='+',
                              choices=nodesgroupby).completer = NodesGroupCompleter
        p_lnodes.set_defaults(func=self.cmd_list_nodes)

        # resources
        p_lreses = subp.add_parser('resources', aliases=['r'],
                                   description='List resources')
        p_lreses.add_argument('-m', '--machine-readable', action="store_true")
        p_lreses.set_defaults(func=self.cmd_list_resources)

        # volumes
        p_lvols = subp.add_parser('volumes', aliases=['v'],
                                  description='List volumes')
        p_lvols.add_argument('-m', '--machine-readable', action="store_true")
        p_lvols.set_defaults(func=self.cmd_list_volumes)

        # snapshots
        p_lsnaps = subp.add_parser('snapshots', aliases=['s'],
                                   description='List snapshots')
        p_lsnaps.add_argument('-m', '--machine-readable', action="store_true")
        p_lsnaps.set_defaults(func=self.cmd_list_snapshots)

        # snapshot-assignments
        p_lsnapas = subp.add_parser('snapshot-assignments', aliases=['sa'],
                                    description='List snapshot assignments')
        p_lsnapas.add_argument('-m', '--machine-readable', action="store_true")
        p_lsnapas.set_defaults(func=self.cmd_list_snapshot_assignments)

        # assignments
        p_assignments = subp.add_parser('assignments', aliases=['a'],
                                        description='List assignments')
        p_assignments.add_argument('-m', '--machine-readable',
                                   action="store_true")
        p_assignments.set_defaults(func=self.cmd_list_assignments)

        # export
        p_export = subp.add_parser('export',
                                   description='Export config')
        p_export.add_argument('resource',
                              help='Name of the resource').completer = ResourceCompleter
        p_export.set_defaults(func=self.cmd_export_conf)

        # howto-join
        p_howtojoin = subp.add_parser('howto-join')
        p_howtojoin.add_argument('node',
                                 help='Name of the node to join').completer = NodeCompleter
        p_howtojoin.set_defaults(func=self.cmd_howto_join)

        # query-conf
        p_queryconf = subp.add_parser('query-conf')
        p_queryconf.add_argument('node', help='Name of the node').completer = NodeCompleter
        p_queryconf.add_argument('resource',
                                 help='Name of the resource').completer = ResourceCompleter
        p_queryconf.set_defaults(func=self.cmd_query_conf)

        # ping
        p_ping = subp.add_parser('ping')
        p_ping.set_defaults(func=self.cmd_ping)

        # startup
        p_startup = subp.add_parser('startup')
        p_startup.set_defaults(func=self.cmd_startup)

        # init
        p_init = subp.add_parser('init')
        p_init.add_argument('-a', '--address-family', metavar="FAMILY",
                            default='ipv4', choices=['ipv4', 'ipv6'],
                            help='FAMILY: "ipv4" (default) or "ipv6"')
        p_init.add_argument('-p', '--port', type=port_type,
                            default=DRBDCTRL_DEFAULT_PORT)
        p_init.add_argument('-q', '--quiet', action="store_true")
        p_init.add_argument('ip', nargs='?', default=default_ip())
        p_init.set_defaults(func=self.cmd_init)

        # uninit
        p_uninit = subp.add_parser('uninit')
        p_uninit.add_argument('-q', '--quiet', action="store_true")
        p_uninit.add_argument('-s', '--shutdown', action="store_true")
        p_uninit.set_defaults(func=self.cmd_uninit)

        # join
        def IPCompleter2(prefix, parsed_args, **kwargs):
            import socket
            name = parsed_args.peer_ip
            ip = socket.gethostbyname(name)
            ip = [ip]
            return ip

        p_join = subp.add_parser('join')
        p_join.add_argument('-a', '--address-family', metavar="FAMILY",
                            default='ipv4', choices=['ipv4', 'ipv6'],
                            help='FAMILY: "ipv4" (default) or "ipv6"')
        p_join.add_argument('-p', '--port', type=port_type,
                            default=DRBDCTRL_DEFAULT_PORT)
        p_join.add_argument('-q', '--quiet', action="store_true")
        p_join.add_argument('local_ip')
        p_join.add_argument('local_node_id')
        p_join.add_argument('peer_name')
        p_join.add_argument('peer_ip').completer = IPCompleter2
        p_join.add_argument('peer_node_id')
        p_join.add_argument('secret')
        p_join.set_defaults(func=self.cmd_join)

        # initcv
        p_join = subp.add_parser('initcv')
        p_join.add_argument('-q', '--quiet', action="store_true")
        p_join.add_argument('dev')
        p_join.set_defaults(func=self.cmd_initcv)

        # debug
        p_debug = subp.add_parser('debug')
        p_debug.add_argument('cmd')
        p_debug.set_defaults(func=self.cmd_debug)

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
        # subcommand dictates sortorder, not its alias
        cmds_sorted = [sorted(cmd, key=len, reverse=True) for cmd in
                       cmds.values()]

        # sort subcommands themselves
        cmds_sorted.sort(lambda a, b: cmp(a[0], b[0]))
        return cmds_sorted

    def cmd_list(self, args):
        print 'Use "help <command>" to get help for a specific command.\n'
        print 'Available commands:'
        # import pprint
        # pp = pprint.PrettyPrinter()
        # pp.pprint(self._all_commands)
        for cmd in self._all_commands:
            print '-', cmd[0],
            if len(cmd) > 1:
                print "(%s)" % (', '.join(cmd[1:])),
            print

    def cmd_interactive(self, args):
        all_cmds = [i for sl in self._all_commands for i in sl]

        # helper function
        def unknown(cmd):
            print '\n' + 'Command "%s" not known!' % (cmd)
            self.cmd_list(args)

        devnull = open(os.devnull, "w")
        stderr = sys.stderr

        # helper function
        def parsecatch(cmds, stoprec=False):
            sys.stderr = devnull
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
                    sys.stderr = stderr
                    print '\n' + 'Wrong synopsis. Use the command as follows:'
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
                print
                cmds = raw_input('> ').strip()

                cmds = [cmd.strip() for cmd in cmds.split()]
                if not cmds:
                    self.cmd_list(args)
                else:
                    parsecatch(cmds)
            except (EOFError, KeyboardInterrupt):  # raised by ctrl-d, ctrl-c
                print  # additional newline, makes shell prompt happy
                return

    def cmd_help(self, args):
        self.parse([args.command, "-h"])

    def cmd_exit(self, _, __):
        exit(0)

    def run(self):
        self.parse(sys.argv[1:])

    def cmd_poke(self, args):
        fn_rc = 1
        self.dbus_init()
        server_rc = self._server.poke()
        fn_rc = self._list_rc_entries(server_rc)
        return fn_rc

    def cmd_new_node(self, args):
        fn_rc = 1
        name = args.name
        ip = args.ip
        af = args.address_family
        if af is None:
            af = drbdmanage.drbd.drbdcore.DrbdNode.AF_IPV4_LABEL
        flag_storage = not args.no_storage
        flag_drbdctrl = not args.no_control_volume
        flag_autojoin = not args.no_autojoin

        props = dbus.Dictionary(signature="ss")
        props[NODE_ADDR] = ip
        props[NODE_AF] = af
        if not flag_drbdctrl:
            props[FLAG_DRBDCTRL] = bool_to_string(flag_drbdctrl)
        if not flag_storage:
            props[FLAG_STORAGE] = bool_to_string(flag_storage)

        self.dbus_init()
        server_rc = self._server.create_node(name, props)
        fn_rc = self._list_rc_entries(server_rc)

        if fn_rc == 0:
            server_rc, joinc = self._server.text_query(["joinc", name])
            joinc_text = str(" ".join(joinc))

            fn_rc = self._list_rc_entries(server_rc)

            # Text queries do not return error codes, so check whether the
            # string returned by the server looks like a join command or
            # like an error message
            if joinc_text.startswith("Error:"):
                sys.stderr.write(joinc_text + "\n")
            elif flag_drbdctrl:
                join_performed = False
                if flag_autojoin:
                    try:
                        sshc = ["ssh", "-oBatchMode=yes",
                                "-oConnectTimeout=2", "root@" + ip]
                        if subprocess.call(sshc + ["true"]) == 0:
                            sys.stdout.write(
                                "\nExecuting join command on "
                                "%s using ssh.\n" % (name)
                            )
                            ssh_joinc = sshc + joinc
                            if args.quiet:
                                ssh_joinc.append("-q")
                            subprocess.check_call(ssh_joinc)
                            join_performed = True
                    except subprocess.CalledProcessError:
                        sys.stderr.write("Error: Attempt to execute the "
                                         "join command remotely failed\n")
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
        server_rc = self._server.create_resource(dbus.String(name),
                                                 props)
        fn_rc = self._list_rc_entries(server_rc)
        return fn_rc

    def cmd_new_volume(self, args):
        fn_rc = 1
        unit = SizeCalc.UNIT_GiB
        size = None
        minor = MinorNr.MINOR_NR_AUTO
        name = args.name
        size = args.size
        unit_str = args.unit
        if not args.minor:
            minor = MinorNr.MINOR_NR_AUTO
        deploy = args.deploy

        try:
            unit = self.UNITS_MAP[unit_str.lower()]

            if unit != SizeCalc.UNIT_kiB:
                size = SizeCalc.convert_round_up(size, unit,
                                                 SizeCalc.UNIT_kiB)

            props = dbus.Dictionary(signature="ss")

            self.dbus_init()
            server_rc = self._server.create_resource(
                dbus.String(name), props
            )
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
                        dbus.String(name), dbus.Int32(deploy), dbus.Int32(0),
                        dbus.Boolean(False)
                    )
                    fn_rc = self._list_rc_entries(server_rc)
        except SyntaxException:
            self.cmd_help(parser, args)

        return fn_rc

    def cmd_modify_resource(self, args):
        fn_rc = 1
        name = args.name
        port = args.port if args.port else RES_PORT_NR_AUTO

        props = dbus.Dictionary(signature="ss")
        props[RES_PORT] = str(port)

        self.dbus_init()
        server_rc = self._server.modify_resource(
            dbus.String(name), props
        )
        fn_rc = self._list_rc_entries(server_rc)
        return fn_rc

    def cmd_remove_node(self, args):
        fn_rc = 1

        node_name = args.name
        force = args.force
        quiet = args.quiet
        if not quiet:
            quiet = self.user_confirm(
                "You are going to remove a node from the cluster. "
                "This will remove all resources from the node.\n"
                "Please confirm:"
            )
        if quiet:
            self.dbus_init()
            server_rc = self._server.remove_node(
                dbus.String(node_name), dbus.Boolean(force)
            )
            fn_rc = self._list_rc_entries(server_rc)
        else:
            fn_rc = 0

        return fn_rc

    def cmd_remove_resource(self, args):
        fn_rc = 1

        res_name = args.name
        force = args.force
        quiet = args.quiet
        if not quiet:
            quiet = self.user_confirm(
                "You are going to remove a resource and all of its "
                "volumes from all nodes of the cluster.\n"
                "Please confirm:"
            )
        if quiet:
            self.dbus_init()
            server_rc = self._server.remove_resource(
                dbus.String(res_name), dbus.Boolean(force)
            )
            fn_rc = self._list_rc_entries(server_rc)
        else:
            fn_rc = 0

        return fn_rc

    def cmd_remove_volume(self, args):
        fn_rc = 1

        vol_name = args.name
        vol_id = args.id
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
            server_rc = self._server.remove_volume(
                dbus.String(vol_name), dbus.Int32(vol_id),
                dbus.Boolean(force)
            )
            fn_rc = self._list_rc_entries(server_rc)
        else:
            fn_rc = 0

        return fn_rc

    def cmd_connect(self, args):
        return self._connect(args, False)

    def cmd_reconnect(self, args):
        return self._connect(parser, args, True)

    def _connect(self, args, reconnect):
        fn_rc = 1

        node_name = args.node
        res_name = args.resource

        self.dbus_init()
        server_rc = self._server.connect(
            dbus.String(node_name), dbus.String(res_name),
            dbus.Boolean(reconnect)
        )
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_disconnect(self, args):
        fn_rc = 1

        node_name = args.node
        res_name = args.resource

        self.dbus_init()
        server_rc = self._server.disconnect(
            dbus.String(node_name), dbus.String(res_name),
            dbus.Boolean(False)
        )
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
        server_rc = self._server.modify_state(
            dbus.String(node_name), dbus.String(res_name), dbus.UInt64(0),
            dbus.UInt64(0), dbus.UInt64(clear_mask), dbus.UInt64(set_mask)
        )
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
        server_rc = func(
            dbus.String(node_name), dbus.String(res_name),
            dbus.Int32(vol_id)
        )
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_assign(self, args):
        fn_rc = 1

        node_name = args.node
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
        server_rc = self._server.assign(
            dbus.String(node_name), dbus.String(res_name), props
        )
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_free_space(self, args):
        fn_rc = 1

        redundancy = args.redundancy

        self.dbus_init()
        server_rc, free_space, total_space = (
            self._server.cluster_free_query(dbus.Int32(redundancy))
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

        if args.decrease:
            count *= -1

        if args.increase or args.decrease:
            count, delta = delta, count

        self.dbus_init()
        server_rc = self._server.auto_deploy(
            dbus.String(res_name), dbus.Int32(count),
            dbus.Int32(delta), dbus.Boolean(False)
        )
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
            server_rc = self._server.auto_undeploy(
                dbus.String(res_name), dbus.Boolean(force)
            )
            fn_rc = self._list_rc_entries(server_rc)
        else:
            fn_rc = 0

        return fn_rc

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

        node_name = args.node
        res_name = args.resource
        force = args.force
        # quiet = args.quiet
        # TODO: implement quiet
        self.dbus_init()
        server_rc = self._server.unassign(node_name, res_name, force)
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_new_snapshot(self, args):
        fn_rc = 1

        res_name = args.resource
        snaps_name = args.snapshot
        node_list = args.nodes

        props = dbus.Dictionary(signature="ss")

        self.dbus_init()
        server_rc = self._server.create_snapshot(
            dbus.String(res_name), dbus.String(snaps_name),
            dbus.Array(node_list, signature="s"), props
        )
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_remove_snapshot(self, args):
        fn_rc = 1

        res_name = args.resource
        snaps_name = args.snapshot
        force = args.force

        self.dbus_init()
        server_rc = self._server.remove_snapshot(
            dbus.String(res_name), dbus.String(snaps_name), force
        )
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_remove_snapshot_assignment(self, args):
        fn_rc = 1

        res_name = args.resource
        snaps_name = args.snapshot
        node_name = args.node
        force = args.force

        self.dbus_init()
        server_rc = self._server.remove_snapshot_assignment(
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
        server_rc = self._server.restore_snapshot(
            dbus.String(res_name), dbus.String(snaps_res_name),
            dbus.String(snaps_name), res_props, vols_props
        )
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_shutdown(self, args):
        quiet = args.quiet
        if not quiet:
            quiet = self.user_confirm(
                "You are going to shut down the drbdmanaged server "
                "process on this node.\nPlease confirm:"
            )
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
        return 0

    def _get_nodes(self, sort=False):
        self.dbus_init()

        server_rc, node_list = self._server.list_nodes(
            dbus.Array([], signature="s"),
            0,
            dbus.Dictionary({}, signature="ss"),
            dbus.Array([], signature="s")
        )

        if sort:
            node_list.sort(key=lambda node_entry: node_entry[0])

        return (server_rc, node_list)

    def cmd_list_nodes(self, args):
        color = self.color

        machine_readable = args.machine_readable

        server_rc, node_list = self._get_nodes(sort=True)

        if (not machine_readable) and (node_list is None
                                       or len(node_list) == 0):
                sys.stdout.write("No nodes defined\n")
                return 0

        t = Table()

        t.addColumn("Name", color=color(COLOR_TEAL))
        t.addColumn("Pool_Size", color=color(COLOR_BROWN), just_txt='>')
        t.addColumn("Pool_Free", color=color(COLOR_BROWN), just_txt='>')
        t.addColumn("Family", just_txt='>')
        t.addColumn("IP", just_txt='>')
        t.addColumn("State", color=color(COLOR_GREEN), just_txt='>', just_col='>')

        # fixed ones we always show
        tview = ["Name", "Pool_Size", "Pool_Free", "State"]
        if args.show:
            tview += args.show
        t.setView(tview)

        t.setGroupBy(args.groupby)

        for node_entry in node_list:
            try:
                node_name, properties = node_entry
                view = DrbdNodeView(properties, machine_readable)
                v_af = self._property_text(view.get_property(NODE_AF))
                v_addr = self._property_text(view.get_property(NODE_ADDR))
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

                    t.addRow([node_name, poolsize_text, poolfree_text,
                              "ipv" + v_af, v_addr, view.get_state()])
                else:
                    v_psize = self._property_text(
                        view.get_property(NODE_POOLSIZE))
                    v_pfree = self._property_text(
                        view.get_property(NODE_POOLFREE))

                    sys.stdout.write(
                        "%s,%s,%s,%s,%s,%s\n"
                        % (node_name, v_af,
                           v_addr, v_psize,
                           v_pfree, view.get_state())
                    )
            except IncompatibleDataException:
                sys.stderr.write("Warning: incompatible table entry skipped\n")
        t.show()
        return 0

    def cmd_list_resources(self, args):
        return self._list_resources(args, False)

    def cmd_list_volumes(self, args):
        return self._list_resources(args, True)

    def __list_resources(self, list_volumes):
        self.dbus_init()

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

        server_rc, res_list = self.__list_resources(list_volumes)

        if (not machine_readable) and (res_list is None or len(res_list) == 0):
                sys.stdout.write("No resources defined\n")
                return 0

        # Header/key for the table
        if not machine_readable:
            sys.stdout.write(
                "%s%-*s%s %7s        %s%s%s\n"
                % (color(COLOR_DARKGREEN),
                   DrbdResourceView.get_name_maxlen(), "Resource",
                   color(COLOR_NONE), "Port",
                   color(COLOR_RED), "state", color(COLOR_NONE))
            )
            if list_volumes:
                sys.stdout.write(
                    "  %s*%s%6s%s %14s %7s  %s%s\n"
                    % (color(COLOR_BROWN), color(COLOR_DARKPINK),
                       "id#", color(COLOR_BROWN), "size (MiB)", "minor#",
                       "state", color(COLOR_NONE))
                )
            sys.stdout.write((self.VIEW_SEPARATOR_LEN * '-') + "\n")

        for res_entry in res_list:
            try:
                if list_volumes:
                    res_name, properties, vol_list = res_entry
                else:
                    res_name, properties = res_entry
                res_view = DrbdResourceView(properties, machine_readable)
                v_port = self._property_text(res_view.get_property(RES_PORT))
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
                    # sort volume list by volume id
                    vol_list.sort(key=lambda vol_entry: vol_entry[0])
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

    def _list_snapshots(self):
        self.dbus_init()

        server_rc, res_list = self._server.list_snapshots(
            dbus.Array([], signature="s"),
            dbus.Array([], signature="s"),
            0,
            dbus.Dictionary({}, signature="ss"),
            dbus.Array([], signature="s")
        )

        # sort the list by resource name
        res_list.sort(key=lambda res_entry: res_entry[0])

        return (server_rc, res_list)

    def cmd_list_snapshots(self, args):
        color = self.color

        machine_readable = args.machine_readable

        server_rc, res_list = self._list_snapshots()

        if (not machine_readable) and (res_list is None
                                       or len(res_list) == 0):
                sys.stdout.write("Snapshot list is empty\n")
                return 0

        if not machine_readable:
            sys.stdout.write(
                "%s%s%s\n"
                % (color(COLOR_DARKGREEN), "Resource", color(COLOR_NONE))
            )
            sys.stdout.write(
                "  %s* %s%s%s\n"
                % (color(COLOR_BROWN), color(COLOR_DARKPINK),
                   "Snapshot", color(COLOR_NONE))
            )
            sys.stdout.write((self.VIEW_SEPARATOR_LEN * '-') + "\n")

        for res_entry in res_list:
            res_name, snaps_list = res_entry
            # sort the list by snapshot name
            if not machine_readable:
                sys.stdout.write(
                    "%s%s%s\n"
                    % (color(COLOR_DARKGREEN), res_name, color(COLOR_NONE))
                )
            snaps_list.sort(key=lambda snaps_entry: snaps_entry[0])
            for snaps_entry in snaps_list:
                snaps_name, snaps_props = snaps_entry
                if machine_readable:
                    sys.stdout.write("%s,%s\n" % (res_name, snaps_name))
                else:
                    sys.stdout.write(
                        "  %s* %s%s%s\n"
                        % (color(COLOR_BROWN), color(COLOR_DARKPINK),
                           snaps_name, color(COLOR_NONE))
                    )
        return 0


    def cmd_list_snapshot_assignments(self, args):
        color = self.color

        self.dbus_init()

        machine_readable = args.machine_readable

        server_rc, assg_list = self._server.list_snapshot_assignments(
            dbus.Array([], signature="s"),
            dbus.Array([], signature="s"),
            dbus.Array([], signature="s"),
            0,
            dbus.Dictionary({}, signature="ss"),
            dbus.Array([], signature="s")
        )

        if (not machine_readable) and (assg_list is None
                                       or len(assg_list) == 0):
                sys.stdout.write("Snapshot assignment list is empty\n")
                return 0

        if not machine_readable:
            sys.stdout.write(
                "%s%-*s %s%s%s\n"
                % (color(COLOR_DARKGREEN), DrbdResourceView.get_name_maxlen(),
                   "Resource",
                   color(COLOR_DARKPINK), "Snapshot", color(COLOR_NONE))
            )
            sys.stdout.write(
                "  %s* %s%-*s %s%s%s\n"
                % (color(COLOR_BROWN), color(COLOR_TEAL),
                   DrbdNodeView.get_name_maxlen(), "Node",
                   color(COLOR_RED), "state", color(COLOR_NONE))
            )
            sys.stdout.write((self.VIEW_SEPARATOR_LEN * '-') + "\n")

        # sort the list by resource name
        assg_list.sort(key=lambda res_entry: res_entry[0])

        for assg_list_entry in assg_list:
            res_name, snaps_name, snaps_assg_list = assg_list_entry
            if not machine_readable:
                sys.stdout.write(
                    "%s%-*s %s%s%s\n"
                    % (color(COLOR_DARKGREEN),
                       DrbdResourceView.get_name_maxlen(), res_name,
                       color(COLOR_DARKPINK), snaps_name,
                       color(COLOR_NONE))
                )
            # sort the assignment entries by node name
            snaps_assg_list.sort(key=lambda list_entry: list_entry[0])
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
                    sys.stdout.write(
                        "  %s* %s%-*s %s%s -> %s%s\n"
                        % (color(COLOR_BROWN), color(COLOR_DARKPINK),
                           DrbdNodeView.get_name_maxlen(), node_name,
                           color(COLOR_RED),
                           snaps_assg.get_cstate(), snaps_assg.get_tstate(),
                           color(COLOR_NONE))
                    )
        return 0

    def cmd_list_assignments(self, args):
        color = self.color

        self.dbus_init()

        machine_readable = args.machine_readable

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
            sys.stdout.write(
                "%s%-*s%s\n"
                % (color(COLOR_TEAL), DrbdNodeView.get_name_maxlen(),
                   "Node", color(COLOR_NONE))
            )
            sys.stdout.write(
                "  %s%-*s%s %5s %35s%s%s%s\n"
                % (color(COLOR_DARKGREEN),
                   DrbdResourceView.get_name_maxlen(),
                   "Resource", color(COLOR_NONE),
                   "Node#", "", color(COLOR_RED),
                   "state (crt -> tgt)",
                   color(COLOR_NONE))
            )
            sys.stdout.write(
                "  %s* %s%6s%s %-48s %s%s%s\n"
                % (color(COLOR_BROWN), color(COLOR_DARKPINK),
                   "Vol#",  color(COLOR_BROWN),
                   "Blockdevice path",
                   color(COLOR_DARKRED), "state (crt -> tgt)",
                   color(COLOR_NONE))
            )
            sys.stdout.write((self.VIEW_SEPARATOR_LEN * '-') + "\n")

        # Sort the assignment list by node name, then by resource name
        assg_list.sort(key=lambda assg_entry: (assg_entry[0], assg_entry[1]))

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
                        sys.stdout.write(
                            "%s%-*s%s\n"
                            % (color(COLOR_TEAL),
                               DrbdNodeView.get_name_maxlen(), node_name,
                               color(COLOR_NONE))
                        )
                    sys.stdout.write(
                        "  %s%-*s%s %5s %35s%s%s -> %s%s\n"
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

                        sys.stdout.write(
                            "  %s* %s%6s%s %-48s %s%s  -> %s%s\n"
                            % (color(COLOR_BROWN), color(COLOR_DARKPINK),
                               vol_id,  color(COLOR_BROWN),
                               v_bdev,
                               color(COLOR_DARKRED), vol_view.get_cstate(),
                               vol_view.get_tstate(), color(COLOR_NONE))
                        )
                else:
                    sys.stdout.write(
                        "%s,%s,%s,%s,%s\n"
                        % (node_name, res_name, v_node_id, v_cstate, v_tstate)
                    )
            except IncompatibleDataException:
                sys.stderr.write("Warning: incompatible table entry skipped\n")
        return 0

    def cmd_export_conf(self, args):
        fn_rc = 1

        res_name = args.resource
        if res_name == "*":
            res_name = ""

        self.dbus_init()
        server_rc = self._server.export_conf(dbus.String(res_name))
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_howto_join(self, args):
        """
        Queries the command line to join a node from the server
        """
        fn_rc = 1

        node_name = args.node
        self.dbus_init()
        server_rc, joinc = self._server.text_query(["joinc", node_name])
        sys.stdout.write("%s\n" % " ".join(joinc))
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
            print json.dumps(res,
                             sort_keys=True,
                             indent=4,
                             separators=(',', ': '))
        except dbus.DBusException as e:
            if e._dbus_error_name == 'org.freedesktop.DBus.Python.TypeError':
                msg = re.sub(r'.*\n(TypeError:)', '\\1',
                             e.message,
                             flags=re.DOTALL + re.MULTILINE)
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
            sys.stderr.write(
                "drbdmanage: cannot connect to the drbdmanage "
                "server through D-Bus.\n"
            )
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

        server_conf = self.load_server_conf()
        drbdctrl_vg = self._get_drbdctrl_vg(server_conf)

        try:
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

            af = args.address_family
            address = args.ip
            port = args.port
            quiet = args.quiet
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
                drbdctrl_blockdev = self._create_drbdctrl("0", server_conf)
                self._ext_command(
                    ["drbdsetup", "primary", DRBDCTRL_RES_NAME, "--force"]
                )
                init_rc = self._drbdctrl_init(DRBDCTRL_DEV)

                if init_rc != 0:
                    # an error message is printed by _drbdctrl_init()
                    raise AbortException
                self._ext_command(
                    ["drbdsetup", "secondary", DRBDCTRL_RES_NAME]
                )

                props = {}
                props[NODE_ADDR] = address
                props[NODE_AF] = af
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
        return fn_rc

    def cmd_uninit(self, args):
        fn_rc = 1

        quiet = args.quiet
        shutdown = args.shutdown

        if not quiet:
            quiet = self.user_confirm(
                "You are going to remove the drbdmanage server from "
                "this node.\n"
                "CAUTION! Note that:\n"
                "  * All temporary configuration files for resources "
                "managed by drbdmanage\n"
                "    will be removed\n"
                "  * Any remaining resources managed by this "
                "drbdmanage installation\n"
                "    that still exist on this system will no longer be "
                "managed by drbdmanage\n"
                "\n"
                "Confirm:\n"
            )
        if quiet:
            if shutdown:
                try:
                    self.dbus_init()
                    self._server.shutdown()
                except dbus.exceptions.DBusException:
                    # The server does not answer after a shutdown,
                    # or it might not have been running in the first place,
                    # both is not considered an error here
                    pass
            try:
                server_conf = self.load_server_conf()
                drbdctrl_vg = self._get_drbdctrl_vg(server_conf)
                conf_path = self._get_conf_path(server_conf)
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

        server_conf = self.load_server_conf()
        drbdctrl_vg = self._get_drbdctrl_vg(server_conf)

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
            node_name = None
            try:
                uname = os.uname()
                if len(uname) >= 2:
                    node_name = uname[1]
            except OSError:
                pass
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

                drbdctrl_blockdev = self._create_drbdctrl(
                    l_node_id, server_conf
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
                        af + ':' + l_addr + ":" + str(port),
                        af + ':' + p_addr + ":" + str(port),
                        "--peer-node-id=" + p_node_id,
                        "--_name=" + p_name,
                        "--shared-secret=" + secret,
                        "--cram-hmac-alg=sha256",
                        "--protocol=C"
                    ]
                )

                # Startup the drbdmanage server and update the local .drbdctrl
                # resource configuration file
                self.dbus_init()
                server_rc = self._server.join_node(
                    drbdctrl_blockdev, port, secret
                )
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
        return fn_rc

    def _init_join_cleanup(self, drbdctrl_vg, conf_path):
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
                ["lvremove", "--force", drbdctrl_vg + "/" + DRBDCTRL_RES_NAME]
            )
        except AbortException:
            pass

    def cmd_initcv(self, args):
        fn_rc = 1

        drbdctrl_file = args.dev
        quiet = args.quiet

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
                sys.stderr.write(
                    "Cannot execute %s, permission denied"
                    % (args[0])
                )
            else:
                sys.stderr.write(
                    "Cannot execute %s, error returned by the OS is: %s\n"
                    % (args[0], oserr.strerror)
                )
            raise AbortException
        return proc_rc

    def _create_drbdctrl(self, node_id, server_conf):
        drbdctrl_vg = self._get_drbdctrl_vg(server_conf)
        conf_path   = self._get_conf_path(server_conf)

        drbdctrl_blockdev = ("/dev/" + drbdctrl_vg + "/" + DRBDCTRL_RES_NAME)

        # ========================================
        # Cleanup
        # ========================================
        self._init_join_cleanup(drbdctrl_vg, conf_path)

        # ========================================
        # Join an existing drbdmanage cluster
        # ========================================

        # Create the .drbdctrl LV
        self._ext_command(
            ["lvcreate", "-n", DRBDCTRL_RES_NAME, "-L", "4m", drbdctrl_vg]
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
            ["drbdsetup", "new-resource", DRBDCTRL_RES_NAME, node_id]
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
        return drbdctrl_blockdev

    def _get_drbdctrl_vg(self, server_conf):
        # ========================================
        # Set up the path to the drbdctrl LV
        # ========================================
        if server_conf is not None:
            drbdctrl_vg = map_val_or_dflt(
                server_conf, KEY_DRBDCTRL_VG, DEFAULT_VG
            )
        else:
            drbdctrl_vg = DEFAULT_VG
        return drbdctrl_vg

    def _get_conf_path(self, server_conf):
        if server_conf is not None:
            conf_path = map_val_or_dflt(
                server_conf, KEY_DRBD_CONFPATH, DEFAULT_DRBD_CONFPATH
            )
        else:
            conf_path = DEFAULT_DRBD_CONFPATH
        return conf_path

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

    def cmd_debug(self, args):
        fn_rc = 1

        command = args.cmd
        try:
            self.dbus_init()
            fn_rc = self._server.debug_console(dbus.String(command))
            sys.stderr.write("fn_rc=%d, %s\n" % (fn_rc, command))
        except dbus.exceptions.DBusException:
            sys.stderr.write(
                "drbdmanage: cannot connect to the drbdmanage "
                "server through D-Bus.\n"
            )
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
        unit = input[split_idx:]
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
        blksz = pers_impl.BLKSZ

        index_name = pers_impl.IDX_NAME
        index_off = pers_impl.IDX_OFFSET
        hash_off = pers_impl.HASH_OFFSET
        data_off = pers_impl.DATA_OFFSET

        assg_len_name = pers_impl.ASSG_LEN_NAME
        assg_off_name = pers_impl.ASSG_OFF_NAME
        nodes_len_name = pers_impl.NODES_LEN_NAME
        nodes_off_name = pers_impl.NODES_OFF_NAME
        res_len_name = pers_impl.RES_LEN_NAME
        res_off_name = pers_impl.RES_OFF_NAME
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
            zeroblk = bytearray('\0' * blksz)
            pos = 0
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
        sys.stdout.write("empty drbdmanage control volume initialized.\n")

        return fn_rc

    def load_server_conf(self):
        in_file = None
        conf_loaded = None
        try:
            in_file = open(SERVER_CONFFILE, "r")
            conffile = ConfFile(in_file)
            conf_loaded = conffile.get_conf()
        except IOError as ioerr:
            sys.stderr.write("No server configuration file loaded:\n")
            if ioerr.errno == errno.EACCES:
                sys.stderr.write(
                    "Cannot open configuration file '%s', "
                    "permission denied\n"
                    % (SERVER_CONFFILE)
                )
            elif ioerr.errno != errno.ENOENT:
                sys.stderr.write(
                    "Cannot open configuration file '%s', "
                    "error returned by the OS is: %s\n"
                    % (SERVER_CONFFILE, ioerr.strerror)
                )
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


def main():
    client = DrbdManage()
    client.run()

if __name__ == "__main__":
    main()
