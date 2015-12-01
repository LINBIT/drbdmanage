#!/usr/bin/env python2
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
import dbus.mainloop.glib
import json
import re
import subprocess
import time
import traceback
import drbdmanage.drbd.drbdcore
import drbdmanage.drbd.persistence
import drbdmanage.argparse.argparse as argparse
import drbdmanage.argcomplete as argcomplete
import gobject

from drbdmanage.consts import (
    KEY_DRBDCTRL_VG, DEFAULT_VG, DRBDCTRL_DEFAULT_PORT,
    DRBDCTRL_RES_NAME, DRBDCTRL_RES_FILE, DRBDCTRL_RES_PATH,
    NODE_ADDR, NODE_AF, NODE_ID, NODE_POOLSIZE, NODE_POOLFREE, RES_PORT,
    VOL_MINOR, VOL_BDEV, RES_PORT_NR_AUTO, FLAG_DISKLESS, FLAG_OVERWRITE,
    FLAG_DRBDCTRL, FLAG_STORAGE, FLAG_EXTERNAL, FLAG_DISCARD, FLAG_CONNECT, FLAG_QIGNORE,
    KEY_DRBD_CONFPATH, DEFAULT_DRBD_CONFPATH, DM_VERSION, DM_GITHASH,
    CONF_NODE, CONF_GLOBAL, KEY_SITE, BOOL_TRUE, BOOL_FALSE, FILE_GLOBAL_COMMON_CONF, KEY_VG_NAME,
    NODE_SITE, NODE_VOL_0, NODE_VOL_1, NODE_PORT, NODE_SECRET,
    DRBDCTRL_LV_NAME_0, DRBDCTRL_LV_NAME_1, DRBDCTRL_DEV_0, DRBDCTRL_DEV_1, NODE_CONTROL_NODE,
    NODE_SATELLITE_NODE, KEY_S_CMD_SHUTDOWN, KEY_ISSATELLITE,
    KEY_COLORS, KEY_UTF8,
)
from drbdmanage.utils import SizeCalc
from drbdmanage.utils import Table
from drbdmanage.utils import DrbdSetupOpts
from drbdmanage.utils import (
    build_path, bool_to_string, rangecheck, ssh_exec,
    load_server_conf_file, filter_prohibited, get_uname
)
from drbdmanage.utils import (
    COLOR_NONE, COLOR_RED, COLOR_DARKRED, COLOR_DARKGREEN, COLOR_BROWN,
    COLOR_DARKPINK, COLOR_TEAL, COLOR_GREEN, COLOR_YELLOW
)
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

    VIEW_SEPARATOR_LEN = 78

    UMHELPER_FILE = "/sys/module/drbd/parameters/usermode_helper"
    UMHELPER_OVERRIDE = "/bin/true"
    UMHELPER_WAIT_TIME = 5.0

    def __init__(self):
        self._parser = self.setup_parser()
        self._all_commands = self.parser_cmds()
        self._config = load_server_conf_file(localonly=True)
        if KEY_COLORS in self._config:
            self._colors = True if self._config[KEY_COLORS].strip().lower() == 'yes' else False
        if KEY_UTF8 in self._config:
            self._utf8 = True if self._config[KEY_UTF8].strip().lower() == 'yes' else False

    def dbus_init(self):
        try:
            if self._server is None:
                dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
                self._dbus = dbus.SystemBus()
                self._server = self._dbus.get_object(
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


    def subscribe(self, signal_name_arg, signal_handler_fn):
        if self._dbus is not None:
            if self._gmainloop is None:
                self._gmainloop = gobject.MainLoop()
            self._dbus.add_signal_receiver(
                handler_function=signal_handler_fn,
                signal_name=signal_name_arg,
                dbus_interface=None,
                bus_name=DbusServer.DBUS_DRBDMANAGED,
                path=DBusServer.DBUS_SERVICE
            )
        else:
            sys.stderr.write(
                "Error: DrbdManageClient.subscribe() without prior dbus_init()\n"
            )


    def wait_for_signals(self):
        if self._gmainloop is not None:
            self._gmainloop.run()


    def setup_parser(self):
        parser = argparse.ArgumentParser(prog='drbdmanage')
        parser.add_argument('--version', '-v', action='version',
                            version='%(prog)s ' + DM_VERSION + '; ' + DM_GITHASH)
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
        def IPCompleter(where):
            def Completer(prefix, parsed_args, **kwargs):
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
            return Completer

        def NodeCompleter(prefix, **kwargs):
            server_rc, node_list = self._get_nodes()
            possible = set()
            for n in node_list:
                name, _ = n
                possible.add(name)

            return possible

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
                                help='This node does not have a control volume'
                                ' on its own. It is used as a satellite node')
        p_new_node.add_argument('-c', '--control-node',
                                help='Node name of the control node (the one with access to control volume).'
                                ' Only valid if "--satellite" was given. By default the hostname of the node'
                                ' where this command was executed.').completer = NodeCompleter
        p_new_node.add_argument('-e', '--external', action="store_true",
                                help='External node that is whether a control node nor a satellite')
        p_new_node.add_argument('-s', '--no-storage', action="store_true")
        p_new_node.add_argument('-j', '--no-autojoin', action="store_true")
        p_new_node.add_argument('name', help='Name of the new node')
        p_new_node.add_argument('ip',
                                help='IP address of the new node').completer = IPCompleter("name")
        p_new_node.set_defaults(func=self.cmd_new_node)

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
        p_rm_node.add_argument('name', nargs="+", help='Name of the node to remove').completer = NodeCompleter
        p_rm_node.set_defaults(func=self.cmd_remove_node)

        # Quorum control, completion of the action parameter
        def QuorumActionCompleter(prefix, **kwargs):
            possible = ["ignore", "unignore"]
            if prefix is not None and prefix != "":
                possible = [item for item in possible if possible.startswith(prefix)]
            return possible

        p_quorum = subp.add_parser("quorum-control",
                                   aliases=["qc"],
                                   description="Sets quorum parameters on drbdmanage cluster nodes")
        p_quorum.add_argument('-o', '--override', action="store_true",
                              help="Override change protection in a partition without quorum")
        p_quorum.add_argument(
            "action", help="The action to perform on the affected nodes"
        ).completer = QuorumActionCompleter
        p_quorum.add_argument(
            "name", nargs="+", help="Name of the affected node or nodes"
        ).completer = NodeCompleter
        p_quorum.set_defaults(func=self.cmd_quorum_control)

        # new-resource
        p_new_res = subp.add_parser('add-resource',
                                    aliases=['nr', 'new-resource', 'ar'],
                                    description='Defines a DRBD resource for use with drbdmanage. '
                                    'Unless a specific IP port-number is supplied, the port-number is '
                                    'automatically selected by the drbdmanage server on the current node. ')
        p_new_res.add_argument('-p', '--port', type=rangecheck(1, 65535))
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

            return possible

        p_mod_res = subp.add_parser('modify-resource',
                                    description='Modify a resource')
        p_mod_res.add_argument('-p', '--port', type=rangecheck(1, 65535))
        p_mod_res.add_argument('name',
                               help='Name of the resource to modify').completer = ResourceCompleter
        p_mod_res.set_defaults(func=self.cmd_modify_resource)

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
                              help='Name of the resource to delete').completer = ResourceCompleter
        p_rm_res.set_defaults(func=self.cmd_remove_resource)

        # new-volume
        def SizeCompleter(prefix, **kwargs):
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

        p_new_vol = subp.add_parser('add-volume',
                                    aliases=['nv', 'new-volume', 'av'],
                                    description='Defines a volume with a capacity of size for use with '
                                    'drbdmanage. If the resource resname exists already, a new volume is '
                                    'added to that resource, otherwise the resource is created automatically '
                                    'with default settings. Unless minornr is specified, a minor number for '
                                    "the volume's DRBD block device is assigned automatically by the "
                                    'drbdmanage server.')
        p_new_vol.add_argument('-m', '--minor', type=int)
        p_new_vol.add_argument('-d', '--deploy', type=int)
        p_new_vol.add_argument('name',
                               help='Name of a new/existing resource').completer = ResourceCompleter
        p_new_vol.add_argument('size',
                               help='Size of the volume in resource. '
                               'The default unit for size is GiB (size * (2 ^ 30) bytes). '
                               'Another unit can be specified by using an according postfix. '
                               "Drbdmanage's internal granularity for the capacity of volumes is one "
                               'Mebibyte (2 ^ 20 bytes). All other unit specifications are implicitly '
                               'converted to Mebibyte, so that the actual size value used by drbdmanage '
                               'is the smallest natural number of Mebibytes that is large enough to '
                               'accomodate a volume of the requested size in the specified size unit.').completer = SizeCompleter
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

            return possible

        p_mod_res = subp.add_parser('remove-volume',
                                    aliases=['rv', 'delete-volume', 'dv'],
                                    description='Removes a volume from the drbdmanage cluster, and removes '
                                    'the volume definition from the resource definition. The volume is '
                                    'undeployed from all nodes and the volume entry is marked for removal '
                                    "from the resource definition in drbdmanage's data tables. After all "
                                    'nodes have undeployed the volume, the volume entry is removed from '
                                    'the resource definition.')
        p_mod_res.add_argument('-q', '--quiet', action="store_true",
                               help='Unless this option is used, drbdmanage will issue a safety question '
                               'that must be answered with yes, otherwise the operation is canceled.')
        p_mod_res.add_argument('-f', '--force', action="store_true",
                               help='If present, then the volume entry is removed from the resource '
                               'definition immediately, without taking any action on the cluster nodes '
                               'that have the volume deployed.')
        p_mod_res.add_argument('name', help='Name of the resource').completer = ResourceCompleter
        p_mod_res.add_argument('id', help='Volume ID', type=int).completer = VolumeCompleter
        p_mod_res.set_defaults(func=self.cmd_remove_volume)

        # connect
        p_conn = subp.add_parser('connect-resource', description='Connect resource on node',
                                 aliases=['connect'])
        p_conn.add_argument('resource').completer = ResourceCompleter
        p_conn.add_argument('node').completer = NodeCompleter
        p_conn.set_defaults(func=self.cmd_connect)

        # reconnect
        p_reconn = subp.add_parser('reconnect-resource', description='Reconnect resource on node',
                                   aliases=['reconnect'])
        p_reconn.add_argument('resource').completer = ResourceCompleter
        p_reconn.add_argument('node').completer = NodeCompleter
        p_reconn.set_defaults(func=self.cmd_reconnect)

        # disconnect
        p_disconn = subp.add_parser('disconnect-resource', description='Disconnect resource on node',
                                    aliases=['disconnect'])
        p_disconn.add_argument('resource').completer = ResourceCompleter
        p_disconn.add_argument('node').completer = NodeCompleter
        p_disconn.set_defaults(func=self.cmd_disconnect)

        # flags
        p_flags = subp.add_parser('set-flags', description='Set flags of resource on node',
                                  aliases=['flags'])
        p_flags.add_argument('resource', help='Name of the resource').completer = ResourceCompleter
        p_flags.add_argument('node', help='Name of the node').completer = NodeCompleter
        p_flags.add_argument('--reconnect', choices=(0, 1), type=int)
        p_flags.add_argument('--updcon', choices=(0, 1), type=int)
        p_flags.add_argument('--overwrite', choices=(0, 1), type=int)
        p_flags.add_argument('--discard', choices=(0, 1), type=int)
        p_flags.set_defaults(func=self.cmd_flags)

        # attach
        p_attach = subp.add_parser('attach-volume', description='Attach volume from node',
                                   aliases=['attach'])
        p_attach.add_argument('resource').completer = ResourceCompleter
        p_attach.add_argument('id', help='Volume ID', type=int).completer = VolumeCompleter
        p_attach.add_argument('node').completer = NodeCompleter
        p_attach.set_defaults(func=self.cmd_attach_detach, fname='attach')
        # detach
        p_detach = subp.add_parser('detach-volume', description='Detach volume from node',
                                   aliases=['detach'])
        p_detach.add_argument('resource').completer = ResourceCompleter
        p_detach.add_argument('id', help='Volume ID', type=int).completer = VolumeCompleter
        p_detach.add_argument('node').completer = NodeCompleter
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
        p_assign.add_argument('resource').completer = ResourceCompleter
        p_assign.add_argument('node', nargs="+").completer = NodeCompleter
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
        p_fspace.add_argument('redundancy', type=redundancy_type,
                              help='Redundancy level (>=1)')
        p_fspace.set_defaults(func=self.cmd_free_space)

        # deploy
        p_deploy = subp.add_parser('deploy-resource',
                                   aliases=['deploy'],
                                   description='Deploys a resource on "N" automatically selected nodes '
                                   "of the drbdmanage cluster.Using the information in drbdmanage's data "
                                   'tables, the drbdmanage server tries to find n nodes that have enough '
                                   'free storage capacity to deploy the resource resname.')
        p_deploy.add_argument('resource').completer = ResourceCompleter
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
        p_deploy.set_defaults(func=self.cmd_deploy)

        # undeploy
        p_undeploy = subp.add_parser('undeploy-resource',
                                     aliases=['undeploy'],
                                     description='Undeploys a resource from all nodes. The resource '
                                     "definition is still kept in drbdmanage's data tables.")
        p_undeploy.add_argument('-q', '--quiet', action="store_true")
        p_undeploy.add_argument('-f', '--force', action="store_true")
        p_undeploy.add_argument('resource').completer = ResourceCompleter
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
        p_unassign.add_argument('resource').completer = ResourceCompleter
        p_unassign.add_argument('node', nargs="+").completer = NodeCompleter
        p_unassign.set_defaults(func=self.cmd_unassign)

        # new-snapshot
        p_nsnap = subp.add_parser('add-snapshot',
                                  aliases=['ns', 'create-snapshot', 'cs',
                                           'new-snapshot', 'as'],
                                  description='Create a LVM snapshot')
        p_nsnap.add_argument('snapshot', help='Name of the snapshot')
        p_nsnap.add_argument('resource', help='Name of the resource').completer = ResourceCompleter
        p_nsnap.add_argument('nodes', help='List of nodes', nargs='+').completer = NodeCompleter
        p_nsnap.set_defaults(func=self.cmd_new_snapshot)

        # Snapshot commands:
        # These commands do not follow the usual option order:
        # For example remove-snapshot should have the snapshot name as first argument and the resource as
        # second argument. BUT: There are (potentially) more snapshots than resources, so specifying the
        # resource first and then completing only the snapshots for that resource makes more sense.

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

            return possible

        p_rmsnap = subp.add_parser('remove-snapshot',
                                   aliases=['delete-snapshot', 'ds'],
                                   description='Remove LVM snapshot of a resource')
        p_rmsnap.add_argument('-f', '--force', action="store_true")
        p_rmsnap.add_argument('resource', help='Name of the resource').completer = ResourceCompleter
        p_rmsnap.add_argument('snapshot', nargs="+", help='Name of the snapshot').completer = SnapsCompleter
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
        p_restsnap.add_argument('resource',
                                help='Name of the new resource that gets created from existing snapshot')
        p_restsnap.add_argument('snapshot_resource',
                                help='Name of the resource that was snapshoted').completer = ResourceCompleter
        p_restsnap.add_argument('snapshot', help='Name of the snapshot').completer = SnapsCompleter
        p_restsnap.set_defaults(func=self.cmd_restore_snapshot)

        # shutdown
        p_shutdown = subp.add_parser('shutdown',
                                     description='Stops the local drbdmanage server process.')
        p_shutdown.add_argument('-l', '--satellite', action="store_true",
                                help='If given, also send a shutdown command to connected satellites.')
        p_shutdown.add_argument('-q', '--quiet', action="store_true",
                                help='Unless this option is used, drbdmanage will issue a safety question '
                                'that must be answered with yes, otherwise the operation is canceled.')
        p_shutdown.set_defaults(func=self.cmd_shutdown)

        # nodes
        nodesverbose = ('Family', 'IP', 'Site', 'CTRL_Node')
        nodesgroupby = ('Name', 'Pool_Size', 'Pool_Free', 'Family', 'IP', 'State')

        def ShowGroupCompleter(lst, where):
            def Completer(prefix, parsed_args, **kwargs):
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
            return Completer

        NodesVerboseCompleter = ShowGroupCompleter(nodesverbose, "show")
        NodesGroupCompleter = ShowGroupCompleter(nodesgroupby, "groupby")
        p_lnodes = subp.add_parser('list-nodes', aliases=['n', 'nodes'],
                                   description='Prints a list of all cluster nodes known to drbdmanage. '
                                   'By default, the list is printed as a human readable table.')
        p_lnodes.add_argument('-m', '--machine-readable', action="store_true")
        p_lnodes.add_argument('-s', '--show', nargs='+',
                              choices=nodesverbose).completer = NodesVerboseCompleter
        p_lnodes.add_argument('-g', '--groupby', nargs='+',
                              choices=nodesgroupby).completer = NodesGroupCompleter
        p_lnodes.add_argument('-N', '--nodes', nargs='+',
                              help='Filter by list of nodes').completer = NodeCompleter
        p_lnodes.add_argument('--separators', action="store_true")
        p_lnodes.set_defaults(func=self.cmd_list_nodes)

        # resources
        resverbose = ('Port',)
        resgroupby = ('Name', 'Port', 'State')
        ResVerboseCompleter = ShowGroupCompleter(resverbose, "show")
        ResGroupCompleter = ShowGroupCompleter(resgroupby, "groupby")

        p_lreses = subp.add_parser('list-resources', aliases=['r', 'resources'],
                                   description='Prints a list of all resource definitions known to '
                                   'drbdmanage. By default, the list is printed as a human readable table.')
        p_lreses.add_argument('-m', '--machine-readable', action="store_true")
        p_lreses.add_argument('-s', '--show', nargs='+',
                              choices=resverbose).completer = ResVerboseCompleter
        p_lreses.add_argument('-g', '--groupby', nargs='+',
                              choices=resgroupby).completer = ResGroupCompleter
        p_lreses.add_argument('-R', '--resources', nargs='+',
                              help='Filter by list of resources').completer = ResourceCompleter
        p_lreses.add_argument('--separators', action="store_true")
        p_lreses.set_defaults(func=self.cmd_list_resources)

        # volumes
        volgroupby = resgroupby + ('Vol_ID', 'Size', 'Minor')
        VolGroupCompleter = ShowGroupCompleter(volgroupby, 'groupby')

        p_lvols = subp.add_parser('list-volumes', aliases=['v', 'volumes'],
                                  description=' Prints a list of all volume definitions known to drbdmanage. '
                                  'By default, the list is printed as a human readable table.')
        p_lvols.add_argument('-m', '--machine-readable', action="store_true")
        p_lvols.add_argument('-s', '--show', nargs='+',
                             choices=resverbose).completer = ResVerboseCompleter
        p_lvols.add_argument('-g', '--groupby', nargs='+',
                             choices=volgroupby).completer = VolGroupCompleter
        p_lvols.add_argument('--separators', action="store_true")
        p_lvols.add_argument('-R', '--resources', nargs='+',
                             help='Filter by list of resources').completer = ResourceCompleter
        p_lvols.set_defaults(func=self.cmd_list_volumes)

        # snapshots
        snapgroupby = ("Resource", "Name", "State")
        SnapGroupCompleter = ShowGroupCompleter(snapgroupby, "groupby")

        p_lsnaps = subp.add_parser('list-snapshots', aliases=['s', 'snapshots'],
                                   description='List available snapshots')
        p_lsnaps.add_argument('-m', '--machine-readable', action="store_true")
        p_lsnaps.add_argument('-g', '--groupby', nargs='+',
                              choices=snapgroupby).completer = SnapGroupCompleter
        p_lsnaps.add_argument('--separators', action="store_true")
        p_lsnaps.add_argument('-R', '--resources', nargs='+',
                              help='Filter by list of resources').completer = ResourceCompleter
        p_lsnaps.set_defaults(func=self.cmd_list_snapshots)

        # snapshot-assignments
        snapasgroupby = ("Resource", "Name", "Node", "State")

        SnapasGroupCompleter = ShowGroupCompleter(snapasgroupby, "groupby")

        p_lsnapas = subp.add_parser('list-snapshot-assignments', aliases=['sa', 'snapshot-assignments'],
                                    description='List snapshot assignments')
        p_lsnapas.add_argument('-m', '--machine-readable', action="store_true")
        p_lsnapas.add_argument('-g', '--groupby', nargs='+',
                               choices=snapasgroupby).completer = SnapasGroupCompleter
        p_lsnapas.add_argument('--separators', action="store_true")
        p_lsnapas.add_argument('-N', '--nodes', nargs='+',
                               help='Filter by list of nodes').completer = NodeCompleter
        p_lsnapas.add_argument('-R', '--resources', nargs='+',
                               help='Filter by list of resources').completer = ResourceCompleter
        p_lsnapas.set_defaults(func=self.cmd_list_snapshot_assignments)

        # assignments
        assignverbose = ('Blockdevice', 'Node_ID')
        assigngroupby = ('Node', 'Resource', 'Vol_ID', 'Blockdevice',
                         'Node_ID', 'State')

        AssVerboseCompleter = ShowGroupCompleter(assignverbose, "show")
        AssGroupCompleter = ShowGroupCompleter(assigngroupby, "groupby")

        p_assignments = subp.add_parser('list-assignments', aliases=['a', 'assignments'],
                                        description="Prints a list of each node's assigned resources."
                                        "Nodes that do not have any resources assigned do not appear in the "
                                        "list. By default, the list is printed as a human readable table.")
        p_assignments.add_argument('-m', '--machine-readable',
                                   action="store_true")
        p_assignments.add_argument('-s', '--show', nargs='+',
                                   choices=assignverbose).completer = AssVerboseCompleter
        p_assignments.add_argument('-g', '--groupby', nargs='+',
                                   choices=assigngroupby).completer = AssGroupCompleter
        p_assignments.add_argument('--separators', action="store_true")
        p_assignments.add_argument('-N', '--nodes', nargs='+',
                                   help='Filter by list of nodes').completer = NodeCompleter
        p_assignments.add_argument('-R', '--resources', nargs='+',
                                   help='Filter by list of resources').completer = ResourceCompleter
        p_assignments.set_defaults(func=self.cmd_list_assignments)

        # export
        p_export = subp.add_parser('export',
                                   description='Exports the configuration files of the specified '
                                   'drbdmanage resource for use with drbdadm. If "*" is used as '
                                   'resource name, the configuration files of all drbdmanage resources '
                                   'deployed on the local node are exported. The configuration files will '
                                   'be created (or updated) in the drbdmanage directory for temporary '
                                   'configuration files, typically /var/lib/drbd.d.')
        p_export.add_argument('resource', nargs="+",
                              help='Name of the resource').completer = ResourceCompleter
        p_export.set_defaults(func=self.cmd_export_conf)

        # howto-join
        p_howtojoin = subp.add_parser('howto-join',
                                      description='Print the command to'
                                      ' execute on the given node in order to'
                                      ' join the cluster')
        p_howtojoin.add_argument('node',
                                 help='Name of the node to join').completer = NodeCompleter
        p_howtojoin.add_argument('-q', '--quiet', action="store_true",
                                 help="If the --quiet option is used, the join command is printed "
                                      "with a --quiet option")
        p_howtojoin.set_defaults(func=self.cmd_howto_join)

        def LowLevelDebugCmdCompleter(prefix, **kwargs):
            self.dbus_init()
            # needed to wait for completion
            self._server.Introspect()
            fns = []
            expected = DBusServer.DBUS_DRBDMANAGED + "."
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
                                      help="DBusServer function to call").completer = LowLevelDebugCmdCompleter

        def LowLevelDebugJsonCompleter(prefix, parsed_args=None, **kwargs):
            self.dbus_init()
            fn = getattr(self._server, parsed_args.cmd)
            if not fn: return []

            # TODO: introspect fn, to see whether array/dict/etc. is wanted..
            if prefix == '':
                return ['[]', '{}']
            return []
        p_lowlevel_debug.add_argument("json",
                                      help="JSON to deserialize",
                                      nargs="*").completer = LowLevelDebugJsonCompleter
        p_lowlevel_debug.set_defaults(func=self.cmd_lowlevel_debug)

        # server-version
        p_server_version = subp.add_parser('server-version',
                                           description='Queries version information from the '
                                           'drbdmanage server')
        p_server_version.set_defaults(func=self.cmd_server_version)

        # query-conf
        p_queryconf = subp.add_parser('query-conf',
                                      description='Print the DRBD'
                                      ' configuration file for a given'
                                      ' resource on a given node')
        p_queryconf.add_argument('node', help='Name of the node').completer = NodeCompleter
        p_queryconf.add_argument('resource',
                                 help='Name of the resource').completer = ResourceCompleter
        p_queryconf.set_defaults(func=self.cmd_query_conf)

        # ping
        p_ping = subp.add_parser('ping', description='Pings the server. The '
                                 'server should anser with a "pong"')
        p_ping.set_defaults(func=self.cmd_ping)

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
                return iter([]) # gives no sane text
                return iter(["any valid IP address"]) # completes this text

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
        p_join.add_argument('peer_name')
        p_join.add_argument('peer_ip').completer = IPCompleter("peer_ip")
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


        def ResVolCompleter(prefix, parsed_args, **kwargs):
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
        p_do = do.genArgParseSubcommand(subp)
        p_do.add_argument('--common', action="store_true")
        p_do.add_argument('--resource',
                          help='Name of the resource to modify').completer = ResourceCompleter
        p_do.add_argument('--volume',
                          help='Name of the volume to modify').completer = ResVolCompleter
        p_do.set_defaults(optsobj=do)
        p_do.set_defaults(type="disko")
        p_do.set_defaults(func=self.cmd_disk_options)

        # peer-device-options (shares func with disk-options)
        pdo = DrbdSetupOpts('peer-device-options')
        p_pdo = pdo.genArgParseSubcommand(subp)
        p_pdo.add_argument('--common', action="store_true")
        p_pdo.add_argument('--resource',
                           help='Name of the resource to modify').completer = ResourceCompleter
        p_pdo.add_argument('--volume',
                           help='Name of the volume to modify').completer = ResVolCompleter
        p_pdo.set_defaults(optsobj=pdo)
        p_pdo.set_defaults(type="peerdisko")
        p_pdo.set_defaults(func=self.cmd_disk_options)

        # resource-options
        ro = DrbdSetupOpts('resource-options')
        p_ro = ro.genArgParseSubcommand(subp)
        p_ro.add_argument('resource', help='Name of the resource').completer = ResourceCompleter
        p_ro.set_defaults(optsobj=ro)
        p_ro.set_defaults(func=self.cmd_res_options)

        # net-options
        # TODO: not allowed to set per connection, drbdmanage currently has no notion of a
        # connection in its object model.
        #
        no = DrbdSetupOpts('net-options')
        p_no = no.genArgParseSubcommand(subp)
        p_no.add_argument('--common', action="store_true")
        p_no.add_argument('--resource',
                          help='Name of the resource to modify').completer = ResourceCompleter
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
        p_handlers.add_argument('--resource',
                                help='Name of the resource to modify').completer = ResourceCompleter
        for handler in handlers:
            p_handlers.add_argument('--' + handler, help='Please refer to drbd.conf(5)', metavar='cmd')
            p_handlers.add_argument('--unset-' + handler, action='store_true')
        p_handlers.set_defaults(func=self.cmd_handlers)

        # list-options
        p_listopts = subp.add_parser('list-options',
                                     description='List drbd options set',
                                     aliases=['show-options'])
        p_listopts.add_argument('resource',
                                help='Name of the resource to show').completer = ResourceCompleter
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
        p_editconf.add_argument('--node', '-n',
                                help='Name of the node. This enables node specific options '
                                '(e.g. plugin settings)').completer = NodeCompleter
        p_editconf.set_defaults(func=self.cmd_edit_config)
        p_editconf.set_defaults(type="edit")

        # export config
        p_exportconf = subp.add_parser('export-config',
                                       description='Export drbdmanage configuration',
                                       aliases=['cat-config'])
        p_exportconf.add_argument('--node', '-n',
                                  help='Name of the node.').completer = NodeCompleter
        p_exportconf.add_argument('--file', '-f',
                                  help='File to save configuration')
        p_exportconf.set_defaults(func=self.cmd_edit_config)
        p_exportconf.set_defaults(type="export")

        # assign-satellite
        p_assign_satellite = subp.add_parser('assign-satellite',
                                             description='Assingn a satellite node to a control node')
        p_assign_satellite.add_argument('satellite',
                                        help='Name of the satellite node').completer = NodeCompleter
        p_assign_satellite.add_argument('controlnode',
                                        help='Name of the control node').completer = NodeCompleter
        p_assign_satellite.set_defaults(func=self.cmd_assign_satellite)

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
                    sys.stdout.write("\nWrong synopsis. Use the command as follows:\n")
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
                sys.stdout.write("\n") # additional newline, makes shell prompt happy
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
        satellite = args.satellite
        control_node = args.control_node
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
            if control_node or satellite:
                sys.stderr.write('Not allowed to mix --external with --control-node or --satellite\n')
                sys.exit(1)
            flag_storage = False
            flag_drbdctrl = False
        elif control_node and not satellite:
            sys.stderr.write('Not allowed to specify --control-node without --satellite\n')
            sys.exit(1)
        elif satellite and not control_node:
            control_node = get_uname()
            if not control_node:
                sys.stderr.write('Could not guess --control-node name\n')
                sys.exit(1)

        props = dbus.Dictionary(signature="ss")
        props[NODE_ADDR] = ip
        props[NODE_AF] = af
        if not flag_drbdctrl:
            props[FLAG_DRBDCTRL] = bool_to_string(flag_drbdctrl)
        if not flag_storage:
            props[FLAG_STORAGE] = bool_to_string(flag_storage)
        if control_node:
            props[NODE_CONTROL_NODE] = control_node
        if flag_external:
            props[FLAG_EXTERNAL] = bool_to_string(flag_external)

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
                    join_performed = ssh_exec("join", ip, name, joinc,
                                              args.quiet)
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
        m = re.match('(\d+)(\D*)', args.size)

        try:
            size = int(m.group(1))
        except AttributeError:
            sys.stderr.write('Size is not a valid number\n')
            return fn_rc

        unit_str = m.group(2)
        if unit_str == "":
            unit_str = "GiB"
        try:
            unit = self.UNITS_MAP[unit_str.lower()]
        except KeyError:
            sys.stderr.write('"%s" is not a valid unit!\n' % (unit_str))
            sys.stderr.write('Valid units: %s\n' % (','.join(self.UNITS_MAP.keys())))
            return fn_rc

        minor = MinorNr.MINOR_NR_AUTO
        if args.minor is not None:
            minor = args.minor
        name = args.name
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
            self.cmd_help(args)

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
                server_rc = self._server.remove_node(
                    dbus.String(node_name), dbus.Boolean(force)
                )
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
                server_rc = self._server.remove_resource(
                    dbus.String(res_name), dbus.Boolean(force)
                )
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
        return self._connect(args, True)

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
            server_rc = self._server.assign(
                dbus.String(node_name), dbus.String(res_name), props
            )
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
            server_rc = self._server.unassign(node_name, res_name, force)
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
            server_rc = self._server.quorum_control(
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
        server_rc = self._server.create_snapshot(
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
            server_rc = self._server.remove_snapshot(
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
        satellites = args.satellite
        props = dbus.Dictionary(signature="ss")
        props[KEY_S_CMD_SHUTDOWN] = bool_to_string(satellites)
        if not quiet:
            quiet = self.user_confirm(
                "You are going to shut down the drbdmanaged server "
                "process on this node.\nPlease confirm:"
            )
        if quiet:
            try:
                self.dbus_init()
                self._server.shutdown(props)
            except dbus.exceptions.DBusException:
                # An exception is expected here, as the server
                # probably will not answer
                pass
            # Continuing the client without a server
            # does not make sense, therefore exit
            exit(0)
        return 0

    def _get_nodes(self, sort=False, node_filter=[]):
        self.dbus_init()

        server_rc, node_list = self._server.list_nodes(
            dbus.Array(node_filter, signature="s"),
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

        node_filter_arg = [] if args.nodes is None else args.nodes

        server_rc, node_list = self._get_nodes(sort=True,
                                               node_filter=node_filter_arg)

        if (not machine_readable) and (node_list is None
                                       or len(node_list) == 0):
                sys.stdout.write("No nodes defined\n")
                return 0

        t = Table(colors=self._colors, utf8=self._utf8)
        if not args.groupby:
            groupby = ["Name"]
        else:
            groupby = args.groupby

        t.add_column("Name", color=color(COLOR_TEAL))
        t.add_column("Pool_Size", color=color(COLOR_BROWN), just_txt='>')
        t.add_column("Pool_Free", color=color(COLOR_BROWN), just_txt='>')
        t.add_column("CTRL_Node", color=color(COLOR_BROWN), just_txt='>')
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
                v_control_node = self._property_text(view.get_property(KEY_ISSATELLITE))
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
                        node_name, poolsize_text, poolfree_text, v_control_node, v_site,
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
            server_rc, res_list = self._server.list_volumes(
                dbus.Array(resource_filter, signature="s"),
                0,
                dbus.Dictionary({}, signature="ss"),
                dbus.Array([], signature="s")
            )
        else:
            server_rc, res_list = self._server.list_resources(
                dbus.Array(resource_filter, signature="s"),
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

        resource_filter_arg = [] if args.resources is None else args.resources

        server_rc, res_list = self.__list_resources(
            list_volumes, resource_filter=resource_filter_arg
        )

        if (not machine_readable) and (res_list is None or len(res_list) == 0):
                sys.stdout.write("No resources defined\n")
                return 0

        t = Table(colors=self._colors, utf8=self._utf8)

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
                                size_MiB = SizeCalc.convert(
                                    vol_view.get_size_kiB(),
                                    SizeCalc.UNIT_kiB, SizeCalc.UNIT_MiB
                                )
                                if size_MiB < 1:
                                    size_MiB_str = "< 1"
                                else:
                                    size_MiB_str = str(size_MiB)
                                level, state_text = vol_view.state_info()
                                level_color = self._level_color(level)
                                row_data = [
                                    res_name, str(vol_view.get_id()),
                                    size_MiB_str, v_minor, v_port, (level_color, state_text)
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

        server_rc, res_list = self._server.list_snapshots(
            dbus.Array(resource_filter, signature="s"),
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

        resource_filter_arg = [] if args.resources is None else args.resources

        server_rc, res_list = self._list_snapshots(
            resource_filter=resource_filter_arg
        )

        if (not machine_readable) and (res_list is None
                                       or len(res_list) == 0):
                sys.stdout.write("Snapshot list is empty\n")
                return 0

        t = Table(colors=self._colors, utf8=self._utf8)
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

        node_filter_arg = [] if args.nodes is None else args.nodes
        resource_filter_arg = [] if args.resources is None else args.resources

        server_rc, assg_list = self._server.list_snapshot_assignments(
            dbus.Array(resource_filter_arg, signature="s"),
            dbus.Array([], signature="s"),
            dbus.Array(node_filter_arg, signature="s"),
            0,
            dbus.Dictionary({}, signature="ss"),
            dbus.Array([], signature="s")
        )

        if (not machine_readable) and (assg_list is None
                                       or len(assg_list) == 0):
                sys.stdout.write("Snapshot assignment list is empty\n")
                return 0

        t = Table(colors=self._colors, utf8=self._utf8)
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

        node_filter_arg = [] if args.nodes is None else args.nodes
        resource_filter_arg = [] if args.resources is None else args.resources

        server_rc, assg_list = self._server.list_assignments(
            dbus.Array(node_filter_arg, signature="s"),
            dbus.Array(resource_filter_arg, signature="s"),
            0,
            dbus.Dictionary({}, signature="ss"),
            dbus.Array([], signature="s")
        )
        if (not machine_readable) and (assg_list is None
                                       or len(assg_list) == 0):
                sys.stdout.write("No assignments defined\n")
                return 0

        t = Table(colors=self._colors, utf8=self._utf8)

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
            server_rc = self._server.export_conf(dbus.String(res_name))
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
        server_rc, joinc = self._server.text_query(["joinc", node_name])
        sys.stdout.write(format % " ".join(joinc))
        fn_rc = self._list_rc_entries(server_rc)

        return fn_rc

    def cmd_server_version(self, args):
        """
        Queries version information from the drbdmanage server
        """
        fn_rc = 1

        self.dbus_init()
        query = [ "version" ]
        server_rc, version_info = self._server.text_query(query)
        for entry in version_info:
            sys.stdout.write("%s\n" % entry)

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
                raise AbortException

            af = args.address_family
            address = args.ip
            port = args.port
            quiet = args.quiet
            flag_storage = not args.no_storage
            # END Setup drbdctrl resource properties

            if not quiet:
                quiet = self.user_confirm("""
You are going to initalize a new drbdmanage cluster.
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

                props = {}
                props[NODE_ADDR] = address
                props[NODE_AF] = af
                props[NODE_VOL_0] = drbdctrl_blockdev_0
                props[NODE_VOL_1] = drbdctrl_blockdev_1
                props[NODE_PORT] = str(port)
                if not flag_storage:
                    props[FLAG_STORAGE] = bool_to_string(flag_storage)
                # Startup the drbdmanage server and add the current node
                self.dbus_init()
                server_rc = self._server.init_node(
                    dbus.String(node_name), props
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
                    self._server.shutdown()
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
                server_rc = self._server.join_node(props)
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
        self._ext_command(
            ["drbdsetup", "down", DRBDCTRL_RES_NAME]
        )

        # Delete any existing .drbdctrl LV
        self._ext_command(
            ["lvremove", "--force", drbdctrl_vg + "/" + DRBDCTRL_LV_NAME_0]
        )
        self._ext_command(
            ["lvremove", "--force", drbdctrl_vg + "/" + DRBDCTRL_LV_NAME_1]
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
                ["lvremove", "--force", drbdctrl_vg + "/" + DRBDCTRL_LV_NAME_0]
            )
        except AbortException:
            pass
        try:
            self._ext_command(
                ["lvremove", "--force", drbdctrl_vg + "/" + DRBDCTRL_LV_NAME_1]
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
        self._ext_command(
            ["lvcreate", "-n", drbdctrl_lv_1, "-L", "4m", drbdctrl_vg]
        )

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
        self._ext_command(
            [
                "drbdmeta", "0", "v09",
                drbdctrl_blockdev_0, "internal", "apply-al"
            ]
        )
        self._ext_command(
            [
                "drbdmeta", "1", "v09",
                drbdctrl_blockdev_1, "internal", "apply-al"
            ]
        )
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
            fn_rc = self._server.debug_console(dbus.String(command))
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
            fn_rc = self._server.set_drbdsetup_props(opts)
        except dbus.exceptions.DBusException:
            sys.stderr.write(
                "drbdmanage: cannot connect to the drbdmanage "
                "server through D-Bus.\n"
            )
        return fn_rc

    def cmd_res_options(self, args):
        fn_rc = 1
        target = "resource"

        newopts = args.optsobj.filterNew(args)
        if not newopts:
            sys.stderr.write('No new options found\n')
            return fn_rc

        newopts["target"] = target
        newopts["type"] = "reso"

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

        # filter net-options drbdmange sets unconditionally.
        net_options = filter_prohibited(net_options, ('shared-secret', 'cram-hmac-alg'))

        colors = {
            'net-options': self.color(COLOR_TEAL),
            'disk-options': self.color(COLOR_BROWN),
            'peer-device-options': self.color(COLOR_GREEN),
            'resource-options': self.color(COLOR_DARKPINK),
        }

        self.dbus_init()
        ret, conf = self._server.get_selected_config_values([KEY_DRBD_CONFPATH])

        res_file = 'drbdmanage_' + args.resource + '.res'
        conf_path = self._get_conf_path(conf)
        res_file = os.path.join(conf_path, res_file)
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
                         "files as they will be overwritten.\n")
        sys.stdout.write("Use the according drbdmange sub-commands to set/unset options.\n")

    def cmd_edit_config(self, args):
        import ConfigParser
        cfg = ConfigParser.RawConfigParser()

        self.dbus_init()

        if args.node:
            cfgtype = CONF_NODE
            server_rc, plugins = self._server.get_plugin_default_config()
            plugin_names = [p['name'] for p in plugins]
        else:
            cfgtype = CONF_GLOBAL

        # get all known config keys
        server_rc, config_keys = self._server.get_config_keys()

        # setting the drbdctrl-vg here is not allowed
        # only allowed via the config file
        prohibited = (KEY_DRBDCTRL_VG,)
        config_keys = filter_prohibited(config_keys, prohibited)

        # get all config options that are set cluster wide (aka GLOBAL)
        server_rc, cluster_config = self._server.get_cluster_config()

        # get all site configuration
        server_rc, site_config = self._server.get_site_config()

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
                            cfgstr += ' # or the value of %s if it is set and %s is unset' % (KEY_DRBDCTRL_VG,
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

        import subprocess
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

        server_rc = self._server.set_cluster_config(cfgdict)

        return server_rc

    def cmd_assign_satellite(self, args):
        fn_rc = 1
        satellite = args.satellite
        control_node = args.controlnode

        props = dbus.Dictionary(signature="ss")
        props[NODE_CONTROL_NODE] = control_node
        props[NODE_SATELLITE_NODE] = satellite
        self.dbus_init()
        server_rc = self._server.assign_satellite(props)
        fn_rc = self._list_rc_entries(server_rc)

        if fn_rc == 0:
            pass

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
        sys.stdout.write("empty drbdmanage control volume initialized.\n")

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
