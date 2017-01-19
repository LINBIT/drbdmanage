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

import logging
import dbus
import dbus.service
import dbus.mainloop.glib
from drbdmanage.utils import add_rc_entry
from drbdmanage.dbustracer import DbusTracer
from drbdmanage.consts import (DBUS_DRBDMANAGED, DBUS_SERVICE)
from drbdmanage.exceptions import (DM_ENOENT, DM_SUCCESS, dm_exc_text)


class DBusServer(dbus.service.Object):

    """
    dbus API to the drbdmanage server API
    """

    _dbus   = None
    _server = None
    _dbustracer = None
    _dbustracer_running = False

    def __init__(self, server):
        dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
        self._dbus = dbus.service.BusName(
            DBUS_DRBDMANAGED,
            bus=dbus.SystemBus()
        )
        dbus.service.Object.__init__(self, self._dbus, DBUS_SERVICE)
        self._server = server
        self._dbustracer = DbusTracer()
        self._dbustracer_running = False

    def run(self):
        """
        Calls the run() function of the server. It is recommended to call
        that function directly instead of going through the dbus interface.
        """
        self._server.run()

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="a{ss}",
        out_signature="a(isa(ss))" "s"
    )
    def dbus_tracer(self, props):
        fn_rc = []
        fname = ''
        succ = False
        if props.get('start'):
            maxlog = props.get('maxlog', 1000)
            succ = self._dbustracer.start(maxlog)
            if succ:
                self._dbustracer_running = True
        elif props.get('stop'):
            succ, fname = self._dbustracer.stop()
            if succ:
                self._dbustracer_running = False

        if not succ:
            add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))

        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))

        return fn_rc, fname

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def poke(self, message=None):
        """
        D-Bus interface for DrbdManageServer.poke(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.poke()

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="sa{ss}",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def create_node(self, node_name, props, message=None):
        """
        D-Bus interface for DrbdManageServer.create_node(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.create_node(node_name, dict(props))

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="sb",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def remove_node(self, node_name, force, message=None):
        """
        D-Bus interface for DrbdManageServer.remove_node(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.remove_node(node_name, force)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="sa{ss}",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def create_resource(self, res_name, props, message=None):
        """
        D-Bus interface for DrbdManageServer.create_resource(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.create_resource(res_name, dict(props))

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="sittt",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def resize_volume(self, res_name, vol_id, serial, size_kiB, delta_kiB, message=None):
        """
        D-Bus interface for DrbdManageServer.resize_volume(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.resize_volume(
            res_name, vol_id, serial, size_kiB, delta_kiB
        )

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="sb",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def remove_resource(self, res_name, force, message=None):
        """
        D-Bus interface for DrbdManageServer.remove_resource(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.remove_resource(res_name, force)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="sxa{ss}",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def create_volume(self, res_name, size_kiB, props, message=None):
        """
        D-Bus interface for DrbdManageServer.create_volume(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.create_volume(res_name, size_kiB, dict(props))

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="sib",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def remove_volume(self, res_name, vol_id, force, message=None):
        """
        D-Bus interface for DrbdManageServer.remove_volume(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.remove_volume(res_name, vol_id, force)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="ssb",
        out_signature="a(isa(ss))"
    )
    def connect(self, node_name, res_name, reconnect):
        """
        D-Bus interface for DrbdManageServer.connect(...)
        """
        return self._server.connect(node_name, res_name, reconnect)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="ssb",
        out_signature="a(isa(ss))"
    )
    def disconnect(self, node_name, res_name, force):
        """
        D-Bus interface for DrbdManageServer.disconnect(...)
        """
        return self._server.disconnect(node_name, res_name, force)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="sta{ss}",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def modify_node(self, node_name, serial, props, message=None):
        """
        D-Bus interface for DrbdManageServer.modify_node(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.modify_node(node_name, serial, dict(props))

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="sta{ss}",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def modify_resource(self, res_name, serial, props, message=None):
        """
        D-Bus interface for DrbdManageServer.modify_resource(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.modify_resource(res_name, serial, dict(props))

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="sita{ss}",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def modify_volume(self, res_name, vol_id, serial, props, message=None):
        """
        D-Bus interface for DrbdManageServer.modify_volume(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.modify_volume(res_name, vol_id, serial, dict(props))

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="ssta{ss}",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def modify_assignment(self, res_name, node_name, serial, props, message=None):
        """
        D-Bus interface for DrbdManageServer.modify_state(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.modify_assignment(res_name, node_name, serial, dict(props))

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="ssi",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def attach(self, node_name, res_name, vol_id, message=None):
        """
        D-Bus interface for DrbdManageServer.attach(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.attach(node_name, res_name, vol_id)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="ssi",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def detach(self, node_name, res_name, vol_id, message=None):
        """
        D-Bus interface for DrbdManageServer.detach(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.detach(node_name, res_name, vol_id)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="ssa{ss}",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def assign(self, node_name, res_name, props, message=None):
        """
        D-Bus interface for DrbdManageServer.assign(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.assign(node_name, res_name, dict(props))

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="ssb",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def unassign(self, node_name, res_name, force, message=None):
        """
        D-Bus interface for DrbdManageServer.unassign(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.unassign(node_name, res_name, force)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="i",
        out_signature="a(isa(ss))" "xx",
        message_keyword='message',
    )
    def cluster_free_query(self, redundancy, message=None):
        """
        D-Bus interface for DrbdManageServer.cluster_free_query(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.cluster_free_query(redundancy)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="is",
        out_signature="a(isa(ss))" "xx",
        message_keyword='message',
    )
    def cluster_free_query_site(self, redundancy, allowed_site, message=None):
        """
        D-Bus interface for DrbdManageServer.cluster_free_query(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.cluster_free_query(redundancy, allowed_site)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="siib",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def auto_deploy(self, res_name, count, delta, site_clients, message=None):
        """
        D-Bus interface for DrbdManageServer.auto_deploy(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.auto_deploy(res_name, int(count), int(delta), site_clients)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="siibs",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def auto_deploy_site(self, res_name, count, delta, site_clients, allowed_site, message=None):
        """
        D-Bus interface for DrbdManageServer.auto_deploy(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.auto_deploy(res_name, int(count), int(delta), site_clients, allowed_site)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="sb",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def auto_undeploy(self, res_name, force, message=None):
        """
        D-Bus interface for DrbdManageServer.auto_undeploy(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.auto_undeploy(res_name, force)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def update_pool_check(self, message=None):
        """
        D-Bus interface for DrbdManageServer.update_pool_check()
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.update_pool_check()

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="as",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def update_pool(self, node_names, message=None):
        """
        D-Bus interface for DrbdManageServer.update_pool(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.update_pool(node_names)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="a{ss}",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def set_drbdsetup_props(self, props, message=None):
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.set_drbdsetup_props(dict(props))

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="asta{ss}as",
        out_signature="a(isa(ss))" "a(sa{ss})",
        message_keyword='message',
    )
    def list_nodes(self, node_names, serial, filter_props, req_props, message=None):
        """
        D-Bus interface for DrbdManageServer.list_nodes(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.list_nodes(
            node_names, serial, filter_props, req_props
        )

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="",
        out_signature="a(isa(ss))" "a{ss}",
        message_keyword='message',
    )
    def get_config_keys(self, message=None):
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.get_config_keys()

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="",
        out_signature="a(isa(ss))" "aa{ss}",
        message_keyword='message',
    )
    def get_plugin_default_config(self, message=None):
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.get_plugin_default_config()

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="",
        out_signature="a(isa(ss))" "a{ss}",
        message_keyword='message',
    )
    def get_cluster_config(self, message=None):
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.get_cluster_config()

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="as",
        out_signature="a(isa(ss))" "a{ss}",
        message_keyword='message',
    )
    def get_selected_config_values(self, keys, message=None):
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.get_selected_config_values(keys)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="",
        out_signature="a(isa(ss))" "aa{ss}",
        message_keyword='message',
    )
    def get_site_config(self, message=None):
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.get_site_config()

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="a(s(a{ss}))",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def set_cluster_config(self, cfgdict, message=None):
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.set_cluster_config(dict(cfgdict))

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="asta{ss}as",
        out_signature="a(isa(ss))" "a(sa{ss})",
        message_keyword='message',
    )
    def list_resources(self, res_names, serial, filter_props, req_props, message=None):
        """
        D-Bus interface for DrbdManageServer.list_resources(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.list_resources(
            res_names, serial, dict(filter_props), req_props
        )

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="asta{ss}as",
        out_signature="a(isa(ss))" "a(sa{ss}a(ia{ss}))",
        message_keyword='message',
    )
    def list_volumes(self, res_names, serial, filter_props, req_props, message=None):
        """
        D-Bus interface for DrbdManageServer.list_volumes(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.list_volumes(
            res_names, serial, dict(filter_props), req_props
        )

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="asasta{ss}as",
        out_signature="a(isa(ss))" "a(ssa{ss}a(ia{ss}))",
        message_keyword='message',
    )
    def list_assignments(self, node_names, res_names,
                         serial, filter_props, req_props, message=None):
        """
        D-Bus interface for DrbdManageServer.list_assignments(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.list_assignments(
            node_names, res_names, serial, dict(filter_props), req_props
        )

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="ssasa{ss}",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def create_snapshot(self, res_name, snaps_name, node_names, props, message=None):
        """
        D-Bus interface for DrbdManageServer.create_snapshot(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.create_snapshot(
            res_name, snaps_name, node_names, dict(props)
        )

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="asasta{ss}as",
        out_signature="a(isa(ss))" "a(sa(sa{ss}))",
        message_keyword='message',
    )
    def list_snapshots(self, res_names, snaps_names, serial,
                       filter_props, req_props, message=None):
        """
        D-Bus interface for DrbdManageServer.list_snapshots(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.list_snapshots(
            res_names, snaps_names, serial, dict(filter_props), req_props
        )

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="asasasta{ss}as",
        out_signature="a(isa(ss))" "a(ssa(sa{ss}))",
        message_keyword='message',
    )
    def list_snapshot_assignments(self, res_names, snaps_names, node_names,
                                  serial, filter_props, req_props, message=None):
        """
        D-Bus interface for DrbdManageServer.list_snapshot_assignments(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.list_snapshot_assignments(
            res_names, snaps_names, node_names, serial,
            dict(filter_props), req_props
        )

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="sssa(ss)a(ia(ss))",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def restore_snapshot(self, res_name, snaps_res_name, snaps_name,
                         res_props, vols_props, message=None):
        """
        D-Bus interface for DrbdManageServer.restore_snapshot(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.restore_snapshot(res_name, snaps_res_name,
                                             snaps_name, res_props, vols_props)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="sssb",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def remove_snapshot_assignment(self, res_name, snaps_name, node_name,
                                   force, message=None):
        """
        D-Bus interface for DrbdManageServer.remove_snapshot_assignment(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.remove_snapshot_assignment(
            res_name, snaps_name, node_name, force
        )

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="ssb",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def remove_snapshot(self, res_name, snaps_name, force, message=None):
        """
        D-Bus interface for DrbdManageServer.remove_snapshot(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.remove_snapshot(res_name, snaps_name, force)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="ss",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def resume(self, node_name, res_name, message=None):
        """
        Clear the fail count of a resource's assignments
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.resume(node_name, res_name)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def resume_all(self, message=None):
        """
        Clear the fail count of a resource's assignments
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.resume_all()

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="s",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def export_conf(self, res_name, message=None):
        """
        D-Bus interface for DrbdManageServer.export_conf(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.export_conf(res_name)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="sa{ss}b",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def quorum_control(self, node_name, props, override_quorum, message=None):
        """
        D-Bus interface for DrbdManageServer.quorum_control(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.quorum_control(node_name, props, override_quorum)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def reconfigure(self, message=None):
        """
        D-Bus interface for DrbdManageServer.reconfigure()
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.reconfigure()

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="as",
        out_signature="a(isa(ss))" "as",
        message_keyword='message',
    )
    def text_query(self, command, message=None):
        """
        D-Bus interface for DrbdManageServer.text_query(...):
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.text_query(command)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="sa{ss}",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def init_node(self, node_name, props, message=None):
        """
        D-Bus interface for DrbdManageServer.init_node(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.init_node(node_name, props)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="",
        out_signature="a(isa(ss))" "s",
        message_keyword='message',
    )
    def role(self, message=None):
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.role()

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="a{ss}",
        out_signature="a(isa(ss))" "s",
        message_keyword='message',
    )
    def reelect(self, props, message=None):
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.reelect(props, False)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="a{ss}",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def join_node(self, props, message=None):
        """
        D-Bus interface for DrbdManageServer.join_node(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.join_node(props)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="sa{ss}",
        out_signature="a(isa(ss))" "a{ss}",
        message_keyword='message',
    )
    def run_external_plugin(self, plugin_name, props, message=None):
        """
        D-Bus interface for DrbdManageServer.run_external_plugin(...)
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.run_external_plugin(plugin_name, dict(props))

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def load_conf(self, message=None):
        """
        D-Bus interface for DrbdManageServer.load_conf()
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.dbus_load_conf()

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def save_conf(self, message=None):
        """
        D-Bus interface for DrbdManageServer.save_conf()
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.dbus_save_conf()

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="",
        out_signature="i",
        message_keyword='message',
    )
    def ping(self, message=None):
        """
        D-Bus ping/pong connection test

        This function can be used to test whether a client can communicate
        with the D-Bus interface of the drbdmanage server.
        If D-Bus service activation is configured, this function can also be
        used to start the drbdmanage server.
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return 0

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def wait_for_startup(self, message=None):
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.wait_for_startup()

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="",
        out_signature="a(isa(ss))" "s",
        message_keyword='message',
    )
    def get_ctrlvol(self, message=None):
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.get_ctrlvol()

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="s",
        out_signature="a(isa(ss))",
        message_keyword='message',
    )
    def set_ctrlvol(self, jsonblob, message=None):
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        return self._server.set_ctrlvol(jsonblob)

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="a{ss}",
        out_signature="",
        message_keyword='message',
    )
    def shutdown(self, props, message=None):
        """
        D-Bus interface for DrbdManageServer.shutdown()
        """
        if self._dbustracer_running:
            self._dbustracer.record(message.get_member(), message.get_args_list())
        logging.info("server shutdown requested through D-Bus")
        self._server.shutdown(dict(props))

    @dbus.service.method(
        DBUS_DRBDMANAGED,
        in_signature="s",
        out_signature="i"
    )
    def debug_console(self, command):
        """
        D-Bus interface for DrbdManageServer.debug_console(...)
        """
        return self._server.debug_console(command)


class DBusSignal(dbus.service.Object):
    PATH_PREFIX = "/objects"

    _path = None

    def __init__(self, path):
        if len(path) >= 1:
            if path[0] == "/":
                self._path = DBusSignal.PATH_PREFIX + path
            else:
                self._path = DBusSignal.PATH_PREFIX + "/" + path
        if self._path is not None:
            dbus.service.Object.__init__(self, dbus.SystemBus(), self._path)
            logging.debug("DBusSignal '%s': Instance created" % self._path)
        else:
            logging.debug("DBusSignal: Dummy instance created (no valid path specified)")

    @dbus.service.signal(DBUS_DRBDMANAGED)
    def notify_changed(self):
        """
        Signal to notify subscribers of a change

        This signal is to be sent to notify subscribers that the state of
        the object this signal is associated with has changed
        """
        logging.debug("DBusSignal '%s': notify_changed()" % self._path)

    @dbus.service.signal(DBUS_DRBDMANAGED)
    def notify_removed(self):
        """
        Signal to notify subscribers to unsubscribe

        This signal is to be sent whenever an instance of this class
        is removed (e.g., the server withdraws the DBus registration
        of the object associated with this signal and will therefore
        discard the DBusSignal instance, too)
        """
        logging.debug("DBusSignal '%s': notify_removed()" % self._path)

    def destroy(self):
        """
        Withdraws this instance from the DBus interface
        """
        # Notify any subscribers of the removal of this DBus service object
        self.notify_removed()
        # Remove the DBus service object
        if self._path is not None:
            self.remove_from_connection()


class DBusSignalFactory():
    """
    Instance factory for the DBusSignal class

    An object of this class is passed to the drbdmanage server to enable
    it to create instances of the DBusSignal class, so the drbdmanage
    server can be kept independent of the DBus layer.
    """

    def __init__(self):
        pass

    def create_signal(self, path):
        return DBusSignal(path)
