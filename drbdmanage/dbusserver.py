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

import sys
import string
import traceback
import dbus
import dbus.service
import dbus.mainloop.glib
import gobject
import logging
import drbdmanage.drbd.drbdcore
from drbdmanage.drbd.drbdcore import Assignment
from drbdmanage.exceptions import *

# TODO: DEBUG: used for debug code only
from drbdmanage.drbd.persistence import *
import drbdmanage.conf.conffile


class DBusServer(dbus.service.Object):
    
    """
    dbus API to the drbdmanage server API
    """
    
    DBUS_DRBDMANAGED = "org.drbd.drbdmanaged"
    DBUS_SERVICE     = "/interface"
    
    _dbus   = None
    _server = None
    
    
    def __init__(self, server):
        dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
        self._dbus = dbus.service.BusName(self.DBUS_DRBDMANAGED,
          bus=dbus.SystemBus())
        dbus.service.Object.__init__(self, self._dbus, self.DBUS_SERVICE)
        self._server = server
    
    
    def run(self):
        """
        Calls the run() function of the server. It is recommended to call
        that function directly instead of going through the dbus interface.
        """
        self._server.run()
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="sa{ss}", out_signature="i")
    def create_node(self, name, props):
        """
        D-Bus interface for DrbdManageServer.create_node(...)
        """
        return self._server.create_node(name, props)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="sb", out_signature="i")
    def remove_node(self, name, force):
        """
        D-Bus interface for DrbdManageServer.remove_node(...)
        """
        return self._server.remove_node(name, force)
        
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="sa{ss}", out_signature="i")
    def create_resource(self, name, props):
        """
        D-Bus interface for DrbdManageServer.create_resource(...)
        """
        return self._server.create_resource(name, props)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="sa{ss}", out_signature="i")
    def modify_resource(self, name, props):
        """
        D-Bus interface for DrbdManageServer.modify_resource(...)
        """
        return self._server.modify_resource(name, props)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="sb", out_signature="i")
    def remove_resource(self, name, force):
        """
        D-Bus interface for DrbdManageServer.remove_resource(...)
        """
        return self._server.remove_resource(name, force)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="sxa{ss}", out_signature="i")
    def create_volume(self, name, size_kiB, props):
        """
        D-Bus interface for DrbdManageServer.create_volume(...)
        """
        return self._server.create_volume(name, size_kiB, props)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="sib", out_signature="i")
    def remove_volume(self, name, vol_id, force):
        """
        D-Bus interface for DrbdManageServer.remove_volume(...)
        """
        return self._server.remove_volume(name, vol_id, force)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="ssb", out_signature="i")
    def connect(self, node_name, resource_name, reconnect):
        """
        D-Bus interface for DrbdManageServer.connect(...)
        """
        return self._server.connect(node_name, resource_name, reconnect)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="ss", out_signature="i")
    def disconnect(self, node_name, resource_name):
        """
        D-Bus interface for DrbdManageServer.disconnect(...)
        """
        return self._server.disconnect(node_name, resource_name)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="ssxxxx", out_signature="i")
    def modify_state(self, node_name, resource_name,
      cstate_clear_mask, cstate_set_mask,
      tstate_clear_mask, tstate_set_mask):
        """
        D-Bus interface for DrbdManageServer.modify_state(...)
        """
        return self._server.modify_state(node_name, resource_name,
          cstate_clear_mask, cstate_set_mask,
          tstate_clear_mask, tstate_set_mask)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="ssi", out_signature="i")
    def attach(self, node_name, resource_name, volume_id):
        """
        D-Bus interface for DrbdManageServer.attach(...)
        """
        return self._server.attach(node_name, resource_name, volume_id)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="ssi", out_signature="i")
    def detach(self, node_name, resource_name, volume_id):
        """
        D-Bus interface for DrbdManageServer.detach(...)
        """
        return self._server.detach(node_name, resource_name, volume_id)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="ssii", out_signature="i")
    def assign(self, node_name, resource_name, cstate, tstate):
        """
        D-Bus interface for DrbdManageServer.assign(...)
        """
        tstate = tstate | Assignment.FLAG_DEPLOY
        return self._server.assign(node_name, resource_name, cstate, tstate)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="ssb", out_signature="i")
    def unassign(self, node_name, volume_name, force):
        """
        D-Bus interface for DrbdManageServer.unassign(...)
        """
        return self._server.unassign(node_name, volume_name, force)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="si", out_signature="i")
    def auto_deploy(self, res_name, count):
        """
        D-Bus interface for DrbdManageServer.auto_deploy(...)
        """
        return self._server.auto_deploy(res_name, count)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="sib", out_signature="i")
    def auto_extend(self, res_name, count, extend):
        """
        D-Bus interface for DrbdManageServer.auto_extend(...)
        """
        return self._server.auto_extend(res_name, count, extend)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="sib", out_signature="i")
    def auto_reduce(self, res_name, count, reduce):
        """
        D-Bus interface for DrbdManageServer.auto_reduce(...)
        """
        return self._server.auto_reduce(res_name, count, reduce)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="sb", out_signature="i")
    def auto_undeploy(self, res_name, count):
        """
        D-Bus interface for DrbdManageServer.auto_undeploy(...)
        """
        return self._server.auto_undeploy(res_name, count)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="", out_signature="i")
    def update_pool(self):
        """
        D-Bus interface for DrbdManageServer.update_pool(...)
        """
        return self._server.update_pool()
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="", out_signature="a(sssxxx)")
    def node_list(self):
        """
        D-Bus interface for DrbdManageServer.node_list(...)
        """
        return self._server.node_list()
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="", out_signature="a(sssxa(ixix))")
    def resource_list(self):
        """
        D-Bus interface for DrbdManageServer.resource_list(...)
        """
        return self._server.resource_list()
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="", out_signature="a(ssixxa(isxx))")
    def assignment_list(self):
        """
        D-Bus interface for DrbdManageServer.assignment_list(...)
        """
        return self._server.assignment_list()
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="s", out_signature="i")
    def export_conf(self, resource):
        """
        D-Bus interface for DrbdManageServer.export_conf(...)
        """
        return self._server.export_conf(resource)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="", out_signature="i")
    def reconfigure(self):
        """
        D-Bus interface for DrbdManageServer.reconfigure()
        """
        return self._server.reconfigure()
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="s", out_signature="s")
    def text_query(self, command):
        """
        D-Bus interface for DrbdManageServer.text_query(...):
        """
        return self._server.text_query(command)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="sa{ss}ss", out_signature="i")
    def init_node(self, name, props, bdev, port):
        """
        D-Bus interface for DrbdManageServer.init_node(...)
        """
        return self._server.init_node(name, props, bdev, port)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="sss", out_signature="i")
    def join_node(self, bdev, port, secret):
        """
        D-Bus interface for DrbdManageServer.join_node(...)
        """
        return self._server.join_node(bdev, port, secret)

    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="", out_signature="i")
    def load_conf(self):
        """
        D-Bus interface for DrbdManageServer.load_conf()
        """
        return self._server.load_conf()
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="", out_signature="i")
    def save_conf(self):
        """
        D-Bus interface for DrbdManageServer.save_conf()
        """
        return self._server.save_conf()
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="", out_signature="i")
    def ping(self):
        """
        D-Bus ping/pong connection test
        
        This function can be used to test whether a client can communicate
        with the D-Bus interface of the drbdmanage server.
        If D-Bus service activation is configured, this function can also be
        used to start the drbdmanage server.
        """
        return 0
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="", out_signature="")
    def shutdown(self):
        """
        D-Bus interface for DrbdManageServer.shutdown()
        """
        logging.info("server shutdown requested through D-Bus")
        self._server.shutdown()
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="s", out_signature="i")
    def debug_console(self, command):
        """
        D-Bus interface for DrbdManageServer.debug_console(...)
        """
        return self._server.debug_console(command)
