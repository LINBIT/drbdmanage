#!/usr/bin/python

import sys
import string
import traceback
import dbus
import dbus.service
import dbus.mainloop.glib
import gobject
import drbdmanage.drbd.drbdcore
from drbdmanage.exceptions import *

__author__="raltnoeder"
__date__ ="$Sep 12, 2013 4:43:41 PM$"

class DBusServer(dbus.service.Object):
    DBUS_DRBDMANAGED = "org.drbd.drbdmanaged"
    DBUS_SERVICE     = "/interface"
    
    _dbus   = None
    _server = None
    
    def __init__(self, server):
        dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
        self._dbus = dbus.service.BusName(self.DBUS_DRBDMANAGED, \
          bus=dbus.SystemBus())
        dbus.service.Object.__init__(self, self._dbus, self.DBUS_SERVICE)
        self._server = server
    
    def run(self):
        gobject.MainLoop().run()
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="sss", out_signature="i")
    def create_node(self, name, ip, ip_type):
        try:
            if ip_type == "ipv6":
                ip_type_n = drbdmanage.drbd.drbdcore.DrbdNode.IPV6_TYPE
            else:
                ip_type_n = drbdmanage.drbd.drbdcore.DrbdNode.IPV4_TYPE
            return self._server.create_node(name, ip, ip_type_n)
        except Exception as exc:
            sys.stderr.write("Oops, " + str(exc))
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="s", out_signature="i")
    def remove_node(self, name):
        return self._server.remove_node(name)
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="si", out_signature="i")
    def create_volume(self, name, size_MiB):
        return self._server.create_volume(name, size_MiB)
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="s", out_signature="i")
    def remove_volume(self, name):
        return self._server.remove_volume(name)
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="ss", out_signature="i")
    def assign(self, node_name, volume_name):
        # TODO: state for assignment
        return self._server.assign(node_name, volume_name, 0)
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="ss", out_signature="i")
    def unassign(self, node_name, volume_name):
        return self._server.unassign(node_name, volume_name)
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="", out_signature="i")
    def reconfigure(self):
        return DM_ENOTIMPL
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="", out_signature="i")
    def shutdown(self):
        self._server.shutdown()
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="s", out_signature="i")
    def debug_cmd(self, cmd):
        try:
            if cmd.endswith("\n"):
                cmd = cmd[:len(cmd) - 1]
            if cmd == "list-nodes":
                sys.stdout.write( \
                  string.ljust("Node name", 17) \
                  + string.ljust("type", 5) \
                  + string.ljust("IP address", 16) \
                  + "\n")
                sys.stdout.write(("-" * 60) + "\n")
                for node in self._server._nodes.itervalues():
                    ip_type = "unkn"
                    if node.get_ip_type() \
                      == drbdmanage.drbd.drbdcore.DrbdNode.IPV4_TYPE:
                        ip_type = "ipv4"
                    elif node.get_ip_type() \
                      == drbdmanage.drbd.drbdcore.DrbdNode.IPV6_TYPE:
                        ip_type = "ipv6"
                    sys.stdout.write( \
                      string.ljust(node.get_name(), 17) \
                      + string.ljust(ip_type, 5) \
                      + string.ljust(node.get_ip(), 16) \
                      + "\n")
            elif cmd == "list-volumes":
                sys.stdout.write( \
                  string.ljust("Volume name", 17) \
                  + string.rjust("size MiB", 17) \
                  + "\n")
                sys.stdout.write(("-" * 60) + "\n")
                for volume in self._server._volumes.itervalues():
                    sys.stdout.write( \
                      string.ljust(volume.get_name(), 17) \
                      + string.rjust(str(volume.get_size_MiB()), 17) \
                      + "\n")
            elif cmd == "list-assignments":
                sys.stdout.write( \
                  string.ljust("Node name", 17) \
                  + string.ljust("Volume name", 17) \
                  + "\n")
                sys.stdout.write(("-" * 60) + "\n")
                for node in self._server._nodes.itervalues():
                    node_name = node.get_name()
                    for assg in node._assignments.itervalues():
                        vol_name = assg.get_volume().get_name()
                        sys.stdout.write( \
                          string.ljust(node_name, 17) \
                          + string.ljust(vol_name, 17) \
                          + "\n")
                        # print the node name in the first line only
                        node_name = ""
            else:
                sys.stderr.write("No such debug command: " + cmd + "\n")
        except Exception:
            sys.stderr.write("Caught exception:\n" \
              + traceback.format_exc() + "\n")
        return DM_SUCCESS
