#!/usr/bin/python

import sys
import string
import traceback
import dbus
import dbus.service
import dbus.mainloop.glib
import gobject
import drbdmanage.drbd.drbdcore
from drbdmanage.drbd.drbdcore import Assignment
from drbdmanage.exceptions import *

# TODO: DEBUG: used for debug code only
from drbdmanage.drbd.persistence import *

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
    def create_node(self, name, ip, af):
        try:
            if af == drbdmanage.drbd.drbdcore.DrbdNode.AF_IPV6_LABEL:
                af_n = drbdmanage.drbd.drbdcore.DrbdNode.AF_IPV6
            else:
                af_n = drbdmanage.drbd.drbdcore.DrbdNode.AF_IPV4
            return self._server.create_node(name, ip, af_n)
        except Exception as exc:
            sys.stderr.write("Oops, " + str(exc))
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="sb", out_signature="i")
    def remove_node(self, name, force):
        return self._server.remove_node(name, force)
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="sxi", out_signature="i")
    def create_volume(self, name, size_MiB, minor):
        return self._server.create_volume(name, size_MiB, minor)
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="sb", out_signature="i")
    def remove_volume(self, name, force):
        return self._server.remove_volume(name, force)
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="ssas", out_signature="i")
    def assign(self, node_name, volume_name, state):
        tstate = 0
        for opt in state:
            if opt == "client":
                tstate = tstate | Assignment.FLAG_DISKLESS
            elif opt == "overwrite":
                tstate = tstate | Assignment.FLAG_OVERWRITE
            elif opt == "discard":
                tstate = tstate | Assignment.FLAG_DISCARD
            else:
                return DM_EINVAL
        tstate = tstate | Assignment.FLAG_DEPLOY
        if tstate & Assignment.FLAG_DISKLESS == 0:
            tstate = tstate | Assignment.FLAG_ATTACH
        return self._server.assign(node_name, volume_name, tstate)
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="ssb", out_signature="i")
    def unassign(self, node_name, volume_name, force):
        return self._server.unassign(node_name, volume_name, force)
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="", out_signature="aas")
    def node_list(self):
        return self._server.node_list()
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="", out_signature="aas")
    def volume_list(self):
        return self._server.volume_list()
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="", out_signature="aas")
    def assignment_list(self):
        return self._server.assignment_list()
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="", out_signature="i")
    def reconfigure(self):
        return self._server.reconfigure()
    
    @dbus.service.method(DBUS_DRBDMANAGED, \
      in_signature="", out_signature="")
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
                  chr(0x1b) + "[0;93m"
                  + string.ljust("Node name", 17) \
                  + string.ljust("type", 5) \
                  + string.ljust("IP address", 16) \
                  + chr(0x1b) + "[0m\n")
                sys.stdout.write(("-" * 60) + "\n")
                for node in self._server._nodes.itervalues():
                    af = "unkn"
                    if node.get_af() \
                      == drbdmanage.drbd.drbdcore.DrbdNode.AF_IPV4:
                        af = "ipv4"
                    elif node.get_af() \
                      == drbdmanage.drbd.drbdcore.DrbdNode.AF_IPV6:
                        af = "ipv6"
                    sys.stdout.write( \
                      string.ljust(node.get_name(), 17) \
                      + string.ljust(af, 5) \
                      + string.ljust(node.get_ip(), 16) \
                      + "\n")
            elif cmd == "list-volumes":
                sys.stdout.write( \
                  chr(0x1b) + "[0;93m"
                  + string.ljust("Volume name", 17) \
                  + string.rjust("size MiB", 17) \
                  + chr(0x1b) + "[0m\n")
                sys.stdout.write(("-" * 60) + "\n")
                for volume in self._server._volumes.itervalues():
                    sys.stdout.write( \
                      string.ljust(volume.get_name(), 17) \
                      + string.rjust(str(volume.get_size_MiB()), 17) \
                      + "\n")
            elif cmd == "list-assignments":
                sys.stdout.write( \
                  chr(0x1b) + "[0;93m"
                  + string.ljust("Node name", 17) \
                  + string.ljust("Volume name", 17) \
                  + chr(0x1b) + "[0m\n")
                sys.stdout.write(("-" * 60) + "\n")
                for node in self._server._nodes.itervalues():
                    node_name = node.get_name()
                    for assg in node._assignments.itervalues():
                        vol_name = assg.get_volume().get_name()
                        sys.stdout.write( \
                          string.ljust(node_name, 17) \
                          + string.ljust(vol_name, 17))
                        if assg.is_deployed():
                            sys.stdout.write(" (deployed)")
                        sys.stdout.write(" " + str(assg.get_cstate()) + "\n")
                        # print the node name in the first line only
                        node_name = ""
            elif cmd == "safe":
                assignments = []
                for node in self._server._nodes.itervalues():
                    for assg in node.iterate_assignments():
                        assignments.append(assg)
                    pnode = DrbdNodePersistence(node)
                    pnode.safe()
                for volume in self._server._volumes.itervalues():
                    pvol = DrbdVolumePersistence(volume)
                    pvol.safe()
                for assg in assignments:
                    passg = AssignmentPersistence(assg)
                    passg.safe()
            else:
                sys.stderr.write("No such debug command: " + cmd + "\n")
            sys.stdout.write("\n")
        except Exception:
            sys.stderr.write("Caught exception:\n" \
              + traceback.format_exc() + "\n")
        return DM_SUCCESS
