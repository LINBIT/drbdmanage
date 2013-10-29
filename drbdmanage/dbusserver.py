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
import drbdmanage.conf.conffile

__author__="raltnoeder"
__date__ ="$Sep 12, 2013 4:43:41 PM$"


class DBusServer(dbus.service.Object):
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
        gobject.MainLoop().run()
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="sss", out_signature="i")
    def create_node(self, name, ip, af):
        try:
            if af == drbdmanage.drbd.drbdcore.DrbdNode.AF_IPV6_LABEL:
                af_n = drbdmanage.drbd.drbdcore.DrbdNode.AF_IPV6
            else:
                af_n = drbdmanage.drbd.drbdcore.DrbdNode.AF_IPV4
            return self._server.create_node(name, ip, af_n)
        except Exception as exc:
            # FIXME
            sys.stderr.write("Oops, " + str(exc))
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="sb", out_signature="i")
    def remove_node(self, name, force):
        return self._server.remove_node(name, force)
        
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="si", out_signature="i")
    def create_resource(self, name, port):
        return self._server.create_resource(name, port)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="sb", out_signature="i")
    def remove_resource(self, name, force):
        return self._server.remove_resource(name, force)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="sxi", out_signature="i")
    def create_volume(self, name, size_MiB, minor):
        return self._server.create_volume(name, size_MiB, minor)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="sib", out_signature="i")
    def remove_volume(self, name, id, force):
        return self._server.remove_volume(name, id, force)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="ssb", out_signature="i")
    def connect(self, node_name, resource_name, reconnect):
        return self._server.connect(node_name, resource_name, reconnect)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="ss", out_signature="i")
    def disconnect(self, node_name, resource_name):
        return self._server.disconnect(node_name, resource_name)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="ssxxxx", out_signature="i")
    def modify_state(self, node_name, resource_name,
      cstate_clear_mask, cstate_set_mask,
      tstate_clear_mask, tstate_set_mask):
        return self._server.modify_state(node_name, resource_name,
          cstate_clear_mask, cstate_set_mask,
          tstate_clear_mask, tstate_set_mask)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="ssi", out_signature="i")
    def attach(self, node_name, resource_name, volume_id):
        return self._server.attach(node_name, resource_name, volume_id)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="ssi", out_signature="i")
    def detach(self, node_name, resource_name, volume_id):
        return self._server.detach(node_name, resource_name, volume_id)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="ssii", out_signature="i")
    def assign(self, node_name, resource_name, cstate, tstate):
        tstate = tstate | Assignment.FLAG_DEPLOY
        return self._server.assign(node_name, resource_name, cstate, tstate)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="ssb", out_signature="i")
    def unassign(self, node_name, volume_name, force):
        return self._server.unassign(node_name, volume_name, force)
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="", out_signature="a(sssxxx)")
    def node_list(self):
        return self._server.node_list()
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="", out_signature="a(sssxa(ixix))")
    def resource_list(self):
        return self._server.resource_list()
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="", out_signature="a(ssixxa(isxx))")
    def assignment_list(self):
        return self._server.assignment_list()
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="s", out_signature="i")
    def export_conf(self, resource):
        return self._server.export_conf(resource)
    
    
    # DEBUG
    """
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="", out_signature="a(sa(ssaas))")
    def assignment_list(self):
        try:
            sys.stdout.write("DEBUG #1\n")
            vol_aa = [ "0", "10240", "103" ]
            vol_ab = [ "1", "8192", "107" ]
            vol_bb = [ "0", "13500", "104" ]
            vols_a = [ vol_aa, vol_ab ]
            vols_b = [ vol_bb ]
            sys.stdout.write("DEBUG #2\n")
            res_a = [ "res01", "0", vols_a ]
            res_b = [ "res02", "1", vols_b ]
            sys.stdout.write("DEBUG #3\n")
            res_node_a = [ res_a, res_b ]
            res_node_b = [ res_b ]
            sys.stdout.write("DEBUG #4\n")
            node_a = [ "node01", res_node_a ]
            node_b = [ "node02", res_node_b ]
            sys.stdout.write("DEBUG #5\n")
            nodes_arr = [ node_a, node_b ]
            nodes = dbus.Array(nodes_arr)
            sys.stdout.write("DEBUG #6\n")
            # this works
        except Exception as exc:
            print exc
        return dbus.Struct(nodes)
    """
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="", out_signature="i")
    def reconfigure(self):
        return self._server.reconfigure()

    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="", out_signature="i")
    def load_conf(self):
        return self._server.load_conf()
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="", out_signature="i")
    def save_conf(self):
        return self._server.save_conf()
    
    
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="", out_signature="")
    def shutdown(self):
        self._server.shutdown()
    
    
    # DEBUG
    @dbus.service.method(DBUS_DRBDMANAGED,
      in_signature="s", out_signature="i")
    def debug_cmd(self, cmd):
        try:
            for node in self._server._nodes.itervalues():
                sys.stdout.write("Node(%s) af=%s ip=%s\n" %
                  (node.get_name(), node.get_af_label(), node.get_ip()))
            sys.stdout.write("\n")
            for resource in self._server.iterate_resources():
                sys.stdout.write("Resource(%s)\n" % (resource.get_name()))
                for volume in resource.iterate_volumes():
                    sys.stdout.write("  --(%d) size(%d) minor(%d)\n"
                      % (volume.get_id(), volume.get_size_MiB(),
                      volume.get_minor().get_value()))
            sys.stdout.write("\n")
            for node in self._server.iterate_nodes():
                sys.stdout.write("on %s:\n" % (node.get_name()))
                for assg in node.iterate_assignments():
                    resource = assg.get_resource()
                    sys.stdout.write("  resource %s:\n" % (resource.get_name()))
                    for vol_st in assg.iterate_volume_states():
                        id = vol_st.get_id()
                        cstate = vol_st.get_cstate()
                        tstate = vol_st.get_tstate()
                        if tstate & DrbdVolumeState.FLAG_DEPLOY != 0:
                            deploy = "deploy"
                        else:
                            deploy = "~deploy"
                        if tstate & DrbdVolumeState.FLAG_ATTACH != 0:
                            attach = "attach"
                        else:
                            attach = "~attach"
                        sys.stdout.write("    %d: %d (%s,%s)\n"
                          % (vol_st.get_id(),
                          vol_st.get_volume().get_size_MiB(), deploy, attach))
            for key in self._server._conf.iterkeys():
                sys.stdout.write("conf[%s] = (%s)\n" % (key,
                  self._server._conf[key]))
            """
            BEGIN Test DrbdAdmConf drbdadm config file writer
            """
            conffile = drbdmanage.conf.conffile.DrbdAdmConf()
            for resource in self._server.iterate_resources():
                for assg in resource.iterate_assignments():
                    conffile.write(sys.stdout, assg, True)
                    sys.stdout.write("\n")
                    # quick'n'dirty print the first assignment only
                    break
            """
            END Test DrbdAdmConf drbdadm config file writer
            """
            sys.stdout.write("--------------------\n\n")
        except Exception:
            sys.stderr.write("Caught exception:\n"
              + traceback.format_exc() + "\n")
        return DM_SUCCESS
