#!/usr/bin/python

import sys
import os
import gobject
import subprocess
import fcntl

from drbdmanage.dbusserver import *
from drbdmanage.exceptions import *
from drbdmanage.drbd.drbdcore import *
from drbdmanage.drbd.persistence import *
from drbdmanage.storage.storagecore import *
from drbdmanage.conf.conffile import *

__author__="raltnoeder"
__date__ ="$Sep 12, 2013 5:09:49 PM$"


def traceit(frame, event, arg):
    if event == "line":
        lineno = frame.f_lineno
        print frame.f_code.co_filename, ":", "line", lineno
    return traceit

#sys.settrace(traceit)

class DrbdManageServer(object):
    CONFFILE = "/etc/drbdmanaged.conf"
    EVT_UTIL = "/usr/local/sbin/drbdsetup"
    
    EVT_TYPE_CHANGE = "change"
    EVT_SRC_CON     = "connection"
    EVT_SRC_RES     = "resource"
    EVT_ARG_NAME    = "name"
    EVT_ARG_ROLE    = "role"
    
    EVT_ROLE_PRIMARY   = "Primary"
    EVT_ROLE_SECONDARY = "Secondary"
    DRBDCTRL_RES_NAME  = "drbdctrl"
    
    KEY_PLUGIN_NAME  = "storage-plugin"
    KEY_MAX_NODE_ID  = "max-node-id"
    KEY_MIN_MINOR_NR = "min-minor-nr"
    KEY_MIN_PORT_NR  = "min-port-nr"
    KEY_MAX_PORT_NR  = "max-port-nr"
    
    KEY_CMD_UP         = "cmd-up"
    KEY_CMD_DOWN       = "cmd-down"
    KEY_CMD_ATTACH     = "cmd-attach"
    KEY_CMD_DETACH     = "cmd-detach"
    KEY_CMD_CONNECT    = "cmd-connect"
    KEY_CMD_DISCONNECT = "cmd-disconnect"
    KEY_CMD_CREATEMD   = "cmd-create-md"
    KEY_CMD_ADJUST     = "cmd-adjust"
    KEY_DRBDADM_PATH   = "drbdadm-path"
    KEY_DRBD_CONFPATH  = "drbd-conf-path"
    
    # defaults
    CONF_DEFAULTS = {
      KEY_PLUGIN_NAME  : "drbdmanage.storage.lvm.LVM",
      KEY_MAX_NODE_ID  :   31,
      KEY_MIN_MINOR_NR :  100,
      KEY_MIN_PORT_NR  : 7000,
      KEY_MAX_PORT_NR  : 7999,
      KEY_CMD_UP         : "dm-up",
      KEY_CMD_DOWN       : "dm-down",
      KEY_CMD_ATTACH     : "dm-attach",
      KEY_CMD_DETACH     : "dm-detach",
      KEY_CMD_CONNECT    : "dm-connect",
      KEY_CMD_DISCONNECT : "dm-disconnect",
      KEY_CMD_CREATEMD   : "dm-create-md",
      KEY_CMD_ADJUST     : "dm-adjust",
      KEY_DRBDADM_PATH   : "/usr/local/sbin",
      KEY_DRBD_CONFPATH  : "/etc/drbd.d"
    }
    
    # BlockDevice manager
    _bd_mgr    = None
    # Configuration objects maps
    _nodes     = None
    _resources = None
    # Events log pipe
    _evt_file  = None
    # Subprocess handle for the events log source
    _proc_evt  = None
    # Reader for the events log
    _reader    = None
    # Event handler for incoming data
    _evt_in_h  = None
    # Event handler for the hangup event on the subprocess pipe
    _evt_hup_h = None
    
    # The name of the node this server is running on
    _instance_node_name = None
    
    # The hash of the currently loaded configuration
    _conf_hash = None
    
    _DEBUG_max_ctr = 0
    
    
    def __init__(self):
        # DEBUG:
        if len(sys.argv) >= 2:
            self._instance_node_name = sys.argv[1]
        # end DEBUG
        self._nodes     = dict()
        self._resources = dict()
        self.load_server_conf()
        self._bd_mgr    = BlockDeviceManager(self._conf[self.KEY_PLUGIN_NAME])
        self._drbd_mgr  = DrbdManager(self)
        self.load_conf()
        self.init_events()


    def init_events(self):
        self._proc_evt = subprocess.Popen([self.EVT_UTIL, "events", "all"], 0,
          self.EVT_UTIL, stdout=subprocess.PIPE)
        self._evt_file = self._proc_evt.stdout
        fcntl.fcntl(self._evt_file.fileno(),
          fcntl.F_SETFL, fcntl.F_GETFL | os.O_NONBLOCK)
        self._reader = NioLineReader(self._evt_file)
        # detect readable data on the pipe
        self._evt_in_h = gobject.io_add_watch(self._evt_file.fileno(),
          gobject.IO_IN, self.drbd_event)
        # detect broken pipe
        self._evt_hup_h = gobject.io_add_watch(self._evt_file.fileno(),
          gobject.IO_HUP, self.restart_events)
    
    
    def restart_events(self, fd, condition):
        # unregister any existing event handlers for the events log
        if self._evt_in_h is not None:
            gobject.source_remove(self._evt_in_h)
        self.init_events()
        self.load_conf()
        self._drbd_mgr.perform_changes()
        # Unregister this event handler
        return False
    
    
    def drbd_event(self, fd, condition):
        changed = False
        while True:
            line = self._reader.readline()
            if line is None:
                break
            else:
                if line.endswith("\n"):
                    line = line[:len(line) - 1]
                sys.stderr.write("%sDEBUG: drbd_event() (%s%s%s)%s\n"
                  % (COLOR_RED, COLOR_NONE, line, COLOR_RED, COLOR_NONE))
                sys.stderr.flush();
                if not changed:
                    event_type   = get_event_type(line)
                    event_source = get_event_source(line)
                    if event_type is not None and event_source is not None:
                        # If the configuration resource changes to "Secondary"
                        # role on a connected node, the configuration may have
                        # changed
                        if event_type == self.EVT_TYPE_CHANGE and \
                          event_source == self.EVT_SRC_CON:
                            event_res  = get_event_arg(line, self.EVT_ARG_NAME)
                            event_role = get_event_arg(line, self.EVT_ARG_ROLE)
                            if event_res == self.DRBDCTRL_RES_NAME and \
                              event_role == self.EVT_ROLE_SECONDARY:
                                changed = True
        if changed:
            self._drbd_mgr.run()
        # True = GMainLoop shall not unregister this event handler
        return True
    
    
    def load_server_conf(self):
        file = None
        try:
            file = open(self.CONFFILE, "r")
            conffile = ConfFile(file)
            conf_loaded = conffile.get_conf()
            if conf_loaded is not None:
                self._conf = (
                  ConfFile.conf_defaults_merge(self.CONF_DEFAULTS, conf_loaded)
                  )
            else:
                self._conf = self.CONF_DEFAULTS
        except IOError as io_err:
            sys.stderr.write("Warning: Cannot open drbdmanage configuration "
              "file %s\n" % (self.CONFFILE))
        finally:
            if file is not None:
                file.close()
    
    
    def get_conf_value(self, key):
        return self._conf.get(key)
    
    
    def get_drbd_mgr(self):
        return self._drbd_mgr
    
    
    def get_bd_mgr(self):
        return self._bd_mgr
    
    
    def iterate_nodes(self):
        return self._nodes.itervalues()
    

    def iterate_resources(self):
        return self._resources.itervalues()
    
    
    def get_node(self, name):
        node = None
        try:
            node = self._nodes[name]
        except KeyError:
            pass
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        return node
    
    
    def get_resource(self, name):
        resource = None
        try:
           resource = self._resources.get(name)
        except Exception as exvc:
            DrbdManageServer.catch_internal_error(exc)
        return resource
    
    
    def get_volume(self, name, id):
        volume = None
        try:
            resource = self._resources.get(name)
            if resource is not None:
                volume = resource.get_volume(id)
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
        return volume
        
    
    # Get the node this server is running on
    def get_instance_node(self):
        node = None
        try:
            node = self._nodes[self._instance_node_name]
        except KeyError:
            pass
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        return node
    
    
    # Get the name of the node this server is running on
    def get_instance_node_name(self):
        return server._instance_node_name
    
    
    def create_node(self, name, ip, af):
        """
        Registers a DRBD cluster node
        """
        rc      = DM_EPERSIST
        persist = None
        node    = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                if self._nodes.get(name) is not None:
                    rc = DM_EEXIST
                else:
                    try:
                        node = DrbdNode(name, ip, af)
                        self._nodes[node.get_name()] = node
                        self.save_conf_data(persist)
                        rc = DM_SUCCESS
                    except InvalidNameException:
                        rc = DM_ENAME
        except PersistenceException:
            pass
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            rc = DM_DEBUG
        finally:
            self.end_modify_conf(persist)
        return rc
    
    
    def remove_node(self, name, force):
        """
        Marks a node for removal from the DRBD cluster
        * Orders the node to undeploy all volumes
        * Orders all other nodes to disconnect from the node
        """
        rc      = DM_EPERSIST
        persist = None
        node    = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node = self._nodes[name]
                if (not force) and node.has_assignments():
                    for assignment in node.iterate_assignments():
                        assignment.undeploy()
                        resource = assignment.get_resource()
                        for peer_assg in resource.iterate_assignments():
                            peer_assg.update_connections()
                    node.remove()
                    self._drbd_mgr.perform_changes()
                else:
                    # drop all associated assignments
                    for assignment in node.iterate_assignments():
                        resource = assignment.get_resource()
                        resource.remove_assignment(assignment)
                        # tell the remaining nodes that have this resource to
                        # drop the connection to the deleted node
                        for peer_assg in resource.iterate_assignments():
                            peer_assg.update_connections()
                    del self._nodes[name]
                self.save_conf_data(persist)
                rc = DM_SUCCESS
        except KeyError:
            rc = DM_ENOENT
        except PersistenceException:
            pass
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            rc = DM_DEBUG
        finally:
            self.end_modify_conf(persist)
        return rc
    
    
    def create_resource(self, name, port):
        """
        Registers a new resource that can be deployed to DRBD cluster nodes
        """
        rc = DM_EPERSIST
        resource = None
        persist  = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                resource = self._resources.get(name)
                if resource is not None:
                    rc = DM_EEXIST
                else:
                    if port == DrbdResource.PORT_NR_AUTO:
                        port = self.get_free_port_nr()
                    if port < 1 or port > 65535:
                        rc = DM_EPORT
                    else:
                        resource = DrbdResource(name, port)
                        self._resources[resource.get_name()] = resource
                        self.save_conf_data(persist)
                        rc = DM_SUCCESS
        except PersistenceException:
            pass
        except InvalidNameException:
            rc = DM_ENAME
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            rc = DM_DEBUG
        finally:
            self.end_modify_conf(persist)
        return rc
    
    
    def remove_resource(self, name, force):
        """
        Marks a resource for removal from the DRBD cluster
        * Orders all nodes to undeploy all volume of this resource
        """
        rc = DM_EPERSIST
        persist  = None
        resource = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                resource = self._resources[name]
                if (not force) and resource.has_assignments():
                    for assg in resource.iterate_assignments():
                        sys.stderr.write("DEBUG: remove-resource: undeploying "
                          "assignment %s:%s\n" % (assg.get_node().get_name(),
                          assg.get_resource().get_name()))
                        assg.undeploy()
                    resource.remove()
                    self._drbd_mgr.perform_changes()
                else:
                    for assg in resource.iterate_assignments():
                        node = assg.get_node()
                        node.remove_assignment(assg)
                    del self._resources[resource.get_name()]
                self.save_conf_data(persist)
                rc = DM_SUCCESS
        except KeyError:
            rc = DM_ENOENT
        except PersistenceException:
            pass
        except Exception as exc:
                DrbdManageServer.catch_internal_error(exc)
                rc = DM_DEBUG
        finally:
            self.end_modify_conf(persist)
        return rc
    
    
    def create_volume(self, name, size, minor):
        """
        Adds a volume to a resource
        """
        rc      = DM_EPERSIST
        volume  = None
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                resource = self._resources.get(name)
                if resource is None:
                    rc = DM_ENOENT
                else:
                    try:
                        vol_id = self.get_free_volume_id(resource)
                        if minor == MinorNr.MINOR_NR_AUTO:
                            minor = self.get_free_minor_nr()
                            if minor == MinorNr.MINOR_NR_ERROR:
                                raise InvalidMinorNrException
                        if vol_id == -1:
                            rc = DM_EVOLID
                        else:
                            volume = DrbdVolume(vol_id, size, MinorNr(minor))
                            resource.add_volume(volume)
                            for assg in resource.iterate_assignments():
                                assg.update_volume_states()
                                vol_st = assg.get_volume_state(volume.get_id())
                                if vol_st is not None:
                                    vol_st.deploy()
                                    vol_st.attach()
                            self._drbd_mgr.perform_changes()
                            self.save_conf_data(persist)
                            rc = DM_SUCCESS
                    except InvalidNameException:
                        rc = DM_ENAME
                    except InvalidMinorNrException:
                        rc = DM_EMINOR
                    except VolSizeRangeException:
                        rc = DM_EVOLSZ
        except PersistenceException:
            pass
        except Exception as exc:
                DrbdManageServer.catch_internal_error(exc)
                rc = DM_DEBUG
        finally:
            self.end_modify_conf(persist)
        return rc
    
    
    def remove_volume(self, name, id, force):
        """
        Marks a volume for removal from the DRBD cluster
        * Orders all nodes to undeploy the volume
        """
        rc      = DM_EPERSIST
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                resource = self._resources[name]
                volume   = resource.get_volume(id)
                if volume is None:
                    raise KeyError
                else:
                    if (not force) and resource.has_assignments():
                        for assg in resource.iterate_assignments():
                            peer_vol_st = assg.get_volume_state(id)
                            if peer_vol_st is not None:
                                peer_vol_st.undeploy()
                        volume.remove()
                        self._drbd_mgr.perform_changes()
                    else:
                        resource.remove_volume(id)
                        for assg in resource.iterate_assignments():
                            assg.remove_volume_state(id)                    
                    self.save_conf_data(persist)
                    rc = DM_SUCCESS
        except KeyError:
            rc = DM_ENOENT
        except PersistenceException:
            pass
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            rc = DM_DEBUG
        finally:
            self.end_modify_conf(persist)
        return rc
    
    
    def assign(self, node_name, resource_name, cstate, tstate):
        """
        Assigns a resource to a node
        * Orders all participating nodes to deploy all volumes of
          resource
        """
        rc      = DM_EPERSIST
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node     = self._nodes.get(node_name)
                resource = self._resources.get(resource_name)
                if node is None or resource is None:
                    rc = DM_ENOENT
                else:
                    assignment = node.get_assignment(resource.get_name())
                    if assignment is not None:
                        rc = DM_EEXIST
                    else:
                        if ((tstate & Assignment.FLAG_DISKLESS) != 0
                          and (tstate & Assignment.FLAG_OVERWRITE) != 0):
                            rc = DM_EINVAL
                        elif ((tstate & Assignment.FLAG_DISCARD) != 0
                          and (tstate & Assignment.FLAG_OVERWRITE) != 0):
                            rc = DM_EINVAL
                        else:
                            rc = self._assign(node, resource, cstate, tstate)
                            if rc == DM_SUCCESS:
                                self._drbd_mgr.perform_changes()
                                self.save_conf_data(persist)
        except PersistenceException:
            pass
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            rc = DM_DEBUG
        finally:
            self.end_modify_conf(persist)
        return rc
    
    
    def unassign(self, node_name, resource_name, force):
        """
        Removes the assignment of a resource to a node
        * Orders the node to undeploy all volumes of the resource
        """
        rc      = DM_EPERSIST
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node   = self._nodes.get(node_name)
                resource = self._resources.get(resource_name)
                if node is None or resource is None:
                    rc = DM_ENOENT
                else:
                    assignment = node.get_assignment(resource.get_name())
                    if assignment is None:
                        rc = DM_ENOENT
                    else:
                        rc = self._unassign(assignment, force)
                        if rc == DM_SUCCESS:
                            self._drbd_mgr.perform_changes()
                            self.save_conf_data(persist)
        except PersistenceException:
            pass
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            rc = DM_DEBUG
        finally:
            self.end_modify_conf(persist)
        return rc
    
    
    def _assign(self, node, resource, cstate, tstate):
        """
        Implementation - see assign()
        """
        rc = DM_DEBUG
        try:
            node_id = self.get_free_node_id(resource)
            if node_id == -1:
                # no free node ids
                rc = DM_ENODEID
            else:
                # The block device is set upon allocation of the backend
                # storage area on the target node
                assignment = Assignment(node, resource, node_id,
                  cstate, tstate)
                for vol_state in assignment.iterate_volume_states():
                    vol_state.deploy()
                    if tstate & Assignment.FLAG_DISKLESS == 0:
                        vol_state.attach()
                node.add_assignment(assignment)
                resource.add_assignment(assignment)
                for assignment in resource.iterate_assignments():
                    if assignment.is_deployed():
                        assignment.update_connections()
                rc = DM_SUCCESS
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
        return rc
    
    
    def _unassign(self, assignment, force):
        """
        Implementation - see unassign()
        """
        try:
            node     = assignment.get_node()
            resource = assignment.get_resource()
            if (not force) and assignment.is_deployed():
                assignment.disconnect()
                assignment.undeploy()
            else:
                assignment.remove()
            for assignment in node.iterate_assignments():
                if assignment.get_node() != node \
                  and assignment.is_deployed():
                    assignment.update_connections()
            self.cleanup()
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        return DM_SUCCESS
    
    
    def modify_state(self, node_name, resource_name,
      cstate_clear_mask, cstate_set_mask, tstate_clear_mask, tstate_set_mask):
        """
        Modifies the tstate (target state) of an assignment
        """
        rc      = DM_EPERSIST
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node = self._nodes.get(node_name)
                if node is None:
                    rc = DM_ENOENT
                else:
                    assg = node.get_assignment(resource_name)
                    if assg is None:
                        rc = DM_ENOENT
                    else:
                        assg.clear_cstate_flags(cstate_clear_mask)
                        assg.set_cstate_flags(cstate_set_mask)
                        assg.clear_tstate_flags(tstate_clear_mask)
                        assg.set_tstate_flags(tstate_set_mask)
                        self._drbd_mgr.perform_changes()
                        self.save_conf_data(persist)
                        rc = DM_SUCCESS
        except PersistenceException:
            pass
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            rc = DM_DEBUG
        finally:
            self.end_modify_conf(persist)
        return rc
    
    
    def connect(self, node_name, resource_name, reconnect):
        """
        Set the CONNECT or RECONNECT flag on a resource's target state
        """
        rc = DM_EPERSIST
        node     = None
        resource = None
        persist  = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node     = self._nodes.get(node_name)
                resource = self._resources.get(resource_name)
                if node is None or resource is None:
                    rc = DM_ENOENT
                else:
                    assignment = node.get_assignment(resource.get_name())
                    if assignment is None:
                        rc = DM_ENOENT
                    else:
                        if reconnect:
                            assignment.reconnect()
                        else:
                            assignment.connect()
                        self._drbd_mgr.perform_changes()
                        self.save_conf_data(persist)
                        rc = DM_SUCCESS
        except PersistenceException:
            pass
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            rc = DM_DEBUG
        finally:
            self.end_modify_conf(persist)
        return rc
    
    
    def disconnect(self, node_name, resource_name):
        """
        Clear the CONNECT flag on a resource's target state
        """
        rc = DM_EPERSIST
        node     = None
        resource = None
        persist  = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node     = self._nodes.get(node_name)
                resource = self._resources.get(resource_name)
                if node is None or resource is None:
                    rc = DM_ENOENT
                else:
                    assignment = node.get_assignment(resource.get_name())
                    if assignment is None:
                        rc = DM_ENOENT
                    else:
                        assignment.disconnect()
                        self._drbd_mgr.perform_changes()
                        self.save_conf_data(persist)
                        rc = DM_SUCCESS
        except PersistenceException:
            pass
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            rc = DM_DEBUG
        finally:
            self.end_modify_conf(persist)
        return rc
    
    
    def attach(self, node_name, resource_name, volume_id):
        """
        Set the ATTACH flag on a volume's target state
        """
        rc = DM_EPERSIST
        node      = None
        resource  = None
        vol_state = None
        persist   = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node     = self._nodes.get(node_name)
                resource = self._resources.get(resource_name)
                if node is None or resource is None:
                    rc = DM_ENOENT
                else:
                    assignment = node.get_assignment(resource.get_name())
                    if assignment is None:
                        rc = DM_ENOENT
                    else:
                        vol_state = assignment.get_volume_state(volume_id)
                        if vol_state is None:
                            rc = DM_ENOENT
                        else:
                            vol_state.attach()
                            self._drbd_mgr.perform_changes()
                            self.save_conf_data(persist)
                            rc = DM_SUCCESS
        except PersistenceException:
            pass
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            rc = DM_DEBUG
        finally:
            self.end_modify_conf(persist)
        return rc
    
    
    def detach(self, node_name, resource_name, volume_id):
        """
        Clear the ATTACH flag on a volume's target state
        """
        rc = DM_EPERSIST
        node      = None
        resource  = None
        vol_state = None
        persist   = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node     = self._nodes.get(node_name)
                resource = self._resources.get(resource_name)
                if node is None or resource is None:
                    rc = DM_ENOENT
                else:
                    assignment = node.get_assignment(resource.get_name())
                    if assignment is None:
                        rc = DM_ENOENT
                    else:
                        vol_state = assignment.get_volume_state(volume_id)
                        if vol_state is None:
                            rc = DM_ENOENT
                        else:
                            vol_state.detach()
                            self._drbd_mgr.perform_changes()
                            self.save_conf_data(persist)
                            rc = DM_SUCCESS
        except PersistenceException:
            pass
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            rc = DM_DEBUG
        finally:
            self.end_modify_conf(persist)
        return rc
    
    
    """
    Remove entries of undeployed nodes, resources, volumes or their
    supporting data structures (volume state and assignment entries)
    """
    def cleanup(self):
        try:
            removable = []
            # delete assignments that have been undeployed
            for node in self._nodes.itervalues():
                for assignment in node.iterate_assignments():
                    tstate = assignment.get_tstate()
                    cstate = assignment.get_cstate()
                    if (cstate & Assignment.FLAG_DEPLOY) == 0 \
                      and (tstate & Assignment.FLAG_DEPLOY) == 0:
                        removable.append(assignment)
            for assignment in removable:
                assignment.remove()
            # delete nodes that are marked for removal and that do not
            # have assignments anymore
            removable = []
            for node in self._nodes.itervalues():
                nodestate = node.get_state()
                if (nodestate & DrbdNode.FLAG_REMOVE) != 0:
                    if not node.has_assignments():
                        removable.append(node)
            for node in removable:
                del self._nodes[node.get_name()]
            # delete volume assignments that are marked for removal
            # and that have been undeployed
            for resource in self._resources.itervalues():
                for assg in resource.iterate_assignments():
                    removable = []
                    for vol_state in assg.iterate_volume_states():
                        vol_cstate = vol_state.get_cstate()
                        vol_tstate = vol_state.get_tstate()
                        if (vol_cstate & DrbdVolumeState.FLAG_DEPLOY == 0) \
                          and (vol_tstate & DrbdVolumeState.FLAG_DEPLOY == 0):
                            removable.append(vol_state)
                    for vol_state in removable:
                        assg.remove_volume_state(vol_state.get_id())
            # delete volumes that are marked for removal and that are not
            # deployed on any node
            for resource in self._resources.itervalues():
                volumes = dict()
                # collect volumes marked for removal
                for volume in resource.iterate_volumes():
                    if volume.get_state() & DrbdVolume.FLAG_REMOVE != 0:
                        volumes[volume.get_id()] = volume;
                for assg in resource.iterate_assignments():
                    removable = []
                    for vol_state in assg.iterate_volume_states():
                        volume = volumes.get(vol_state.get_id())
                        if volume is not None:
                            if vol_state.get_cstate() \
                              & DrbdVolumeState.FLAG_DEPLOY != 0:
                                # delete the volume from the removal list
                                del volumes[vol_state.get_id()]
                            else:
                                removable.append(vol_state)
                        for vol_state in removable:
                            assg.remove_volume_state(vol_state.get_id())
                for id in volumes.iterkeys():
                    resource.remove_volume(id)
            # delete resources that are marked for removal and that do not
            # have assignments any more
            removable = []
            for resource in self._resources.itervalues():
                res_state = resource.get_state()
                if (res_state & DrbdResource.FLAG_REMOVE) != 0:
                    if not resource.has_assignments():
                        removable.append(resource)
            for resource in removable:
                del self._resources[resource.get_name()]
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        return DM_SUCCESS
    
    
    def node_list(self):
        try:
            node_list = []
            for node in self._nodes.itervalues():
                properties = DrbdNodeView.get_properties(node)
                node_list.append(properties)
            return node_list
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
        return None
    
    
    def resource_list(self):
        try:
            resource_list = []
            for resource in self._resources.itervalues():
                properties = DrbdResourceView.get_properties(resource)
                resource_list.append(properties)
            return resource_list
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
        return None
    
    
    def assignment_list(self):
        try:
            assignment_list = []
            for node in self._nodes.itervalues():
                for assignment in node.iterate_assignments():
                    properties = AssignmentView.get_properties(assignment)
                    assignment_list.append(properties)
            return assignment_list
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
        return None
    
    
    def save_conf(self):
        rc = DM_EPERSIST
        persist  = None
        try:
            persist = persistence_impl()
            if persist.open(True):
                self.save_conf_data(persist)
                rc = DM_SUCCESS
        except PersistenceException:
            pass
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        finally:
            self.end_modify_conf(persist)
        return rc
    
    
    def load_conf(self):
        rc = DM_EPERSIST
        persist  = None
        try:
            persist = persistence_impl()
            if persist.open(False):
                self.load_conf_data(persist)
                persist.close()
                rc = DM_SUCCESS
        except PersistenceException:
            pass
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        finally:
            self.end_modify_conf(persist)
        return rc
    
    
    def load_conf_data(self, persist):
        hash_obj = None
        persist.load(self._nodes, self._resources)
        self._conf_hash = persist.get_stored_hash()
    
    
    def save_conf_data(self, persist):
        hash_obj = None
        persist.save(self._nodes, self._resources)
        hash_obj = persist.get_hash_obj()
        if hash_obj is not None:
            self._conf_hash = hash_obj.get_hash()
    
    
    def open_conf(self):
        """
        Opens the configuration on persistent storage for reading
        This function is only there because drbdcore cannot import anything
        from persistence, so the code for creating a PersistenceImpl object
        has to be somwhere else.
        Returns a PersistenceImpl object on success, or None if the operation
        fails due to errors in the persistence layer
        """
        ret_persist = None
        persist     = None
        try:
            persist = persistence_impl()
            if persist.open(False):
                ret_persist = persist
        except Exception as exc:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print exc_type
            print exc_obj
            print exc_tb
            persist.close()
        return ret_persist
    
    
    def begin_modify_conf(self):
        """
        Opens the configuration on persistent storage for writing,
        implicitly locking out all other nodes, and reloads the configuration
        if it has changed.
        Returns a PersistenceImpl object on success, or None if the operation
        fails due to errors in the persistence layer
        """
        ret_persist = None
        persist     = None
        try:
            persist = persistence_impl()
            if persist.open(True):
                if not self.hashes_match(persist.get_stored_hash()):
                    self.load_conf_data(persist)
                ret_persist = persist
        except Exception as exc:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print exc_type
            print exc_obj
            print exc_tb
            persist.close()
        return ret_persist
    
    
    def end_modify_conf(self, persist):
        """
        Closes the configuration on persistent storage.
        """
        try:
            if persist is not None:
                persist.close()
        except Exception:
            pass
    
    
    # TODO: more precise error handling
    def export_conf(self, res_name):
        rc = DM_SUCCESS
        node = self.get_instance_node()
        if node is not None:
            if res_name is None:
                res_name = ""
            if len(res_name) > 0 and res_name != "*":
                assg = node.get_assignment(res_name)
                if assg is not None:
                    if self._export_assignment_conf(assg) != 0:
                        rc = DM_DEBUG
                else:
                    rc = DM_ENOENT
            else:
                for assg in node.iterate_assignments():
                    if self._export_assignment_conf(assg) != 0:
                        rc = DM_DEBUG
        return rc
    
    
    def _export_assignment_conf(self, assignment):
        rc = 0
        resource = assignment.get_resource()
        file_path = self._conf[self.KEY_DRBD_CONFPATH]
        if not file_path.endswith("/"):
            file_path += "/"
        file_path += "drbdmanage_" + resource.get_name() + ".res"
        assg_conf = None
        try:
            assg_conf = open(file_path, "w")
            writer    = DrbdAdmConf()
            writer.write(assg_conf, assignment, False)
        except IOError as ioerr:
            sys.stderr.write("Cannot write configuration file '%s'\n"
              % (file_path))
            rc = 1
        finally:
            if assg_conf is not None:
                assg_conf.close()
        return rc
    
    
    def get_conf_hash(self):
        return self._conf_hash
    
    
    def hashes_match(self, hash):
        if self._conf_hash is not None and hash is not None:
            if self._conf_hash == hash:
                return True
        return False
    
    
    def reconfigure(self):
        rc      = DM_EPERSIST
        try:
            self.load_server_conf()
            rc = self.load_conf()
            self._drbd_mgr.reconfigure()
            self._bd_mgr.reconfigure()
        except PersistenceException:
            pass
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            rc = DM_DEBUG
        return rc
    
    
    def shutdown(self):
        exit(0)
    
    
    def get_free_minor_nr(self):
        try:
            min_nr = int(self._conf[self.KEY_MIN_MINOR_NR])
            
            minor_list = []
            for resource in self._resources.itervalues():
                for vol in resource.iterate_volumes():
                    minor_obj = vol.get_minor()
                    nr = minor_obj.get_value()
                    if nr >= min_nr and nr <= MinorNr.MINOR_NR_MAX:
                        minor_list.append(nr)
            minor_nr = get_free_number(min_nr, MinorNr.MINOR_NR_MAX,
              minor_list)
            if minor_nr == -1:
                raise ValueError
        except ValueError:
            minor_nr = MinorNr.MINOR_NR_ERROR
        return minor_nr
    
    
    def get_free_port_nr(self):
        try:
            min_nr    = int(self._conf[self.KEY_MIN_PORT_NR])
            max_nr    = int(self._conf[self.KEY_MAX_PORT_NR])
            
            port_list = []
            for resource in self._resources.itervalues():
                nr = resource.get_port()
                if nr >= min_nr and nr <= max_nr:
                    port_list.append(nr)
            port = get_free_number(min_nr, max_nr, port_list)
            if port == -1:
                raise ValueError
        except ValueError:
            port = DrbdResource.PORT_NR_ERROR
        return port
    
    
    def get_free_node_id(self, resource):
        try:
            max_node_id = int(self._conf[self.KEY_MAX_NODE_ID])
            
            id_list = []
            for assg in resource.iterate_assignments():
                id = assg.get_node_id()
                if id >= 0 and id <= int(max_node_id):
                    id_list.append(id)
            node_id = get_free_number(0, int(max_node_id),
              id_list)
            if node_id == -1:
                raise ValueError
        except ValueError:
            node_id = Assignment.NODE_ID_ERROR
        return node_id
    
    
    def get_free_volume_id(self, resource):
        id_list = []
        for vol in resource.iterate_volumes():
            id = vol.get_id()
            if id >= 0 and id <= DrbdResource.MAX_RES_VOLS:
                id_list.append(id)
        vol_id = get_free_number(0, DrbdResource.MAX_RES_VOLS, id_list)
        return vol_id
    
    
    @staticmethod
    def catch_internal_error(exc):
        try:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            sys.stderr.write("Internal error: Unexpected exception: %s\n" 
              % (str(exc)))
        except Exception:
            pass
