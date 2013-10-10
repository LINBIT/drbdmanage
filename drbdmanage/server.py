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
    
    KEY_PLUGIN_NAME = "storage-plugin"
    
    # BlockDevice manager
    _bd_mgr    = None
    # Configuration objects maps
    _nodes     = None
    _volumes   = None
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
    
    # The name of the storage plugin to be loaded into the block device manager
    _plugin_name = None
    
    _DEBUG_max_ctr = 0
    
    
    def __init__(self):
        # DEBUG:
        if len(sys.argv) >= 2:
            self._instance_node_name = sys.argv[1]
        # end DEBUG
        self._nodes    = dict()
        self._volumes  = dict()
        self.load_server_conf()
        self._bd_mgr   = BlockDeviceManager(self._plugin_name)
        self._drbd_mgr = DrbdManager(self)
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
            conf = conffile.get_conf()
            if conf is not None:
                val = conf.get(self.KEY_PLUGIN_NAME)
                if val is not None:
                    self._plugin_name = val
        except IOError as io_err:
            sys.stderr.write("Warning: Cannot open drbdmanage configuration "
              "file %s\n", self.CONFFILE)
        finally:
            if file is not None:
                file.close()
    
    
    def iterate_nodes(self):
        return self._nodes.itervalues()
    
    
    def iterate_volumes(self):
        return self._volumes.itervalues()
    
    
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
    
    
    def get_volume(self, name):
        volume = None
        try:
            volume = self._volumes[name]
        except KeyError:
            pass
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
                        volume = assignment.get_volume()
                        for peer_assg in volume.iterate_assignments():
                            peer_assg.update_connections()
                    node.mark_remove()
                    self.cleanup()
                else:
                    # drop all associated assignments
                    for assignment in node.iterate_assignments():
                        volume = assignment.get_volume()
                        volume.remove_assignment(assignment)
                        # tell the remaining nodes that have this volume to
                        # drop the connection to the deleted node
                        for peer_assg in volume.iterate_assignments():
                            peer_assg.update_connections()
                    del self._nodes[name]
                self._drbd_mgr.perform_changes()
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
        Registers a new volume that can subsequently be deployed on
        DRBD cluster nodes
        """
        rc      = DM_EPERSIST
        volume  = None
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                volume = self._volumes.get(name)
                if volume is not None:
                    rc = DM_EEXIST
                else:
                    try:
                        if minor == MinorNr.MINOR_AUTO:
                            # TODO: generate the minor number
                            pass
                        volume = DrbdVolume(name, size, MinorNr(minor))
                        self._volumes[volume.get_name()] = volume
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
    
    
    def remove_volume(self, name, force):
        """
        Marks a volume for removal from the DRBD cluster
        * Orders all nodes to undeploy the volume
        """
        rc      = DM_EPERSIST
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                volume = self._volumes[name]
                if (not force) and volume.has_assignments():
                    for assignment in volume.iterate_assignments():
                        assignment.undeploy()
                    volume.mark_remove()
                    self.cleanup()
                else:
                    # drop all associated assignments
                    for assignment in volume.iterate_assignments():
                        node = assignment.get_node()
                        node.remove_assignment(assignment)
                    del self._volumes[name]
                self._drbd_mgr.perform_changes()
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
    
    
    def assign(self, node_name, volume_name, tstate):
        """
        Assigns a volume to a node
        * Orders all participating nodes to deploy the volume
        """
        rc      = DM_EPERSIST
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node   = self._nodes.get(node_name)
                volume = self._volumes.get(volume_name)
                if node is None or volume is None:
                    rc = DM_ENOENT
                else:
                    assignment = node.get_assignment(volume.get_name())
                    if assignment is not None:
                        rc = DM_EEXIST
                    else:
                        if (tstate & Assignment.FLAG_DISKLESS) != 0 \
                          and (tstate & Assignment.FLAG_OVERWRITE) != 0:
                            rc = DM_EINVAL
                        elif (tstate & Assignment.FLAG_DISCARD) != 0 \
                          and (tstate & Assignment.FLAG_OVERWRITE) != 0:
                            rc = DM_EINVAL
                        else:
                            rc = self._assign(node, volume, tstate)
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
    
    
    def unassign(self, node_name, volume_name, force):
        """
        Removes the assignment of a volume to a node
        * Orders the node to undeploy the volume
        """
        rc      = DM_EPERSIST
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node   = self._nodes.get(node_name)
                volume = self._volumes.get(volume_name)
                if node is None or volume is None:
                    rc = DM_ENOENT
                else:
                    assignment = node.get_assignment(volume.get_name())
                    if assignment is None:
                        rc = DM_ENOENT
                    else:
                        rc = self._unassign(assignment, force)
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
    
    
    def _assign(self, node, volume, tstate):
        """
        Implementation - see assign()
        """
        try:
            # TODO: generate the node-id
            node_id = 0
            # The block device is set upon allocation of the backend storage
            # area on the target node
            assignment = Assignment(node, volume, node_id, 0, tstate)
            node.add_assignment(assignment)
            volume.add_assignment(assignment)
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        return DM_SUCCESS
    
    
    def _unassign(self, assignment, force):
        """
        Implementation - see unassign()
        """
        try:
            node   = assignment.get_node()
            volume = assignment.get_volume()
            if (not force) and assignment.is_deployed():
                assignment.disconnect()
                assignment.detach()
                assignment.undeploy()
                for assignment in node.iterate_assignments():
                    if assignment.get_node() != node \
                      and assignment.is_deployed():
                        assignment.update_connections()
            else:
                node.remove_assignment(assignment)
                volume.remove_assignment(assignment)
            self.cleanup()
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        return DM_SUCCESS
    
    
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
            # delete volumes that are marked for removal and that do not
            # have assignments any more
            removable = []
            for volume in self._volumes.itervalues():
                volstate = volume.get_state()
                if (volstate & DrbdVolume.FLAG_REMOVE) != 0:
                    if not volume.has_assignments():
                        removable.append(volume)
            for volume in removable:
                del self._volumes[volume.get_name()]
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
            return DM_DEBUG
        return DM_SUCCESS
    
    
    def volume_list(self):
        try:
            volume_list = []
            for volume in self._volumes.itervalues():
                properties = DrbdVolumeView.get_properties(volume)
                volume_list.append(properties)
            return volume_list
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        return DM_SUCCESS
    
    
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
            return DM_DEBUG
        return DM_SUCCESS
    
    
    def save_conf(self):
        rc = DM_EPERSIST
        persist  = None
        try:
            persist = PersistenceImpl()
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
            persist = PersistenceImpl()
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
        persist.load(self._nodes, self._volumes)
        hash_obj = persist.get_hash_obj()
        if hash_obj is not None:
            self._conf_hash = hash_obj.get_hash()
    
    
    def save_conf_data(self, persist):
        hash_obj = None
        persist.save(self._nodes, self._volumes)
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
            persist = PersistenceImpl()
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
            persist = PersistenceImpl()
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
    
    
    def get_conf_hash(self):
        return self._conf_hash
    
    
    def hashes_match(self, hash):
        if self._conf_hash is not None and hash is not None:
            if self._conf_hash == hash:
                return True
        return False
    
    
    def reconfigure(self):
        rc      = DM_EPERSIST
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
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
    
    def shutdown(self):
        exit(0)
    
    
    @staticmethod
    def catch_internal_error(exc):
        try:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            sys.stderr.write("Internal error: Unexpected exception: %s\n" 
              % (str(exc)))
        except Exception:
            pass
