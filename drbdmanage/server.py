#!/usr/bin/python

import sys
import os
import traceback
import gobject
import subprocess
import fcntl

from drbdmanage.dbusserver import *
from drbdmanage.exceptions import *
from drbdmanage.drbd.drbdcore import *
from drbdmanage.drbd.persistence import *
from drbdmanage.storage.storagecore import *

__author__="raltnoeder"
__date__ ="$Sep 12, 2013 5:09:49 PM$"


class DrbdManageServer(object):
    EVT_UTIL  = "/usr/local/sbin/drbdsetup"
    
    EVT_TYPE_CHANGE = "change"
    EVT_SRC_CON     = "connection"
    EVT_SRC_RES     = "resource"
    EVT_ARG_NAME    = "name"
    EVT_ARG_ROLE    = "role"
    
    EVT_ROLE_PRIMARY   = "Primary"
    EVT_ROLE_SECONDARY = "Secondary"
    DRBDCTRL_RES_NAME  = "drbdctrl"
    
    _bd_mgr   = None
    _nodes    = None
    _volumes  = None
    _evt_file = None
    _proc_evt = None
    _reader   = None
    
    
    def __init__(self):
        self._nodes   = dict()
        self._volumes = dict()
        self._bd_mgr  = BlockDeviceManager()
        self.load_conf()
        self.init_events()


    def init_events(self):
        self._proc_evt = subprocess.Popen([self.EVT_UTIL, "events", "all"], 0,
          self.EVT_UTIL, stdout=subprocess.PIPE)
        self._evt_file = self._proc_evt.stdout
        fcntl.fcntl(self._evt_file.fileno(),
          fcntl.F_SETFL, fcntl.F_GETFL | os.O_NONBLOCK)
        self._reader = NioLineReader(self._evt_file)
        gobject.io_add_watch(self._evt_file.fileno(), gobject.IO_IN, 
          self.drbd_event)
        # TODO: there should be a restart/reopen mechanism if the pipe breaks
        # or the subprocess goes away somehow (which will break the pipe, too)
        # Maybe try a HUP (hang-up) event handler from GMainLoop
    
    
    def drbd_event(self, fd, condition):
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
                event_type   = get_event_type(line)
                event_source = get_event_source(line)
                if event_type is not None and event_source is not None:
                    # If the configuration resource changes to "Secondary" role
                    # on a connected node, the configuration may have changed
                    if event_type == self.EVT_TYPE_CHANGE and \
                      event_source == self.EVT_SRC_CON:
                        event_res  = get_event_arg(line, self.EVT_ARG_NAME)
                        event_role = get_event_arg(line, self.EVT_ARG_ROLE)
                        if event_res == self.DRBDCTRL_RES_NAME and \
                          event_role == self.EVT_ROLE_SECONDARY:
                            self.load_conf()
                            sys.stderr.write("DEBUG: load_conf(), "
                              "-> enter new target state\n")
        # True = GMainLoop shall not unregister this event handler
        return True
    
    
    def create_node(self, name, ip, af):
        """
        Registers a DRBD cluster node
        """
        node = None
        try:
            try:
                node = self._nodes[name]
            except KeyError:
                pass
            if node is not None:
                return DM_EEXIST
            try:
                node = DrbdNode(name, ip, af)
                self._nodes[node.get_name()] = node
            except InvalidNameException:
                return DM_ENAME
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        return DM_SUCCESS
    
    
    def remove_node(self, name, force):
        """
        Marks a node for removal from the DRBD cluster
        * Orders the node to undeploy all volumes
        * Orders all other nodes to disconnect from the node
        """
        try:
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
        except KeyError:
            return DM_ENOENT
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
        return DM_SUCCESS
    
    
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
    
    
    def create_volume(self, name, size, minor):
        """
        Registers a new volume that can subsequently be deployed on
        DRBD cluster nodes
        """
        volume = None
        try:
            try:
                volume = self._volumes[name]
            except KeyError:
                pass
            if volume is not None:
                return DM_EEXIST
            try:
                if minor == MinorNr.MINOR_AUTO:
                    # TODO: generate the minor number
                    pass
                volume = DrbdVolume(name, size, MinorNr(minor))
                self._volumes[volume.get_name()] = volume
            except InvalidNameException:
                return DM_ENAME
            except InvalidMinorNrException:
                return DM_EMINOR
            except VolSizeRangeException:
                return DM_EVOLSZ
        except Exception as exc:
                DrbdManageServer.catch_internal_error(exc)
                return DM_DEBUG
        return DM_SUCCESS
    
    
    def remove_volume(self, name, force):
        """
        Marks a volume for removal from the DRBD cluster
        * Orders all nodes to undeploy the volume
        """
        try:
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
        except KeyError:
            return DM_ENOENT
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        return DM_SUCCESS
    
    
    def get_volume(self, name):
        volume = None
        try:
            volume = self._volumes[name]
        except KeyError:
            pass
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
        return volume
    
    
    def assign(self, node_name, volume_name, tstate):
        """
        Assigns a volume to a node
        * Orders all participating nodes to deploy the volume
        """
        try:
            try:
                node   = self._nodes[node_name]
                volume = self._volumes[volume_name]
            except KeyError:
                return DM_ENOENT
            assignment = node.get_assignment(volume.get_name())
            if assignment is not None:
                return DM_EEXIST
            if (tstate & Assignment.FLAG_DISKLESS) != 0 \
              and (tstate & Assignment.FLAG_OVERWRITE) != 0:
                return DM_EINVAL
            if (tstate & Assignment.FLAG_DISCARD) != 0 \
              and (tstate & Assignment.FLAG_OVERWRITE) != 0:
                return DM_EINVAL
            return self._assign(node, volume, tstate)
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
        return DM_DEBUG
    
    
    def unassign(self, node_name, volume_name, force):
        """
        Removes the assignment of a volume to a node
        * Orders the node to undeploy the volume
        """
        try:
            try:
                node   = self._nodes[node_name]
                volume = self._volumes[volume_name]
            except KeyError:
                return DM_ENOENT
            assignment = node.get_assignment(volume.get_name())
            if assignment is None:
                return DM_ENOENT
            return self._unassign(assignment, force)
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
        return DM_DEBUG
    
    
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
            # TODO: This is DEBUG code; BlockDeviceManager is a dummy a.t.m.
            bd = self._bd_mgr.create_blockdevice(volume.get_name(), \
              volume.get_size_MiB())
            assignment.set_blockdevice(bd.get_name(), bd.get_path())
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
        try:
            persist = PersistenceImpl()
            if persist.open_modify():
                if persist.save(self._nodes, self._volumes) == True:
                    rc = DM_SUCCESS
                persist.close()
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        return rc
    
    
    def load_conf(self):
        rc = DM_EPERSIST
        try:
            persist = PersistenceImpl()
            if persist.open():
                if persist.load(self._nodes, self._volumes) == True:
                    rc = DM_SUCCESS
                persist.close()
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        return rc
    
    
    def reconfigure(self):
        # TODO: this is debug code only
        rc = DM_EPERSIST
        try:
            self.save_conf()
            self._nodes   = dict()
            self._volumes = dict()
            rc = self.load_conf()
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            rc = DM_DEBUG
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
