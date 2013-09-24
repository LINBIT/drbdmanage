#!/usr/bin/python

import sys
import traceback

from drbdmanage.dbusserver import *
from drbdmanage.exceptions import *
from drbdmanage.drbd.drbdcore import *
from drbdmanage.drbd.persistence import *
from drbdmanage.storage.storagecore import *

__author__="raltnoeder"
__date__ ="$Sep 12, 2013 5:09:49 PM$"

class DrbdManageServer(object):
    _bd_mgr      = None
    
    _nodes   = None
    _volumes = None
    
    def __init__(self):
        self._nodes   = dict()
        self._volumes = dict()
        self._bd_mgr  = BlockDeviceManager()

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
            return None
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
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
        return DM_SUCCESS
    
    def get_volume(self, name):
        volume = None
        try:
            volume = self._volumes[name]
        except KeyError:
            return None
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
    
    def unassign(self, node_name, volume_name):
        """
        Removes the assignment of a volume to a node
        * Orders the node to undeploy the volume
        """
        try:
            node = self._nodes["remus"]
            assg = node.get_assignment("homes")
            assg.set_cstate(assg.get_cstate() | Assignment.FLAG_DEPLOY \
              | Assignment.FLAG_ATTACH | Assignment.FLAG_CONNECT)
        except Exception:
            sys.stderr.write("Oops.")
        try:
            try:
                node   = self._nodes[node_name]
                volume = self._volumes[volume_name]
            except KeyError:
                return DM_ENOENT
            assignment = node.get_assignment(volume.get_name())
            if assignment is None:
                return DM_ENOENT
            return self._unassign(assignment)
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
            assignment = Assignment(node, volume, None, node_id, 0, tstate)
            node.add_assignment(assignment)
            volume.add_assignment(assignment)
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        return DM_SUCCESS
    
    def _unassign(self, assignment):
        """
        Implementation - see unassign()
        """
        try:
            node   = assignment.get_node()
            volume = assignment.get_volume()
            if assignment.is_deployed():
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
        node_list = []
        for node in self._nodes.itervalues():
            properties = []
            properties.append(node.get_name())
            properties.append(node.get_af_label())
            properties.append(node.get_ip())
            properties.append(str(node.get_poolsize()))
            properties.append(str(node.get_poolfree()))
            properties.append(str(node.get_state()))
            node_list.append(properties)
        return node_list
    
    def volume_list(self):
        volume_list = []
        for volume in self._volumes.itervalues():
            properties = []
            properties.append(volume.get_name())
            properties.append(str(volume.get_size_MiB()))
            properties.append(str(volume.get_minor().get_value()))
            properties.append(str(volume.get_state()))
            volume_list.append(properties)
        return volume_list
        
    def reconfigure(self):
        return DM_ENOTIMPL
    
    def shutdown(self):
        exit(0)
    
    @staticmethod
    def catch_internal_error(exc):
        try:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            sys.stderr.write("Internal error: Unexpected exception: " \
              + str(exc) + "\n")
            sys.stderr.write(exc_type + ":\n" \
              + traceback.format_exc() + "\n")
        except Exception:
            pass
