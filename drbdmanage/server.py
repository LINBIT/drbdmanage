#!/usr/bin/python

import sys
import traceback

from drbdmanage.dbusserver import *
from drbdmanage.exceptions import *
from drbdmanage.drbd.drbdcore import *
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

    def create_node(self, name, ip, ip_type):
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
                node = DrbdNode(name, ip, ip_type)
                self._nodes[node.get_name()] = node
            except InvalidNameException:
                return DM_ENAME
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        return DM_SUCCESS
    
    def remove_node(self, name):
        """
        Marks a node for removal from the DRBD cluster
        * Orders the node to undeploy all volumes
        * Orders all other nodes to disconnect from the node
        """
        try:
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
    
    def remove_volume(self, name):
        """
        Marks a volume for removal from the DRBD cluster
        * Orders all nodes to undeploy the volume
        """
        try:
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
    
    def assign(self, node_name, volume_name, state):
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
            return self._assign(node, volume, state)
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        return DM_SUCCESS
    
    def unassign(self, node_name, volume_name):
        """
        Removes the assignment of a volume to a node
        * Orders the node to undeploy the volume
        """
        try:
            try:
                node   = self._nodes[node_name]
                volume = self._volumes[volume_name]
                assignment = node.get_assignment(volume.get_name())
            except KeyError:
                return DM_ENOENT
            return self._unassign(assignment)
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        return DM_SUCCESS
        
    
    def _assign(self, node, volume, state):
        """
        Implementation - see assign()
        """
        try:
            # TODO: generate the node-id
            node_id = 0
            # The block device is set upon allocation of the backend storage
            # area on the target node
            assignment = Assignment(node, volume, None, node_id, state)
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
            node.remove_assignment(assignment)
            volume.remove_assignment(assignment)
        except Exception as exc:
            DrbdManageServer.catch_internal_error(exc)
            return DM_DEBUG
        return DM_SUCCESS
    
    def reconfigure(self):
        return DM_ENOTIMPL
    
    def shutdown(self):
        exit(0)
    
    @staticmethod
    def catch_internal_error(exc):
        try:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            sys.stderr.write("Internal error: Unexpected exception: " \
              + exc_type + ": " + str(exc) + "\n" \
              + traceback.format_exc() + "\n")
        except Exception:
            pass
