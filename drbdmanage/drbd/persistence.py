#!/usr/bin/python

__author__="raltnoeder"
__date__ ="$Sep 24, 2013 3:33:50 PM$"

from drbdmanage.storage.storagecore import GenericStorage
from drbdmanage.exceptions import *
import sys
import json

class GenericPersistence(object):
    _obj = None
    
    def __init__(self, obj):
        self._obj = obj
    
    def get_object(self):
        return self._obj
    
    def load_dict(self, serializable):
        properties = dict()
        for key in serializable:
            try:
                val = self._obj.__dict__[key]
                properties[key] = val
            except KeyError:
                pass
        return properties
    
    def serialize(self, properties):
        return json.dumps(properties, indent=4, sort_keys=True)

class DrbdNodePersistence(GenericPersistence):
    SERIALIZABLE = [ "_name", "_ip", "_af", "_state", \
      "_poolsize", "_poolfree" ]
    
    def __init__(self, node):
        super(DrbdNodePersistence, self).__init__(node)
    
    def safe(self):
        properties  = self.load_dict(self.SERIALIZABLE)
        
        # Serialize the name of the assigned volumes only
        node = self.get_object()
        assignments = []
        for assg in node.iterate_assignments():
            volume = assg.get_volume()
            vol_name = volume.get_name()
            assignments.append(vol_name)
        properties["assignments"] = assignments
        
        serialized = self.serialize(properties)
        sys.stdout.write(serialized + "\n")

class DrbdVolumePersistence(GenericPersistence):
    SERIALIZABLE = [ "_name", "_state", "_size_MiB" ]
    
    def __init__(self, volume):
        super(DrbdVolumePersistence, self).__init__(volume)
    
    def safe(self):
        properties = self.load_dict(self.SERIALIZABLE)
        
        # Serialize the name of the assigned nodes only
        volume = self.get_object()
        assignments = []
        for assg in volume.iterate_assignments():
            node = assg.get_node()
            node_name = node.get_name()
            assignments.append(node_name)
        properties["assignments"] = assignments
        minor = volume.get_minor()
        properties["minor"] = minor.get_value()
        
        serialized = self.serialize(properties)
        sys.stdout.write(serialized + "\n")

class AssignmentPersistence(GenericPersistence):
    SERIALIZABLE = [ "_node_id", "_cstate", "_tstate", "_rc" ]
    
    def __init__(self, assignment):
        super(AssignmentPersistence, self).__init__(assignment)
        
    def safe(self):
        properties = self.load_dict(self.SERIALIZABLE)
        
        # Serialize the names of nodes and volumes only
        assignment = self.get_object()
        node       = assignment.get_node()
        volume     = assignment.get_volume()
        node_name  = node.get_name()
        vol_name   = volume.get_name()
        
        properties["node"]   = node_name
        properties["volume"] = vol_name
        
        serialized = self.serialize(properties)
        sys.stdout.write(serialized + "\n")
