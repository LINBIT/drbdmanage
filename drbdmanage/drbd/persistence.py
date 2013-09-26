#!/usr/bin/python

__author__="raltnoeder"
__date__ ="$Sep 24, 2013 3:33:50 PM$"

from drbdmanage.storage.storagecore import MinorNr
from drbdmanage.drbd.drbdcore import *
from drbdmanage.exceptions import *
import sys
import json
from StringIO import StringIO


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


class PersistenceImpl(object):
    _file      = None
    _server    = None
    _writeable = False
    _offset    = 0
    
    BLKSZ     = 0x1000 # 4096
    CONF_FILE = "/tmp/drbdmanaged.bin"
    
    def __init__(self):
        pass
    
    def open(self):
        rc = False
        try:
            self._file      = open(self.CONF_FILE, "r")
            self._writeable = False
            self._offset    = 0
            rc = True
        except Exception:
            pass
        return rc
    
    def open_modify(self):
        rc = False
        try:
            self._file      = open(self.CONF_FILE, "r+")
            self._writeable = True
            self._offset    = 0
            rc = True
        except Exception:
            pass
        return rc
    
    # TODO: clean implementation - this is a prototype
    def save(self, nodes, volumes):
        rc = False
        try:
            if self._writeable:
                self._file.seek(self.BLKSZ)
                self._offset = self.BLKSZ
                
                # Save nodes
                nodes_offset = self._offset
                assignments = []
                for node in nodes.itervalues():
                    p_node = DrbdNodePersistence(node)
                    self._offset += p_node.save(self._file)
                    for assg in node.iterate_assignments():
                        assignments.append(assg)
                nodes_length = self._offset
                
                self._align_offset()
                self._file.seek(self._offset)
                
                # Save volumes
                volumes_offset = self._offset
                for volume in volumes.itervalues():
                    p_volume = DrbdVolumePersistence(volume)
                    self._offset += p_volume.save(self._file)
                volumes_length = self._offset
                
                self._align_offset()
                self._file.seek(self._offset)
                
                # Save assignments
                assignments_offset = self._offset
                for assignment in assignments:
                    p_assignment = AssignmentPersistence(assignment)
                    self._offset += p_assignment.save(self._file)
                assignments_length = self._offset
                
                self._file.seek(0)
                self._file.write(str(nodes_offset) + ";")
                self._file.write(str(nodes_length) + ";")
                self._file.write(str(volumes_offset) + ";")
                self._file.write(str(volumes_length) + ";")
                self._file.write(str(assignments_offset) + ";")
                self._file.write(str(assignments_length) + ";")
                self._file.write(str(assignments_length) + "#")
                self._file.seek(self._offset)
                
                rc = True
        except Exception as exc:
            sys.stderr.write(str(exc) + "\n")
        return rc
    
    # TODO: clean implementation - this is a prototype
    def load(self, nodes, volumes):
        rc = False
        try:
            if self._file is not None:
                self._file.seek(0)
                storeinfo = self._file.read(self.BLKSZ)
                idx = storeinfo.find("#")
                if idx != -1:
                    storeinfo = storeinfo[:idx]
                else:
                    return rc
                numbers = storeinfo.split(";")
                nodes_offset = int(numbers[0])
                nodes_length = int(numbers[1])
                volumes_offset = int(numbers[2])
                volumes_length = int(numbers[3])
                assignments_offset = int(numbers[4])
                assignments_length = int(numbers[5])
                
                sys.stderr.write("nodes@" + str(nodes_offset) + "\n")
                sys.stderr.write("volumes@" + str(volumes_offset) + "\n")
                sys.stderr.write("assignments@" + str(volumes_offset) + "\n")
                
                self._file.seek(nodes_offset)
                nodes_dump = self._file.read(nodes_length)
                
                self._file.seek(volumes_offset)
                volumes_dump = self._file.read(volumes_length)
                
                self._file.seek(assignments_offset)
                assignments_dump = self._file.read(assignments_length)
                
                sys.stderr.write("DEBUG: #1\n")
                nodes_stream = StringIO(nodes_dump)
                json_blk = self._next_json(nodes_stream)
                while json_blk is not None:
                    node = DrbdNodePersistence.load(json_blk)
                    if node is not None:
                        nodes[node.get_name()] = node
                    json_blk = self._next_json(nodes_stream)
                nodes_stream.close()
                
                sys.stderr.write("DEBUG: #2\n")
                volumes_stream = StringIO(volumes_dump)
                json_blk = self._next_json(volumes_stream)
                while json_blk is not None:
                    volume = DrbdVolumePersistence.load(json_blk)
                    if volume is not None:
                        volumes[volume.get_name()] = volume
                    json_blk = self._next_json(volumes_stream)
                volumes_stream.close()
                
                sys.stderr.write("DEBUG: #3\n")
                assignments_stream = StringIO(assignments_dump)
                json_blk = self._next_json(assignments_stream)
                while json_blk is not None:
                    assignment = AssignmentPersistence.load(json_blk, \
                      nodes, volumes)
                    if assignment is not None:
                        node = assignment.get_node()
                        volume = assignment.get_volume()
                        node.add_assignment(assignment)
                        volume.add_assignment(assignment)
                    json_blk = self._next_json(assignments_stream)
                assignments_stream.close()
                rc = True
        except Exception as exc:
            sys.stderr.write(str(exc) + "\n")
        return rc
    
    def close(self):
        try:
            if self._file is not None:
                self._writeable = False
                self._file.close()
                self._file      = None
                self._offset    = 0
        except Exception:
            pass
    
    def _align_offset(self):
        if self._offset % self.BLKSZ != 0:
            self._offset = ((self._offset / self.BLKSZ) + 1) * self.BLKSZ
    
    def _next_json(self, stream):
        read = False
        json_blk = None
        cfgline = stream.readline()
        while len(cfgline) > 0:
            if cfgline == "{\n":
                read = True
            if read:
                if json_blk is None:
                    json_blk = ""
                json_blk += cfgline
            if cfgline == "}\n":
                break
            cfgline = stream.readline()
        if json_blk is not None:
            sys.stderr.write("DEBUG: json_blk:\n" + json_blk + "\n")
        else:
            sys.stderr.write("DEBUG: json_blk = None\n")
        return json_blk

class DrbdNodePersistence(GenericPersistence):
    SERIALIZABLE = [ "_name", "_ip", "_af", "_state", \
      "_poolsize", "_poolfree" ]
    
    def __init__(self, node):
        super(DrbdNodePersistence, self).__init__(node)
    
    def save(self, stream):
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
        stream.write(serialized + "\n")
        return len(serialized) + 1
    
    @classmethod
    def load(cls, conf):
        node = None
        try:
            properties = json.loads(conf)
            node = DrbdNode( \
              properties["_name"], \
              properties["_ip"], \
              properties["_af"] \
              )
            node.set_state(properties["_state"])
            node.set_poolsize(properties["_poolsize"])
            node.set_poolfree(properties["_poolfree"])
        except Exception:
            pass
        return node


class DrbdVolumePersistence(GenericPersistence):
    SERIALIZABLE = [ "_name", "_state", "_size_MiB" ]
    
    def __init__(self, volume):
        super(DrbdVolumePersistence, self).__init__(volume)
    
    def save(self, stream):
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
        stream.write(serialized + "\n")
        return len(serialized) + 1
    
    @classmethod
    def load(cls, conf):
        volume = None
        try:
            properties = json.loads(conf)
            minor_nr = properties["minor"]
            minor = MinorNr(minor_nr)
            volume = DrbdVolume( \
              properties["_name"], \
              properties["_size_MiB"], \
              minor
              )
            volume.set_state(properties["_state"])
        except Exception:
            pass
        return volume


class AssignmentPersistence(GenericPersistence):
    SERIALIZABLE = [ "_blockdevice", "bd_path", "_node_id", \
      "_cstate", "_tstate", "_rc" ]
    
    def __init__(self, assignment):
        super(AssignmentPersistence, self).__init__(assignment)
        
    def save(self, stream):
        properties = self.load_dict(self.SERIALIZABLE)
        
        # Serialize the names of nodes and volumes only
        assignment  = self.get_object()
        node        = assignment.get_node()
        volume      = assignment.get_volume()
        node_name   = node.get_name()
        vol_name    = volume.get_name()
        
        properties["node"]        = node_name
        properties["volume"]      = vol_name
        
        serialized = self.serialize(properties)
        stream.write(serialized + "\n")
        return len(serialized) + 1
    
    @classmethod
    def load(cls, conf, nodes, volumes):
        assignment = None
        try:
            properties = json.loads(conf)
            node = nodes[properties["node"]]
            volume = volumes[properties["volume"]]
            assignment = Assignment( \
              node, \
              volume, \
              properties["_node_id"], \
              properties["_cstate"], \
              properties["_tstate"] \
              )
            blockdevice = properties["_blockdevice"]
            bd_path     = properties["_bd_path"]
            if blockdevice is not None and bd_path is not None:
                assignment.set_blockdevice(blockdevice, bd_path)
            assignment.set_rc(properties["_rc"])
        except Exception:
            pass
        return assignment
