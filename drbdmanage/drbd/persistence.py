#!/usr/bin/python

from drbdmanage.storage.storagecore import MinorNr
from drbdmanage.drbd.drbdcore import *
from drbdmanage.persistence import *
from drbdmanage.exceptions import *
from drbdmanage.utils import *
import sys
import os
import fcntl
import errno
import time
import json
import mmap

__author__="raltnoeder"
__date__ ="$Sep 24, 2013 3:33:50 PM$"


def persistence_impl():
    """
    Return the persistence implementation.
    This function serves for easy and centralized replacement of
    the class that implements persistence (saving object state to disk)
    """
    return PersistenceImpl()


class PersistenceImplDummy(object):
    def __init__(self):
        pass
    
    
    def close(self):
        pass
    
    
    def get_hash_obj(self):
        return DataHash()
    
    
    def get_stored_hash(self):
        dh = DataHash()
        dh.get_hash()
    
    
    def load(self, nodes, resources):
        pass
    
    
    def save(self, nodes, resources):
        pass
    
    
    def open(self, modify):
        return True
    

class PersistenceImpl(object):
    _file       = None
    _i_file     = None
    _server     = None
    _writeable  = False
    _hash_obj   = None
    
    BLKSZ       = 0x1000 # 4096
    IDX_OFFSET  = 0x1800 # 6144
    HASH_OFFSET = 0x1900 # 6400
    DATA_OFFSET = 0x2000 # 8192
    ZEROFILLSZ  = 0x0400 # 1024
    CONF_FILE   = "/opt/tmp/drbdmanaged.bin"
    
    MMAP_BUFSZ  = 0x100000 # 1048576 == 1 MiB
    
    BLKFLSBUF = 0x00001261 # <include/linux/fs.h>
    
    # fail counter for attempts to open the config file (CONF_FILE)
    MAX_FAIL_COUNT = 10
    
    # wait 2 seconds before every open() retry if the file was not found
    ENOENT_REOPEN_TIMER = 2
    # wait at least half a second between open() retries
    MIN_REOPEN_TIMER    = 0.5
    
    
    def __init__(self):
        pass
    
    
    def open_new(self, modify):
        rc = False
        fail_ctr = 0
        if modify:
            mode = (os.O_RDWR | os.O_DIRECT)
        else:
            mode = (os.O_RDONLY | os.O_DIRECT)
        while fail_ctr < self.MAX_FAIL_COUNT:
            try:
                self._i_file    = os.open(self.CONF_FILE, mode)
                # self._file      = mmap.mmap(self._i_file, self.MMAP_BUFSZ)
                # FIXME -- length is still fixed to 16384
                self._file      = mmap.mmap(self._i_file, 16384, MAP_SHARED,
                  PROT_READ | PROT_WRITE, 0)
                self._writeable = modify
                rc = True
                break
            except IOError as io_err:
                fail_ctr += 1
                if io_err.errno == errno.ENOENT:
                    sys.stderr.write("Cannot open %s: not found\n"
                      % (self.CONF_FILE))
                    cs = self.ENOENT_REOPEN_TIMER
                else:
                    b = os.urandom(1)
                    cs = ord(b) / 100 + self.MIN_REOPEN_TIMER
                time.sleep(cs)
        try:
            if rc:
                fcntl.ioctl(self._i_file, self.BLKFLSBUF)
        except IOError:
            pass
        if not fail_ctr < 10:
            sys.stderr.write("Cannot open %s (%d failed attempts)\n"
              % (self.CONF_FILE, self.MAX_FAIL_COUNT))
        return rc
    
    
    def open(self, modify):
        """
        Open the persistent storage for reading or writing, depending on
        the modify flag. If (modify == True), open for writing, otherwise
        open readonly.
        """
        rc = False
        fail_ctr = 0
        error    = 0
        if modify:
            modeflags = os.O_RDWR
            mode      = "r+"
        else:
            modeflags = os.O_RDONLY
            mode      = "r"
        while fail_ctr < self.MAX_FAIL_COUNT:
            try:
                fail = False
                fd = os.open(self.CONF_FILE, modeflags)
                
                # Try to flush the buffer cache
                # This can fail depending on the type of the file's
                # underlying device
                try:
                    fcntl.ioctl(fd, self.BLKFLSBUF)
                except OSError:
                    pass
                except IOError:
                    pass
                
                self._file = os.fdopen(fd, mode)
                self._writeable = modify
                rc = True
                break
            except IOError as io_err:
                fail  = True
                error = io_err.errno
            except OSError as os_err:
                fail  = True
                error = os_err.errno
            if fail:
                fail_ctr += 1
                if error == errno.ENOENT:
                    sys.stderr.write("Cannot open %s: not found\n"
                      % (self.CONF_FILE))
                    cs = self.ENOENT_REOPEN_TIMER
                else:
                    b = os.urandom(1)
                    cs = ord(b) / 100 + self.MIN_REOPEN_TIMER
                time.sleep(cs)
        if not fail_ctr < self.MAX_FAIL_COUNT:
            sys.stderr.write("Cannot open %s (%d failed attempts)\n"
              % (self.CONF_FILE, self.MAX_FAIL_COUNT))
        return rc
        
    
    # TODO: clean implementation - this is a prototype
    def save(self, nodes, resources):
        if self._writeable:
            try:
                p_nodes_con = dict()
                p_res_con   = dict()
                p_assg_con  = dict()
                hash        = DataHash()
                
                # Prepare nodes container (and build assignments list)
                assignments = []
                for node in nodes.itervalues():
                    p_node = DrbdNodePersistence(node)
                    p_node.save(p_nodes_con)
                    for assg in node.iterate_assignments():
                        assignments.append(assg)
                
                # Prepare resources container
                for resource in resources.itervalues():
                    p_resource = DrbdResourcePersistence(resource)
                    p_resource.save(p_res_con)
                              
                # Prepare assignments container
                for assignment in assignments:
                    p_assignment = AssignmentPersistence(assignment)
                    p_assignment.save(p_assg_con)
                
                # Save data
                self._file.seek(self.DATA_OFFSET)
                
                nodes_off = self._file.tell()
                save_data = self._container_to_json(p_nodes_con)
                hash.update(save_data)
                self._file.write(save_data)
                nodes_len = self._file.tell() - nodes_off
                
                self._align_zero_fill()
                
                res_off = self._file.tell()
                save_data = self._container_to_json(p_res_con)
                self._file.write(save_data)
                hash.update(save_data)
                res_len = self._file.tell() - res_off
                
                self._align_zero_fill()
                
                assg_off = self._file.tell()
                save_data = self._container_to_json(p_assg_con)
                self._file.write(save_data)
                hash.update(save_data)
                assg_len = self._file.tell() - assg_off
                
                self._file.seek(self.IDX_OFFSET)
                self._file.write(
                  long_to_bin(nodes_off)
                  + long_to_bin(nodes_len)
                  + long_to_bin(res_off)
                  + long_to_bin(res_len)
                  + long_to_bin(assg_off)
                  + long_to_bin(assg_len))
                self._file.seek(self.HASH_OFFSET)
                self._file.write(hash.get_hash())
                sys.stderr.write("%sDEBUG: persistence save/hash: %s%s\n"
                  % (COLOR_BLUE, hash.get_hex_hash(), COLOR_NONE))
                self._hash_obj = hash
            except Exception as exc:
                sys.stderr.write("persistence save(): " + str(exc) + "\n")
                raise PersistenceException
        else:
            # file not open for writing
            raise IOError("Persistence save() without a "
              "writeable file descriptor")
    
    
    # Get the hash of the configuration on persistent storage
    def get_stored_hash(self):
        stored_hash = None
        if self._file is not None:
            try:
                hash = DataHash()
                self._file.seek(self.HASH_OFFSET)
                stored_hash = self._file.read(hash.get_hash_len())
            except Exception:
                raise PersistenceException
        else:
            # file not open
            raise IOError("Persistence load() without an "
              "open file descriptor")
        return stored_hash
    
    
    # TODO: clean implementation - this is a prototype
    def load(self, nodes, resources):
        errors = False
        if self._file is not None:
            try:
                hash = DataHash()
                self._file.seek(self.IDX_OFFSET)
                f_index = self._file.read(48)
                nodes_off = long_from_bin(f_index[0:8])
                nodes_len = long_from_bin(f_index[8:16])
                res_off   = long_from_bin(f_index[16:24])
                res_len   = long_from_bin(f_index[24:32])
                assg_off  = long_from_bin(f_index[32:40])
                assg_len  = long_from_bin(f_index[40:48])
                
                nodes_con = None
                res_con   = None
                assg_con  = None
                
                self._file.seek(nodes_off)
                load_data = self._file.read(nodes_len)
                hash.update(load_data)
                try:
                    nodes_con = self._json_to_container(load_data)
                except Exception:
                    pass
                
                self._file.seek(res_off)
                load_data = self._file.read(res_len)
                hash.update(load_data)
                try:
                    res_con   = self._json_to_container(load_data)
                except Exception:
                    pass
                
                self._file.seek(assg_off)
                load_data = self._file.read(assg_len)
                hash.update(load_data)
                try:
                    assg_con  = self._json_to_container(load_data)
                except Exception:
                    pass
                
                self._file.seek(self.HASH_OFFSET)
                computed_hash = hash.get_hash()
                stored_hash   = self._file.read(hash.get_hash_len())
                sys.stderr.write("%sDEBUG: persistence load/hash: %s%s\n"
                  % (COLOR_BLUE, hex_from_bin(stored_hash), COLOR_NONE))
                if computed_hash != stored_hash:
                    sys.stderr.write("Warning: configuration data does not "
                      "match its signature\n")
                # TODO: if the signature is wrong, load an earlier backup
                #       of the configuration
                
                nodes.clear()
                if nodes_con is not None:
                    for properties in nodes_con.itervalues():
                        node = DrbdNodePersistence.load(properties)
                        if node is not None:
                            nodes[node.get_name()] = node
                        else:
                            print "DEBUG Nodes", properties # DEBUG
                            errors = True
                
                resources.clear()
                if res_con is not None:
                    for properties in res_con.itervalues():
                        resource = DrbdResourcePersistence.load(properties)
                        if resource is not None:
                            resources[resource.get_name()] = resource
                        else:
                            print "DEBUG Resources", properties # DEBUG
                            errors = True
                
                if assg_con is not None:
                    for properties in assg_con.itervalues():
                        assignment = AssignmentPersistence.load(properties,
                          nodes, resources)
                        if assignment is None:
                            print "DEBUG Assignments", properties # DEBUG
                            errors = True
                    self._hash_obj = hash
            except Exception as exc:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                sys.stderr.write("DEBUG: Exception %s (%s), %s\n%s\n"
                  % (str(exc), exc_type, exc_obj, exc_tb))
                raise PersistenceException
        else:
            sys.stderr.write("DEBUG: File not open\n" % str(exc))
            # file not open
            raise IOError("Persistence load() without an "
              "open file descriptor")
        if errors:
            raise PersistenceException
    
    
    def close_new(self):
        try:
            if self._i_file is not None:
                if self._writeable:
                    self._writeable = False
                    if self._file is not None:
                        self._file.flush()
                    os.fsync(self._i_file)
                if self._file is not None:
                    self._file.close()
                os.close(self._i_file)
                self._i_file    = None
                self._file      = None
        except IOError:
            pass
    
    
    def close(self):
        try:
            if self._file is not None:
                if self._writeable:
                    self._file.flush()
                    os.fsync(self._file.fileno())
                    self._writeable = False
                # fcntl.ioctl(self._file.fileno(), self.BLKFLSBUF)
        except IOError:
            pass
        finally:
            self._file.close()
            self._file = None
    
    
    def get_hash_obj(self):
        return self._hash_obj
    
    
    def _container_to_json(self, container):
        return (json.dumps(container, indent=4, sort_keys=True) + "\n")
    
    
    def _json_to_container(self, json_doc):
        return json.loads(json_doc)
    
    
    def _align_offset(self):
        if self._file is not None:
            offset = self._file.tell()
            if offset % self.BLKSZ != 0:
                offset = ((offset / self.BLKSZ) + 1) * self.BLKSZ
                self._file.seek(offset)
    
    
    def _align_zero_fill(self):
        if self._file is not None:
            offset = self._file.tell()
            if offset % self.BLKSZ != 0:
                fillbuf = ('\0' * self.ZEROFILLSZ)
                blk  = ((offset / self.BLKSZ) + 1) * self.BLKSZ
                diff = blk - offset;
                fillnr = diff / self.ZEROFILLSZ
                ctr = 0
                while ctr < fillnr:
                    self._file.write(fillbuf)
                    ctr += 1
                diff -= (self.ZEROFILLSZ * fillnr)
                self._file.write(fillbuf[:diff])
    
    
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
        return json_blk


class DrbdNodePersistence(GenericPersistence):
    SERIALIZABLE = [ "_name", "_ip", "_af", "_state",
      "_poolsize", "_poolfree" ]
    
    
    def __init__(self, node):
        super(DrbdNodePersistence, self).__init__(node)
    
    
    def save(self, container):
        node = self.get_object()
        properties  = self.load_dict(self.SERIALIZABLE)
        container[node.get_name()] = properties
    
        
    @classmethod
    def load(cls, properties):
        node = None
        try:
            node = DrbdNode(
              properties["_name"],
              properties["_ip"],
              int(properties["_af"])
              )
            node.set_state(long(properties["_state"]))
            node.set_poolsize(long(properties["_poolsize"]))
            node.set_poolfree(long(properties["_poolfree"]))
        except Exception as exc:
            # FIXME
            raise exc
        return node


class DrbdResourcePersistence(GenericPersistence):
    SERIALIZABLE = [ "_name", "_secret", "_state" ]
    
    def __init__(self, resource):
        super(DrbdResourcePersistence, self).__init__(resource)
    
    
    def save(self, container):
        resource = self.get_object()
        properties = self.load_dict(self.SERIALIZABLE)
        volume_list = dict()
        for volume in resource.iterate_volumes():
            p_vol = DrbdVolumePersistence(volume)
            p_vol.save(volume_list)
        properties["volumes"] = volume_list
        container[resource.get_name()] = properties
    
    
    @classmethod
    def load(cls, properties):
        resource = None
        try:
            resource = DrbdResource(properties["_name"])
            secret = properties.get("_secret")
            if secret is not None:
                resource.set_secret(secret)
            resource.set_state(properties["_state"])
            volume_list = properties["volumes"]
            for vol_properties in volume_list.itervalues():
                volume = DrbdVolumePersistence.load(vol_properties)
                resource.add_volume(volume)
        except Exception as exc:
            # FIXME
            raise exc
        return resource
    
    
class DrbdVolumePersistence(GenericPersistence):
    SERIALIZABLE = [ "_id", "_state", "_size_MiB" ]
    
    
    def __init__(self, volume):
        super(DrbdVolumePersistence, self).__init__(volume)
    
    
    def save(self, container):
        volume = self.get_object()
        properties  = self.load_dict(self.SERIALIZABLE)
        minor = volume.get_minor()
        properties["minor"] = minor.get_value()
        container[volume.get_id()] = properties
    
    
    @classmethod
    def load(cls, properties):
        volume = None
        try:
            minor_nr = properties["minor"]
            minor = MinorNr(minor_nr)
            volume = DrbdVolume(
              properties["_id"],
              long(properties["_size_MiB"]),
              minor
              )
            volume.set_state(long(properties["_state"]))
        except Exception as exc:
            # FIXME
            raise exc
        return volume


class AssignmentPersistence(GenericPersistence):
    SERIALIZABLE = [ "_node_id", "_cstate", "_tstate", "_rc" ]
    
    
    def __init__(self, assignment):
        super(AssignmentPersistence, self).__init__(assignment)
        
        
    def save(self, container):
        properties = self.load_dict(self.SERIALIZABLE)
        
        # Serialize the names of nodes and resources only
        assignment  = self.get_object()
        node        = assignment.get_node()
        resource    = assignment.get_resource()
        node_name   = node.get_name()
        res_name    = resource.get_name()
        
        properties["node"]        = node_name
        properties["resource"]    = res_name
        
        assg_name = node_name + ":" + res_name
        
        vol_state_list = dict()
        for vol_state in assignment.iterate_volume_states():
            p_vol_state = DrbdVolumeStatePersistence(vol_state)
            p_vol_state.save(vol_state_list)
        properties["volume_states"] = vol_state_list
        container[assg_name] = properties
    
    
    @classmethod
    def load(cls, properties, nodes, resources):
        assignment = None
        try:
            node = nodes[properties["node"]]
            resource = resources[properties["resource"]]
            assignment = Assignment(
              node,
              resource,
              int(properties["_node_id"]),
              long(properties["_cstate"]),
              long(properties["_tstate"])
              )
            assignment.set_rc(properties["_rc"])
            node.add_assignment(assignment)
            resource.add_assignment(assignment)
            vol_state_list = properties.get("volume_states")
            for vol_state_props in vol_state_list.itervalues():
                vol_state = DrbdVolumeStatePersistence.load(
                  vol_state_props, assignment)
                assignment.add_volume_state(vol_state)
        except Exception as exc:
            # FIXME
            raise exc
        return assignment


class DrbdVolumeStatePersistence(GenericPersistence):
    SERIALIZABLE = [ "_bd_path", "_blockdevice", "_cstate", "_tstate" ]
    
    
    def __init__(self, vol_state):
        super(DrbdVolumeStatePersistence, self).__init__(vol_state)
    
    
    def save(self, container):
            vol_state = self.get_object()
            id = vol_state.get_id()
            properties = self.load_dict(self.SERIALIZABLE)
            properties["id"] = id
            container[id] = properties
    
    
    @classmethod
    def load(cls, properties, assignment):
        vol_state = None
        try:
            resource = assignment.get_resource()
            volume   = resource.get_volume(properties["id"])
            vol_state = DrbdVolumeState(volume)
            blockdevice = properties.get("_blockdevice")
            bd_path     = properties.get("_bd_path")
            if blockdevice is not None and bd_path is not None:
                vol_state.set_blockdevice(blockdevice, bd_path)
            vol_state.set_cstate(properties["_cstate"])
            vol_state.set_tstate(properties["_tstate"])
        except Exception as exc:
            # FIXME
            raise exc
        return vol_state
