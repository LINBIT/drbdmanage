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
import logging

__author__ = "raltnoeder"
__date__   = "$Sep 24, 2013 3:33:50 PM$"


def persistence_impl():
    """
    Return the persistence implementation of the drbdmanage control volume
    
    This function serves for easy and centralized replacement of
    the class that implements persistence (saving object state to disk)
    
    @return: persistence layer object
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
        data_hash = DataHash()
        data_hash.get_hex_hash()
    
    
    def load(self, nodes, resources):
        pass
    
    
    def save(self, nodes, resources):
        pass
    
    
    def open(self, modify):
        return True
    

class PersistenceImpl(object):
    
    """
    Persistence layer for the drbdmanage control volume
    """
    
    _file       = None
    _server     = None
    _writeable  = False
    _hash_obj   = None
    
    IDX_NAME       = "index"
    NODES_OFF_NAME = "nodes_off"
    NODES_LEN_NAME = "nodes_len"
    RES_OFF_NAME   = "res_off"
    RES_LEN_NAME   = "res_len"
    ASSG_OFF_NAME  = "assg_off"
    ASSG_LEN_NAME  = "assg_len"
    HASH_NAME      = "hash"
    
    BLKSZ       = 0x1000 # 4096
    IDX_OFFSET  = 0x1800 # 6144
    IDX_MAXLEN  =  0x400 # 1024
    HASH_OFFSET = 0x1C00 # 6400
    HASH_MAXLEN = 0x0100 #  256
    DATA_OFFSET = 0x2000 # 8192
    ZEROFILLSZ  = 0x0400 # 1024
    # FIXME: This should probably use the DRBDCTRL_RES_NAME from server.py
    #CONF_FILE   = "/dev/drbd/by-res/.drbdctrl/0"
    CONF_FILE   = "/dev/drbd0"
    
    MMAP_BUFSZ  = 0x100000 # 1048576 == 1 MiB
    
    # FIXME: That constant should probably not be here, but there does not
    #        seem to be a good way to get it otherwise
    BLKFLSBUF = 0x00001261 # <include/linux/fs.h>
    
    # fail counter for attempts to open the config file (CONF_FILE)
    MAX_FAIL_COUNT = 10
    
    # wait 2 seconds before every open() retry if the file was not found
    ENOENT_REOPEN_TIMER = 2
    # wait at least half a second between open() retries
    MIN_REOPEN_TIMER    = 0.5
    
    
    def __init__(self):
        pass
    
    
    def open(self, modify):
        """
        Open the persistent storage for reading or writing, depending on
        the modify flag. If (modify == True), open for writing, otherwise
        open readonly.
        """
        fn_rc = False
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
                file_fd = os.open(self.CONF_FILE, modeflags)
                
                # Try to flush the buffer cache
                # This can fail depending on the type of the file's
                # underlying device
                try:
                    fcntl.ioctl(file_fd, self.BLKFLSBUF)
                except OSError:
                    pass
                except IOError:
                    pass
                
                self._file = os.fdopen(file_fd, mode)
                self._writeable = modify
                fn_rc = True
                break
            except IOError as ioerr:
                fail  = True
                error = ioerr.errno
            except OSError as oserr:
                fail  = True
                error = oserr.errno
            if fail:
                fail_ctr += 1
                if error == errno.ENOENT:
                    logging.error("cannot open control volume '%s': "
                      "object not found" % self.CONF_FILE)
                    secs = self.ENOENT_REOPEN_TIMER
                else:
                    rnd_byte = os.urandom(1)
                    secs = float(ord(rnd_byte)) / 100 + self.MIN_REOPEN_TIMER
                time.sleep(secs)
        if not fail_ctr < self.MAX_FAIL_COUNT:
            logging.error("cannot open control volume '%s' "
              "(%d failed attempts)"
              % (self.CONF_FILE, self.MAX_FAIL_COUNT))
        return fn_rc
        
    
    def save(self, nodes, resources):
        """
        Saves the configuration to the drbdmanage control volume
        
        The persistent storage must have been opened for writing before
        calling save(). See open().
        
        @raise   PersistenceException: on I/O error
        @raise   IOError: if no writable file descriptor is open
        """
        if self._writeable:
            try:
                p_index_con = dict()
                p_nodes_con = dict()
                p_res_con   = dict()
                p_assg_con  = dict()
                data_hash   = DataHash()
                
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
                data_hash.update(save_data)
                self._file.write(save_data)
                nodes_len = self._file.tell() - nodes_off
                self._file.write(chr(0))
                
                self._align_zero_fill()
                
                res_off = self._file.tell()
                save_data = self._container_to_json(p_res_con)
                self._file.write(save_data)
                data_hash.update(save_data)
                res_len = self._file.tell() - res_off
                self._file.write(chr(0))
                
                self._align_zero_fill()
                
                assg_off = self._file.tell()
                save_data = self._container_to_json(p_assg_con)
                self._file.write(save_data)
                data_hash.update(save_data)
                assg_len = self._file.tell() - assg_off
                self._file.write(chr(0))
                
                # clean up behind the assignment
                self._align_zero_fill()
                
                self._file.seek(self.IDX_OFFSET)
                p_index = {
                  self.NODES_OFF_NAME : nodes_off,
                  self.NODES_LEN_NAME : nodes_len,
                  self.RES_OFF_NAME   : res_off,
                  self.RES_LEN_NAME   : res_len,
                  self.ASSG_OFF_NAME  : assg_off,
                  self.ASSG_LEN_NAME  : assg_len
                }
                p_index_con[self.IDX_NAME] = p_index
                save_data = self._container_to_json(p_index_con)
                self._file.write(save_data)
                self._file.write(chr(0))
                
                computed_hash = data_hash.get_hex_hash()
                self.update_stored_hash(computed_hash)
                logging.debug("save/hash: %s" % computed_hash)
                self._hash_obj = data_hash
            except Exception as exc:
                logging.error("cannot save data tables, "
                  "encountered exception: %s" % str(exc))
                raise PersistenceException
        else:
            # file not open for writing
            raise IOError("Persistence save() without a "
              "writeable file descriptor")
    
    
    # Get the hash of the configuration on persistent storage
    def get_stored_hash(self):
        """
        Retrieves the hash code of the stored configuration
        
        The hash code stored with the configuration on the drbdmanage
        control volume is read and returned without updating the
        hash code currently known to the server.
        Used to compare the server's known hash code with the hash code on
        the control volume, to detect whether the configuration has changed.
        
        @return: hash code from the drbdmanage control volume
        @rtype:  str
        """
        stored_hash = None
        if self._file is not None:
            try:
                data_hash = DataHash()
                self._file.seek(self.HASH_OFFSET)
                hash_json = self._null_trunc(self._file.read(self.HASH_MAXLEN))
                hash_con = self._json_to_container(hash_json)
                stored_hash = hash_con.get(self.HASH_NAME)
            except Exception:
                raise PersistenceException
        else:
            # file not open
            raise IOError("Persistence get_stored_hash() without an "
              "open file descriptor")
        return stored_hash
    
    
    def update_stored_hash(self, hex_hash):
        """
        Updates the hash code of the stored configuration
        
        @param   hex_hash: hexadecimal string representation of the hash code
        """
        if self._file is not None and self._writeable:
            try:
                self._file.seek(self.HASH_OFFSET)
                hash_con = { self.HASH_NAME : hex_hash }
                hash_json = self._container_to_json(hash_con)
                self._file.write(hash_json)
                self._file.write(chr(0))
            except Exception:
                raise PersistenceException
        else:
            raise IOError("Persistence update_stored_hash() without a "
              "writeable file descriptor")
    
    
    def load(self, nodes, resources):
        """
        Loads the configuration from the drbdmanage control volume
        
        The persistent storage must have been opened before calling load().
        See open().
        
        @raise   PersistenceException: on I/O error
        @raise   IOError: if no file descriptor is open
        """
        errors = False
        if self._file is not None:
            try:
                data_hash = DataHash()
                self._file.seek(self.IDX_OFFSET)
                f_index = self._null_trunc(self._file.read(self.IDX_MAXLEN))
                p_index_con = self._json_to_container(f_index)
                p_index = p_index_con[self.IDX_NAME]
                nodes_off  = p_index[self.NODES_OFF_NAME]
                nodes_len  = p_index[self.NODES_LEN_NAME]
                res_off    = p_index[self.RES_OFF_NAME]
                res_len    = p_index[self.RES_LEN_NAME]
                assg_off   = p_index[self.ASSG_OFF_NAME]
                assg_len   = p_index[self.ASSG_LEN_NAME]
                
                nodes_con = None
                res_con   = None
                assg_con  = None
                
                self._file.seek(nodes_off)
                load_data = self._null_trunc(self._file.read(nodes_len))
                data_hash.update(load_data)
                try:
                    nodes_con = self._json_to_container(load_data)
                except Exception:
                    pass
                
                self._file.seek(res_off)
                load_data = self._null_trunc(self._file.read(res_len))
                data_hash.update(load_data)
                try:
                    res_con   = self._json_to_container(load_data)
                except Exception:
                    pass
                
                self._file.seek(assg_off)
                load_data = self._null_trunc(self._file.read(assg_len))
                data_hash.update(load_data)
                try:
                    assg_con  = self._json_to_container(load_data)
                except Exception:
                    pass
                
                computed_hash = data_hash.get_hex_hash()
                stored_hash   = self.get_stored_hash()
                logging.debug("load/hash: %s" % stored_hash)
                if computed_hash != stored_hash:
                    logging.warning("information in the data tables "
                      "does not match its signature")
                # TODO: if the signature is wrong, load an earlier backup
                #       of the configuration
                
                nodes.clear()
                if nodes_con is not None:
                    for properties in nodes_con.itervalues():
                        node = DrbdNodePersistence.load(properties)
                        if node is not None:
                            nodes[node.get_name()] = node
                        else:
                            logging.debug("persistence: Failed to load a "
                              "DrbdNode object")
                            errors = True
                
                resources.clear()
                if res_con is not None:
                    for properties in res_con.itervalues():
                        resource = DrbdResourcePersistence.load(properties)
                        if resource is not None:
                            resources[resource.get_name()] = resource
                        else:
                            logging.debug("persistence: Failed to load a "
                              "DrbdResource object")
                            errors = True
                
                if assg_con is not None:
                    for properties in assg_con.itervalues():
                        assignment = AssignmentPersistence.load(properties,
                          nodes, resources)
                        if assignment is None:
                            logging.debug("persistence: Failed to load an "
                              "Assignment object")
                            errors = True
                    self._hash_obj = data_hash
            except Exception as exc:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                logging.debug("persistence: load failed: "
                  "Exception %s (%s), %s\n%s"
                  % (str(exc), exc_type, exc_obj, exc_tb))
                raise PersistenceException
        else:
            # file not open
            logging.debug("persistence: data tables load attempted before "
              "opening the control volume")
            raise IOError("data tables load attempted before opening the "
              "control volume")
        if errors:
            raise PersistenceException
    
    
    def close(self):
        """
        Closes the persistent storage
        
        Can be called multiple times and/or no matter whether the persistent
        storage is actually open without causing any error.
        """
        try:
            if self._file is not None:
                if self._writeable:
                    self._file.flush()
                    os.fsync(self._file.fileno())
        except IOError:
            pass
        finally:
            if self._file is not None:
                self._file.close()
            self._writeable = False
            self._file = None
    
    
    def get_hash_obj(self):
        """
        Returns the DataHash object used by this instance
        
        The DataHash object that is used to calculate the hash code of the
        configuration is returned.
        
        @return: DataHash object. See drbdmanage.utils
        """
        return self._hash_obj
    
    
    def _container_to_json(self, container):
        """
        Serializes a dictionary into a JSON string
        
        Indent level is 4 spaces, keys are sorted.
        
        @param   container: the data collection to serialize into a JSON string
        @type    container: dict
        @return: JSON representation of the container
        @rtype:  str
        """
        return (json.dumps(container, indent=4, sort_keys=True) + "\n")
    
    
    def _json_to_container(self, json_doc):
        """
        Deserializes a JSON string into a dictionary
        
        @param   json_doc: the JSON string to deserialize
        @type    json_doc: str (or compatible)
        @return: data collection (key/value) deserialized from the JSON string
        @rtype:  dict
        """
        return json.loads(json_doc)
    
    
    def _align_offset(self):
        """
        Aligns the file offset on the next block boundary
        
        The file offset for reading or writing is advanced to the next block
        boundary as specified by BLKSZ.
        """
        if self._file is not None:
            offset = self._file.tell()
            if offset % self.BLKSZ != 0:
                offset = ((offset / self.BLKSZ) + 1) * self.BLKSZ
                self._file.seek(offset)
    
    
    def _align_zero_fill(self):
        """
        Fills the file with zero bytes up to the next block boundary
        
        The file is filled with zero bytes from the current file offset up to
        the next block boundary as specified by BLKSZ.
        """
        
        if self._file is not None:
            offset = self._file.tell()
            if offset % self.BLKSZ != 0:
                fillbuf = ('\0' * self.ZEROFILLSZ)
                blk  = ((offset / self.BLKSZ) + 1) * self.BLKSZ
                diff = blk - offset
                fillnr = diff / self.ZEROFILLSZ
                ctr = 0
                while ctr < fillnr:
                    self._file.write(fillbuf)
                    ctr += 1
                diff -= (self.ZEROFILLSZ * fillnr)
                self._file.write(fillbuf[:diff])
    
    
    def _null_trunc(self, data):
        """
        Returns the supplied data truncated at the first zero byte
        
        Used for sanitizing JSON strings read from the persistent storage
        before passing them to the JSON parser, because the JSON parser does
        not like to see any data behind the end of a JSON string.
        
        @return: data truncated at the first zero byte
        @rtype:  str
        """
        idx = data.find(chr(0))
        if idx != -1:
            data = data[:idx]
        return data
    
    
    def _next_json(self, stream):
        """
        Extracts JSON documents from a stream of multiple JSON documents
        
        Looks for lines that only contain a single "{" or "}" byte to identify
        beginning and end of JSON documents.
        
        The current persistence implementation does not write or read multiple
        JSON documents to or from the same string, so this function is
        currently unused.
        
        @return: the next JSON document
        @rtype:  str
        """
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
    
    """
    Serializes/deserializes DrbdNode objects
    """
    
    SERIALIZABLE = [ "_name", "_addr", "_addrfam", "_state",
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
              properties["_addr"],
              int(properties["_addrfam"])
              )
            node.set_state(long(properties["_state"]))
            node.set_poolsize(long(properties["_poolsize"]))
            node.set_poolfree(long(properties["_poolfree"]))
        except Exception as exc:
            # FIXME
            raise exc
        return node


class DrbdResourcePersistence(GenericPersistence):
    
    """
    Serializes/deserializes DrbdResource objects
    """
    
    SERIALIZABLE = [ "_name", "_secret", "_port", "_state" ]
    
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
            resource = DrbdResource(properties["_name"], properties["_port"])
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
    
    """
    Serializes/deserializes DrbdVolume objects
    """
    
    SERIALIZABLE = [ "_id", "_state", "_size_kiB" ]
    
    
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
              long(properties["_size_kiB"]),
              minor
              )
            volume.set_state(long(properties["_state"]))
        except Exception as exc:
            # FIXME
            raise exc
        return volume


class AssignmentPersistence(GenericPersistence):
    
    """
    Serializes/deserializes Assignment objects
    """
    
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
    
    """
    Serializes/deserializes DrbdVolumeState objects
    """
    
    SERIALIZABLE = [ "_bd_path", "_blockdevice", "_cstate", "_tstate" ]
    
    
    def __init__(self, vol_state):
        super(DrbdVolumeStatePersistence, self).__init__(vol_state)
    
    
    def save(self, container):
        vol_state = self.get_object()
        vol_id = vol_state.get_id()
        properties = self.load_dict(self.SERIALIZABLE)
        properties["id"] = vol_id
        container[vol_id] = properties
    
    
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
