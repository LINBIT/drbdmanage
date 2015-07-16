#!/usr/bin/env python2
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2013, 2014   LINBIT HA-Solutions GmbH
                               Author: R. Altnoeder

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import sys
import os
import fcntl
import errno
import time
import json
import logging
import traceback
import drbdmanage.server
import drbdmanage.consts
import drbdmanage.snapshots.persistence as snapspers

from drbdmanage.exceptions import PersistenceException
from drbdmanage.utils import DataHash
from drbdmanage.utils import map_val_or_dflt
from drbdmanage.utils import read_lines
from drbdmanage.persistence import GenericPersistence
from drbdmanage.storage.storagecore import MinorNr
from drbdmanage.drbd.drbdcore import (
    DrbdCommon, DrbdNode, DrbdResource, DrbdVolume, DrbdVolumeState, Assignment
)


def persistence_impl(ref_server):
    """
    Return the persistence implementation of the drbdmanage control volume

    This function serves for easy and centralized replacement of
    the class that implements persistence (saving object state to disk)

    @return: persistence layer object
    """
    return PersistenceImpl(ref_server)


class PersistenceImplDummy(object):
    def __init__(self, ref_server):
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

    # crc32 of "drbdmanage control volume"
    PERSISTENCE_MAGIC   = "\x1a\xdb\x98\xa2";

    # serial number, big-endian
    PERSISTENCE_VERSION = "\x00\x00\x00\x01";

    _file       = None
    _server     = None
    _writeable  = False
    _hash_obj   = None
    _server     = None

    IDX_NAME        = "index"
    NODES_OFF_NAME  = "nodes_off"
    NODES_LEN_NAME  = "nodes_len"
    RES_OFF_NAME    = "res_off"
    RES_LEN_NAME    = "res_len"
    ASSG_OFF_NAME   = "assg_off"
    ASSG_LEN_NAME   = "assg_len"
    CCONF_OFF_NAME  = "cconf_off"
    CCONF_LEN_NAME  = "cconf_len"
    COMMON_OFF_NAME = "common_off"
    COMMON_LEN_NAME = "common_len"
    HASH_NAME       = "hash"

    BLKSZ          = 0x1000 # 4096
    MAGIC_OFFSET   = 0x1000 # 4096
    VERSION_OFFSET = 0x1004 # 4100
    IDX_OFFSET     = 0x1800 # 6144
    IDX_MAXLEN     =  0x400 # 1024
    HASH_OFFSET    = 0x1C00 # 6400
    HASH_MAXLEN    = 0x0100 #  256
    DATA_OFFSET    = 0x2000 # 8192
    ZEROFILLSZ     = 0x0400 # 1024
    CONF_FILE      = drbdmanage.consts.DRBDCTRL_DEV

    MMAP_BUFSZ = 0x100000 # 1048576 == 1 MiB

    # FIXME: That constant should probably not be here, but there does not
    #        seem to be a good way to get it otherwise
    BLKFLSBUF = 0x00001261 # <include/linux/fs.h>

    # fail counter for attempts to open the config file (CONF_FILE)
    MAX_FAIL_COUNT = 10

    # wait 2 seconds before every open() retry if the file was not found
    ENOENT_REOPEN_TIMER = 2
    # wait at least half a second between open() retries
    MIN_REOPEN_TIMER    = 0.5


    def __init__(self, ref_server):
        self._server = ref_server


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
                    logging.error(
                        "cannot open control volume '%s': "
                        "object not found" % (self.CONF_FILE))
                    secs = self.ENOENT_REOPEN_TIMER
                else:
                    rnd_byte = os.urandom(1)
                    secs = float(ord(rnd_byte)) / 100 + self.MIN_REOPEN_TIMER
                time.sleep(secs)
        if not fail_ctr < self.MAX_FAIL_COUNT:
            logging.error(
                "cannot open control volume '%s' "
                "(%d failed attempts)"
                % (self.CONF_FILE, self.MAX_FAIL_COUNT)
            )
        return fn_rc


    def save(self, objects_root):
        """
        Saves the configuration to the drbdmanage control volume

        The persistent storage must have been opened for writing before
        calling save(). See open().

        @raise   PersistenceException: on I/O error
        @raise   IOError: if no writable file descriptor is open
        """
        if self._writeable:
            try:
                p_index_con   = {}
                p_nodes_con   = {}
                p_res_con     = {}
                p_assg_con    = {}
                p_common_con  = {}
                data_hash     = DataHash()

                cconf_key     = drbdmanage.server.DrbdManageServer.OBJ_CCONF_NAME
                nodes_key     = drbdmanage.server.DrbdManageServer.OBJ_NODES_NAME
                resources_key = drbdmanage.server.DrbdManageServer.OBJ_RESOURCES_NAME
                common_key    = drbdmanage.server.DrbdManageServer.OBJ_COMMON_NAME
                cluster_conf  = objects_root[cconf_key]
                nodes         = objects_root[nodes_key]
                resources     = objects_root[resources_key]
                common        = objects_root[common_key]

                # Prepare nodes container and build assignments list
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

                # Prepare common DRBD options container
                p_common = DrbdCommonPersistence(common)
                p_common.save(p_common_con)

                # Save data
                self._file.seek(self.MAGIC_OFFSET)
                self._file.write(self.PERSISTENCE_MAGIC)

                self._file.seek(self.VERSION_OFFSET)
                self._file.write(self.PERSISTENCE_VERSION)

                self._file.seek(self.DATA_OFFSET)

                nodes_off = self._file.tell()
                save_data = self._container_to_json(p_nodes_con)
                self._file.write(save_data)
                data_hash.update(save_data)
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

                self._align_zero_fill()

                cconf_off = self._file.tell()
                save_data = self._container_to_json(cluster_conf)
                self._file.write(save_data)
                data_hash.update(save_data)
                cconf_len = self._file.tell() - cconf_off
                self._file.write(chr(0))

                self._align_zero_fill()

                common_off = self._file.tell()
                save_data = self._container_to_json(p_common_con)
                self._file.write(save_data)
                data_hash.update(save_data)
                common_len = self._file.tell() - common_off
                self._file.write(chr(0))

                # clean up to the end of the block
                self._align_zero_fill()

                self._file.seek(self.IDX_OFFSET)
                p_index = {
                    self.NODES_OFF_NAME  : nodes_off,
                    self.NODES_LEN_NAME  : nodes_len,
                    self.RES_OFF_NAME    : res_off,
                    self.RES_LEN_NAME    : res_len,
                    self.ASSG_OFF_NAME   : assg_off,
                    self.ASSG_LEN_NAME   : assg_len,
                    self.CCONF_OFF_NAME  : cconf_off,
                    self.CCONF_LEN_NAME  : cconf_len,
                    self.COMMON_OFF_NAME : common_off,
                    self.COMMON_LEN_NAME : common_len
                }
                p_index_con[self.IDX_NAME] = p_index
                save_data = self._container_to_json(p_index_con)
                self._file.write(save_data)
                self._file.write(chr(0))

                self._align_zero_fill()

                computed_hash = data_hash.get_hex_hash()
                self.update_stored_hash(computed_hash)
                logging.debug("save/hash: %s" % (computed_hash))
                self._hash_obj = data_hash
            except Exception as exc:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                logging.error(
                    "cannot save data tables, Exception=%s"
                    % (str(exc_type))
                )
                logging.debug(
                    "persistence: save failed: Exception=%s: %s"
                    % (exc_type, exc_obj)
                )
                logging.debug("*** begin stack trace")
                for line in traceback.format_tb(exc_tb):
                    logging.debug("    %s" % (line))
                logging.debug("*** end stack trace")
                raise PersistenceException
        else:
            # file not open for writing
            raise IOError(
                "Persistence save() without a writeable file descriptor"
            )


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
            raise IOError(
                "Persistence get_stored_hash() without an open file descriptor"
            )
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
            raise IOError(
                "Persistence update_stored_hash() without a "
                "writeable file descriptor"
            )


    def load(self, objects_root):
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
                self._file.seek(self.MAGIC_OFFSET)
                magic = self._file.read(len(self.PERSISTENCE_MAGIC))
                if magic != self.PERSISTENCE_MAGIC:
                    logging.error(
                        "Unusable control volume, "
                        "the control volume magic number is missing"
                    )
                    raise PersistenceException

                self._file.seek(self.VERSION_OFFSET)
                version = self._file.read(len(self.PERSISTENCE_VERSION))
                if version != self.PERSISTENCE_VERSION:
                    logging.error(
                        "Can not load data tables, "
                        "control volume version does not match server version"
                    )
                    raise PersistenceException

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
                cconf_off  = p_index[self.CCONF_OFF_NAME]
                cconf_len  = p_index[self.CCONF_LEN_NAME]
                common_off = p_index[self.COMMON_OFF_NAME]
                common_len = p_index[self.COMMON_LEN_NAME]

                nodes_con  = None
                res_con    = None
                assg_con   = None
                cconf_con  = None
                common_con = None

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
                    errors = True

                self._file.seek(assg_off)
                load_data = self._null_trunc(self._file.read(assg_len))
                data_hash.update(load_data)
                try:
                    assg_con  = self._json_to_container(load_data)
                except Exception:
                    errors = True

                self._file.seek(cconf_off)
                load_data = self._null_trunc(self._file.read(cconf_len))
                data_hash.update(load_data)
                try:
                    cconf_con = self._json_to_container(load_data)
                except Exception:
                    errors = True

                self._file.seek(common_off)
                load_data = self._null_trunc(self._file.read(common_len))
                data_hash.update(load_data)
                try:
                    common_con = self._json_to_container(load_data)
                except Exception:
                    errors = True

                # Discard the (potentially large) JSON string
                # to free some memory as soon as possible
                load_data = None

                computed_hash = data_hash.get_hex_hash()
                stored_hash   = self.get_stored_hash()
                logging.debug("load/hash: %s" % stored_hash)
                if computed_hash != stored_hash:
                    logging.warning(
                        "information in the data tables does not match "
                        "its signature"
                    )
                # TODO: if the signature is wrong, load an earlier backup
                #       of the configuration

                # Cache the currently loaded assignment objects
                # (Required later to figure out which assignments have been added or
                # removed after reloading the configuration)
                nodes_key = drbdmanage.server.DrbdManageServer.OBJ_NODES_NAME
                nodes = objects_root[nodes_key]

                assg_map_cache = {}
                for node in nodes.itervalues():
                    node_assg_map = {}
                    for assg in node.iterate_assignments():
                        node_assg_map[assg.get_resource().get_name()] = assg
                    if len(node_assg_map) > 0:
                        assg_map_cache[node.get_name()] = node_assg_map

                # Load DrbdNode objects from data tables
                nodes.clear()
                if nodes_con is not None:
                    for properties in nodes_con.itervalues():
                        node = DrbdNodePersistence.load(
                            properties,
                            self._server.get_serial
                        )
                        if node is not None:
                            nodes[node.get_name()] = node
                        else:
                            logging.debug(
                                "persistence: Failed to load a DrbdNode object"
                            )
                            errors = True

                # Load DrbdResource objects from data tables
                resources_key = drbdmanage.server.DrbdManageServer.OBJ_RESOURCES_NAME
                resources = objects_root[resources_key]
                resources.clear()
                if res_con is not None:
                    for properties in res_con.itervalues():
                        resource = DrbdResourcePersistence.load(
                            properties,
                            self._server.get_serial
                        )
                        if resource is not None:
                            resources[resource.get_name()] = resource
                        else:
                            logging.debug(
                                "persistence: Failed to load a "
                                "DrbdResource object"
                            )
                            errors = True

                # Load and reestablish Assignment objects from data tables
                if assg_con is not None:
                    for properties in assg_con.itervalues():
                        assignment = AssignmentPersistence.load(
                            properties, nodes, resources,
                            self._server.get_serial
                        )
                        if assignment is None:
                            logging.debug(
                                "persistence: Failed to load an Assignment object"
                            )
                            errors = True


                # Reestablish assignments signals
                for node in nodes.itervalues():
                    node_name = node.get_name()
                    node_assg_map = assg_map_cache.get(node_name)
                    for cur_assg in node.iterate_assignments():
                        res_name = cur_assg.get_resource().get_name()
                        prev_assg = None
                        if node_assg_map is not None:
                            prev_assg = node_assg_map.get(res_name)
                        if prev_assg is not None:
                            signal = prev_assg.get_signal()
                            cur_assg.set_signal(signal)
                            del node_assg_map[res_name]
                        else:
                            signal = self._server.create_signal(
                                "assignments/" + node_name + "/" + res_name
                            )
                            cur_assg.set_signal(signal)
                    if node_assg_map is not None and len(node_assg_map) == 0:
                        del assg_map_cache[node_name]
                for node_assg_map in assg_map_cache.itervalues():
                    for prev_assg in node_assg_map.itervalues():
                        prev_assg.notify_removed()

                # Load the cluster configuration
                cconf_key = drbdmanage.server.DrbdManageServer.OBJ_CCONF_NAME
                cluster_conf = objects_root[cconf_key]
                cluster_conf.clear()
                cluster_conf.update(cconf_con)

                # Load the common DRBD setup options object from data tables
                if common_con is not None:
                    common = DrbdCommonPersistence.load(
                        common_con, self._server.get_serial
                    )
                    if common is not None:
                        common_key = drbdmanage.server.DrbdManageServer.OBJ_COMMON_NAME
                        objects_root[common_key] = common
                    else:
                        logging.debug(
                            "persistence: Failed to load the "
                            "DrbdCommon object"
                        )
                        errors = True

                if not errors:
                    self._hash_obj = data_hash
            except Exception as exc:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                logging.error(
                    "cannot load data tables, Exception=%s"
                    % (str(exc_type))
                )
                logging.debug(
                    "persistence: load failed: Exception=%s: %s"
                    % (exc_type, exc_obj)
                )
                logging.debug("*** begin stack trace")
                for line in traceback.format_tb(exc_tb):
                    logging.debug("    %s" % (line))
                logging.debug("*** end stack trace")
                raise PersistenceException
        else:
            # file not open
            errmsg = ("data tables load attempted before opening "
                      "the control volume")
            logging.debug("persistence: " + errmsg)
            raise IOError(errmsg)
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
        for cfgline in read_lines(stream):
            if cfgline == "{\n":
                read = True
            if read:
                if json_blk is None:
                    json_blk = ""
                json_blk += cfgline
            if cfgline == "}\n":
                break
        return json_blk


class DrbdCommonPersistence(GenericPersistence):
    """
    Serializes/deserializes the DrbdCommon object
    """

    SERIALIZABLE = []


    def __init__(self, common):
        super(DrbdCommonPersistence, self).__init__(common)


    def save(self, container):
        common = self.get_object()
        # Put the data directly into the container
        container.update(self.load_dict(self.SERIALIZABLE))
        container["props"] = common.get_props().get_all_props()


    @classmethod
    def load(cls, properties, get_serial_fn):
        common = None
        try:
            init_props = properties.get("props")
            common = DrbdCommon(get_serial_fn, None, init_props)
        except Exception as exc:
            # FIXME
            raise exc
        return common


class DrbdNodePersistence(GenericPersistence):

    """
    Serializes/deserializes DrbdNode objects
    """

    SERIALIZABLE = ["_name", "_addr", "_addrfam", "_node_id", "_state",
                    "_poolsize", "_poolfree"]


    def __init__(self, node):
        super(DrbdNodePersistence, self).__init__(node)


    def save(self, container):
        node = self.get_object()
        properties  = self.load_dict(self.SERIALIZABLE)
        properties["props"] = node.get_props().get_all_props()
        container[node.get_name()] = properties


    @classmethod
    def load(cls, properties, get_serial_fn):
        node = None
        try:
            init_props  = properties.get("props")
            state       = long(map_val_or_dflt(properties, "_state", 0))
            poolsize    = long(map_val_or_dflt(properties, "_poolsize", -1))
            poolfree    = long(map_val_or_dflt(properties, "_poolfree", -1))
            node = DrbdNode(
                properties["_name"],
                properties["_addr"],
                int(properties["_addrfam"]),
                int(properties["_node_id"]),
                state,
                poolsize,
                poolfree,
                get_serial_fn,
                None,
                init_props
            )
        except Exception as exc:
            # FIXME
            raise exc
        return node


class DrbdResourcePersistence(GenericPersistence):

    """
    Serializes/deserializes DrbdResource objects
    """

    SERIALIZABLE = ["_name", "_secret", "_port", "_state"]

    def __init__(self, resource):
        super(DrbdResourcePersistence, self).__init__(resource)


    def save(self, container):
        resource = self.get_object()
        properties = self.load_dict(self.SERIALIZABLE)
        volume_list = {}
        # Save volumes
        for volume in resource.iterate_volumes():
            p_vol = DrbdVolumePersistence(volume)
            p_vol.save(volume_list)
        properties["volumes"] = volume_list

        # Save the DrbdSnapshot objects
        snapshot_list = {}
        for snapshot in resource.iterate_snapshots():
            p_snaps = snapspers.DrbdSnapshotPersistence(snapshot)
            p_snaps.save(snapshot_list)
        properties["snapshots"] = snapshot_list

        # Save properties
        properties["props"] = resource.get_props().get_all_props()

        container[resource.get_name()] = properties


    @classmethod
    def load(cls, properties, get_serial_fn):
        resource = None
        try:
            init_props    = properties.get("props")
            secret        = properties.get("_secret")
            state         = properties.get("_state")
            volume_list   = properties["volumes"]
            snapshot_list = properties["snapshots"]

            # Load DrbdVolume objects
            init_volumes = []
            for vol_properties in volume_list.itervalues():
                volume = DrbdVolumePersistence.load(
                    vol_properties,
                    get_serial_fn
                )
                init_volumes.append(volume)

            # Create the DrbdResource object
            resource = DrbdResource(
                properties["_name"], properties["_port"],
                secret, state, init_volumes,
                get_serial_fn, None, init_props
            )

            # Load DrbdSnapshot objects
            for snaps_properties in snapshot_list.itervalues():
                snapshot = snapspers.DrbdSnapshotPersistence.load(
                    snaps_properties, resource, get_serial_fn
                )
                resource.init_add_snapshot(snapshot)
        except Exception as exc:
            raise exc
        return resource


class DrbdVolumePersistence(GenericPersistence):

    """
    Serializes/deserializes DrbdVolume objects
    """

    SERIALIZABLE = ["_id", "_state", "_size_kiB"]


    def __init__(self, volume):
        super(DrbdVolumePersistence, self).__init__(volume)


    def save(self, container):
        volume = self.get_object()
        properties  = self.load_dict(self.SERIALIZABLE)
        minor = volume.get_minor()
        properties["minor"] = minor.get_value()
        properties["props"] = volume.get_props().get_all_props()
        container[volume.get_id()] = properties


    @classmethod
    def load(cls, properties, get_serial_fn):
        volume = None
        try:
            minor_nr = properties["minor"]
            minor = MinorNr(minor_nr)
            init_props  = properties.get("props")
            volume = DrbdVolume(
                properties["_id"],
                long(properties["_size_kiB"]),
                minor,
                properties["_state"],
                get_serial_fn,
                None,
                init_props
            )
        except Exception as exc:
            # FIXME
            raise exc
        return volume


class AssignmentPersistence(GenericPersistence):

    """
    Serializes/deserializes Assignment objects
    """

    SERIALIZABLE = ["_node_id", "_cstate", "_tstate", "_rc"]


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

        properties["node"] = node_name
        properties["resource"] = res_name
        assg_name = node_name + ":" + res_name

        properties["props"] = node.get_props().get_all_props()

        # Save the DrbdVolumeState objects
        vol_state_list = {}
        for vol_state in assignment.iterate_volume_states():
            p_vol_state = DrbdVolumeStatePersistence(vol_state)
            p_vol_state.save(vol_state_list)
        properties["volume_states"] = vol_state_list

        # Save the DrbdSnapshotAssignment objects
        snaps_assgs_list = {}
        for snaps_assg in assignment.iterate_snaps_assgs():
            p_snaps_assg = snapspers.DrbdSnapshotAssignmentPersistence(
                snaps_assg
            )
            p_snaps_assg.save(snaps_assgs_list)
        properties["snapshot_assignments"] = snaps_assgs_list

        container[assg_name] = properties


    @classmethod
    def load(cls, properties, nodes, resources, get_serial_fn):
        assignment = None
        try:
            node       = nodes[properties["node"]]
            resource   = resources[properties["resource"]]
            init_props = properties.get("props")

            # Load the DrbdVolumeState objects
            vol_states = []
            vol_state_list = properties["volume_states"]
            for vol_state_props in vol_state_list.itervalues():
                vol_state = DrbdVolumeStatePersistence.load(
                    vol_state_props, resource, get_serial_fn
                )
                vol_states.append(vol_state)

            assignment = Assignment(
                node,
                resource,
                int(properties["_node_id"]),
                long(properties["_cstate"]),
                long(properties["_tstate"]),
                properties["_rc"],
                vol_states,
                get_serial_fn,
                None,
                init_props
            )

            # Load the DrbdSnapshotAssignment objects
            snaps_assgs_list = properties["snapshot_assignments"]
            for snaps_assg_props in snaps_assgs_list.itervalues():
                snaps_assg = snapspers.DrbdSnapshotAssignmentPersistence.load(
                    snaps_assg_props, assignment, get_serial_fn
                )
                assignment.init_add_snaps_assg(snaps_assg)

            node.init_add_assignment(assignment)
            resource.init_add_assignment(assignment)

            # Link the DrbdSnapshotAssignment objects into the assignments
            # list of their corresponding DrbdSnapshot objects
            for snaps_assg in assignment.iterate_snaps_assgs():
                snapshot = snaps_assg.get_snapshot()
                snapshot.init_add_snaps_assg(snaps_assg)
        except Exception as exc:
            # FIXME
            raise exc
        return assignment


class DrbdVolumeStatePersistence(GenericPersistence):

    """
    Serializes/deserializes DrbdVolumeState objects
    """

    SERIALIZABLE = ["_bd_path", "_bd_name", "_cstate", "_tstate"]


    def __init__(self, vol_state):
        super(DrbdVolumeStatePersistence, self).__init__(vol_state)


    def save(self, container):
        vol_state = self.get_object()
        vol_id = vol_state.get_id()
        properties = self.load_dict(self.SERIALIZABLE)
        properties["id"]    = vol_id
        properties["props"] = vol_state.get_props().get_all_props()
        container[vol_id] = properties


    @classmethod
    def load(cls, properties, resource, get_serial_fn):
        vol_state = None
        try:
            volume = resource.get_volume(properties["id"])

            init_props  = properties.get("props")

            vol_state   = DrbdVolumeState(
                volume,
                properties["_cstate"], properties["_tstate"],
                properties.get("_bd_name"), properties.get("_bd_path"),
                get_serial_fn, None, init_props
            )
        except Exception as exc:
            # FIXME
            raise exc
        return vol_state
