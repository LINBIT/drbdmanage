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
import drbdmanage.propscontainer as propscon

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
    return PersistenceDualImpl(ref_server)


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


class BasePersistence(object):


    NODES_KEY  = "nodes"
    RES_KEY    = "res"
    ASSG_KEY   = "assg"
    CCONF_KEY  = "cconf"
    COMMON_KEY = "common"

    # Reference to the server instance
    _server = None

    # Buffered JSON data and its hash
    _json_data      = None
    _json_data_hash = None


    def __init__(self, ref_server):
        self._json_data = ""
        self._server = ref_server


    def set_json_data(self, data):
        self._json_data = str(data)
        data_hash = DataHash()
        data_hash.update(self._json_data)
        self._json_data_hash = data_hash.get_hex_hash()


    def get_json_data(self):
        return self._json_data


    def get_stored_hash(self):
        if self._json_data_hash is None:
            raise PersistenceException
        return self._json_data_hash


    def container_to_json(self, container):
        """
        Serializes a dictionary into a JSON string

        Indent level is 4 spaces, keys are sorted.

        @param   container: the data collection to serialize into a JSON string
        @type    container: dict
        @return: JSON representation of the container
        @rtype:  str
        """
        return (json.dumps(container, indent=4, sort_keys=True) + "\n")


    def json_to_container(self, json_doc):
        """
        Deserializes a JSON string into a dictionary

        @param   json_doc: the JSON string to deserialize
        @type    json_doc: str (or compatible)
        @return: data collection (key/value) deserialized from the JSON string
        @rtype:  dict
        """
        return json.loads(json_doc)


    def load_containers(self, objects_root, nodes_con, res_con, assg_con, cconf_con, common_con):
        """
        Loads drbdmanage objects from their key/value maps
        """
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

        # Load nodes
        loaded_nodes = {}
        for properties in nodes_con.itervalues():
            node = DrbdNodePersistence.load(
                properties,
                self._server.get_serial
            )
            loaded_nodes[node.get_name()] = node

        # Load resources
        loaded_resources = {}
        for properties in res_con.itervalues():
            resource = DrbdResourcePersistence.load(
                properties,
                self._server.get_serial
            )
            loaded_resources[resource.get_name()] = resource

        # Load assignments
        for properties in assg_con.itervalues():
            assignment = AssignmentPersistence.load(
                properties, loaded_nodes, loaded_resources,
                self._server.get_serial
            )

        # Reestablish assignments and snapshot assignments signals
        for node in loaded_nodes.itervalues():
            node_name = node.get_name()
            node_assg_map = assg_map_cache.get(node_name)
            for cur_assg in node.iterate_assignments():
                res_name = cur_assg.get_resource().get_name()
                prev_assg = None
                if node_assg_map is not None:
                    prev_assg = node_assg_map.get(res_name)

                if prev_assg is not None:
                    # Assignment was present in the previous configuration
                    signal = prev_assg.get_signal()
                    cur_assg.set_signal(signal)
                    del node_assg_map[res_name]
                    # If the current state or target state of that assignment
                    # has changed, send out a change notification
                    if (cur_assg.get_cstate() != prev_assg.get_cstate() or
                        cur_assg.get_tstate() != prev_assg.get_tstate()):
                        # cstate or tstate changed
                        cur_assg.notify_changed()

                    # Collect the previous snapshot assignments
                    prev_snaps_assg_map = {}
                    for snaps_assg in prev_assg.iterate_snaps_assgs():
                        snaps = snaps_assg.get_snapshot()
                        snaps_name = snaps.get_name()
                        prev_snaps_assg_map[snaps_name] = snaps_assg

                    # Cross-check for changes (creation/removal) of
                    # snapshot assignments
                    for cur_snaps_assg in cur_assg.iterate_snaps_assgs():
                        cur_snaps = cur_snaps_assg.get_snapshot()
                        cur_snaps_name = cur_snaps.get_name()
                        prev_snaps_assg = prev_snaps_assg_map.get(cur_snaps_name)

                        # Transfer the existing signal or create a new signal
                        # for the snapshot assignment
                        signal = None
                        if prev_snaps_assg is not None:
                            del prev_snaps_assg_map[cur_snaps_name]
                            # Transfer the signal from the previous configuration's
                            # snapshot assignment
                            signal = prev_snaps_assg.get_signal()
                            cur_snaps_assg.set_signal(signal)
                            # If the current state or target state of that
                            # snapshot assignment has changed, send out
                            # a change notification
                            if (cur_snaps_assg.get_cstate() != prev_snaps_assg.get_cstate() or
                                cur_snaps_assg.get_tstate() != prev_snaps_assg.get_tstate()):
                                # cstate or tstate change
                                cur_snaps_assg.notify_changed()
                        else:
                            # Create a new signal for the newly present
                            # snapshot assignment
                            signal = self._server.create_signal(
                                "snapshots/" + node_name + "/" + res_name + "/" + snaps_name
                            )
                            cur_snaps_assg.set_signal(signal)

                    # Send remove signals for those snapshot assignments that
                    # existed in the previous configuration but do no longer
                    # exist in the current configuration
                    for prev_snaps_assg in prev_snaps_assg_map.itervalues():
                        prev_snaps_assg.notify_removed()
                else:
                    # Assignment was not present in the previous configuration
                    # (e.g. it was created by another node)
                    signal = self._server.create_signal(
                        "assignments/" + node_name + "/" + res_name
                    )
                    cur_assg.set_signal(signal)
                    # Create signals for the snapshot assignments
                    for snaps_assg in cur_assg.iterate_snaps_assgs():
                        snaps = snaps_assg.get_snapshot()
                        snaps_name = snaps.get_name()
                        signal = self._server.create_signal(
                            "snapshots/" + node_name + "/" + res_name + "/" + snaps_name
                        )
                        snaps_assg.set_signal(signal)
            if node_assg_map is not None and len(node_assg_map) == 0:
                del assg_map_cache[node_name]
        for node_assg_map in assg_map_cache.itervalues():
            for prev_assg in node_assg_map.itervalues():
                prev_assg.notify_removed()

        # Load the cluster configuration
        loaded_cluster_conf = propscon.PropsContainer(None, None, cconf_con)
        loaded_serial_gen = loaded_cluster_conf.new_serial_gen()

        # Load the common configuration
        loaded_common_conf = DrbdCommonPersistence.load(
            common_con, self._server.get_serial
        )

        # Update the server's object directory
        objects_root[drbdmanage.server.DrbdManageServer.OBJ_NODES_NAME]     = loaded_nodes
        objects_root[drbdmanage.server.DrbdManageServer.OBJ_RESOURCES_NAME] = loaded_resources
        objects_root[drbdmanage.server.DrbdManageServer.OBJ_SGEN_NAME]      = loaded_serial_gen
        objects_root[drbdmanage.server.DrbdManageServer.OBJ_CCONF_NAME]     = loaded_cluster_conf
        objects_root[drbdmanage.server.DrbdManageServer.OBJ_COMMON_NAME]    = loaded_common_conf
        # NOTE: Caller must update the server's objects directory cache

        # Quorum: Clear the quorum-ignore flag on each node that is
        #         currently connected and update the number of
        #         expected nodes
        quorum = self._server.get_quorum()
        quorum.readjust_qignore_flags()
        quorum.readjust_full_member_count()


    def save_containers(self, objects_root):
        nodes        = objects_root[drbdmanage.server.DrbdManageServer.OBJ_NODES_NAME]
        resources    = objects_root[drbdmanage.server.DrbdManageServer.OBJ_RESOURCES_NAME]
        cluster_conf = objects_root[drbdmanage.server.DrbdManageServer.OBJ_CCONF_NAME]
        common_conf  = objects_root[drbdmanage.server.DrbdManageServer.OBJ_COMMON_NAME]

        # Prepare nodes and assignments containers
        nodes_con = {}
        assg_con = {}
        for node in nodes.itervalues():
            DrbdNodePersistence(node).save(nodes_con)
            for assg in node.iterate_assignments():
                AssignmentPersistence(assg).save(assg_con)

        # Prepare resources container
        res_con = {}
        for resource in resources.itervalues():
            DrbdResourcePersistence(resource).save(res_con)

        # Prepare cluster configuration container
        cluster_conf_con = cluster_conf.get_all_props()

        # Prepare common configuration container
        common_conf_con = {}
        DrbdCommonPersistence(common_conf).save(common_conf_con)

        return (nodes_con, res_con, assg_con, cluster_conf_con, common_conf_con)


    def json_import(self, objects_root):
        """
        Imports the configuration from JSON data streams
        """
        try:
            import_con = self.json_to_container(self._json_data)

            # Extract the various containers
            # FIXME: Establish constants for the container keys
            nodes_con  = import_con[BasePersistence.NODES_KEY]
            res_con    = import_con[BasePersistence.RES_KEY]
            assg_con   = import_con[BasePersistence.ASSG_KEY]
            cconf_con  = import_con[BasePersistence.CCONF_KEY]
            common_con = import_con[BasePersistence.COMMON_KEY]

            self.load_containers(objects_root, nodes_con, res_con, assg_con, cconf_con, common_con)
        except PersistenceException as pers_exc:
            # Rethrow
            raise pers_exc
        except Exception as exc:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logging.error(
                "PersistenceDualImpl: Cannot import configuration, Exception=%s"
                % (str(exc_type))
            )
            logging.debug("*** begin stack trace")
            for line in traceback.format_tb(exc_tb):
                logging.debug("    %s" % (line))
            logging.debug("*** end stack trace")
            raise PersistenceException


    def json_export(self, objects_root):
        """
        Exports JSON data streams of the configuration

        The persistent storage must have been opened before calling load().
        See open().

        @raise   PersistenceException: on I/O error
        @raise   IOError: if no file descriptor is open
        """
        try:
            nodes_con, res_con, assg_con, cconf_con, common_con = self.save_containers(objects_root)

            export_con = {}
            export_con[BasePersistence.NODES_KEY]  = nodes_con
            export_con[BasePersistence.RES_KEY]    = res_con
            export_con[BasePersistence.ASSG_KEY]   = assg_con
            export_con[BasePersistence.CCONF_KEY]  = cconf_con
            export_con[BasePersistence.COMMON_KEY] = common_con

            self._json_data = self.container_to_json(export_con)
        except PersistenceException as pers_exc:
            # Rethrow
            raise pers_exc
        except Exception as exc:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logging.error(
                "PersistenceDualImpl: Cannot export configuration, Exception=%s"
                % (str(exc_type))
            )
            logging.debug("*** begin stack trace")
            for line in traceback.format_tb(exc_tb):
                logging.debug("    %s" % (line))
            logging.debug("*** end stack trace")
            raise PersistenceException


class SatellitePersistence(BasePersistence):

    _data_hash   = None


    def __init__(self, ref_server):
        super(SatellitePersistence, self).__init__(ref_server)


    def open(self, modify):
        return True;


    def get_hash_obj(self):
        return self._data_hash


    def close(self):
        self._data_hash = None


    def load(self, objects_root):
        self.json_import(objects_root)


    def save(self, objects_root):
        self.json_export(objects_root)
        self._data_hash = DataHash()
        self._data_hash.update(self.get_json_data())



class PersistenceDualImpl(BasePersistence):
    """
    Persistence layer for dual drbdmanage control volumes
    """

    """
    Persistence layer for the drbdmanage control volume
    """

    # crc32 of "drbdmanage control volume"
    PERSISTENCE_MAGIC   = "\x1a\xdb\x98\xa2";

    # serial number, big-endian
    PERSISTENCE_VERSION = "\x00\x00\x00\x02";

    _load_file     = None
    _save_file     = None
    _writable      = False

    _data_hash     = None
    _load_hash     = None
    _server        = None

    INDEX_KEY      = "index"
    NODES_OFF_KEY  = "nodes_off"
    NODES_LEN_KEY  = "nodes_len"
    RES_OFF_KEY    = "res_off"
    RES_LEN_KEY    = "res_len"
    ASSG_OFF_KEY   = "assg_off"
    ASSG_LEN_KEY   = "assg_len"
    CCONF_OFF_KEY  = "cconf_off"
    CCONF_LEN_KEY  = "cconf_len"
    COMMON_OFF_KEY = "common_off"
    COMMON_LEN_KEY = "common_len"
    HASH_KEY       = "hash"

    BLOCK_SIZE     = 0x1000 # 4096
    MAGIC_OFFSET   = 0x1000 # 4096
    VERSION_OFFSET = 0x1004 # 4100
    INDEX_OFFSET   = 0x1800 # 6144
    INDEX_SIZE     =  0x400 # 1024
    HASH_OFFSET    = 0x1C00 # 6400
    HASH_SIZE      = 0x0100 #  256
    DATA_OFFSET    = 0x2000 # 8192
    ZERO_FILL_SIZE = 0x0400 # 1024

    # MMAP_BUFFER_SIZE: 1048576 == 1 MiB
    MMAP_BUFFER_SIZE = 0x100000

    # Linux specific ioctl()/fcntl() constant
    # FIXME: That constant should probably not be here, but there does not
    #        seem to be a good way to get it otherwise
    BLKFLSBUF = 0x00001261 # <include/linux/fs.h>

    # fail counter for attempts to open the config file (CONF_FILE)
    MAX_FAIL_COUNT = 10

    # wait 2 seconds before every open() retry if the file was not found
    ENOENT_REOPEN_TIMER = 2

    # wait at least half a second between open() retries
    MIN_REOPEN_TIMER = 0.5


    def __init__(self, ref_server):
        super(PersistenceDualImpl, self).__init__(ref_server)


    def open(self, modify):
        """
        Open the persistent storage for reading or writing, depending on
        the modify flag. If (modify == True), open for writing, otherwise
        open readonly.
        """
        fn_rc = False
        fail_ctr = 0
        file_0 = None
        file_1 = None

        # Prevent leaking file descriptors due to double open()
        if self._load_file is not None or self._save_file is not None:
            # Log the error
            logging.error(
                "PersistenceDualImpl: open(): "
                "Double open() detected, this is a programming error. "
                "Please report this problem to the developers."
            )
            # Recover and continue
            self.close()

        try:
            while (fail_ctr < self.MAX_FAIL_COUNT and
                   (file_0 is None or file_1 is None)):
                try:
                    # A writable file must be opened first, otherwise, multiple
                    # nodes may succeed with opening the read-only volume first,
                    # thereby deadlocking write-access by any of the nodes
                    if file_0 is None:
                        file_0 = self._open_control_volume(drbdmanage.consts.DRBDCTRL_DEV_0, modify)
                    if file_1 is None:
                        file_1 = self._open_control_volume(drbdmanage.consts.DRBDCTRL_DEV_1, modify)

                    self._check_magic(file_0)
                    self._check_magic(file_1)

                    self._check_version(file_0)
                    self._check_version(file_1)
                except (OSError, IOError) as error:
                    secs = 0
                    if error.errno == errno.ENOENT:
                        secs = PersistenceDualImpl.ENOENT_REOPEN_TIMER
                    else:
                        rnd_byte = os.urandom(1)
                        secs = float(ord(rnd_byte)) / 100 + PersistenceDualImpl.MIN_REOPEN_TIMER
                    time.sleep(secs)
                fail_ctr += 1
            # end while loop

            # If opening the second file failed, close the first one
            # If opening the first one failed, there would not have
            # been an attempt to open the second one anyway
            if file_0 is not None and file_1 is None:
                try:
                    self._close_file(file_0)
                    file_0 = None
                except:
                    pass
            elif file_0 is not None and file_1 is not None:
                # Select from which file to load data
                # and to which file to save data
                load_file, load_hash, save_file = self._order_files(file_0, file_1)

                # Assign the instance's save and load files
                # TODO: The load file can be downgraded to read-only access
                if modify:
                    self._save_file = save_file
                else:
                    self._close_file(save_file)
                    self._save_file = None
                self._load_file = load_file
                self._load_hash = load_hash

                self._writable = modify
                fn_rc = True
        except PersistenceException:
            self._close_file(file_0)
            self._close_file(file_1)
        return fn_rc


    def close(self):
        """
        Closes the persistent storage

        Can be called multiple times and/or no matter whether the persistent
        storage is actually open without causing any error.
        """
        self._close_file(self._load_file)
        self._close_file(self._save_file)
        self._load_file = None
        self._save_file = None
        self._writable = False
        self._data_hash = None
        self._load_hash = None


    def _close_file(self, drbdctrl_file):
        if drbdctrl_file is not None:
            if self._writable:
                try:
                    drbdctrl_file.flush()
                except IOError:
                    pass
                try:
                    os.fsync(drbdctrl_file.fileno())
                except OSError:
                    pass
            try:
                drbdctrl_file.close()
            except IOError:
                pass


    def load(self, objects_root):
        """
        Loads the configuration from the drbdmanage control volume

        The persistent storage must have been opened before calling load().
        See open().

        @raise   PersistenceException: on I/O error
        @raise   IOError: if no file descriptor is open
        """
        if self._load_file is not None:
            try:
                load_file = self._load_file
                index = self._load_index(load_file)

                # Get data section offsets and lengths

                # Offset & length: Nodes section
                nodes_off = index[PersistenceDualImpl.NODES_OFF_KEY]
                nodes_len = index[PersistenceDualImpl.NODES_LEN_KEY]

                # Offset & length: Resources section
                res_off = index[PersistenceDualImpl.RES_OFF_KEY]
                res_len = index[PersistenceDualImpl.RES_LEN_KEY]

                # Offset & length: Assignments section
                assg_off = index[PersistenceDualImpl.ASSG_OFF_KEY]
                assg_len = index[PersistenceDualImpl.ASSG_LEN_KEY]

                # Offset & length: Cluster configuration section
                cconf_off = index[PersistenceDualImpl.CCONF_OFF_KEY]
                cconf_len = index[PersistenceDualImpl.CCONF_LEN_KEY]

                # Offset & length: Common configuration section
                common_off = index[PersistenceDualImpl.COMMON_OFF_KEY]
                common_len = index[PersistenceDualImpl.COMMON_LEN_KEY]

                nodes_con  = self._import_container(load_file, nodes_off, nodes_len)
                res_con    = self._import_container(load_file, res_off, res_len)
                assg_con   = self._import_container(load_file, assg_off, assg_len)
                cconf_con  = self._import_container(load_file, cconf_off, cconf_len)
                common_con = self._import_container(load_file, common_off, common_len)

                self.load_containers(objects_root, nodes_con, res_con, assg_con, cconf_con, common_con)
            except PersistenceException as pers_exc:
                # Rethrow
                raise pers_exc
            except Exception as exc:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                logging.error(
                    "PersistenceDualImpl: Cannot load configuration, Exception=%s"
                    % (str(exc_type))
                )
                logging.debug("*** begin stack trace")
                for line in traceback.format_tb(exc_tb):
                    logging.debug("    %s" % (line))
                logging.debug("*** end stack trace")
                raise PersistenceException
        else:
            logging.debug("PersistenceDualImpl: load(): source file is not open")
            raise PersistenceException


    def save(self, objects_root):
        """
        Saves the configuration to the drbdmanage control volume

        The persistent storage must have been opened for writing before
        calling save(). See open().

        @raise   PersistenceException: on I/O error
        @raise   IOError: if no writable file descriptor is open
        """
        if self._save_file is not None:
            try:
                save_file = self._save_file

                data_hash = DataHash()

                nodes_con, res_con, assg_con, cluster_conf_con, common_conf_con = (
                    self.save_containers(objects_root)
                )

                save_file.seek(PersistenceDualImpl.DATA_OFFSET)

                nodes_off, nodes_len   = self._export_container(save_file, nodes_con, data_hash)
                res_off, res_len       = self._export_container(save_file, res_con, data_hash)
                assg_off, assg_len     = self._export_container(save_file, assg_con, data_hash)
                cconf_off, cconf_len   = self._export_container(save_file, cluster_conf_con, data_hash)
                common_off, common_len = self._export_container(save_file, common_conf_con, data_hash)

                index_con = {
                    PersistenceDualImpl.INDEX_KEY: {
                        PersistenceDualImpl.NODES_OFF_KEY:  nodes_off,
                        PersistenceDualImpl.NODES_LEN_KEY:  nodes_len,
                        PersistenceDualImpl.RES_OFF_KEY:    res_off,
                        PersistenceDualImpl.RES_LEN_KEY:    res_len,
                        PersistenceDualImpl.ASSG_OFF_KEY:   assg_off,
                        PersistenceDualImpl.ASSG_LEN_KEY:   assg_len,
                        PersistenceDualImpl.CCONF_OFF_KEY:  cconf_off,
                        PersistenceDualImpl.CCONF_LEN_KEY:  cconf_len,
                        PersistenceDualImpl.COMMON_OFF_KEY: common_off,
                        PersistenceDualImpl.COMMON_LEN_KEY: common_len
                    }
                }
                self._save_index(save_file, index_con)

                self._update_stored_hash(save_file, data_hash.get_hex_hash())
                self._data_hash = data_hash
            except PersistenceException as pers_exc:
                # Rethrow
                raise pers_exc
            except Exception as exc:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                logging.error(
                    "cannot save data tables, Exception=%s"
                    % (str(exc_type))
                )
                logging.debug("*** begin stack trace")
                for line in traceback.format_tb(exc_tb):
                    logging.debug("    %s" % (line))
                logging.debug("*** end stack trace")
                raise PersistenceException
        else:
            logging.debug("PersistenceDualImpl: save(): destination file is not open")
            raise PersistenceException


    def get_hash_obj(self):
        """
        Returns the DataHash object used by this instance

        The DataHash object that is used to calculate the hash code of the
        configuration is returned.

        @return: DataHash object. See drbdmanage.utils
        """
        return self._data_hash


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
        if self._load_file is None:
            raise PersistenceException
        return self._load_hash


    def _open_control_volume(self, drbdctrl_file, modify):
        file_stream = None
        try:
            file_fd = None
            mode    = None
            if modify:
                file_fd = os.open(drbdctrl_file, os.O_RDWR | os.O_EXCL)
                mode    = "r+"
            else:
                file_fd = os.open(drbdctrl_file, os.O_RDONLY | os.O_EXCL)
                mode    = "r"

            try:
                fcntl.ioctl(file_fd, PersistenceDualImpl.BLKFLSBUF)
            except (OSError, IOError):
                pass

            file_stream = os.fdopen(file_fd, mode)
        except (OSError, IOError) as error:
            if error.errno == errno.ENOENT:
                logging.error(
                    "Cannot open control volume '%s': "
                    "Object not found" % (drbdctrl_file)
                )
            raise error
        finally:
            if file_stream is None and file_fd is not None:
                try:
                    os.close(file_fd)
                except (OSError, IOError):
                    pass
        return file_stream


    def _check_magic(self, drbdctrl_file):
        drbdctrl_file.seek(PersistenceDualImpl.MAGIC_OFFSET)
        magic = drbdctrl_file.read(len(PersistenceDualImpl.PERSISTENCE_MAGIC))
        if magic != PersistenceDualImpl.PERSISTENCE_MAGIC:
            logging.error(
                "Unusable control volume, "
                "the control volume magic number is missing"
            )
            raise PersistenceException


    def _check_version(self, drbdctrl_file):
        drbdctrl_file.seek(PersistenceDualImpl.VERSION_OFFSET)
        version = drbdctrl_file.read(len(PersistenceDualImpl.PERSISTENCE_VERSION))
        if version != PersistenceDualImpl.PERSISTENCE_VERSION:
            logging.error(
                "Can not load data tables, "
                "control volume version does not match server version"
            )
            raise PersistenceException


    def _order_files(self, file_0, file_1):
        load_file = None
        load_hash = None
        save_file = None
        try:
            index_0 = self._load_index(file_0)
            index_1 = self._load_index(file_1)

            serial_0, stored_hash_0 = self._get_serial_integrity_check(file_0, index_0)
            serial_1, stored_hash_1 = self._get_serial_integrity_check(file_1, index_1)

            if serial_0 is not None and serial_1 is not None:
                if serial_0 < serial_1:
                    save_file = file_0
                    load_file = file_1
                    load_hash = stored_hash_1
                else:
                    load_file = file_0
                    load_hash = stored_hash_0
                    save_file = file_1
            else:
                if serial_0 is None and serial_1 is None:
                    # Both volumes are invalid, cannot load any data,
                    # but may attempt to save the current state by
                    # overwriting an arbitrarily selected damaged volume
                    save_file = file_0
                    logging.error(
                        "None of both control volumes contains valid data, "
                        "loading is disabled"
                    )
                else:
                    if serial_0 is None:
                        save_file = file_0
                        load_file = file_1
                        load_hash = stored_hash_1
                    else:
                        load_file = file_0
                        load_hash = stored_hash_0
                        save_file = file_1
        except (OSError, IOError):
            raise PersistenceException
        return load_file, load_hash, save_file


    def _load_index(self, drbdctrl_file):
        index_data = self._import_index(drbdctrl_file)
        index_con = self.json_to_container(index_data)
        index = index_con[PersistenceDualImpl.INDEX_KEY]
        return index


    def _save_index(self, drbdctrl_file, index_con):
        index_data = self.container_to_json(index_con)
        self._export_index(drbdctrl_file, index_data)


    def _import_index(self, drbdctrl_file):
        drbdctrl_file.seek(PersistenceDualImpl.INDEX_OFFSET)
        index_data = self._null_trunc(drbdctrl_file.read(PersistenceDualImpl.INDEX_SIZE))
        return index_data


    def _export_index(self, drbdctrl_file, index_data):
        drbdctrl_file.seek(PersistenceDualImpl.INDEX_OFFSET)
        drbdctrl_file.write(index_data)
        drbdctrl_file.write(chr(0))
        diff_size = PersistenceDualImpl.INDEX_SIZE - len(index_data) - 1
        if diff_size > 0:
            drbdctrl_file.write(diff_size * '\0')


    def _import_data(self, drbdctrl_file, offset, length):
        drbdctrl_file.seek(offset)
        load_data = self._null_trunc(drbdctrl_file.read(length))
        return load_data


    def _export_data(self, drbdctrl_file, save_data):
        offset = drbdctrl_file.tell()
        drbdctrl_file.write(save_data)
        length = drbdctrl_file.tell() - offset
        drbdctrl_file.write(chr(0))
        self._align_zero_fill(drbdctrl_file)
        return offset, length


    def _import_container(self, drbdctrl_file, offset, length):
        load_data = self._import_data(drbdctrl_file, offset, length)
        container = self.json_to_container(load_data)
        return container


    def _export_container(self, drbdctrl_file, container, data_hash):
        save_data = self.container_to_json(container)
        data_hash.update(save_data)
        offset, length = self._export_data(drbdctrl_file, save_data)
        return offset, length


    def _align_zero_fill(self, drbdctrl_file):
        """
        Fills the file with zero bytes up to the next block boundary

        The file is filled with zero bytes from the current file offset up to
        the next block boundary as specified by BLOCK_SIZE.
        """
        offset = drbdctrl_file.tell()
        if offset % PersistenceDualImpl.BLOCK_SIZE != 0:
            fill_buffer = ('\0' * PersistenceDualImpl.ZERO_FILL_SIZE)
            upper_bound  = ((offset / PersistenceDualImpl.BLOCK_SIZE) + 1) * PersistenceDualImpl.BLOCK_SIZE
            diff = upper_bound - offset
            fill_count = diff / PersistenceDualImpl.ZERO_FILL_SIZE
            counter = 0
            while counter < fill_count:
                counter += 1
                drbdctrl_file.write(fill_buffer)
            diff -= (PersistenceDualImpl.ZERO_FILL_SIZE * counter)
            drbdctrl_file.write(fill_buffer[:diff])


    def _null_trunc(self, data):
        """
        Returns the supplied data truncated at the first zero byte

        Used for sanitizing JSON strings read from the persistent storage
        before passing them to the JSON parser, because the JSON parser does
        not like to see any data behind the end of a JSON string.

        @return: data truncated at the first zero byte
        @rtype:  str
        """
        index = data.find(chr(0))
        if index != -1:
            data = data[:index]
        return data


    def _update_stored_hash(self, drbdctrl_file, hex_hash):
        hash_con = {
            PersistenceDualImpl.HASH_KEY: hex_hash
        }
        hash_json = self.container_to_json(hash_con)
        drbdctrl_file.seek(PersistenceDualImpl.HASH_OFFSET)
        drbdctrl_file.write(hash_json)
        drbdctrl_file.write(chr(0))


    def _get_serial_integrity_check(self, drbdctrl_file, index):
        serial = None
        stored_hash = None
        try:
            data_hash = drbdmanage.utils.DataHash()

            # Hash nodes data
            load_data = self._import_data(drbdctrl_file, index[PersistenceDualImpl.NODES_OFF_KEY],
                                          index[PersistenceDualImpl.NODES_LEN_KEY])
            data_hash.update(load_data)

            # Hash resources data
            load_data = self._import_data(drbdctrl_file, index[PersistenceDualImpl.RES_OFF_KEY],
                                          index[PersistenceDualImpl.RES_LEN_KEY])
            data_hash.update(load_data)

            # Hash assignments data
            load_data = self._import_data(drbdctrl_file, index[PersistenceDualImpl.ASSG_OFF_KEY],
                                          index[PersistenceDualImpl.ASSG_LEN_KEY])
            data_hash.update(load_data)

            # Hash cluster configuration data
            load_data = self._import_data(drbdctrl_file, index[PersistenceDualImpl.CCONF_OFF_KEY],
                                          index[PersistenceDualImpl.CCONF_LEN_KEY])
            data_hash.update(load_data)
            cluster_conf = self.json_to_container(load_data)

            # Hash common configuration data
            load_data = self._import_data(drbdctrl_file, index[PersistenceDualImpl.COMMON_OFF_KEY],
                                          index[PersistenceDualImpl.COMMON_LEN_KEY])
            data_hash.update(load_data)

            # Compute the hash value
            computed_hash = data_hash.get_hex_hash()

            # Load the stored hash value
            load_data = self._import_data(drbdctrl_file, PersistenceDualImpl.HASH_OFFSET,
                                          PersistenceDualImpl.HASH_SIZE)
            stored_hash_con = self.json_to_container(load_data)
            stored_hash = stored_hash_con[PersistenceDualImpl.HASH_KEY]

            if stored_hash == computed_hash:
                serial = int(cluster_conf[drbdmanage.consts.SERIAL])
        except (OSError, IOError, KeyError, ValueError, TypeError):
            pass
        return serial, stored_hash


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
                "Cannot open the control volume '%s' "
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
                # TODO: This can probably be simplified by saving
                #       only the PropsContainer
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
                save_data = self._container_to_json(cluster_conf.get_all_props())
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

                quorum = self._server.get_quorum()
                # Quorum: Clear the quorum-ignore flag on each node that is currently connected
                quorum.readjust_qignore_flags()

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


                # Reestablish assignments and snapshot assignments signals
                for node in nodes.itervalues():
                    node_name = node.get_name()
                    node_assg_map = assg_map_cache.get(node_name)
                    for cur_assg in node.iterate_assignments():
                        res_name = cur_assg.get_resource().get_name()
                        prev_assg = None
                        if node_assg_map is not None:
                            prev_assg = node_assg_map.get(res_name)

                        if prev_assg is not None:
                            # Assignment was present in the previous configuration
                            signal = prev_assg.get_signal()
                            cur_assg.set_signal(signal)
                            del node_assg_map[res_name]
                            # If the current state or target state of that assignment
                            # has changed, send out a change notification
                            if (cur_assg.get_cstate() != prev_assg.get_cstate() or
                                cur_assg.get_tstate() != prev_assg.get_tstate()):
                                # cstate or tstate changed
                                cur_assg.notify_changed()

                            # Collect the previous snapshot assignments
                            prev_snaps_assg_map = {}
                            for snaps_assg in prev_assg.iterate_snaps_assgs():
                                snaps = snaps_assg.get_snapshot()
                                snaps_name = snaps.get_name()
                                prev_snaps_assg_map[snaps_name] = snaps_assg

                            # Cross-check for changes (creation/removal) of
                            # snapshot assignments
                            for cur_snaps_assg in cur_assg.iterate_snaps_assgs():
                                cur_snaps = cur_snaps_assg.get_snapshot()
                                cur_snaps_name = cur_snaps.get_name()
                                prev_snaps_assg = prev_snaps_assg_map.get(cur_snaps_name)

                                # Transfer the existing signal or create a new signal
                                # for the snapshot assignment
                                signal = None
                                if prev_snaps_assg is not None:
                                    del prev_snaps_assg_map[cur_snaps_name]
                                    # Transfer the signal from the previous configuration's
                                    # snapshot assignment
                                    signal = prev_snaps_assg.get_signal()
                                    cur_snaps_assg.set_signal(signal)
                                    # If the current state or target state of that
                                    # snapshot assignment has changed, send out
                                    # a change notification
                                    if (cur_snaps_assg.get_cstate() != prev_snaps_assg.get_cstate() or
                                        cur_snaps_assg.get_tstate() != prev_snaps_assg.get_tstate()):
                                        # cstate or tstate change
                                        cur_snaps_assg.notify_changed()
                                else:
                                    # Create a new signal for the newly present
                                    # snapshot assignment
                                    signal = self._server.create_signal(
                                        "snapshots/" + node_name + "/" + res_name + "/" + snaps_name
                                    )
                                    cur_snaps_assg.set_signal(signal)

                            # Send remove signals for those snapshot assignments that
                            # existed in the previous configuration but do no longer
                            # exist in the current configuration
                            for prev_snaps_assg in prev_snaps_assg_map.itervalues():
                                prev_snaps_assg.notify_removed()
                        else:
                            # Assignment was not present in the previous configuration
                            # (e.g. it was created by another node)
                            signal = self._server.create_signal(
                                "assignments/" + node_name + "/" + res_name
                            )
                            cur_assg.set_signal(signal)
                            # Create signals for the snapshot assignments
                            for snaps_assg in cur_assg.iterate_snaps_assgs():
                                snaps = snaps_assg.get_snapshot()
                                snaps_name = snaps.get_name()
                                signal = self._server.create_signal(
                                    "snapshots/" + node_name + "/" + res_name + "/" + snaps_name
                                )
                                snaps_assg.set_signal(signal)
                    if node_assg_map is not None and len(node_assg_map) == 0:
                        del assg_map_cache[node_name]
                for node_assg_map in assg_map_cache.itervalues():
                    for prev_assg in node_assg_map.itervalues():
                        prev_assg.notify_removed()

                # Load the cluster configuration
                cconf_key = drbdmanage.server.DrbdManageServer.OBJ_CCONF_NAME
                cluster_conf = propscon.PropsContainer(None, None, cconf_con)
                serial_gen = cluster_conf.new_serial_gen()
                objects_root[drbdmanage.server.DrbdManageServer.OBJ_SGEN_NAME] = serial_gen
                objects_root[cconf_key] = cluster_conf

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

                # Quorum: Update the number of expected nodes
                quorum.readjust_full_member_count()
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
            raise PersistenceException
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
            raise PersistenceException
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
            raise PersistenceException
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
            raise PersistenceException
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
            raise PersistenceException
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
            raise PersistenceException
        return vol_state
