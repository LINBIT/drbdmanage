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


import logging
import drbdmanage.utils

from drbdmanage.storage.storagecommon import GenericStorage
from drbdmanage.exceptions import (
    InvalidMajorNrException, InvalidMinorNrException
)
from drbdmanage.exceptions import (
    DM_SUCCESS, DM_ENOENT, DM_ESTORAGE, DM_ENOTIMPL
)


class BlockDevice(GenericStorage):
    """
    Represents a block device
    """
    NAME_MAXLEN            = 4096 ## at least as long as res name
    # Valid characters in addition to [a-zA-Z0-9]
    NAME_VALID_CHARS       = "_"
    # Additional valid characters, but not allowed as the first character
    NAME_VALID_INNER_CHARS = "-."

    _path     = None
    _name     = None


    def __init__(self, name, size_kiB, path):
        super(BlockDevice, self).__init__(size_kiB)
        self._path     = path
        self._name     = self.name_check(name)


    def name_check(self, name):
        return drbdmanage.drbd.drbdcore.GenericDrbdObject.name_check(
            name, BlockDevice.NAME_MAXLEN,
            BlockDevice.NAME_VALID_CHARS, BlockDevice.NAME_VALID_INNER_CHARS
        )


    def get_name(self):
        return self._name


    def get_path(self):
        return self._path


class BlockDeviceManager(object):
    _plugin = None


    def __init__(self, plugin_name, plugin_mgr):
        """
        Creates a new instance of the BlockDeviceManager
        """
        self._plugin = plugin_mgr.get_plugin_instance(plugin_name)
        if self._plugin is None:
            logging.error(
                "BlockDeviceManager: Import of the "
                "storage management plugin '%s' failed"
                % (plugin_name)
            )


    def get_blockdevice(self, bd_name):
        """
        Retrieves a registered BlockDevice object
        """
        blockdev = None
        if self._plugin is not None:
            try:
                blockdev = self._plugin.get_blockdevice(bd_name)
            except NotImplementedError:
                self._log_not_implemented("get_blockdevice")
        else:
            self._log_no_plugin()
        return blockdev


    def create_blockdevice(self, name, vol_id, size):
        """
        Allocates a block device as backing storage for a DRBD volume
        """
        blockdev = None
        if self._plugin is not None:
            try:
                blockdev = self._plugin.create_blockdevice(name, vol_id, size)
                status = "successful" if blockdev is not None else "failed"
                logging.debug(
                    "BlockDeviceManager: create_blockdevice('%s', %u, %u): %s"
                    % (name, vol_id, size, status)
                )
            except NotImplementedError:
                self._log_not_implemented("create_blockdevice")
        else:
            self._log_no_plugin()
        return blockdev


    def extend_blockdevice(self, bd_name, new_size):
        """
        Extends the block device of an existing DRBD volume
        """
        fn_rc = DM_ESTORAGE
        if self._plugin is not None:
            try:
                blockdev = self.get_blockdevice(bd_name)
                if blockdev is not None:
                    fn_rc = self._plugin.extend_blockdevice(blockdev, new_size)
                    status = "successful" if fn_rc == DM_SUCCESS else "failed"
                    logging.debug(
                        "BlockDeviceManager: extend_blockdevice('%s', %d): "
                        "%s fn_rc=%d"
                        % (bd_name, new_size, status, fn_rc)
                    )
                else:
                    logging.debug(
                        "BlockDeviceManager: extend_blockdevice('%s', %d): "
                        "Cannot find the corresponding BlockDevice object"
                        % (bd_name, new_size)
                    )
                    fn_rc = DM_ENOENT
            except NotImplementedError:
                self._log_not_implemented("extend_blockdevice")
                fn_rc = DM_ENOTIMPL
        else:
            self._log_no_plugin()
        return fn_rc


    def remove_blockdevice(self, bd_name):
        """
        Deallocates a block device
        """
        fn_rc = DM_ESTORAGE
        if self._plugin is not None:
            try:
                blockdev = self.get_blockdevice(bd_name)
                if blockdev is not None:
                    fn_rc = self._plugin.remove_blockdevice(blockdev)
                    status = "successful" if fn_rc == DM_SUCCESS else "failed"
                    logging.debug(
                        "BlockDeviceManager: remove_blockdevice('%s'): "
                        "%s fn_rc=%d"
                        % (bd_name, status, fn_rc)
                    )
                else:
                    logging.debug(
                        "BlockDeviceManager: remove_blockdevice('%s'): "
                        "Cannot find the corresponding BlockDevice object"
                        % (bd_name)
                    )
                    fn_rc = DM_ENOENT
            except NotImplementedError:
                self._log_not_implemented("remove_blockdevice")
                fn_rc = DM_ENOTIMPL
        else:
            self._log_no_plugin()
        return fn_rc


    def up_blockdevice(self, bd_name):
        """
        Activates a block device (e.g., connects an iSCSI resource)
        """
        fn_rc = DM_ESTORAGE
        if self._plugin is not None:
            try:
                blockdev = self.get_blockdevice(bd_name)
                if blockdev is not None:
                    fn_rc = self._plugin.up_blockdevice(blockdev)
                else:
                    logging.debug(
                        "BlockDeviceManager: up_blockdevice('%s'): "
                        "Cannot find the corresponding BlockDevice object"
                        % (bd_name)
                    )
            except NotImplementedError:
                self._log_not_implemented("up_blockdevice")
                fn_rc = DM_ENOTIMPL
        else:
            self._log_no_plugin()
        return fn_rc


    def down_blockdevice(self, bd_name):
        """
        Deactivates a block device (e.g., disconnects an iSCSI resource)
        """
        fn_rc = DM_ESTORAGE
        if self._plugin is not None:
            try:
                blockdev = self.get_blockdevice(bd_name)
                if blockdev is not None:
                    fn_rc = self._plugin.down_blockdevice(blockdev)
                else:
                    logging.debug(
                        "BlockDeviceManager: down_blockdevice('%s'): "
                        "Cannot find the corresponding BlockDevice object"
                        % (bd_name)
                    )
            except NotImplementedError:
                self._log_not_implemented("up_blockdevice")
                fn_rc = DM_ENOTIMPL
        else:
            self._log_no_plugin()
        return fn_rc


    def create_snapshot(self, name, vol_id, src_bd_name):
        """
        Creates a snapshot of the volume of an existing resource
        """
        blockdev = None
        if self._plugin is not None:
            try:
                src_blockdev = self.get_blockdevice(src_bd_name)
                if src_blockdev is not None:
                    blockdev = self._plugin.create_snapshot(
                        name, vol_id, src_blockdev
                    )
                else:
                    logging.error(
                        "BlockDeviceManager: Cannot find the source "
                        "BlockDevice object '%s' required for "
                        "snapshot creation"
                        % (src_bd_name)
                    )
                status_text = (
                    "successful" if blockdev is not None else "failed"
                )
                logging.debug(
                    "BlockDeviceManager: create snapshot('%s', %u, '%s'): %s"
                    % (name, vol_id, src_bd_name, status_text)
                )
            except NotImplementedError:
                logging.error(
                    "BlockDeviceManager: The currently loaded storage "
                    "management plugin does not implement "
                    "snapshot capabilities"
                )
        else:
            self._log_no_plugin()
        return blockdev


    def restore_snapshot(self, name, vol_id, src_bd_name):
        """
        Creates a volume for a new resource from a snapshot
        """
        blockdev = None
        if self._plugin is not None:
            try:
                src_blockdev = self.get_blockdevice(src_bd_name)
                if src_blockdev is not None:
                    blockdev = self._plugin.restore_snapshot(
                        name, vol_id, src_blockdev
                    )
                else:
                    logging.error(
                        "BlockDeviceManager: Cannot find the source "
                        "BlockDevice object '%s' required for "
                        "snapshot creation"
                        % (src_bd_name)
                    )
                status_text = (
                    "successful" if blockdev is not None else "failed"
                )
                logging.debug(
                    "BlockDeviceManager: create snapshot('%s', %u, '%s'): %s"
                    % (name, vol_id, src_bd_name, status_text)
                )
            except NotImplementedError:
                logging.error(
                    "BlockDeviceManager: The currently loaded storage "
                    "management plugin does not implement "
                    "snapshot capabilities"
                )
        else:
            self._log_no_plugin()
        return blockdev


    def remove_snapshot(self, bd_name):
        """
        Deallocates a snapshot block device
        """
        fn_rc = DM_ESTORAGE
        if self._plugin is not None:
            try:
                rm_blockdev = self.get_blockdevice(bd_name)
                if rm_blockdev is not None:
                    fn_rc = self._plugin.remove_blockdevice(rm_blockdev)
                else:
                    logging.debug(
                        "BlockDeviceManager: remove snapshot: "
                        "volume '%s' not found"
                        % (bd_name)
                    )
                status_text = "successful" if fn_rc == 0 else "failed"
                logging.debug(
                    "BlockDeviceManager: remove snapshot blockdev=%s, "
                    "rc=%d, %s"
                    % (bd_name, fn_rc, status_text)
                )
            except NotImplementedError:
                logging.error(
                    "BlockDeviceManager: The currently loaded storage "
                    "management plugin does not implement "
                    "snapshot capabilities"
                )
                fn_rc = DM_ENOTIMPL
        else:
            self._log_no_plugin()
        return fn_rc


    def update_pool(self, drbd_node):
        """
        Retrieves storage pool space information
        """
        fn_rc = DM_ESTORAGE
        pool_size = -1
        pool_free = -1
        if self._plugin is not None:
            try:
                fn_rc, pool_size, pool_free = (
                    self._plugin.update_pool(drbd_node)
                )
            except NotImplementedError:
                logging.error(
                    "BlockDeviceManager: The currently loaded storage "
                    "management plugin does not implement pool space queries"
                )
                fn_rc = DM_ENOTIMPL
        else:
            self._log_no_plugin()
        return fn_rc, pool_size, pool_free


    def reconfigure(self):
        """
        Reconfigures the storage plugin
        """
        fn_rc = DM_ESTORAGE
        if self._plugin is not None:
            try:
                self._plugin.reconfigure()
                fn_rc = DM_SUCCESS
            except NotImplementedError:
                logging.error(
                    "BlockDeviceManager: The currently loaded storage "
                    "management plugin does not support "
                    "on-the-fly reconfiguration"
                )
                fn_rc = DM_ENOTIMPL
        else:
            self._log_no_plugin()
        return fn_rc


    def _log_not_implemented(self, function_name):
        logging.error(
            "BlockDeviceManager: The currently loaded storage "
            "management plugin does not implement the mandatory function %s()"
            % (function_name)
        )


    def _log_no_plugin(self):
        logging.error(
            "BlockDeviceManager: No storage management plugin is loaded, "
            "storage management is inoperational"
        )


class MinorNr(object):
    """
    Contains the minor number of a unix device file
    """
    _minor = None
    MINOR_NR_MAX = 0xfffff

    MINOR_NR_AUTO     = -1
    MINOR_NR_ERROR    = -2

    def __init__(self, minor):
        self._minor = MinorNr.minor_check(minor)


    def get_value(self):
        return self._minor


    @classmethod
    def minor_check(cls, minor):
        if minor != cls.MINOR_NR_AUTO:
            if minor < 0 or minor > cls.MINOR_NR_MAX:
                raise InvalidMinorNrException
        return minor


class MajorNr(object):
    """
    Contains the major number of a unix device file
    """
    _major = None
    MAJOR_NR_MAX = 0xfff


    def __init__(self, major):
        self._major = MajorNr.major_check(major)


    def get_value(self):
        return self._major


    @classmethod
    def major_check(cls, major):
        if major < 0 or major > cls.MAJOR_NR_MAX:
            raise InvalidMajorNrException
        return major


class DevNr(object):
    """
    Contains the major/minor numbers of unix device files
    """
    _minor = None
    _major = None


    def __init__self(self, major, minor):
        self._minor = minor
        self._major = major


    def get_minor(self):
        self._minor.get_value()


    def get_major(self):
        self._major.get_value()


class StoragePlugin(object):

    """
    Interface for storage plugins

    Storage plugins are loaded dynamically at runtime. The block device manager
    expects storage plugins to implement the functions in this interface.
    Storage plugins should be subclasses of this interface, although
    technically, this is not strictly required, because Python does not care
    about the class hierarchy of objects as long as it finds all the
    functions it looks for.
    """

    def __init__(self):
        """
        Initializes the storage plugin
        """
        pass


    def get_blockdevice(self, bd_name):
        """
        Retrieves a registered BlockDevice object

        The BlockDevice object allocated and registered under the supplied
        resource name and volume id is returned.

        @return: the specified block device; None on error
        @rtype:  BlockDevice object
        """
        raise NotImplementedError


    def create_blockdevice(self, name, vol_id, size):
        """
        Allocates a block device as backing storage for a DRBD volume

        @param   name: resource name; subject to name constraints
        @type    name: str
        @param   vol_id: volume id
        @type    vol_id: int
        @param   size: size of the block device in kiB (binary kilobytes)
        @type    size: long
        @return: block device of the specified size
        @rtype:  BlockDevice object; None if the allocation fails
        """
        raise NotImplementedError


    def extend_blockdevice(self, blockdevice, size):
        """
        Deallocates a block device

        @param   blockdevice: the block device to deallocate
        @type    blockdevice: BlockDevice object
        @param   size: new size of the block device in kiB (binary kilobytes)
        @type    size: long
        @return: standard return code (see drbdmanage.exceptions)
        """
        raise NotImplementedError


    def remove_blockdevice(self, blockdevice):
        """
        Deallocates a block device

        @param   blockdevice: the block device to deallocate
        @type    blockdevice: BlockDevice object
        @return: standard return code (see drbdmanage.exceptions)
        """
        raise NotImplementedError


    def up_blockdevice(self, blockdevice):
        """
        Activates a block device (e.g., connects an iSCSI resource)

        @param blockdevice: the block device to deactivate
        @type  blockdevice: BlockDevice object
        """
        raise NotImplementedError


    def down_blockdevice(self, blockdevice):
        """
        Deactivates a block device (e.g., disconnects an iSCSI resource)

        @param blockdevice: the block device to deactivate
        @type  blockdevice: BlockDevice object
        """
        raise NotImplementedError


    def create_snapshot(self, name, vol_id, blockdevice):
        """
        Creates a snapshot of a volume under the same resource prefix name

        @param   name: snapshot name; subject to name constraints
        @type    name: str
        @param   vol_id: volume id
        @type    vol_id: int
        @param   blockdevice: the existing block device to snapshot
        @type    blockdevice: BlockDevice object
        @return: block device of the specified size
        @rtype:  BlockDevice object; None if the allocation fails
        """
        raise NotImplementedError


    def restore_snapshot(self, name, vol_id, blockdevice):
        """
        Creates a snapshot of a volume under a new resource prefix name

        @param   name: resource name; subject to name constraints
        @type    name: str
        @param   vol_id: volume id
        @type    vol_id: int
        @param   blockdevice: the existing block device to snapshot
        @type    blockdevice: BlockDevice object
        @return: block device of the specified size
        @rtype:  BlockDevice object; None if the allocation fails
        """
        raise NotImplementedError


    def remove_snapshot(self, blockdevice):
        """
        Deallocates a snapshot block device

        @param   blockdevice: the block device to deallocate
        @type    blockdevice: BlockDevice object
        @return: standard return code (see drbdmanage.exceptions)
        """
        raise NotImplementedError


    def update_pool(self, drbdnode):
        """
        Retrieves storage pool space information

        Determines the current total and free space that is available for
        allocation on the host this instance of the drbdmanage server is
        running on and updates the DrbdNode object with that information.

        @param   node: The node to update
        @type    node: DrbdNode object
        @return: standard return code, pool total size, pool free space
        """
        raise NotImplementedError


    def reconfigure(self):
        """
        Reconfigures the storage plugin
        """
        raise NotImplementedError
