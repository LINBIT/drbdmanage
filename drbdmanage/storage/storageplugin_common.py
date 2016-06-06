#!/usr/bin/env python2
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2013-2016 LINBIT HA-Solutions GmbH
                            Author: Roland Kammerer

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
import errno
import json
import logging
import time
import drbdmanage.exceptions as exc
import drbdmanage.storage.storagecore as storcore
import drbdmanage.storage.persistence as storpers
import drbdmanage.utils as utils


class StoragePluginCheckFailedException(Exception):
    """
    Indicates failure to check for existing logical volumes.
    Not to be exposed to other parts of drbdmanage.
    """

    def __init__(self):
        super(StoragePluginCheckFailedException, self).__init__()


class StoragePluginException(Exception):
    """
    Indicates failure during the execution of plugin's internal functions.
    Not to be exposed to other parts of drbdmanage.
    """

    def __init__(self):
        super(StoragePluginException, self).__init__()


class StoragePluginUnmanagedVolumeException(Exception):
    """
    Indicates the attempt to operate on a volume not managed by drbdmanage
    """

    def __init__(self):
        super(StoragePluginUnmanagedVolumeException, self).__init__()


class StoragePluginCommon(object):

    # Traits map, str = str key/value pairs
    traits = None

    def __init__(self):
        self.traits = {}

    def _deserialize(self, data):
        """
        Used for load_state to deserialize the json blob read from file
        """
        loaded_objects = {}
        for blockdev_properties in data.itervalues():
            blockdev = storpers.BlockDevicePersistence.load(
                blockdev_properties
            )
            if blockdev is not None:
                loaded_objects[blockdev.get_name()] = blockdev
            else:
                raise exc.PersistenceException

        return loaded_objects

    def load_state(self):
        """
        Load the saved state of this module's managed logical volumes
        """

        state_file = None
        state_filename = self.STATEFILE
        plugin_name = self.NAME
        ret = {}
        try:
            state_file = open(state_filename, "r")

            loaded_data = state_file.read()

            state_file.close()
            state_file = None

            stored_hash = None
            line_begin = 0
            line_end = 0
            while line_end >= 0 and stored_hash is None:
                line_end = loaded_data.find("\n", line_begin)
                if line_end != -1:
                    line = loaded_data[line_begin:line_end]
                else:
                    line = loaded_data[line_begin:]
                if line.startswith("sig:"):
                    stored_hash = line[4:]
                else:
                    line_begin = line_end + 1
            if stored_hash is not None:
                # truncate load_data so it does not contain the signature line
                loaded_data = loaded_data[:line_begin]
                data_hash = utils.DataHash()
                data_hash.update(loaded_data)
                computed_hash = data_hash.get_hex_hash()
                if computed_hash != stored_hash:
                    logging.warning(
                        plugin_name + ": Data in state file '%s' has "
                        "an invalid signature, this file may be corrupt"
                        % (state_filename)
                    )
            else:
                logging.warning(
                    plugin_name + ": Data in state file '%s' is unsigned"
                    % (state_filename)
                )

            # Deserialize the saved objects
            loaded_property_map = json.loads(loaded_data)
            ret = self._deserialize(loaded_property_map)

        except exc.PersistenceException as pers_exc:
            # re-raise
            raise pers_exc
        except IOError as io_err:
            if io_err.errno == errno.ENOENT:
                # State file does not exist, probably because the module
                # is being used for the first time.
                #
                # Generate an empty configuration
                pass
                ret = self._deserialize({})
            else:
                logging.error(
                    plugin_name + ": Loading the state file '%s' failed due to an "
                    "I/O error, error message from the OS: %s"
                    % (state_filename, io_err.strerror)
                )
                raise exc.PersistenceException
        except OSError as os_err:
            logging.error(
                plugin_name + ": Loading the state file '%s' failed, "
                "error message from the OS: %s"
                % (state_filename, str(os_err))
            )
            raise exc.PersistenceException
        except Exception as unhandled_exc:
            logging.error(
                plugin_name + ": Loading the state file '%s' failed, "
                "unhandled exception: %s"
                % (state_filename, str(unhandled_exc))
            )
            raise exc.PersistenceException
        finally:
            if state_file is not None:
                state_file.close()

        return ret

    def _serialize(self, save_objects):
        save_bd_properties = {}
        for blockdev in save_objects.itervalues():
            bd_persist = storpers.BlockDevicePersistence(blockdev)
            bd_persist.save(save_bd_properties)
        return save_bd_properties

    def save_state(self, save_objects):
        """
        Save the state of this module's managed logical volumes
        """
        state_file = None
        state_filename = self.STATEFILE
        plugin_name = self.NAME

        try:
            save_bd_properties = self._serialize(save_objects)
            try:
                state_file = open(state_filename, "w")
                data_hash = utils.DataHash()
                save_data = json.dumps(
                    save_bd_properties, indent=4, sort_keys=True
                )
                save_data += "\n"
                data_hash.update(save_data)
                state_file.write(save_data)
                state_file.write("sig:%s\n" % (data_hash.get_hex_hash()))
            except IOError as io_err:
                logging.error(
                    "%s: Saving to the state file '%s' failed due to an "
                    "I/O error, error message from the OS: %s"
                    % (plugin_name, state_filename, str(io_err))
                )
                raise exc.PersistenceException
            except OSError as os_err:
                logging.error(
                    "%s: Saving to the state file '%s' failed, "
                    "error message from the OS: %s"
                    % (plugin_name, state_filename, str(os_err))
                )
                raise exc.PersistenceException
        except exc.PersistenceException as pers_exc:
            # re-raise
            raise pers_exc
        except Exception as unhandled_exc:
            logging.error(
                "%s: Saving to the state file '%s' failed, "
                "unhandled exception: %s"
                % (plugin_name, state_filename, str(unhandled_exc))
            )
            raise exc.PersistenceException
        finally:
            if state_file is not None:
                state_file.close()

    def get_blockdevice(self, bd_name):
        """
        Retrieves a registered BlockDevice object

        The BlockDevice object allocated and registered under the supplied
        resource name and volume id is returned.

        @return: the specified block device; None on error
        @rtype:  BlockDevice object
        """
        blockdev = None
        try:
            blockdev = self._volumes[bd_name]
        except KeyError:
            pass
        return blockdev

    def create_blockdevice(self, name, vol_id, size):
        """
        Allocates a block device as backing storage for a DRBD volume

        @param   name: resource name; subject to name constraints
        @type    name: str
        @param   id: volume id
        @type    id: int
        @param   size: size of the block device in kiB (binary kilobytes)
        @type    size: long
        @return: block device of the specified size
        @rtype:  BlockDevice object; None if the allocation fails
        """
        blockdev = None
        vol_name = self.vol_name(name, vol_id)

        try:
            # Remove any existing vol
            tries = 0
            # Check whether an vol with that name exists already
            vol_exists = self._check_vol_exists(vol_name)
            if vol_exists:
                if self._volumes.get(vol_name) is None:
                    # Unknown vol, possibly user-generated and not managed
                    # by drbdmanage. Abort.
                    raise StoragePluginUnmanagedVolumeException
                logging.warning(
                    "%s: Volume '%s' exists already, attempting to remove it."
                    % (self.NAME, vol_name)
                )
            while vol_exists and tries < self.MAX_RETRIES:
                if tries > 0:
                    try:
                        time.sleep(self.RETRY_DELAY)
                    except OSError:
                        pass

                # vol exists, maybe from an earlier attempt at creating
                # the volume. Remove existing vol and recreate it.
                logging.warning(
                    "%s: Attempt %d of %d: "
                    "Removal of volume '%s' failed."
                    % (self.NAME, tries + 1, self.MAX_RETRIES, vol_name)
                )
                self._remove_vol(vol_name)
                # Check whether the removal was successful
                vol_exists = self._check_vol_exists(vol_name)
                if not vol_exists:
                    try:
                        del self._volumes[vol_name]
                    except KeyError:
                        pass
                    self.save_state(self._volumes)
                tries += 1

            # Create the vol, unless the removal of any existing vol under
            # the specified name was unsuccessful
            if not vol_exists:
                tries = 0
                while blockdev is None and tries < self.MAX_RETRIES:
                    if tries > 0:
                        try:
                            time.sleep(self.RETRY_DELAY)
                        except OSError:
                            pass

                    self._create_vol(vol_name, size)
                    vol_exists = self._check_vol_exists(vol_name)
                    if vol_exists:
                        # volM reports that the vol exists, create the
                        # blockdevice object representing the vol in
                        # drbdmanage and register it in the volM module's
                        # persistent data structures
                        blockdev = storcore.BlockDevice(
                            vol_name, size,
                            self._vg_path + vol_name
                        )
                        self._volumes[vol_name] = blockdev
                        self.save_state(self._volumes)
                    else:
                        logging.error(
                            "%s: Attempt %d of %d: "
                            "Creation of vol '%s' failed."
                            % (self.NAME, tries + 1, self.MAX_RETRIES, vol_name)
                        )
                    tries += 1
            else:
                logging.error(
                    "%s: Removal of an existing volume '%s' failed, aborting "
                    % (self.NAME, vol_name)
                )
        except (StoragePluginCheckFailedException, StoragePluginException):
            # Unable to run one of the volM commands
            # The error is reported by the corresponding function
            #
            # Abort
            pass
        except exc.PersistenceException:
            # save_state() failed
            # If the vol was created, attempt to roll back
            if blockdev is not None:
                try:
                    self._remove_vol(vol_name)
                except StoragePluginException:
                    pass
                try:
                    vol_exists = self._check_vol_exists(vol_name)
                    if not vol_exists:
                        blockdev = None
                        del self._volumes[vol_name]
                except (StoragePluginCheckFailedException, KeyError):
                    pass
        except StoragePluginUnmanagedVolumeException:
            # Collision with a volume not managed by drbdmanage
            logging.error(
                "%s: vol '%s' exists already, but is unknown to "
                "drbdmanage's storage subsystem. Aborting."
                % (self.NAME, vol_name)
            )
        except Exception as unhandled_exc:
            logging.error(
                "%s: Block device creation failed, "
                "unhandled exception: %s"
                % (self.NAME, str(unhandled_exc))
            )

        return blockdev

    def remove_blockdevice(self, blockdevice):
        """
        Deallocates a block device

        @param   blockdevice: the block device to deallocate
        @type    blockdevice: BlockDevice object
        @return: standard return code (see drbdmanage.exceptions)
        """
        fn_rc = exc.DM_ESTORAGE
        vol_name = blockdevice.get_name()

        try:
            if self._volumes.get(vol_name) is not None:
                tries = 0
                vol_exists = self._check_vol_exists(vol_name)
                if not vol_exists:
                    fn_rc = exc.DM_SUCCESS
                while vol_exists and tries < self.MAX_RETRIES:
                    if tries > 0:
                        try:
                            time.sleep(self.RETRY_DELAY)
                        except OSError:
                            pass

                    self._remove_vol(vol_name)
                    # Check whether the removal was successful
                    vol_exists = self._check_vol_exists(vol_name)
                    if not vol_exists:
                        # Removal successful. Any potential further errors,
                        # e.g. with saving the state, can be corrected later.
                        fn_rc = exc.DM_SUCCESS
                        try:
                            del self._volumes[vol_name]
                        except KeyError:
                            pass
                        self.save_state(self._volumes)
                    else:
                        logging.warning(
                            "%s: Attempt %d of %d: "
                            "Removal of vol '%s' failed"
                            % (self.NAME, tries + 1, self.MAX_RETRIES, vol_name)
                        )
                    tries += 1
            else:
                raise StoragePluginUnmanagedVolumeException
        except (StoragePluginCheckFailedException, StoragePluginException):
            # Unable to run one of the commands
            # The error is reported by the corresponding function
            #
            # Abort
            pass
        except StoragePluginUnmanagedVolumeException:
            # Collision with a volume not managed by drbdmanage
            logging.error(
                "%s: vol '%s' exists, but is unknown to "
                "drbdmanage's storage subsystem. Aborting removal." % (self.NAME)
            )
        except exc.PersistenceException:
            # save_state() failed
            # If the module has a volume listed although it has actually been
            # removed successfully, then that can easily be corrected later
            pass
        except Exception as unhandled_exc:
            logging.error(
                "%s: Removal of a block device failed, "
                "unhandled exception: %s"
                % (self.NAME, str(unhandled_exc))
            )

        if fn_rc != exc.DM_SUCCESS:
            logging.error(
                "%s: Removal of vol '%s' failed"
                % (self.NAME, vol_name)
            )

        return fn_rc

    def extend_blockdevice(self, blockdevice, size):
        """
        Deallocates a block device

        @param   blockdevice: the block device to deallocate
        @type    blockdevice: BlockDevice object
        @param   size: new size of the block device in kiB (binary kilobytes)
        @type    size: long
        @return: standard return code (see drbdmanage.exceptions)
        """
        fn_rc = exc.DM_ESTORAGE
        vol_name = blockdevice.get_name()

        try:
            if self._volumes.get(vol_name) is not None:
                if self._extend_vol(vol_name, size):
                    fn_rc = exc.DM_SUCCESS
            else:
                raise StoragePluginUnmanagedVolumeException
        except (StoragePluginCheckFailedException, StoragePluginException):
            # Unable to run one of the commands
            # The error is reported by the corresponding function
            #
            # Abort
            pass
        except StoragePluginUnmanagedVolumeException:
            # Collision with a volume not managed by drbdmanage
            logging.error(
                "%s: vol '%s' is unknown to drbdmanage's storage subsystem. "
                "Aborting extend operation." % (self.NAME)
            )
        except exc.PersistenceException:
            # save_state() failed
            # If the module has an vol listed although it has actually been
            # removed successfully, then that can easily be corrected later
            pass
        except Exception as unhandled_exc:
            logging.error(
                "%s: Removal of a block device failed, "
                "unhandled exception: %s"
                % (self.NAME, str(unhandled_exc))
            )
        return fn_rc

    def up_blockdevice(self, blockdev):
        """
        Activates a block device (e.g., connects an iSCSI resource)

        @param blockdevice: the block device to deactivate
        @type  blockdevice: BlockDevice object
        """
        return exc.DM_SUCCESS

    def down_blockdevice(self, blockdev):
        """
        Deactivates a block device (e.g., disconnects an iSCSI resource)

        @param blockdevice: the block device to deactivate
        @type  blockdevice: BlockDevice object
        """
        return exc.DM_SUCCESS

    def vol_name(self, name, vol_id):
        """
        Build a volume name from the resource name and volume id
        """
        return ("%s_%.2d" % (name, vol_id))

    # SNAPSHOT METHODS
    def snapshot_volume_name(self, snaps_name, source_name, vol_id):
        """
        Generate a unique name for a snapshot of a volume
        """
        snaps_prefix = source_name
        idx = snaps_prefix.find(".")
        if idx != -1:
            snaps_prefix = snaps_prefix[:idx]
        else:
            idx = snaps_prefix.rfind("_")
            if idx != -1:
                snaps_prefix = snaps_prefix[:idx]
        generated_name = "%s.%s_%.2d" % (snaps_prefix, snaps_name, vol_id)
        return generated_name

    def _create_snapshot(self, vol_name, source_blockdev):
        """
        Implements snapshot operations (snapshot/restore)

        @param   vol_name: name for the newly created block device
        @type    vol_name: str
        @param   vol_id: volume id
        @type    vol_id: int
        @param   source_blockdev: the existing block device to snapshot
        @type    source_blockdev: BlockDevice object
        @return: block device of the specified size
        @rtype:  BlockDevice object; None if the allocation fails
        """
        blockdev = None
        lv_name = source_blockdev.get_name()

        try:
            tries = 0
            while blockdev is None and tries < self.MAX_RETRIES:
                if tries > 0:
                    try:
                        time.sleep(self.RETRY_DELAY)
                    except OSError:
                        pass
                tries += 1

                self._create_snapshot_impl(vol_name, lv_name)
                snaps_exists = self._check_vol_exists(vol_name)
                if snaps_exists:
                    size = source_blockdev.get_size_kiB()
                    blockdev = storcore.BlockDevice(
                        vol_name, size,
                        self._vg_path + vol_name
                    )
                    self._volumes[vol_name] = blockdev
                    self.up_blockdevice(blockdev)
                    self.save_state(self._volumes)
                else:
                    logging.warning(
                        "%s: Attempt %d of %d: "
                        "Creation of snapshot volume '%s' failed."
                        % (self.NAME, tries + 1, self.MAX_RETRIES, vol_name)
                    )
        except (StoragePluginCheckFailedException, StoragePluginException):
            # Unable to run one of the LV commands
            # The error is reported by the corresponding function
            #
            # Abort
            pass
        except StoragePluginUnmanagedVolumeException:
            # Collision with a volume not managed by drbdmanage
            logging.error(
                "%s: LV '%s' exists already, but is unknown to "
                "drbdmanage's storage subsystem. Aborting."
                % (self.NAME, lv_name)
            )
        except exc.PersistenceException:
            # save_state() failed
            # If the snapshot was created, attempt to roll back
            if blockdev is not None:
                vol_name = blockdev.get_name()
                try:
                    self._remove_vol(vol_name)
                except StoragePluginException:
                    pass
                try:
                    vol_exists = self._check_vol_exists(vol_name)
                    if not vol_exists:
                        blockdev = None
                        try:
                            del self._volumes[vol_name]
                        except KeyError:
                            pass
                except StoragePluginCheckFailedException:
                    pass
        except NotImplementedError:
            raise NotImplementedError
        except Exception as unhandled_exc:
            logging.error(
                "%s: Block device creation failed, "
                "unhandled exception: %s"
                % (self.NAME, str(unhandled_exc))
            )

        return blockdev

    def create_snapshot(self, snaps_name, vol_id, source_blockdev):
        """
        Creates a snapshot of a volume under the same resource prefix name

        @param   snaps_name: snapshot name; subject to name constraints
        @type    snaps_name: str
        @param   vol_id: volume id
        @type    vol_id: int
        @param   source_blockdev: the existing block device to snapshot
        @type    source_blockdev: BlockDevice object
        @return: block device of the specified size
        @rtype:  BlockDevice object; None if the allocation fails
        """
        blockdev = None
        vol_name = source_blockdev.get_name()

        try:
            vol_name = self.snapshot_volume_name(snaps_name, vol_name, vol_id)

            blockdev = self._create_snapshot(vol_name, source_blockdev)
        except exc.PersistenceException:
            logging.debug(
                "%s: create_snapshot(): caught PersistenceException, "
                "state saving failed" % (self.NAME)
            )
        except NotImplementedError:
            raise NotImplementedError
        except Exception as unhandled_exc:
            logging.error(
                "%s: Block device creation failed, "
                "unhandled exception: %s"
                % (self.NAME, str(unhandled_exc))
            )

        return blockdev

    def restore_snapshot(self, restore_name, vol_id, source_blockdev):
        """
        Creates a snapshot of a volume under a new resource prefix name

        @param   restore_name: new resource name; subject to name constraints
        @type    restore_name: str
        @param   vol_id: volume id
        @type    vol_id: int
        @param   source_blockdev: the existing block device to snapshot
        @type    source_blockdev: BlockDevice object
        @return: block device of the specified size
        @rtype:  BlockDevice object; None if the allocation fails
        """
        blockdev = None

        try:
            vol_name = self.vol_name(restore_name, vol_id)

            blockdev = self._restore_snapshot(vol_name, source_blockdev)
        except NotImplementedError:
            raise NotImplementedError
        except exc.PersistenceException:
            logging.debug(
                "%s: restore_snapshot(): caught PersistenceException, "
                "state saving failed" % (self.NAME)
            )
        except Exception as unhandled_exc:
            logging.error(
                "%s: Block device creation failed, "
                "unhandled exception: %s"
                % (self.NAME, str(unhandled_exc))
            )

        return blockdev

    def remove_snapshot(self, blockdevice):
        print "remove_snapshot", self
        """
        Deallocates a snapshot block device

        @param   blockdevice: the block device to deallocate
        @type    blockdevice: BlockDevice object
        @return: standard return code (see drbdmanage.exceptions)
        """
        try:
            return self._remove_snapshot(blockdevice)
        except NotImplementedError:
            raise NotImplementedError

    def get_trait(self, key):
        return self.traits.get(key)
