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

import os
import time
import logging
import subprocess
import drbdmanage.drbd.drbdcommon
import drbdmanage.storage.storagecommon
import drbdmanage.storage.storagecore as storcore
import drbdmanage.storage.persistence as storpers
import drbdmanage.storage.lvm_common as lvmcom

import drbdmanage.consts as consts
import drbdmanage.exceptions as exc
import drbdmanage.utils as utils

from drbdmanage.storage.storageplugin_common import (
    StoragePluginException, StoragePluginCheckFailedException, StoragePluginUnmanagedVolumeException)


class LvmThinPool(lvmcom.LvmCommon):

    """
    LVM thin pools/thin volumes backing store plugin for the drbdmanage server

    Provides backing store block devices for DRBD volumes by managing the
    allocation of logical volumes backed by thin pools allocated inside a
    volume group of the logical volume manager (LVM)
    """

    NAME = 'ThinPool'
    SAVE_POOLS, SAVE_VOLUMES = range(2)

    # Configuration file keys
    KEY_DEV_PATH   = "dev-path"
    KEY_LVM_PATH   = "lvm-path"
    KEY_POOL_RATIO = "pool-ratio"

    # The ratio, in percent, between the initial size of a thin pool and the
    # size of the volume the thin pool is allocated for
    DEFAULT_POOL_RATIO = 135

    # Path to state file of this module
    STATEFILE = "/var/lib/drbdmanage/drbdmanaged-lvm-thinpool.local.json"

    # Command names of LVM utilities
    LVM_CREATE    = "lvcreate"
    LVM_EXTEND    = "lvextend"
    LVM_REMOVE    = "lvremove"
    LVM_LV_CHANGE = "lvchange"
    LVM_VG_CHANGE = "vgchange"
    LVM_LVS       = "lvs"
    LVM_VGS       = "vgs"

    # lvs exit code if the LV was not found
    LVM_LVS_ENOENT = 5

    # Delay (float, in seconds) for lvcreate/lvremove retries
    RETRY_DELAY = 1

    # Maximum number of retries
    MAX_RETRIES = 2

    # Module configuration defaults
    CONF_DEFAULTS = {
        KEY_DEV_PATH: "/dev/",
        consts.KEY_VG_NAME: consts.DEFAULT_VG,
        KEY_LVM_PATH: "/sbin",
        KEY_POOL_RATIO: str(DEFAULT_POOL_RATIO)
    }

    # Plugin configuration
    _conf = None

    # Map of volumes allocated by the plugin
    _volumes = None

    # Map of pools allocated by the plugin
    _pools = None

    # Lookup table for finding the pool that contains a volume
    _pool_lookup = None

    # Cached settings
    # Set during initialization
    _vg_path      = None
    _cmd_create   = None
    _cmd_extend   = None
    _cmd_remove   = None
    _cmd_lvchange = None
    _cmd_vgchange = None
    _cmd_lvs      = None
    _cmd_vgs      = None
    _subproc_env  = None

    def __init__(self, server):
        super(LvmThinPool, self).__init__()
        self.traits[storcore.StoragePlugin.KEY_PROV_TYPE] = storcore.StoragePlugin.PROV_TYPE_THIN
        self.reconfigure()

    def get_default_config(self):
        return LvmThinPool.CONF_DEFAULTS.copy()

    def get_config(self):
        return self._conf

    def set_config(self, config):
        self.reconfigure(config)
        return True

    def reconfigure(self, config=None):
        try:
            # Setup the environment for subprocesses
            self._subproc_env = dict(os.environ.items())
            self._subproc_env["LC_ALL"] = "C"
            self._subproc_env["LANG"]   = "C"

            if config:
                self._conf = config
            else:
                self._conf = LvmThinPool.CONF_DEFAULTS.copy()

            # Setup cached settings
            self._vg_path = utils.build_path(
                self._conf[LvmThinPool.KEY_DEV_PATH], self._conf[consts.KEY_VG_NAME]
            ) + "/"
            self._cmd_create   = utils.build_path(self._conf[LvmThinPool.KEY_LVM_PATH], LvmThinPool.LVM_CREATE)
            self._cmd_extend   = utils.build_path(self._conf[LvmThinPool.KEY_LVM_PATH], LvmThinPool.LVM_EXTEND)
            self._cmd_remove   = utils.build_path(self._conf[LvmThinPool.KEY_LVM_PATH], LvmThinPool.LVM_REMOVE)
            self._cmd_lvchange = utils.build_path(self._conf[LvmThinPool.KEY_LVM_PATH], LvmThinPool.LVM_LV_CHANGE)
            self._cmd_vgchange = utils.build_path(self._conf[LvmThinPool.KEY_LVM_PATH], LvmThinPool.LVM_VG_CHANGE)
            self._cmd_lvs      = utils.build_path(self._conf[LvmThinPool.KEY_LVM_PATH], LvmThinPool.LVM_LVS)
            self._cmd_vgs      = utils.build_path(self._conf[LvmThinPool.KEY_LVM_PATH], LvmThinPool.LVM_VGS)

            # Load the saved state
            self._pools, self._volumes, self._pool_lookup = self.load_state()
        except exc.PersistenceException as pers_exc:
            logging.warning(
                "LvmThinPool plugin: Cannot load state file '%s'"
                % (self.STATEFILE)
            )
            raise pers_exc
        except Exception as unhandled_exc:
            logging.error(
                "LvmThinPool: initialization failed, unhandled exception: %s"
                % (str(unhandled_exc))
            )
            # Re-raise
            raise unhandled_exc

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
        blockdev  = None
        pool      = None

        # Indicates that the plugin's state needs to be saved
        save_state_flag = False

        try:
            # Calculate the size of the backing thin pool
            pool_ratio = LvmThinPool.DEFAULT_POOL_RATIO
            try:
                pool_ratio = float(self._conf[LvmThinPool.KEY_POOL_RATIO])
                # Fall back to the default if the size_ratio really does not
                # make any sense
                if pool_ratio <= 0:
                    pool_ratio = LvmThinPool.DEFAULT_POOL_RATIO
            except ValueError:
                pass
            pool_size = long(size * (pool_ratio / 100))

            # Generate the volume and pool names
            lv_name = self.vol_name(name, vol_id)
            pool_name = ThinPool.generate_pool_name(name, vol_id)

            # Check for collisions (very unlikely)
            pool_exists = self._check_vol_exists(pool_name)
            if not pool_exists:
                tries = 0
                while pool is None and tries < LvmThinPool.MAX_RETRIES:
                    if tries > 0:
                        try:
                            time.sleep(LvmThinPool.RETRY_DELAY)
                        except OSError:
                            pass
                    tries += 1

                    # Create the thin pool
                    self.__create_pool(pool_name, pool_size)
                    pool_exists = self._check_vol_exists(pool_name)
                    if pool_exists:
                        pool = ThinPool(pool_name, pool_size)
                        self._pools[pool_name] = pool
                        save_state_flag = True
                    else:
                        logging.warning(
                            "LvmThinPool: Attempt %d of %d: "
                            "Creation of pool '%s' failed."
                            % (tries, LvmThinPool.MAX_RETRIES, pool_name)
                        )

                if pool_exists:
                    tries = 0
                    while (blockdev is None and
                           tries < LvmThinPool.MAX_RETRIES):
                        if tries > 0:
                            try:
                                time.sleep(LvmThinPool.RETRY_DELAY)
                            except OSError:
                                pass
                        tries += 1

                        # Create the logical volume
                        self._create_vol(lv_name, pool_name, size)
                        lv_exists = self._check_vol_exists(lv_name)
                        if lv_exists:
                            # LVM reports that the LV exists, create the
                            # blockdevice object representing the LV in
                            # drbdmanage and register it in the LVM
                            # module's persistent data structures
                            blockdev = storcore.BlockDevice(
                                lv_name, size,
                                self._vg_path + lv_name
                            )
                            pool.add_volume(lv_name)
                            self._volumes[lv_name] = blockdev
                            self._pool_lookup[lv_name] = pool_name
                            self.up_blockdevice(blockdev)
                            save_state_flag = True
                        else:
                            logging.warning(
                                "LvmThinPool: Attempt %d of %d: "
                                "Creation of LV '%s' failed."
                                % (tries, LvmThinPool.MAX_RETRIES, lv_name)
                            )
            else:
                logging.error(
                    "LvmThinPool: Creation of pool '%s' failed, "
                    "name collision detected. "
                    "(This is commonly a temporary error that is "
                    "automatically resolved later)"
                    % (pool_name)
                )
        except (StoragePluginCheckFailedException, StoragePluginException):
            # Unable to run one of the LVM commands
            # The error is reported by the corresponding function
            #
            # Abort
            pass
        except StoragePluginUnmanagedVolumeException:
            # Collision with a volume not managed by drbdmanage
            logging.error(
                "LvmThinPool: LV '%s' exists already, but is unknown to "
                "drbdmanage's storage subsystem. Aborting."
                % (lv_name)
            )
        except Exception as unhandled_exc:
            logging.error(
                "LvmThinPool: Block device creation failed, "
                "unhandled exception: %s"
                % (str(unhandled_exc))
            )
        try:
            if save_state_flag:
                self.save_state([self._pools, self._volumes])
        except exc.PersistenceException:
            # save_state() failed
            # If the LV was created, attempt to roll back
            if blockdev is not None:
                lv_name   = blockdev.get_name()
                pool_name = pool.get_name()
                try:
                    self._remove_vol(lv_name)
                except StoragePluginException:
                    pass
                try:
                    self._remove_vol(pool_name)
                except StoragePluginException:
                    pass
                try:
                    lv_exists = self._check_vol_exists(lv_name)
                    if not lv_exists:
                        blockdev = None
                        try:
                            del self._volumes[lv_name]
                        except KeyError:
                            pass
                    pool_exists = self._check_vol_exists(pool_name)
                    if not pool_exists:
                        pool.remove_volume(lv_name)
                        try:
                            del self._pool_lookup[lv_name]
                        except KeyError:
                            pass
                        try:
                            del self._pools[pool_name]
                        except KeyError:
                            pass
                except (StoragePluginCheckFailedException, KeyError):
                    pass

        return blockdev

    def remove_blockdevice(self, blockdev):
        """
        Deallocates a block device

        @param   blockdevice: the block device to deallocate
        @type    blockdevice: BlockDevice object
        @return: standard return code (see drbdmanage.exceptions)
        """
        fn_rc = exc.DM_ESTORAGE

        # indicates that the plugin's state needs to be saved
        save_state_flag = False

        try:
            lv_name = blockdev.get_name()
            if self._volumes.get(lv_name) is not None:
                pool_name = None
                pool      = None

                # Find the volume's thin pool
                try:
                    pool_name = self._pool_lookup[lv_name]
                    pool      = self._pools[pool_name]
                except KeyError:
                    logging.warning(
                        "LvmThinPool: Cannot find the thin pool "
                        "for LV '%s'"
                        % (lv_name)
                    )

                tries = 0
                lv_exists = self._check_vol_exists(lv_name)
                while lv_exists and tries < LvmThinPool.MAX_RETRIES:
                    if tries > 0:
                        try:
                            time.sleep(LvmThinPool.RETRY_DELAY)
                        except OSError:
                            pass
                    tries += 1

                    self._remove_vol(lv_name)
                    lv_exists = self._check_vol_exists(lv_name)
                    if lv_exists:
                        logging.warning(
                            "LvmThinPool: Attempt %d of %d: "
                            "Removal of LV '%s' failed."
                            % (tries, LvmThinPool.MAX_RETRIES, lv_name)
                        )

                # Even if the LV is not present anymore, its pool may still
                # exist and may be pending removal
                #
                # FIXME: If the LV is removed successfully, but the pool
                #        removal fails, that will leave a stale pool entry
                #        that needs to be cleaned up later
                if not lv_exists:
                    save_state_flag = True
                    del self._volumes[lv_name]
                    try:
                        del self._pool_lookup[lv_name]
                    except KeyError:
                        pass
                    if pool is not None:
                        pool.remove_volume(lv_name)

                        # If the last volume was removed from the pool,
                        # remove the pool itself too
                        if pool.is_empty():
                            pool_tries = 0
                            pool_exists = True
                            while (pool_exists and
                                   pool_tries < LvmThinPool.MAX_RETRIES):
                                if pool_tries > 0:
                                    try:
                                        time.sleep(
                                            LvmThinPool.RETRY_DELAY
                                        )
                                    except OSError:
                                        pass
                                pool_tries += 1

                                self._remove_vol(pool_name)
                                pool_exists = self._check_vol_exists(
                                    pool_name
                                )
                                if not pool_exists:
                                    del self._pools[pool_name]
                                    # Removal of the LV and its corresponding
                                    # pool was successful
                                    fn_rc = exc.DM_SUCCESS
                                else:
                                    logging.error(
                                        "LvmThinPool: Attempt %d of %d: "
                                        "Removal of thin pool '%s' failed."
                                        % (pool_tries,
                                           LvmThinPool.MAX_RETRIES,
                                           pool_name)
                                    )
                        else:
                            # Condition: pool is not empty
                            # Report successful removal of the LV, there
                            # is nothing to do with the pool
                            fn_rc = exc.DM_SUCCESS
                    else:
                        # Condition: pool is None
                        #
                        # No pool to remove, report successful removal of
                        # the LV
                        # TODO: Check whether that makes sense or it would
                        #       be better to return an error here
                        fn_rc = exc.DM_SUCCESS
            else:
                raise StoragePluginUnmanagedVolumeException
        except (StoragePluginCheckFailedException, StoragePluginException):
            # Unable to run one of the LVM commands
            # The error is reported by the corresponding function
            #
            # Abort
            pass
        except StoragePluginUnmanagedVolumeException:
            # Collision with a volume not managed by drbdmanage
            logging.error(
                "LvmThinPool: LV '%s' exists, but is unknown to "
                "drbdmanage's storage subsystem. Aborting removal."
            )
        except exc.PersistenceException:
            # save_state() failed
            # If the module has an LV listed although it has actually been
            # removed successfully, then that can easily be corrected later
            pass
        except Exception as unhandled_exc:
            logging.error(
                "LvmThinPool: Removal of a block device failed, "
                "unhandled exception: %s"
                % (str(unhandled_exc))
            )

        try:
            if save_state_flag:
                self.save_state([self._pools, self._volumes])
        except exc.PersistenceException:
            # save_state() failed
            # Can be corrected later, whenever the plugin figures out that
            # an LV that is still listed in its datastructures actually does
            # not exist
            pass

        return fn_rc

    def create_snapshot(self, name, vol_id, source_blockdev):
        """
        Allocates a block device as a snapshot of an existing block device

        @param   name: snapshot name; subject to name constraints
        @type    name: str
        @param   vol_id: volume id
        @type    vol_id: int
        @param   source_blockdev: the existing block device to snapshot
        @type    source_blockdev: BlockDevice object
        @return: block device of the specified size
        @rtype:  BlockDevice object; None if the allocation fails
        """
        blockdev  = None
        lv_name   = source_blockdev.get_name()
        pool_name = None
        pool      = None

        try:
            pool_name = self._pool_lookup[lv_name]
            pool      = self._pools[pool_name]
        except KeyError:
            logging.error(
                "LvmThinPool: Creation of snapshot '%s' of LV '%s' failed: "
                "Cannot find the associated thin pool"
                % (name, lv_name)
            )

        try:
            if pool is not None:
                snaps_base_name = self.vol_name(name, vol_id)
                snaps_suffix    = pool.extract_pool_name_suffix()
                snaps_name      = snaps_base_name + snaps_suffix

                tries = 0
                while blockdev is None and tries < LvmThinPool.MAX_RETRIES:
                    if tries > 0:
                        try:
                            time.sleep(LvmThinPool.RETRY_DELAY)
                        except OSError:
                            pass
                    tries += 1

                    self._create_snapshot_impl(snaps_name, lv_name)
                    snaps_exists = self._check_vol_exists(snaps_name)
                    if snaps_exists:
                        size = source_blockdev.get_size_kiB()
                        blockdev = storcore.BlockDevice(
                            snaps_name, size,
                            self._vg_path + snaps_name
                        )
                        pool.add_volume(snaps_name)
                        self._volumes[snaps_name] = blockdev
                        self._pool_lookup[snaps_name] = pool_name
                        self.up_blockdevice(blockdev)
                        self.save_state([self._pools, self._volumes])
        except (StoragePluginCheckFailedException, StoragePluginException):
            # Unable to run one of the LVM commands
            # The error is reported by the corresponding function
            #
            # Abort
            pass
        except StoragePluginUnmanagedVolumeException:
            # Collision with a volume not managed by drbdmanage
            logging.error(
                "LvmThinPool: LV '%s' exists already, but is unknown to "
                "drbdmanage's storage subsystem. Aborting."
                % (lv_name)
            )
        except exc.PersistenceException:
            # save_state() failed
            # If the snapshot was created, attempt to roll back
            if blockdev is not None:
                snaps_name = blockdev.get_name()
                try:
                    self._remove_vol(snaps_name)
                except StoragePluginException:
                    pass
                try:
                    snaps_exists = self._check_vol_exists(snaps_name)
                    if not snaps_exists:
                        blockdev = None
                        try:
                            del self._volumes[snaps_name]
                        except KeyError:
                            pass
                        try:
                            if pool is not None:
                                pool.remove_volume(snaps_name)
                                try:
                                    del self._pool_lookup[snaps_name]
                                except KeyError:
                                    pass
                        except KeyError:
                            pass
                    pool_exists = self._check_vol_exists(pool_name)
                    if not pool_exists:
                        pool.remove_volume(lv_name)
                        try:
                            del self._pools[pool_name]
                        except KeyError:
                            pass
                except (StoragePluginCheckFailedException, KeyError):
                    pass
        except Exception as unhandled_exc:
            logging.error(
                "LvmThinPool: Block device creation failed, "
                "unhandled exception: %s"
                % (str(unhandled_exc))
            )

        return blockdev

    def restore_snapshot(self, name, vol_id, source_blockdev):
        return self.create_snapshot(name, vol_id, source_blockdev)

    def _remove_snapshot(self, blockdevice):
        return self.remove_blockdevice(blockdevice)

    def up_blockdevice(self, blockdevice):
        fn_rc = exc.DM_ESTORAGE
        try:
            lv_name   = blockdevice.get_name()
            pool_name = None
            try:
                pool_name = self._pool_lookup[lv_name]
            except KeyError:
                pass

            vg_activated   = False
            pool_activated = False
            lv_activated   = False

            try:
                exec_args = [
                    self._cmd_vgchange, "-ay",
                    self._conf[consts.KEY_VG_NAME],
                ]
                utils.debug_log_exec_args(self.__class__.__name__, exec_args)
                lvm_rc = subprocess.call(
                    exec_args,
                    0, self._cmd_vgchange,
                    env=self._subproc_env, close_fds=True
                )
                if lvm_rc == 0:
                    vg_activated = True
            except OSError as os_err:
                logging.error(
                    "LvmThinPool: Volume group activation failed, "
                    "unable to run external program '%s', error message "
                    "from the OS: %s"
                    % (self._cmd_vgchange, str(os_err))
                )
                raise StoragePluginException

            if pool_name is not None:
                try:
                    exec_args = [
                        self._cmd_lvchange, "-ay", "-kn", "-K",
                        self._conf[consts.KEY_VG_NAME] + "/" +
                        pool_name
                    ]
                    utils.debug_log_exec_args(self.__class__.__name__, exec_args)
                    lvm_rc = subprocess.call(
                        exec_args,
                        0, self._cmd_lvchange,
                        env=self._subproc_env, close_fds=True
                    )
                    if lvm_rc == 0:
                        pool_activated = True
                except OSError as os_err:
                    logging.error(
                        "LvmThinPool: Thin pool activation failed, "
                        "unable to run external program '%s', error message "
                        "from the OS: %s"
                        % (self._cmd_lvchange, str(os_err))
                    )
                    raise StoragePluginException
            else:
                logging.error(
                    "LvmThinPool: Incomplete activation of volume '%s', "
                    "cannot find the associated thin pool"
                    % (lv_name)
                )
            try:
                exec_args = [
                    self._cmd_lvchange, "-ay", "-kn", "-K",
                    self._conf[consts.KEY_VG_NAME] + "/" +
                    lv_name
                ]
                utils.debug_log_exec_args(self.__class__.__name__, exec_args)
                lvm_rc = subprocess.call(
                    exec_args,
                    0, self._cmd_lvchange,
                    env=self._subproc_env, close_fds=True
                )
                if lvm_rc == 0:
                    lv_activated = True
            except OSError as os_err:
                logging.error(
                    "LvmThinPool: LV activation failed, unable to run "
                    "external program '%s', error message from the OS: %s"
                    % (self._cmd_lvchange, str(os_err))
                )
                raise StoragePluginException
            if vg_activated and pool_activated and lv_activated:
                fn_rc = exc.DM_SUCCESS
        except (StoragePluginCheckFailedException, StoragePluginException):
            # Unable to run one of the LVM commands
            # The error is reported by the corresponding function
            #
            # Abort
            pass
        except Exception as unhandled_exc:
            logging.error(
                "LvmThinPool: Block device creation failed, "
                "unhandled exception: %s"
                % (str(unhandled_exc))
            )

        return fn_rc

    def update_pool(self, node):
        """
        Updates the DrbdNode object with the current storage status

        Determines the current total and free space that is available for
        allocation on the host this instance of the drbdmanage server is
        running on and updates the DrbdNode object with that information.

        @param   node: The node to update
        @type    node: DrbdNode object
        @return: standard return code (see drbdmanage.exceptions)
        """
        fn_rc     = exc.DM_ESTORAGE
        pool_size = -1
        pool_free = -1

        lvm_proc = None
        try:
            exec_args = [
                self._cmd_vgs, "--noheadings", "--nosuffix",
                "--units", "k", "--separator", ",",
                "--options", "vg_size,vg_free",
                self._conf[consts.KEY_VG_NAME]
            ]
            utils.debug_log_exec_args(self.__class__.__name__, exec_args)
            lvm_proc = subprocess.Popen(
                exec_args,
                env=self._subproc_env, stdout=subprocess.PIPE,
                close_fds=True
            )
            pool_data = lvm_proc.stdout.readline()
            if len(pool_data) > 0:
                pool_data.strip()
                try:
                    size_data, free_data = pool_data.split(",")
                    size_data = self.discard_fraction(size_data)
                    free_data = self.discard_fraction(free_data)

                    # Parse values and assign them in two steps, so that
                    # either both values or none of them will be assigned,
                    # depending on whether parsing succeeds or not
                    size_value = long(size_data)
                    free_value = long(free_data)

                    # Assign values after successful parsing
                    pool_size = size_value
                    pool_free = free_value
                    fn_rc = exc.DM_SUCCESS
                except ValueError:
                    pass
        except Exception as unhandled_exc:
            logging.error(
                "LvmThinPool: Retrieving storage pool information failed, "
                "unhandled exception: %s"
                % (str(unhandled_exc))
            )
        finally:
            if lvm_proc is not None:
                try:
                    lvm_proc.stdout.close()
                except Exception:
                    pass
                lvm_proc.wait()

        return (fn_rc, pool_size, pool_free)

    def _deserialize(self, data):
        loaded_pools = {}
        loaded_volumes = {}
        loaded_lookup = {}
        if not data:
            return loaded_pools, loaded_volumes, loaded_lookup

        # Deserialize the block devices from the volumes map
        volumes_con = data["volumes"]
        for properties in volumes_con.itervalues():
            blockdev = storpers.BlockDevicePersistence.load(properties)
            if blockdev is not None:
                loaded_volumes[blockdev.get_name()] = blockdev
            else:
                raise exc.PersistenceException

        # Deserialize the thin pools from the pools map
        pools_con = data["pools"]
        for properties in pools_con.itervalues():
            thin_pool = ThinPoolPersistence.load(properties)
            if thin_pool is not None:
                loaded_pools[thin_pool.get_name()] = thin_pool
            else:
                raise exc.PersistenceException

        # Build the pool lookup table
        for pool in loaded_pools.itervalues():
            pool_name = pool.get_name()
            for volume_name in pool.iterate_volumes():
                loaded_lookup[volume_name] = pool_name

        return loaded_pools, loaded_volumes, loaded_lookup

    def _serialize(self, save_objects):
        save_pools = save_objects[self.SAVE_POOLS]
        save_volumes = save_objects[self.SAVE_VOLUMES]
        state_con = {}

        # Serialize the block devices into the volumes map
        volumes_con = {}
        for blockdev in save_volumes.itervalues():
            bd_persist = storpers.BlockDevicePersistence(blockdev)
            bd_persist.save(volumes_con)

        # Save the volumes map to the state map
        state_con["volumes"] = volumes_con

        # Save the thin pools to the state map
        pools_con = {}
        for thin_pool in save_pools.itervalues():
            p_thin_pool = ThinPoolPersistence(thin_pool)
            p_thin_pool.save(pools_con)
        state_con["pools"] = pools_con

        return state_con

    def _create_vol(self, lv_name, pool_name, size):
        try:
            exec_args = [
                self._cmd_create, "--wipesignatures=y", "-n", lv_name, "-V", str(size) + "k",
                "--thinpool", pool_name,
                self._conf[consts.KEY_VG_NAME]
            ]
            utils.debug_log_exec_args(self.__class__.__name__, exec_args)
            subprocess.call(
                exec_args,
                0, self._cmd_create,
                env=self._subproc_env, close_fds=True
            )
        except OSError as os_err:
            logging.error(
                "LvmThinPool: ThinPool creation failed, unable to run "
                "external program '%s', error message from the OS: %s"
                % (self._cmd_create, str(os_err))
            )
            raise StoragePluginException

    def _check_vol_exists(self, lv_name):
        return self.check_lv_exists(
            lv_name, self._conf[consts.KEY_VG_NAME],
            self._cmd_lvs, self._subproc_env, "LvmThinPool"
        )

    def _extend_vol(self, lv_name, size):
        return self.extend_lv(lv_name, self._conf[consts.KEY_VG_NAME], size,
                              self._cmd_extend, self._subproc_env, "LvmThinPool")

    def _remove_vol(self, lv_name):
        self.remove_lv(lv_name, self._conf[consts.KEY_VG_NAME],
                       self._cmd_remove, self._subproc_env, "LvmThinPool")

    def _create_snapshot_impl(self, snaps_name, lv_name):
        # "LVMThinPool: exec: %s -s %s/%s -n %s"
        #    % (lvcreate, self._conf[consts.KEY_VG_NAME], lv_name, snaps_name)
        try:
            exec_args = [
                self._cmd_create, "-s",
                self._conf[consts.KEY_VG_NAME] + "/" +
                lv_name, "-n", snaps_name
            ]
            utils.debug_log_exec_args(self.__class__.__name__, exec_args)
            subprocess.call(
                exec_args,
                0, self._cmd_create,
                env=self._subproc_env, close_fds=True
            )
        except OSError as os_err:
            logging.error(
                "LvmThinPool: Snapshot creation failed, unable to run "
                "external program '%s', error message from the OS: %s"
                % (self._cmd_create, str(os_err))
            )
            raise StoragePluginException

    def __create_pool(self, pool_name, size):
        """
        Creates an LVM thin pool
        """
        try:
            exec_args = [
                self._cmd_create, "-L", str(size) + "k",
                "-T", self._conf[consts.KEY_VG_NAME] + "/" + pool_name
            ]
            utils.debug_log_exec_args(self.__class__.__name__, exec_args)
            subprocess.call(
                exec_args,
                0, self._cmd_create,
                env=self._subproc_env, close_fds=True
            )
        except OSError as os_err:
            logging.error(
                "LvmThinPool: LV creation failed, unable to run "
                "external program '%s', error message from the OS: %s"
                % (self._cmd_create, str(os_err))
            )
            raise StoragePluginException


class ThinPool(drbdmanage.storage.storagecommon.GenericStorage):

    """
    Represents the configuration of an LVM ThinPool
    """

    # Name generation for thin pools:
    # Resource name, underscore, 2-digit volume id, underscore,
    # up to 15 digit time value (YYYYYMMDDhhmmss)
    # Therefore, 3 characters are required for the volume id string, and
    # up to 16 characters are required for the time value, so the maximum
    # length of a thin pool name must be allowed to be 19 characters longer
    # than a resource name.
    THINPOOL_NAME_MINLEN = 1
    THINPOOL_NAME_MAXLEN = consts.RES_NAME_MAXLEN + 19
    # Valid characters in addition to [a-zA-Z0-9]
    NAME_VALID_CHARS      = "_"
    # Additional valid characters, but not allowed as the first character
    NAME_VALID_INNER_CHARS = "-"

    _name    = None
    _volumes = None

    def __init__(self, name, size_kiB):
        super(ThinPool, self).__init__(size_kiB)
        self._name = drbdmanage.drbd.drbdcommon.GenericDrbdObject.name_check(
            name, ThinPool.THINPOOL_NAME_MINLEN, ThinPool.THINPOOL_NAME_MAXLEN,
            ThinPool.NAME_VALID_CHARS, ThinPool.NAME_VALID_INNER_CHARS
        )
        self._volumes = {}

    def get_name(self):
        return self._name

    def add_volume(self, name):
        self._volumes[name] = None

    def remove_volume(self, name):
        try:
            del self._volumes[name]
        except KeyError:
            pass

    def iterate_volumes(self):
        return self._volumes.iterkeys()

    def is_empty(self):
        """
        Indicates whether the thinpool contains any volumes
        """
        return False if len(self._volumes) > 0 else True

    @classmethod
    def generate_pool_name(cls, name, vol_id):
        pool_name = (
            "%s_%.2d_%lu"
            % (name, vol_id, long(time.strftime("%Y%m%d%H%M%S")))
        )
        return pool_name

    def extract_pool_name_suffix(self):
        index  = self._name.rfind("_")
        suffix = ""
        if index != -1:
            suffix = self._name[index:]
        return suffix


class ThinPoolPersistence(storpers.GenericPersistence):

    """
    Serializes ThinPool objects
    """

    SERIALIZABLE = ["_name", "_size_kiB"]

    def __init__(self, thin_pool):
        super(ThinPoolPersistence, self).__init__(thin_pool)

    def save(self, container):
        thin_pool  = self.get_object()
        properties = self.load_dict(self.SERIALIZABLE)
        volume_con = []
        for volume_name in thin_pool.iterate_volumes():
            volume_con.append(volume_name)
        properties["volumes"] = volume_con
        container[thin_pool.get_name()] = properties

    @classmethod
    def load(cls, properties):
        thin_pool = None
        try:
            thin_pool = ThinPool(
                properties["_name"],
                properties["_size_kiB"]
            )
            volume_con = properties["volumes"]
            for volume_name in volume_con:
                thin_pool.add_volume(volume_name)
        except (KeyError, TypeError):
            pass
        return thin_pool
