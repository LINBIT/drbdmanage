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
import json
import logging
import subprocess
import drbdmanage.storage.storagecore as storcore
import drbdmanage.storage.persistence as storpers
import drbdmanage.storage.lvm_common as lvmcom

import drbdmanage.consts as consts
import drbdmanage.exceptions as exc
import drbdmanage.storage.lvm_exceptions as lvmexc
import drbdmanage.utils as utils


class LvmThinLv(lvmcom.LvmCommon):

    """
    LVM logical volume backing store plugin for the drbdmanage server

    Provides backing store block devices for DRBD volumes by managing the
    allocation of logical volumes inside a single thin pool of a volume
    group of the logical volume manager (LVM).
    """

    # Configuration file keys
    KEY_DEV_PATH   = "dev-path"
    KEY_LVM_PATH   = "lvm-path"
    KEY_POOL_NAME  = "pool-name"

    # Paths to configuration and state files of this module
    LVM_CONFFILE  = "/etc/drbdmanaged-lvm-thinlv.conf"
    LVM_STATEFILE = "/var/lib/drbdmanage/drbdmanaged-lvm-thinlv.local.json"

    # Command names of LVM utilities
    LVM_CREATE    = "lvcreate"
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
        KEY_DEV_PATH:   "/dev/",
        consts.KEY_VG_NAME:    consts.DEFAULT_VG,
        KEY_LVM_PATH:   "/sbin",
        KEY_POOL_NAME:  "drbdthinpool"
    }

    # Plugin configuration
    _conf = None

    # Map of volumes allocated by the plugin
    _volumes = None

    # Map of pools allocated by the plugin
    _pools   = None

    # Lookup table for finding the pool that contains a volume
    _pool_lookup = None

    # Cached settings
    # Set during initialization
    _vg_path      = None
    _cmd_create   = None
    _cmd_remove   = None
    _cmd_lvchange = None
    _cmd_vgchange = None
    _cmd_lvs      = None
    _cmd_vgs      = None
    _subproc_env  = None


    def __init__(self):
        """
        Initializes a new instance
        """
        super(LvmThinLv, self).__init__()
        self.reconfigure()

    def get_default_config(self):
        return LvmThinLv.CONF_DEFAULTS.copy()

    def get_config(self):
        return self._conf

    def set_config(self, config):
        self.reconfigure(config)
        return True

    def reconfigure(self, config=None):
        """
        Reconfigures the module and reloads state information
        """
        try:
            # Setup the environment for subprocesses
            self._subproc_env = dict(os.environ.items())
            self._subproc_env["LC_ALL"] = "C"
            self._subproc_env["LANG"]   = "C"

            if config:
                self._conf = config
            else:
                self._conf = LvmThinLv.CONF_DEFAULTS.copy()

            # Setup cached settings
            self._vg_path = utils.build_path(
                self._conf[LvmThinLv.KEY_DEV_PATH],
                self._conf[consts.KEY_VG_NAME]
            ) + "/"
            self._cmd_create = utils.build_path(
                self._conf[LvmThinLv.KEY_LVM_PATH],
                LvmThinLv.LVM_CREATE
            )
            self._cmd_remove = utils.build_path(
                self._conf[LvmThinLv.KEY_LVM_PATH],
                LvmThinLv.LVM_REMOVE
            )
            self._cmd_lvchange = utils.build_path(
                self._conf[LvmThinLv.KEY_LVM_PATH],
                LvmThinLv.LVM_LV_CHANGE
            )
            self._cmd_vgchange = utils.build_path(
                self._conf[LvmThinLv.KEY_LVM_PATH],
                LvmThinLv.LVM_VG_CHANGE
            )
            self._cmd_lvs    = utils.build_path(
                self._conf[LvmThinLv.KEY_LVM_PATH], LvmThinLv.LVM_LVS
            )
            self._cmd_vgs    = utils.build_path(
                self._conf[LvmThinLv.KEY_LVM_PATH], LvmThinLv.LVM_VGS
            )

            # Load the saved state
            self._volumes = self._load_state()
        except exc.PersistenceException as pers_exc:
            logging.warning(
                "LvmThinLv plugin: Cannot load state file '%s'"
                % (LvmThinLv.LVM_STATEFILE)
            )
            raise pers_exc
        except Exception as unhandled_exc:
            logging.error(
                "LvmThinLv: initialization failed, unhandled exception: %s"
                % (str(unhandled_exc))
            )
            # Re-raise
            raise unhandled_exc


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
        lv_name = self.lv_name(name, vol_id)

        try:
            # Remove any existing LV
            tries = 0
            # Check whether an LV with that name exists already
            lv_exists = self._check_lv_exists(lv_name)
            if lv_exists:
                if self._volumes.get(lv_name) is None:
                    # Unknown LV, possibly user-generated and not managed
                    # by drbdmanage. Abort.
                    raise lvmexc.LvmUnmanagedVolumeException
                logging.warning(
                    "LvmThinLv: LV '%s' exists already, attempting "
                    "to remove it."
                    % (lv_name)
                )
            while lv_exists and tries < LvmThinLv.MAX_RETRIES:
                if tries > 0:
                    try:
                        time.sleep(LvmThinLv.RETRY_DELAY)
                    except OSError:
                        pass

                # LV exists, maybe from an earlier attempt at creating
                # the volume. Remove existing LV and recreate it.
                logging.warning(
                    "LvmThinLv: Attempt %d of %d: "
                    "Removal of LV '%s' failed."
                    % (tries + 1, LvmThinLv.MAX_RETRIES, lv_name)
                )
                self._remove_lv(lv_name)
                # Check whether the removal was successful
                lv_exists = self._check_lv_exists(lv_name)
                if not lv_exists:
                    try:
                        del self._volumes[lv_name]
                    except KeyError:
                        pass
                    self._save_state(self._volumes)
                tries += 1

            # Create the LV, unless the removal of any existing LV under
            # the specified name was unsuccessful
            if not lv_exists:
                tries = 0
                while blockdev is None and tries < LvmThinLv.MAX_RETRIES:
                    if tries > 0:
                        try:
                            time.sleep(LvmThinLv.RETRY_DELAY)
                        except OSError:
                            pass

                    self._create_lv(lv_name, size)
                    lv_exists = self._check_lv_exists(lv_name)
                    if lv_exists:
                        # LVM reports that the LV exists, create the
                        # blockdevice object representing the LV in
                        # drbdmanage and register it in the LVM module's
                        # persistent data structures
                        blockdev = storcore.BlockDevice(
                            lv_name, size,
                            self._vg_path + lv_name
                        )
                        self._volumes[lv_name] = blockdev
                        self._save_state(self._volumes)
                    else:
                        logging.error(
                            "LvmThinLv: Attempt %d of %d: "
                            "Creation of LV '%s' failed."
                            % (tries + 1, LvmThinLv.MAX_RETRIES, lv_name)
                        )
                    tries += 1
            else:
                logging.error(
                    "LvmThinLv: Removal of an existing LV '%s' failed, aborting "
                    "creation of the LV"
                    % (lv_name)
                )
        except (lvmexc.LvmCheckFailedException, lvmexc.LvmException):
            # Unable to run one of the LVM commands
            # The error is reported by the corresponding function
            #
            # Abort
            pass
        except exc.PersistenceException:
            # save_state() failed
            # If the LV was created, attempt to roll back
            if blockdev is not None:
                try:
                    self._remove_lv(lv_name)
                except lvmexc.LvmException:
                    pass
                try:
                    lv_exists = self._check_lv_exists(lv_name)
                    if not lv_exists:
                        blockdev = None
                        del self._volumes[lv_name]
                except (lvmexc.LvmCheckFailedException, KeyError):
                    pass
        except lvmexc.LvmUnmanagedVolumeException:
            # Collision with a volume not managed by drbdmanage
            logging.error(
                "LvmThinLv: LV '%s' exists already, but is unknown to "
                "drbdmanage's storage subsystem. Aborting."
                % (lv_name)
            )
        except Exception as unhandled_exc:
            logging.error(
                "LvmThinLv: Block device creation failed, "
                "unhandled exception: %s"
                % (str(unhandled_exc))
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
        lv_name = blockdevice.get_name()

        try:
            if self._volumes.get(lv_name) is not None:
                tries = 0
                lv_exists = self._check_lv_exists
                while lv_exists and tries < LvmThinLv.MAX_RETRIES:
                    if tries > 0:
                        try:
                            time.sleep(LvmThinLv.RETRY_DELAY)
                        except OSError:
                            pass

                    self._remove_lv(lv_name)
                    # Check whether the removal was successful
                    lv_exists = self._check_lv_exists(lv_name)
                    if not lv_exists:
                        # Removal successful. Any potential further errors,
                        # e.g. with saving the state, can be corrected later.
                        fn_rc = exc.DM_SUCCESS
                        try:
                            del self._volumes[lv_name]
                        except KeyError:
                            pass
                        self._save_state(self._volumes)
                    else:
                        logging.warning(
                            "LvmThinLv: Attempt %d of %d: "
                            "Removal of LV '%s' failed"
                            % (tries + 1, LvmThinLv.MAX_RETRIES, lv_name)
                        )
        except (lvmexc.LvmCheckFailedException, lvmexc.LvmException):
            # Unable to run one of the LVM commands
            # The error is reported by the corresponding function
            #
            # Abort
            pass
        except lvmexc.LvmUnmanagedVolumeException:
            # FIXME: this exception does not seem to be thrown anywhere?
            #
            # Collision with a volume not managed by drbdmanage
            logging.error(
                "LvmThinLv: LV '%s' exists, but is unknown to "
                "drbdmanage's storage subsystem. Aborting removal."
            )
        except exc.PersistenceException:
            # save_state() failed
            # If the module has an LV listed although it has actually been
            # removed successfully, then that can easily be corrected later
            pass
        except Exception as unhandled_exc:
            logging.error(
                "LvmThinLv: Removal of a block device failed, "
                "unhandled exception: %s"
                % (str(unhandled_exc))
            )

        if fn_rc != exc.DM_SUCCESS:
            logging.error(
                "LvmThinLv: Removal of LV '%s' failed"
                % (lv_name)
            )

        return fn_rc


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
        blockdev  = None
        lv_name   = source_blockdev.get_name()

        try:
            vol_name = self._gen_snapshot_volume_name(
                snaps_name, lv_name, vol_id
            )

            blockdev = self._snapshot_impl(vol_name, source_blockdev)
        except exc.PersistenceException:
            logging.debug(
                "LvmThinLv: create_snapshot(): caught PersistenceException, "
                "state saving failed"
            )
        except Exception as unhandled_exc:
            logging.error(
                "LvmThinLv: Block device creation failed, "
                "unhandled exception: %s"
                % (str(unhandled_exc))
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
        blockdev  = None

        try:
            vol_name = self.lv_name(restore_name, vol_id)

            blockdev = self._snapshot_impl(vol_name, source_blockdev)
        except exc.PersistenceException:
            logging.debug(
                "LvmThinLv: restore_snapshot(): caught PersistenceException, "
                "state saving failed"
            )
        except Exception as unhandled_exc:
            logging.error(
                "LvmThinLv: Block device creation failed, "
                "unhandled exception: %s"
                % (str(unhandled_exc))
            )

        return blockdev


    def remove_snapshot(self, blockdevice):
        """
        Deallocates a snapshot block device

        @param   blockdevice: the block device to deallocate
        @type    blockdevice: BlockDevice object
        @return: standard return code (see drbdmanage.exceptions)
        """
        return self.remove_blockdevice(blockdevice)


    def up_blockdevice(self, blockdevice):
        """
        Activates a block device (e.g., connects an iSCSI resource)

        @param blockdevice: the block device to deactivate
        @type  blockdevice: BlockDevice object
        """
        fn_rc = exc.DM_ESTORAGE
        try:
            lv_name   = blockdevice.get_name()

            vg_activated   = False
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
                    "LvmThinLv: Volume group activation failed, "
                    "unable to run external program '%s', error message "
                    "from the OS: %s"
                    % (self._cmd_vgchange, str(os_err))
                )
                raise lvmexc.LvmException

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
                    "LvmThinLv: LV activation failed, unable to run "
                    "external program '%s', error message from the OS: %s"
                    % (self._cmd_lvchange, str(os_err))
                )
                raise lvmexc.LvmException
            if vg_activated and lv_activated:
                fn_rc = exc.DM_SUCCESS
        except lvmexc.LvmException:
            # Unable to run one of the LVM commands
            # The error is reported by the corresponding function
            #
            # Abort
            pass
        except Exception as unhandled_exc:
            logging.error(
                "LvmThinLv: Block device creation failed, "
                "unhandled exception: %s"
                % (str(unhandled_exc))
            )

        return fn_rc


    def down_blockdevice(self, blockdevice):
        """
        Deactivates a block device (e.g., disconnects an iSCSI resource)

        @param blockdevice: the block device to deactivate
        @type  blockdevice: BlockDevice object
        """
        return exc.DM_SUCCESS


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
                self._cmd_lvs, "--noheadings", "--nosuffix",
                "--units", "k", "--separator", ",",
                "--options",
                "size,data_percent,metadata_percent,snap_percent",
                self._conf[consts.KEY_VG_NAME] + "/" +
                self._conf[LvmThinLv.KEY_POOL_NAME]
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
                    size_data, data_part, meta_part, snap_part = (
                        pool_data.split(",")
                    )
                    size_data = self.discard_fraction(size_data)
                    space_size = long(size_data)

                    # Data percentage
                    data_perc = float(0)
                    if len(data_part) > 0:
                        try:
                            data_perc = float(data_part) / 100
                        except ValueError:
                            pass

                    # Metadata percentage
                    meta_perc = float(0)
                    if len(meta_part) > 0:
                        try:
                            meta_perc = float(meta_part) / 100
                        except ValueError:
                            pass

                    # Snapshots percentage
                    snap_perc = float(0)
                    if len(snap_part) > 0:
                        try:
                            snap_perc = float(snap_part) / 100
                        except ValueError:
                            pass

                    # Calculate the amount of occupied space
                    data_used = data_perc * space_size
                    meta_used = meta_perc * space_size
                    snap_used = snap_perc * space_size

                    space_used = data_used + meta_used + snap_used

                    space_free = int(space_size - space_used)
                    if space_free < 0:
                        space_free = 0

                    # Finally, assign the results to the variables
                    # that will be returned, so that neither will be set
                    # if any of the earlier parsers or calculations fail
                    pool_size = space_size
                    pool_free = space_free
                    fn_rc = exc.DM_SUCCESS
                except ValueError:
                    pass
        except Exception as unhandled_exc:
            logging.error(
                "LvmThinLv: Retrieving storage pool information failed, "
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


    def _check_lv_exists(self, lv_name):
        """
        Check whether an LVM logical volume exists

        @returns: True if the LV exists, False if the LV does not exist
        Throws an LvmCheckFailedException if the check itself fails
        """
        return self.check_lv_exists(
            lv_name, self._conf[consts.KEY_VG_NAME],
            self._cmd_lvs, self._subproc_env, "LvmThinLv"
        )


    def _create_lv(self, lv_name, size):
        """
        Creates an LVM logical volume backed by a thin pool
        """
        try:
            exec_args = [
                self._cmd_create, "-n", lv_name, "-V", str(size) + "k",
                "--thinpool", self._conf[LvmThinLv.KEY_POOL_NAME],
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
                "LvmThinLv: LV creation failed, unable to run "
                "external program '%s', error message from the OS: %s"
                % (self._cmd_create, str(os_err))
            )
            raise lvmexc.LvmException


    def _create_snapshot(self, snaps_name, lv_name):
        """
        Creates an LVM snapshot LV of an existing LV
        """
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
                "LvmThinLv: Snapshot creation failed, unable to run "
                "external program '%s', error message from the OS: %s"
                % (self._cmd_create, str(os_err))
            )
            raise lvmexc.LvmException


    def _remove_lv(self, lv_name):
        """
        Removes an LVM logical volume
        """
        self.remove_lv(lv_name, self._conf[consts.KEY_VG_NAME],
                       self._cmd_remove, self._subproc_env, "LvmThinLv")


    def _snapshot_impl(self, vol_name, source_blockdev):
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
        blockdev  = None
        lv_name   = source_blockdev.get_name()

        try:
            tries = 0
            while blockdev is None and tries < LvmThinLv.MAX_RETRIES:
                if tries > 0:
                    try:
                        time.sleep(LvmThinLv.RETRY_DELAY)
                    except OSError:
                        pass
                tries += 1

                self._create_snapshot(vol_name, lv_name)
                snaps_exists = self._check_lv_exists(vol_name)
                if snaps_exists:
                    size = source_blockdev.get_size_kiB()
                    blockdev = storcore.BlockDevice(
                        vol_name, size,
                        self._vg_path + vol_name
                    )
                    self._volumes[vol_name] = blockdev
                    self.up_blockdevice(blockdev)
                    self._save_state(self._volumes)
                else:
                    logging.warning(
                        "LvmThinLv: Attempt %d of %d: "
                        "Creation of snapshot volume '%s' failed."
                        % (tries + 1, LvmThinLv.MAX_RETRIES, vol_name)
                    )
        except (lvmexc.LvmCheckFailedException, lvmexc.LvmException):
            # Unable to run one of the LVM commands
            # The error is reported by the corresponding function
            #
            # Abort
            pass
        except lvmexc.LvmUnmanagedVolumeException:
            # Collision with a volume not managed by drbdmanage
            logging.error(
                "LvmThinLv: LV '%s' exists already, but is unknown to "
                "drbdmanage's storage subsystem. Aborting."
                % (lv_name)
            )
        except exc.PersistenceException:
            # save_state() failed
            # If the snapshot was created, attempt to roll back
            if blockdev is not None:
                vol_name   = blockdev.get_name()
                try:
                    self._remove_lv(vol_name)
                except lvmexc.LvmException:
                    pass
                try:
                    vol_exists = self._check_lv_exists(vol_name)
                    if not vol_exists:
                        blockdev = None
                        try:
                            del self._volumes[vol_name]
                        except KeyError:
                            pass
                except lvmexc.LvmCheckFailedException:
                    pass
        except Exception as unhandled_exc:
            logging.error(
                "LvmThinLv: Block device creation failed, "
                "unhandled exception: %s"
                % (str(unhandled_exc))
            )

        return blockdev


    def _gen_snapshot_volume_name(self, snaps_name, source_name, vol_id):
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


    def _load_state(self):
        """
        Load the saved state of this module's managed logical volumes
        """
        return self.load_state(LvmThinLv.LVM_STATEFILE, "LvmThinLv")


    def _save_state(self, save_objects):
        """
        Save the state of this module's managed logical volumes
        """
        state_file = None
        try:
            save_bd_properties = {}
            for blockdev in save_objects.itervalues():
                bd_persist = storpers.BlockDevicePersistence(blockdev)
                bd_persist.save(save_bd_properties)
            try:
                state_file = open(LvmThinLv.LVM_STATEFILE, "w")
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
                    "LvmThinLv: Saving to the state file '%s' failed due "
                    "to an I/O error, error message from the OS: %s"
                    % (LvmThinLv.LVM_STATEFILE, str(io_err))
                )
                raise exc.PersistenceException
            except OSError as os_err:
                logging.error(
                    "LvmThinLv: Saving to the state file '%s' failed, "
                    "error message from the OS: %s"
                    % (LvmThinLv.LVM_STATEFILE, str(os_err))
                )
                raise exc.PersistenceException
        except exc.PersistenceException as pers_exc:
            # re-raise
            raise pers_exc
        except Exception as unhandled_exc:
            logging.error(
                "LvmThinLv: Saving to the state file '%s' failed, "
                "unhandled exception: %s"
                % (LvmThinLv.LVM_STATEFILE, str(unhandled_exc))
            )
            raise exc.PersistenceException
        finally:
            if state_file is not None:
                state_file.close()


    def _load_conf(self):
        """
        Loads settings from the module configuration file
        """
        return self.load_conf(LvmThinLv.LVM_CONFFILE, "LvmThinLv")
