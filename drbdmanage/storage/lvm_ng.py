#!/usr/bin/python
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
import drbdmanage.conf.conffile as cf


class LvmNg(lvmcom.LvmCommon):

    """
    LVM logical volume backing store plugin for the drbdmanage server

    Provides backing store block devices for DRBD volumes by managing the
    allocation of logical volumes inside a volume group of the
    logical volume manager (LVM).
    """

    # Configuration file keys
    KEY_DEV_PATH = "dev-path"
    KEY_VG_NAME  = "volume-group"
    KEY_LVM_PATH = "lvm-path"

    # Paths to configuration and state files of this module
    LVM_CONFFILE  = "/etc/drbdmanaged-lvm.conf"
    LVM_STATEFILE = "/var/lib/drbdmanage/drbdmanaged-lvm.local.json"

    # Command names of LVM utilities
    LVM_CREATE = "lvcreate"
    LVM_REMOVE = "lvremove"
    LVM_LVS    = "lvs"
    LVM_VGS    = "vgs"

    # lvs exit code if the LV was not found
    LVM_LVS_ENOENT = 5

    # Delay (float, in seconds) for lvcreate/lvremove retries
    RETRY_DELAY = 1

    # Maximum number of retries
    MAX_RETRIES = 2

    # Module configuration defaults
    CONF_DEFAULTS = {
        KEY_DEV_PATH: "/dev/",
        KEY_VG_NAME:  consts.DEFAULT_VG,
        KEY_LVM_PATH: "/sbin"
    }

    # Volumes (LVM logical volumes) managed by this module
    _volumes  = None

    # Loaded module configuration
    _conf     = None

    # Cached settings
    # Set during initialization
    _vg_path     = None
    _cmd_create  = None
    _cmd_remove  = None
    _cmd_lvs     = None
    _cmd_vgs     = None
    _subproc_env = None


    def __init__(self):
        """
        Initializes a new instance
        """
        super(LvmNg, self).__init__()
        self.reconfigure()


    def reconfigure(self):
        """
        Reconfigures the module and reloads state information
        """
        try:
            # Setup the environment for subprocesses
            self._subproc_env = dict(os.environ.items())
            self._subproc_env["LC_ALL"] = "C"
            self._subproc_env["LANG"]   = "C"

            # Load the module configuration
            conf_loaded = None
            try:
                conf_loaded = self._load_conf()
            except IOError:
                logging.warning(
                    "LvmNg: Cannot load configuration file '%s'"
                    % (LvmNg.LVM_CONFFILE)
                )
            if conf_loaded is None:
                self._conf = LvmNg.CONF_DEFAULTS
            else:
                self._conf = cf.ConfFile.conf_defaults_merge(
                    LvmNg.CONF_DEFAULTS, conf_loaded
                )

            # Setup cached settings
            self._vg_path = utils.build_path(
                self._conf[LvmNg.KEY_DEV_PATH],
                self._conf[LvmNg.KEY_VG_NAME]
            ) + "/"
            self._cmd_create = utils.build_path(
                self._conf[LvmNg.KEY_LVM_PATH], LvmNg.LVM_CREATE
            )
            self._cmd_remove = utils.build_path(
                self._conf[LvmNg.KEY_LVM_PATH], LvmNg.LVM_REMOVE
            )
            self._cmd_lvs    = utils.build_path(
                self._conf[LvmNg.KEY_LVM_PATH], LvmNg.LVM_LVS
            )
            self._cmd_vgs    = utils.build_path(
                self._conf[LvmNg.KEY_LVM_PATH], LvmNg.LVM_VGS
            )

            # Load the saved state
            self._volumes = self._load_state()
        except exc.PersistenceException as pers_exc:
            logging.warning(
                "LvmNg plugin: Cannot load state file '%s'"
                % (LvmNg.LVM_STATEFILE)
            )
            raise pers_exc
        except Exception as unhandled_exc:
            logging.error(
                "LvmNg: initialization failed, unhandled exception: %s"
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
                    raise lvmexc.LvmNgUnmanagedVolumeException
                logging.warning(
                    "LvmNg: LV '%s' exists already, attempting to remove it."
                    % (lv_name)
                )
            while lv_exists and tries < LvmNg.MAX_RETRIES:
                if tries > 0:
                    try:
                        time.sleep(LvmNg.RETRY_DELAY)
                    except OSError:
                        pass

                # LV exists, maybe from an earlier attempt at creating
                # the volume. Remove existing LV and recreate it.
                logging.warning(
                    "LvmNg: Attempt %d of %d: "
                    "Removal of LV '%s' failed."
                    % (tries + 1, LvmNg.MAX_RETRIES, lv_name)
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
                while blockdev is None and tries < LvmNg.MAX_RETRIES:
                    if tries > 0:
                        try:
                            time.sleep(LvmNg.RETRY_DELAY)
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
                            "LvmNg: Attempt %d of %d: "
                            "Creation of LV '%s' failed."
                            % (tries + 1, LvmNg.MAX_RETRIES, lv_name)
                        )
                    tries += 1
            else:
                logging.error(
                    "LvmNg: Removal of an existing LV '%s' failed, aborting "
                    "creation of the LV"
                    % (lv_name)
                )
        except (lvmexc.LvmNgCheckFailedException, lvmexc.LvmNgException):
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
                except lvmexc.LvmNgException:
                    pass
                try:
                    lv_exists = self._check_lv_exists(lv_name)
                    if not lv_exists:
                        blockdev = None
                        del self._volumes[lv_name]
                except (lvmexc.LvmNgCheckFailedException, KeyError):
                    pass
        except lvmexc.LvmNgUnmanagedVolumeException:
            # Collision with a volume not managed by drbdmanage
            logging.error(
                "LvmNg: LV '%s' exists already, but is unknown to "
                "drbdmanage's storage subsystem. Aborting."
                % (lv_name)
            )
        except Exception as unhandled_exc:
            logging.error(
                "LvmNg: Block device creation failed, "
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
                while lv_exists and tries < LvmNg.MAX_RETRIES:
                    if tries > 0:
                        try:
                            time.sleep(LvmNg.RETRY_DELAY)
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
                            "LvmNg: Attempt %d of %d: "
                            "Removal of LV '%s' failed"
                            % (tries + 1, LvmNg.MAX_RETRIES, lv_name)
                        )
        except (lvmexc.LvmNgCheckFailedException, lvmexc.LvmNgException):
            # Unable to run one of the LVM commands
            # The error is reported by the corresponding function
            #
            # Abort
            pass
        except lvmexc.LvmNgUnmanagedVolumeException:
            # FIXME: this exception does not seem to be thrown anywhere?
            #
            # Collision with a volume not managed by drbdmanage
            logging.error(
                "LvmNg: LV '%s' exists, but is unknown to "
                "drbdmanage's storage subsystem. Aborting removal."
            )
        except exc.PersistenceException:
            # save_state() failed
            # If the module has an LV listed although it has actually been
            # removed successfully, then that can easily be corrected later
            pass
        except Exception as unhandled_exc:
            logging.error(
                "LvmNg: Removal of a block device failed, "
                "unhandled exception: %s"
                % (str(unhandled_exc))
            )

        if fn_rc != exc.DM_SUCCESS:
            logging.error(
                "LvmNg: Removal of LV '%s' failed"
                % (lv_name)
            )

        return fn_rc


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
                self._conf[LvmNg.KEY_VG_NAME]
            ]
            self.debug_log_exec_args(exec_args)
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
                "LvmNg: Retrieving storage pool information failed, "
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


    def _create_lv(self, lv_name, size):
        """
        Creates an LVM logical volume
        """
        try:
            exec_args = [
                self._cmd_create, "-n", lv_name, "-L", str(size) + "k",
                self._conf[LvmNg.KEY_VG_NAME]
            ]
            self.debug_log_exec_args(exec_args)
            subprocess.call(
                exec_args,
                0, self._cmd_create,
                env=self._subproc_env, close_fds=True
            )
        except OSError as os_err:
            logging.error(
                "LvmNg: LV creation failed, unable to run "
                "external program '%s', error message from the OS: %s"
                % (self._cmd_create, str(os_err))
            )
            raise lvmexc.LvmNgException


    def _remove_lv(self, lv_name):
        """
        Removes an LVM logical volume
        """
        self.remove_lv(lv_name, self._conf[LvmNg.KEY_VG_NAME],
                       self._cmd_remove, self._subproc_env, "LvmNg")


    def _check_lv_exists(self, lv_name):
        """
        Check whether an LVM logical volume exists

        @returns: True if the LV exists, False if the LV does not exist
        Throws an LvmNgCheckFailedException if the check itself fails
        """
        return self.check_lv_exists(
            lv_name, self._conf[LvmNg.KEY_VG_NAME],
            self._cmd_lvs, self._subproc_env, "LvmNg"
        )


    def _load_state(self):
        """
        Load the saved state of this module's managed logical volumes
        """
        return self.load_state(LvmNg.LVM_STATEFILE, "LvmNg")


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
                state_file = open(LvmNg.LVM_STATEFILE, "w")
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
                    "LvmNg: Saving to the state file '%s' failed due to an "
                    "I/O error, error message from the OS: %s"
                    % (LvmNg.LVM_STATEFILE, str(io_err))
                )
                raise exc.PersistenceException
            except OSError as os_err:
                logging.error(
                    "LvmNg: Saving to the state file '%s' failed, "
                    "error message from the OS: %s"
                    % (LvmNg.LVM_STATEFILE, str(os_err))
                )
                raise exc.PersistenceException
        except exc.PersistenceException as pers_exc:
            # re-raise
            raise pers_exc
        except Exception as unhandled_exc:
            logging.error(
                "LvmNg: Saving to the state file '%s' failed, "
                "unhandled exception: %s"
                % (LvmNg.LVM_STATEFILE, str(unhandled_exc))
            )
            raise exc.PersistenceException
        finally:
            if state_file is not None:
                state_file.close()


    def _load_conf(self):
        """
        Loads settings from the module configuration file
        """
        return self.load_conf(LvmNg.LVM_CONFFILE, "LvmNg")
