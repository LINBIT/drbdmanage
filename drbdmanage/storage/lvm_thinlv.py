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
import logging
import subprocess
import drbdmanage.storage.lvm_common as lvmcom
import drbdmanage.storage.storagecore as storcore

import drbdmanage.consts as consts
import drbdmanage.exceptions as exc
import drbdmanage.utils as utils

from drbdmanage.storage.storageplugin_common import (
    StoragePluginException, StoragePluginUnmanagedVolumeException)


class LvmThinLv(lvmcom.LvmCommon):

    """
    LVM logical volume backing store plugin for the drbdmanage server

    Provides backing store block devices for DRBD volumes by managing the
    allocation of logical volumes inside a single thin pool of a volume
    group of the logical volume manager (LVM).
    """
    NAME = 'ThinLV'

    # Configuration file keys
    KEY_DEV_PATH  = "dev-path"
    KEY_LVM_PATH  = "lvm-path"
    KEY_POOL_NAME = "pool-name"

    # Path to state file of this module
    STATEFILE = "/var/lib/drbdmanage/drbdmanaged-lvm-thinlv.local.json"

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
    _cmd_extend   = None
    _cmd_remove   = None
    _cmd_lvchange = None
    _cmd_vgchange = None
    _cmd_lvs      = None
    _cmd_vgs      = None
    _subproc_env  = None

    def __init__(self, server):
        super(LvmThinLv, self).__init__()
        self.traits[storcore.StoragePlugin.KEY_PROV_TYPE] = storcore.StoragePlugin.PROV_TYPE_THIN
        self.reconfigure()

    def get_default_config(self):
        return LvmThinLv.CONF_DEFAULTS.copy()

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
                self._conf = LvmThinLv.CONF_DEFAULTS.copy()

            # Setup cached settings
            self._vg_path = utils.build_path(
                self._conf[LvmThinLv.KEY_DEV_PATH], self._conf[consts.KEY_VG_NAME]
            ) + "/"
            self._cmd_create   = utils.build_path(self._conf[LvmThinLv.KEY_LVM_PATH], LvmThinLv.LVM_CREATE)
            self._cmd_extend   = utils.build_path(self._conf[LvmThinLv.KEY_LVM_PATH], LvmThinLv.LVM_EXTEND)
            self._cmd_remove   = utils.build_path(self._conf[LvmThinLv.KEY_LVM_PATH], LvmThinLv.LVM_REMOVE)
            self._cmd_lvchange = utils.build_path(self._conf[LvmThinLv.KEY_LVM_PATH], LvmThinLv.LVM_LV_CHANGE)
            self._cmd_vgchange = utils.build_path(self._conf[LvmThinLv.KEY_LVM_PATH], LvmThinLv.LVM_VG_CHANGE)
            self._cmd_lvs      = utils.build_path(self._conf[LvmThinLv.KEY_LVM_PATH], LvmThinLv.LVM_LVS)
            self._cmd_vgs      = utils.build_path(self._conf[LvmThinLv.KEY_LVM_PATH], LvmThinLv.LVM_VGS)

            # Load the saved state
            self._volumes = self.load_state()
        except exc.PersistenceException as pers_exc:
            logging.warning(
                "LvmThinLv plugin: Cannot load state file '%s'"
                % (LvmThinLv.STATEFILE)
            )
            raise pers_exc
        except Exception as unhandled_exc:
            logging.error(
                "LvmThinLv: initialization failed, unhandled exception: %s"
                % (str(unhandled_exc))
            )
            # Re-raise
            raise unhandled_exc

    def up_blockdevice(self, blockdevice):
        fn_rc = exc.DM_ESTORAGE
        try:
            lv_name = blockdevice.get_name()

            vg_activated = False
            lv_activated = False

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
                raise StoragePluginException

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
                raise StoragePluginException
            if vg_activated and lv_activated:
                fn_rc = exc.DM_SUCCESS
        except StoragePluginException:
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

    def update_pool(self, node):
        fn_rc = exc.DM_ESTORAGE
        pool_size = -1
        pool_free = -1

        lvm_proc = None
        try:
            exec_args = [
                self._cmd_lvs, "--noheadings", "--nosuffix",
                "--units", "k", "--separator", ",",
                "--options",
                "size,data_percent,snap_percent",
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
                    size_data, data_part, snap_part = (
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

                    # Snapshots percentage
                    snap_perc = float(0)
                    if len(snap_part) > 0:
                        try:
                            snap_perc = float(snap_part) / 100
                        except ValueError:
                            pass

                    # Calculate the amount of occupied space
                    data_used = data_perc * space_size
                    snap_used = snap_perc * space_size

                    space_used = data_used + snap_used

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

    def _check_vol_exists(self, lv_name):
        return self.check_lv_exists(
            lv_name, self._conf[consts.KEY_VG_NAME],
            self._cmd_lvs, self._subproc_env, "LvmThinLv"
        )

    def _create_vol(self, lv_name, size):
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
            raise StoragePluginException

    def _extend_vol(self, lv_name, size):
        return self.extend_lv(lv_name, self._conf[consts.KEY_VG_NAME], size,
                              self._cmd_extend, self._subproc_env, "LvmThinLv")

    def _remove_vol(self, lv_name):
        self.remove_lv(lv_name, self._conf[consts.KEY_VG_NAME],
                       self._cmd_remove, self._subproc_env, "LvmThinLv")

    # SNAPSHOTTING
    def _create_snapshot_impl(self, snaps_name, lv_name):
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
            raise StoragePluginException

    def _remove_snapshot(self, blockdevice):
        return self.remove_blockdevice(blockdevice)

