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
from drbdmanage.storage.storageplugin_common import (
    StoragePluginException, StoragePluginUnmanagedVolumeException)

import drbdmanage.consts as consts
import drbdmanage.exceptions as exc
import drbdmanage.utils as utils


class Lvm(lvmcom.LvmCommon):

    """
    LVM logical volume backing store plugin for the drbdmanage server

    Provides backing store block devices for DRBD volumes by managing the
    allocation of logical volumes inside a volume group of the
    logical volume manager (LVM).
    """

    NAME = 'Lvm'

    # Configuration file keys
    KEY_DEV_PATH = "dev-path"
    KEY_LVM_PATH = "lvm-path"

    # Path state file of this module
    STATEFILE = "/var/lib/drbdmanage/drbdmanaged-lvm.local.json"

    # Command names of LVM utilities
    LVM_CREATE = "lvcreate"
    LVM_EXTEND = "lvextend"
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
        consts.KEY_VG_NAME:  consts.DEFAULT_VG,
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
    _cmd_extend  = None
    _cmd_remove  = None
    _cmd_lvs     = None
    _cmd_vgs     = None
    _subproc_env = None

    def __init__(self, server):
        super(Lvm, self).__init__()
        self.traits[storcore.StoragePlugin.KEY_PROV_TYPE] = storcore.StoragePlugin.PROV_TYPE_FAT
        self.reconfigure()

    def get_default_config(self):
        return Lvm.CONF_DEFAULTS.copy()

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
                self._conf = Lvm.CONF_DEFAULTS.copy()

            # Setup cached settings
            self._vg_path = utils.build_path(
                self._conf[Lvm.KEY_DEV_PATH], self._conf[consts.KEY_VG_NAME]
            ) + "/"
            self._cmd_create = utils.build_path(self._conf[Lvm.KEY_LVM_PATH], Lvm.LVM_CREATE)
            self._cmd_extend = utils.build_path(self._conf[Lvm.KEY_LVM_PATH], Lvm.LVM_EXTEND)
            self._cmd_remove = utils.build_path(self._conf[Lvm.KEY_LVM_PATH], Lvm.LVM_REMOVE)
            self._cmd_lvs    = utils.build_path(self._conf[Lvm.KEY_LVM_PATH], Lvm.LVM_LVS)
            self._cmd_vgs    = utils.build_path(self._conf[Lvm.KEY_LVM_PATH], Lvm.LVM_VGS)

            # Load the saved state
            self._volumes = self.load_state()
        except exc.PersistenceException as pers_exc:
            logging.warning(
                "Lvm plugin: Cannot load state file '%s'"
                % (Lvm.STATEFILE)
            )
            raise pers_exc
        except Exception as unhandled_exc:
            logging.error(
                "Lvm: initialization failed, unhandled exception: %s"
                % (str(unhandled_exc))
            )
            # Re-raise
            raise unhandled_exc

    def update_pool(self, node):
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
                "Lvm: Retrieving storage pool information failed, "
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

    def _create_vol(self, lv_name, size):
        try:
            exec_args = [
                self._cmd_create, "-n", lv_name, "-L", str(size) + "k",
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
                "Lvm: LV creation failed, unable to run "
                "external program '%s', error message from the OS: %s"
                % (self._cmd_create, str(os_err))
            )
            raise StoragePluginException

    def _extend_vol(self, lv_name, size):
        return self.extend_lv(lv_name, self._conf[consts.KEY_VG_NAME], size,
                              self._cmd_extend, self._subproc_env, "Lvm")

    def _remove_vol(self, lv_name):
        self.remove_lv(lv_name, self._conf[consts.KEY_VG_NAME],
                       self._cmd_remove, self._subproc_env, "Lvm")

    def _check_vol_exists(self, lv_name):
        return self.check_lv_exists(
            lv_name, self._conf[consts.KEY_VG_NAME],
            self._cmd_lvs, self._subproc_env, "Lvm"
        )
