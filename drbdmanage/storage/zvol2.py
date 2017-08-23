#!/usr/bin/env python2
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2013 - 2017  LINBIT HA-Solutions GmbH
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

import os
import logging
import subprocess
import drbdmanage.storage.storagecore as storcore
from drbdmanage.storage.storageplugin_common import (
    StoragePluginException, StoragePluginCheckFailedException)
from drbdmanage.storage.zvol import Zvol

import drbdmanage.consts as consts
import drbdmanage.exceptions as exc
import drbdmanage.utils as utils


class Zvol2(Zvol):

    """
    ZFS zpool backing store plugin for the drbdmanage server

    Provides backing store block devices for DRBD volumes by managing the
    allocation of zfs volumes. This version avoids creating additional snapshots
    """

    NAME = 'Zvol2'

    # Configuration file keys
    KEY_DEV_PATH = 'dev-path'
    KEY_ZVOL_PATH = 'zvol-path'

    # Path state file of this module
    STATEFILE = '/var/lib/drbdmanage/drbdmanaged-zvol2.local.json'

    # Command names of zfs utilities
    ZFS_BASE = "zfs"
    ZVOL_VGS = "zfs"
    ZFS_CREATE = "create"
    ZFS_EXTEND = "set"
    ZVOL_REMOVE = 'destroy'
    ZVOL_LIST = 'list'
    ZVOL_SNAP_CREATE = 'snapshot'
    ZVOL_SNAP_CLONE = 'clone'

    # Delay (float, in seconds) for lvcreate/lvremove retries
    RETRY_DELAY = 1

    # Maximum number of retries
    MAX_RETRIES = 2

    # Module configuration defaults
    CONF_DEFAULTS = {
        KEY_DEV_PATH: '/dev/zvol',
        consts.KEY_VG_NAME: consts.DEFAULT_VG,
        KEY_ZVOL_PATH: '/sbin',
        consts.KEY_BLOCKSIZE: consts.DEFAULT_BLOCKSIZE,
    }

    # Volumes managed by this module
    _volumes = None

    # Loaded module configuration
    _conf = None

    # Cached settings
    # Set during initialization
    _vg_path = None
    _cmd_create = None
    _cmd_extend = None
    _cmd_remove = None
    _cmd_vgs = None
    _cmd_list = None
    _subproc_env = None

    def __init__(self, server):
        super(Zvol2, self).__init__(server)
        self.traits[storcore.StoragePlugin.KEY_PROV_TYPE] = storcore.StoragePlugin.PROV_TYPE_FAT
        self.reconfigure()

    def get_default_config(self):
        return Zvol2.CONF_DEFAULTS.copy()

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
            self._subproc_env["LANG"] = "C"

            if config:
                self._conf = config
            else:
                self._conf = Zvol2.CONF_DEFAULTS.copy()

            # Setup cached settings
            self._vg_path = utils.build_path(
                self._conf[Zvol2.KEY_DEV_PATH], self._conf[consts.KEY_VG_NAME]
            ) + "/"
            self._cmd_create = utils.build_path(self._conf[Zvol2.KEY_ZVOL_PATH], Zvol2.ZFS_BASE)
            self._cmd_extend = utils.build_path(self._conf[Zvol2.KEY_ZVOL_PATH], Zvol2.ZFS_BASE)
            self._cmd_remove = utils.build_path(self._conf[Zvol2.KEY_ZVOL_PATH], Zvol2.ZFS_BASE)
            self._cmd_list = utils.build_path(self._conf[Zvol2.KEY_ZVOL_PATH], Zvol2.ZFS_BASE)
            self._cmd_vgs = utils.build_path(self._conf[Zvol2.KEY_ZVOL_PATH], Zvol2.ZVOL_VGS)

            # Load the saved state
            self._volumes = self.load_state()
        except exc.PersistenceException as pers_exc:
            logging.warning(
                "Zvol2 plugin: Cannot load state file '%s'"
                % (Zvol2.STATEFILE)
            )
            raise pers_exc
        except Exception as unhandled_exc:
            logging.error(
                "Zvol2: initialization failed, unhandled exception: %s"
                % (str(unhandled_exc))
            )
            # Re-raise
            raise unhandled_exc

    def _vol_name_to_snapshot(self, vol_name):
        orig = vol_name
        vol = orig[-3:]
        orig = orig.split('.')[0]
        orig = orig + vol
        return orig + '@' + vol_name

    def _remove_vol(self, vol_name):
        if '.' in vol_name:
            vol_name = self._vol_name_to_snapshot(vol_name)

        try:
            exec_args = [
                self._cmd_remove, self.ZVOL_REMOVE,
                utils.build_path(self._conf[consts.KEY_VG_NAME], vol_name)
            ]
            utils.debug_log_exec_args(self.__class__.__name__, exec_args)
            subprocess.call(
                exec_args,
                0, self._cmd_remove,
                env=self._subproc_env, close_fds=True
            )
        except OSError as os_err:
            logging.error(
                "Zvol2: LV remove failed, unable to run "
                "external program '%s', error message from the OS: %s"
                % (self._cmd_remove, str(os_err))
            )
            raise StoragePluginException

    def _check_vol_exists(self, vol_name):
        exists = False

        try:
            exec_args = [self._cmd_list, self.ZVOL_LIST]
            if '.' in vol_name:
                vol_name = self._vol_name_to_snapshot(vol_name)
                exec_args += ['-t', 'snapshot']

            exec_args.append(utils.build_path(self._conf[consts.KEY_VG_NAME], vol_name))
            utils.debug_log_exec_args(self.__class__.__name__, exec_args)
            zfs_proc = subprocess.Popen(
                exec_args,
                0, self._cmd_list,
                env=self._subproc_env,
                close_fds=True
            )
            zfs_rc = zfs_proc.wait()
            if zfs_rc == 0:
                exists = True
        except OSError:
            logging.error(
                "Zvol2: Unable to retrieve the list of existing Zvols"
            )
            raise StoragePluginCheckFailedException

        return exists

    # SNAPSHOTTING
    def _create_snapshot_impl(self, snaps_name, lv_name):
        try:
            zfs_snap_name = utils.build_path(self._conf[consts.KEY_VG_NAME], lv_name) + '@' + snaps_name
            exec_args = [
                self._cmd_create, self.ZVOL_SNAP_CREATE,
                zfs_snap_name
            ]
            utils.debug_log_exec_args(self.__class__.__name__, exec_args)
            subprocess.call(
                exec_args,
                0, self._cmd_create,
                env=self._subproc_env, close_fds=True
            )
        except OSError as os_err:
            logging.error(
                "Zvol2: Snapshot creation failed, unable to run "
                "external program '%s', error message from the OS: %s"
                % (self._cmd_create, str(os_err))
            )
            raise StoragePluginException

    def _restore_snapshot(self, vol_name, source_blockdev):
        try:
            snaps_name = source_blockdev.get_name()
            orig = source_blockdev.get_name()
            vol = orig[-3:]
            orig = orig.split('.')[0]
            orig = orig + vol
            zfs_snap_name = utils.build_path(self._conf[consts.KEY_VG_NAME], orig) + '@' + snaps_name
            new_vol = utils.build_path(self._conf[consts.KEY_VG_NAME], vol_name)
            exec_args = [
                self._cmd_create, self.ZVOL_SNAP_CLONE, zfs_snap_name,
                new_vol
            ]
            utils.debug_log_exec_args(self.__class__.__name__, exec_args)
            zfs_proc = subprocess.Popen(
                exec_args,
                0, self._cmd_create,
                env=self._subproc_env, close_fds=True
            )
            zfs_rc = zfs_proc.wait()
            if zfs_rc == 0:
                path = os.path.join(self._conf[self.KEY_DEV_PATH],
                                    new_vol)
                if not self._wait_dev_to_settle(path):
                    raise StoragePluginException
        except OSError as os_err:
            logging.error(
                "Zvol: Snapshot creation failed, unable to run "
                "external program '%s', error message from the OS: %s"
                % (self._cmd_create, str(os_err))
            )
            raise StoragePluginException
        retblockdevice = storcore.BlockDevice(vol_name, 0, self._vg_path + vol_name)
        self._volumes[vol_name] = retblockdevice
        self.save_state(self._volumes)
        return retblockdevice

    def _remove_snapshot(self, blockdevice):
        # actually unused, see remove_snapshot in storagecore
        return self.remove_blockdevice(blockdevice)
