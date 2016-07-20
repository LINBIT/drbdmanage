#!/usr/bin/env python2
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2013-2016   LINBIT HA-Solutions GmbH
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
    StoragePluginCommon, StoragePluginException, StoragePluginCheckFailedException)

import drbdmanage.consts as consts
import drbdmanage.exceptions as exc
import drbdmanage.utils as utils


class Zvol(StoragePluginCommon, storcore.StoragePlugin):

    """
    ZFS zpool backing store plugin for the drbdmanage server

    Provides backing store block devices for DRBD volumes by managing the
    allocation of zfs volumes.
    """

    NAME = 'Zvol'

    # Configuration file keys
    KEY_DEV_PATH = 'dev-path'
    KEY_ZVOL_PATH = 'zvol-path'

    # Path state file of this module
    STATEFILE = '/var/lib/drbdmanage/drbdmanaged-zvol.local.json'

    # Command names of zfs utilities
    ZFS_BASE = "zfs"
    ZVOL_VGS = "zpool"
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
        super(Zvol, self).__init__()
        self.traits[storcore.StoragePlugin.KEY_PROV_TYPE] = storcore.StoragePlugin.PROV_TYPE_FAT
        self.reconfigure()

    def get_default_config(self):
        return Zvol.CONF_DEFAULTS.copy()

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
                self._conf = Zvol.CONF_DEFAULTS.copy()

            # Setup cached settings
            self._vg_path = utils.build_path(
                self._conf[Zvol.KEY_DEV_PATH], self._conf[consts.KEY_VG_NAME]
            ) + "/"
            self._cmd_create = utils.build_path(self._conf[Zvol.KEY_ZVOL_PATH], Zvol.ZFS_BASE)
            self._cmd_extend = utils.build_path(self._conf[Zvol.KEY_ZVOL_PATH], Zvol.ZFS_BASE)
            self._cmd_remove = utils.build_path(self._conf[Zvol.KEY_ZVOL_PATH], Zvol.ZFS_BASE)
            self._cmd_list = utils.build_path(self._conf[Zvol.KEY_ZVOL_PATH], Zvol.ZFS_BASE)
            self._cmd_vgs = utils.build_path(self._conf[Zvol.KEY_ZVOL_PATH], Zvol.ZVOL_VGS)

            # Load the saved state
            self._volumes = self.load_state()
        except exc.PersistenceException as pers_exc:
            logging.warning(
                "Zvol plugin: Cannot load state file '%s'"
                % (Zvol.STATEFILE)
            )
            raise pers_exc
        except Exception as unhandled_exc:
            logging.error(
                "Zvol: initialization failed, unhandled exception: %s"
                % (str(unhandled_exc))
            )
            # Re-raise
            raise unhandled_exc

    def update_pool(self, node):
        fn_rc = exc.DM_ESTORAGE
        pool_size = -1
        pool_free = -1

        zpool_proc = None
        try:
            exec_args = [
                self._cmd_vgs, 'get', '-H', '-p', 'size,free',
                self._conf[consts.KEY_VG_NAME]
            ]
            utils.debug_log_exec_args(self.__class__.__name__, exec_args)
            zpool_proc = subprocess.Popen(
                exec_args,
                env=self._subproc_env, stdout=subprocess.PIPE,
                close_fds=True
            )

            # output should be sorted, but just to be sure
            for i in range(2):
                pool_data = zpool_proc.stdout.readline()
                if len(pool_data) > 0:
                    pool_data = pool_data.strip().split()
                    if pool_data[1].strip() == 'free':
                        pool_free = long(pool_data[2].strip())
                    elif pool_data[1].strip() == 'size':
                        pool_size = long(pool_data[2].strip())

            if pool_size == -1 or pool_free == -1:
                pool_size, pool_free = -1, -1
            else:
                pool_size /= 1024
                pool_free /= 1024
                fn_rc = exc.DM_SUCCESS

        except Exception as unhandled_exc:
            logging.error(
                "Zvol: Retrieving storage pool information failed, "
                "unhandled exception: %s"
                % (str(unhandled_exc))
            )
        finally:
            if zpool_proc is not None:
                try:
                    zpool_proc.stdout.close()
                except Exception:
                    pass
                zpool_proc.wait()

        return (fn_rc, pool_size, pool_free)

    def _roundup_k(self, size_k, mult_of_str=consts.DEFAULT_BLOCKSIZE):
        units = {
            'k': 1 << 0,  # k is already the base!
            'm': 1 << 10,
            'g': 1 << 20,
            't': 1 << 30,
            'p': 1 << 40,
            'z': 1 << 50,
        }
        if mult_of_str[-1].lower() == 'b':
            mult_of_str = mult_of_str[:-1]
        unit = mult_of_str[-1].lower()
        if unit not in units.keys():
            return -1

        mult_of_str = mult_of_str[:-1]

        try:
            multi = int(mult_of_str)
            if multi < 4 and unit == 'k':
                return -1  # which then should use a sane default
        except:
            return -1

        multi *= units[unit]

        mod = size_k % multi
        delta = 0 if mod == 0 else multi - mod
        return size_k + delta

    def _final_size(self, size):
        bs = self._conf.get(consts.KEY_BLOCKSIZE, consts.DEFAULT_BLOCKSIZE)
        final_size = self._roundup_k(size, bs)
        if final_size == -1:
            final_size = size
            bs = consts.DEFAULT_BLOCKSIZE
        return final_size, bs

    def _create_vol(self, vol_name, size):
        size, bs = self._final_size(size)
        try:
            exec_args = [
                self._cmd_create, self.ZFS_CREATE, '-b'+bs,
                '-V', str(size) + 'k',
                utils.build_path(self._conf[consts.KEY_VG_NAME], vol_name)
            ]
            utils.debug_log_exec_args(self.__class__.__name__, exec_args)
            subprocess.call(
                exec_args,
                0, self._cmd_create,
                env=self._subproc_env, close_fds=True
            )
        except OSError as os_err:
            logging.error(
                "Zvol: LV creation failed, unable to run "
                "external program '%s', error message from the OS: %s"
                % (self._cmd_create, str(os_err))
            )
            raise StoragePluginException

    def _extend_vol(self, vol_name, size):
        status = False
        size, _ = self._final_size(size)
        try:
            exec_args = [
                self._cmd_extend, self.ZFS_EXTEND, 'volsize=%sk' % str(size),
                utils.build_path(self._conf[consts.KEY_VG_NAME], vol_name)
            ]
            utils.debug_log_exec_args(self.__class__.__name__, exec_args)
            proc_rc = subprocess.call(
                exec_args,
                0, self._cmd_extend,
                env=self._subproc_env, close_fds=True
            )
            if proc_rc == 0:
                status = True
        except OSError as os_err:
            logging.error(
                self.NAME + ": vol extension failed, unable to run "
                "external program '%s', error message from the OS: %s"
                % (self._cmd_extend, str(os_err))
            )
        return status

    def _remove_vol(self, vol_name):
        try:
            exec_args = [
                self._cmd_remove, self.ZVOL_REMOVE, '-R',
                utils.build_path(self._conf[consts.KEY_VG_NAME], vol_name)
            ]
            subprocess.call(
                exec_args,
                0, self._cmd_remove,
                env=self._subproc_env, close_fds=True
            )
        except OSError as os_err:
            logging.error(
                "Zvol: LV remove failed, unable to run "
                "external program '%s', error message from the OS: %s"
                % (self._cmd_remove, str(os_err))
            )
            raise StoragePluginException

    def _check_vol_exists(self, vol_name):
        exists = False

        try:
            exec_args = [
                self._cmd_list, self.ZVOL_LIST,
                utils.build_path(self._conf[consts.KEY_VG_NAME], vol_name)
            ]
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
                "Zvol: Unable to retrieve the list of existing Zvols"
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

            exec_args = [
                self._cmd_create, self.ZVOL_SNAP_CLONE, zfs_snap_name,
                utils.build_path(self._conf[consts.KEY_VG_NAME], snaps_name)
            ]
            utils.debug_log_exec_args(self.__class__.__name__, exec_args)
            subprocess.call(
                exec_args,
                0, self._cmd_create,
                env=self._subproc_env, close_fds=True
            )
        except OSError as os_err:
            logging.error(
                "Zvol: Snapshot creation failed, unable to run "
                "external program '%s', error message from the OS: %s"
                % (self._cmd_create, str(os_err))
            )
            raise StoragePluginException

    def _restore_snapshot(self, vol_name, source_blockdev):
        return self._create_snapshot(vol_name, source_blockdev)

    def _remove_snapshot(self, blockdevice):
        return self.remove_blockdevice(blockdevice)
