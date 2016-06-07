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

import logging
import subprocess
import drbdmanage.storage.storagecore as storcore
from drbdmanage.storage.zvol import Zvol
from drbdmanage.storage.storageplugin_common import StoragePluginException

import drbdmanage.consts as consts
import drbdmanage.utils as utils


class ZvolThinLv(Zvol):

    """
    ZFS zpool backing store plugin for the drbdmanage server, thinly provisioned

    Provides backing store block devices for DRBD volumes by managing the
    allocation of zfs volumes which are thinly provisioned.
    """

    NAME = 'ZvolThinLV'
    # Path state file of this module
    STATEFILE = '/var/lib/drbdmanage/drbdmanaged-zvol-thinlv.local.json'

    def __init__(self, server):
        super(ZvolThinLv, self).__init__(server)
        self.traits[storcore.StoragePlugin.KEY_PROV_TYPE] = storcore.StoragePlugin.PROV_TYPE_THIN
        self.reconfigure()

    def _create_vol(self, vol_name, size):
        try:
            exec_args = [
                self._cmd_create, self.ZFS_CREATE, '-s', '-b4k',
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
                "ZvolThinLv: LV creation failed, unable to run "
                "external program '%s', error message from the OS: %s"
                % (self._cmd_create, str(os_err))
            )
            raise StoragePluginException
