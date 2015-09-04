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

"""
Global constants for drbdmanage
"""

DM_VERSION = "0.49"
try:
    from drbdmanage.consts_githash import DM_GITHASH
except:
    DM_GITHASH = 'GIT-hash: UNKNOWN'

SERIAL              = "serial"
NODE_NAME           = "node_name"
NODE_ADDR           = "addr"
NODE_AF             = "addrfam"
NODE_ID             = "node_id"
NODE_SITE           = "site"
NODE_POOLSIZE       = "node_poolsize"
NODE_POOLFREE       = "node_poolfree"
NODE_STATE          = "node_state"
RES_NAME            = "res_name"
RES_PORT            = "port"
RES_SECRET          = "secret"
VOL_ID              = "vol_id"
VOL_MINOR           = "minor"
VOL_SIZE            = "vol_size"
VOL_BDEV            = "vol_bdev"
SNAPS_NAME          = "snaps_name"
ERROR_CODE          = "error_code"
COMMON_NAME         = "common_name"

# Keys for the version text-query
KEY_SERVER_VERSION       = "server_version"
KEY_SERVER_GITHASH       = "server_git_hash"
KEY_DRBD_KERNEL_VERSION  = "drbd_kernel_version"
KEY_DRBD_KERNEL_GIT_HASH = "drbd_kernel_git_hash"
KEY_DRBD_UTILS_VERSION   = "drbd_utils_version"
KEY_DRBD_UTILS_GIT_HASH  = "drbd_utils_git_hash"

SNAPS_SRC_BLOCKDEV  = "snapshot-source-blockdev"

NODE_NAME_MAXLEN    = 128
RES_NAME_MAXLEN     = 48    # Enough for a UUID string plus prefix
SNAPS_NAME_MAXLEN   = 48

DRBDCTRL_DEFAULT_PORT = 6999

KEY_DRBDCTRL_VG     = "drbdctrl-vg"
KEY_VG_NAME         = "volume-group"
DRBDCTRL_RES_NAME   = ".drbdctrl"
DRBDCTRL_RES_FILE   = "drbdctrl.res"
DRBDCTRL_DEV        = "/dev/drbd0"
DEFAULT_VG          = "drbdpool"
DRBDCTRL_RES_PATH   = "/etc/drbd.d/"

SERVER_CONFFILE     = "/etc/drbdmanaged.cfg"
KEY_DRBD_CONFPATH = "drbd-conf-path"
DEFAULT_DRBD_CONFPATH = "/var/lib/drbd.d"
FILE_GLOBAL_COMMON_CONF = "drbdmanage_global_common.conf"

# server instance
KEY_SERVER_INSTANCE = "serverinstance"

# additional configuration keys
KEY_SITE = 'site'

# auxiliary property prefix
AUX_PROP_PREFIX     = "aux:"

# flags prefixes
CSTATE_PREFIX       = "cstate:"
TSTATE_PREFIX       = "tstate:"

# resources, nodes, volumes:
FLAG_REMOVE         = "remove"

# nodes:
FLAG_UPD_POOL       = "upd_pool"
FLAG_UPDATE         = "update"
FLAG_DRBDCTRL       = "drbdctrl"
FLAG_STORAGE        = "storage"

# assignments, volume states:
FLAG_DEPLOY         = "deploy"

# assignments:
FLAG_DISKLESS       = "diskless"
FLAG_CONNECT        = "connect"
FLAG_UPD_CON        = "upd_con"
FLAG_RECONNECT      = "reconnect"
FLAG_OVERWRITE      = "overwrite"
FLAG_DISCARD        = "discard"
FLAG_UPD_CONFIG     = "upd_config"
FLAG_STANDBY        = "standby"
FLAG_QIGNORE        = "qignore"

IND_NODE_OFFLINE    = "node_offline"

# volume states:
FLAG_ATTACH         = "attach"

# boolean expressions
BOOL_TRUE           = "true"
BOOL_FALSE          = "false"

RES_PORT_NR_AUTO    = -1
RES_PORT_NR_ERROR   = -2

TQ_GET_PATH         = "get_path"

CONF_GLOBAL = 'global'
CONF_NODE = 'node'
PLUGIN_PREFIX = 'Plugin:'
