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

DM_VERSION = "0.97.3"
try:
    from drbdmanage.consts_githash import DM_GITHASH
except:
    DM_GITHASH = 'GIT-hash: UNKNOWN'

DBUS_DRBDMANAGED = "org.drbd.drbdmanaged"
DBUS_SERVICE     = "/interface"

SERIAL              = "serial"
NODE_NAME           = "node_name"
NODE_ADDR           = "addr"
NODE_AF             = "addrfam"
NODE_ID             = "node_id"
NODE_VOL_0          = "drbdctrl_0"
NODE_VOL_1          = "drbdctrl_1"
NODE_ADDRESS        = "address"
NODE_PORT           = "drbdctrl_port"
NODE_SECRET         = "drbdctrl_secret"
NODE_SITE           = "site"
NODE_POOLSIZE       = "node_poolsize"
NODE_POOLFREE       = "node_poolfree"
NODE_STATE          = "node_state"
NODE_CONTROL_NODE   = "control_node"
NODE_SATELLITE_NODE = "satellite_node"
RES_NAME            = "res_name"
RES_PORT            = "port"
RES_SECRET          = "secret"
VOL_ID              = "vol_id"
VOL_MINOR           = "minor"
VOL_SIZE            = "vol_size"
VOL_BDEV            = "vol_bdev"
SNAPS_NAME          = "snaps_name"
ERROR_CODE          = "error_code"
FAIL_COUNT          = "fail-count"
COMMON_NAME         = "common_name"
MANAGED             = "managed"

# Keys for the version text-query
KEY_SERVER_VERSION       = "server_version"
KEY_SERVER_GITHASH       = "server_git_hash"
KEY_DRBD_KERNEL_VERSION  = "drbd_kernel_version"
KEY_DRBD_KERNEL_GIT_HASH = "drbd_kernel_git_hash"
KEY_DRBD_UTILS_VERSION   = "drbd_utils_version"
KEY_DRBD_UTILS_GIT_HASH  = "drbd_utils_git_hash"

# Shut down resources on drbdmanage server shutdown
KEY_SHUTDOWN_RES = "shutdown-res"

SNAPS_SRC_BLOCKDEV  = "snapshot-source-blockdev"

# RFC952 / RFC1035 / RFC1123 host name constraints; do not change
NODE_NAME_MINLEN = 2
NODE_NAME_MAXLEN = 255
NODE_NAME_LABEL_MAXLEN = 63

# drbdmanage object name constraints
RES_NAME_MINLEN = 1
RES_NAME_MAXLEN = 48    # Enough for a UUID string plus prefix
RES_NAME_VALID_CHARS = "_"
RES_NAME_VALID_INNER_CHARS = "-"
RES_ALL_KEYWORD = "all"
SNAPS_NAME_MINLEN = 1
SNAPS_NAME_MAXLEN = 48
SNAPS_NAME_VALID_CHARS = "_"
SNAPS_NAME_VALID_INNER_CHARS = "-"

DRBDCTRL_DEFAULT_PORT = 6999

KEY_DRBDCTRL_VG     = "drbdctrl-vg"
KEY_VG_NAME         = "volume-group"
KEY_LOGLEVEL        = "loglevel"
DRBDCTRL_RES_NAME   = ".drbdctrl"
DRBDCTRL_RES_FILE   = "drbdctrl.res"
DRBDCTRL_DEV        = "/dev/drbd0"
DEFAULT_VG          = "drbdpool"
DRBDCTRL_RES_PATH   = "/etc/drbd.d/"
DRBDCTRL_DEV_0      = "/dev/drbd0"
DRBDCTRL_DEV_1      = "/dev/drbd1"
DRBDCTRL_LV_NAME_0  = ".drbdctrl_0"
DRBDCTRL_LV_NAME_1  = ".drbdctrl_1"

AF_IPV4 = 4
AF_IPV6 = 6

AF_IPV4_LABEL = "ipv4"
AF_IPV6_LABEL = "ipv6"

KEY_BLOCKSIZE = 'blocksize'
DEFAULT_BLOCKSIZE = '4k'

SERVER_CONFFILE     = "/etc/drbdmanaged.cfg"
KEY_DRBD_CONFPATH = "drbd-conf-path"
DEFAULT_DRBD_CONFPATH = "/var/lib/drbd.d"
FILE_GLOBAL_COMMON_CONF = "drbdmanage_global_common.conf"

# server instance
KEY_SERVER_INSTANCE = "serverinstance"

# additional configuration keys
KEY_SITE = 'site'

KEY_ISSATELLITE = "isSatellite"

KEY_COLORS = "colors"
KEY_UTF8 = "utf8"

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
FLAG_EXTERNAL       = "external"

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

# ### satellites ###
# is satellite, should be a control node but could not access ctrlvol, is control node
SAT_SATELLITE, SAT_SHOULD_CONTROL_NODE, SAT_CONTROL_NODE = range(3)
# connection state to satellite
# SHUTDOWN and STARTUP are states that should be reached but are not currently reached
SAT_CON_SHUTDOWN, SAT_CON_ESTABLISHED, SAT_CON_STARTUP = range(3)
# cfg
KEY_SAT_CFG_SATELLITE = 'satellite'
KEY_SAT_CFG_CONTROL_NODE = 'controlnode'
KEY_SAT_CFG_ROLE = 'ctrl-volume-access-mode'
KEY_SAT_CFG_TCP_KEEPIDLE = 'tcp-keepidle'
KEY_SAT_CFG_TCP_KEEPINTVL = 'tcp-keepintvl'
KEY_SAT_CFG_TCP_KEEPCNT = 'tcp-keepcnt'

# after 10 sec of no other traffic,
# send a keep-alive every 7 seconds
# and fail if 5 not delivered
DEFAULT_SAT_CFG_TCP_KEEPIDLE = 10
DEFAULT_SAT_CFG_TCP_KEEPINTVL = 7
DEFAULT_SAT_CFG_TCP_KEEPCNT = 5
# communication protocol
KEY_S_CMD_INIT = 'CMD_INIT'
KEY_S_CMD_UPDATE = 'CMD_UPDATE'
KEY_S_CMD_SHUTDOWN = 'CMD_SHUTDOWN'
KEY_S_INT_SHUTDOWN = 'INT_SHUTDOWN'
KEY_S_ANS_OK = 'ANS_OK'
KEY_S_ANS_CHANGED = 'ANS_CHANGED'
KEY_S_ANS_CHANGED_FAILED = 'ANS_CHANGED_FAILED'
KEY_S_ANS_UNCHANGED = 'ANS_UNCHANGED'
KEY_S_ANS_E_OP_INVALID = 'ANS_OP_INVALID'
KEY_S_ANS_E_TOO_LONG = 'ANS_E_TOO_LONG'
KEY_S_ANS_E_COMM = 'ANS_E_COMM'
