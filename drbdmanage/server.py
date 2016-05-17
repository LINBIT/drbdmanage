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
import sys
import os
import errno
import signal
import time
import gobject
import subprocess
import fcntl
import select
import logging
import logging.handlers
import re
import traceback
import inspect
import StringIO
from functools import wraps
import drbdmanage.drbd.persistence
import drbdmanage.quorum
import drbdmanage.drbd.metadata as md

from drbdmanage.consts import (
    SERIAL, NODE_NAME, NODE_ADDR, NODE_AF, RES_NAME, RES_PORT, VOL_MINOR, VOL_ID,
    DEFAULT_VG, KEY_DRBDCTRL_VG, DRBDCTRL_DEFAULT_PORT, KEY_LOGLEVEL,
    DRBDCTRL_RES_NAME, DRBDCTRL_RES_FILE, DRBDCTRL_RES_PATH, RES_PORT_NR_AUTO,
    RES_PORT_NR_ERROR, FLAG_OVERWRITE, FLAG_DISCARD, FLAG_DISKLESS,
    FLAG_CONNECT, FLAG_DRBDCTRL, FLAG_STORAGE, FLAG_EXTERNAL, FLAG_STANDBY, FLAG_QIGNORE,
    IND_NODE_OFFLINE, SNAPS_SRC_BLOCKDEV, DM_VERSION, DM_GITHASH,
    KEY_SERVER_VERSION, KEY_DRBD_KERNEL_VERSION, KEY_DRBD_UTILS_VERSION, KEY_SERVER_GITHASH,
    KEY_DRBD_KERNEL_GIT_HASH, KEY_DRBD_UTILS_GIT_HASH,
    CONF_NODE, CONF_GLOBAL, KEY_SITE, BOOL_TRUE, FILE_GLOBAL_COMMON_CONF, KEY_VG_NAME,
    KEY_SERVER_INSTANCE, NODE_VOL_0, NODE_VOL_1, NODE_PORT, NODE_SECRET, NODE_ADDRESS,
    DRBDCTRL_LV_NAME_0, DRBDCTRL_LV_NAME_1, NODE_CONTROL_NODE, NODE_SATELLITE_NODE, KEY_ISSATELLITE,
    SAT_SATELLITE, SAT_SHOULD_CONTROL_NODE, SAT_CONTROL_NODE,
    KEY_SAT_CFG_SATELLITE, KEY_SAT_CFG_CONTROL_NODE, KEY_SAT_CFG_ROLE,
    KEY_SAT_CFG_TCP_KEEPIDLE, KEY_SAT_CFG_TCP_KEEPINTVL, KEY_SAT_CFG_TCP_KEEPCNT,
    DEFAULT_SAT_CFG_TCP_KEEPIDLE, DEFAULT_SAT_CFG_TCP_KEEPINTVL, DEFAULT_SAT_CFG_TCP_KEEPCNT,
    KEY_S_CMD_INIT, KEY_S_ANS_OK, KEY_S_CMD_SHUTDOWN, SAT_CON_SHUTDOWN, SAT_CON_ESTABLISHED, SAT_CON_STARTUP,
    KEY_SHUTDOWN_RES, RES_ALL_KEYWORD
)
from drbdmanage.utils import NioLineReader
from drbdmanage.utils import (
    build_path, extend_path, generate_secret, get_free_number,
    add_rc_entry, serial_filter, props_filter, string_to_bool, bool_to_string,
    aux_props_selector, is_set, is_unset, key_value_string, load_server_conf_file,
    filter_prohibited, filter_allowed
)
from drbdmanage.exceptions import (
    DM_DEBUG, DM_ECTRLVOL, DM_EEXIST, DM_EINVAL, DM_EMINOR, DM_ENAME,
    DM_ENODECNT, DM_ENODEID, DM_ENOENT, DM_EPERSIST, DM_EPLUGIN, DM_EPORT,
    DM_ESECRETG, DM_ESTORAGE, DM_EVOLID, DM_EVOLSZ, DM_ENOSPC, DM_EQUORUM,
    DM_ENOTIMPL, DM_SUCCESS, DM_ESATELLITE, DM_INFO
)
from drbdmanage.exceptions import (
    DrbdManageException, InvalidMinorNrException, InvalidNameException, PersistenceException,
    PluginException, SyntaxException, VolSizeRangeException, AbortException, QuorumException,
    DeployerException, InvalidAddrFamException, DebugException, dm_exc_text
)
from drbdmanage.drbd.drbdcore import (
    Assignment, DrbdManager, DrbdNode, DrbdResource, DrbdVolume,
    DrbdVolumeState, DrbdCommon
)
from drbdmanage.snapshots.snapshots import (
    DrbdSnapshot, DrbdSnapshotAssignment, DrbdSnapshotVolumeState
)
from drbdmanage.storage.storagecore import BlockDeviceManager, MinorNr
from drbdmanage.conf.conffile import DrbdAdmConf
from drbdmanage.propscontainer import PropsContainer

from drbdmanage.plugins.plugin import PluginManager
from drbdmanage.proxy import DrbdManageProxy


class DrbdManageServer(object):

    """
    drbdmanage server - main class
    """

    OBJ_NODES_NAME     = "nodes"
    OBJ_RESOURCES_NAME = "resources"
    OBJ_CCONF_NAME     = "cluster_conf"
    OBJ_SCONF_NAME     = "server_conf"
    OBJ_COMMON_NAME    = "common"
    OBJ_SGEN_NAME      = "serial_nr_gen"
    OBJ_PCONF_NAME     = "plugin_conf"
    OBJ_PERSIST_NAME   = "persistence"

    EVT_UTIL = "drbdsetup"

    EVT_TYPE_CHANGE = "change"
    EVT_TYPE_EXISTS = "exists"
    EVT_SRC_CON     = "connection"
    EVT_SRC_PEERDEV = "peer-device"
    EVT_SRC_RES     = "resource"
    EVT_ARG_NAME    = "name"
    EVT_ARG_ROLE    = "role"
    EVT_ARG_REPL    = "replication"
    EVT_ARG_CON     = "connection"

    EVT_ROLE_PRIMARY   = "Primary"
    EVT_ROLE_SECONDARY = "Secondary"

    EVT_REPL_SYNCTARGET = "SyncTarget"
    EVT_REPL_ON         = "Established"
    EVT_REPL_OFF        = "Off"

    EVT_CONN_NAME       = "conn-name"

    # Sleep times (in seconds) for various stages of the
    # events subprocess termination loop
    EVT_TERM_SLEEP_SHORT = 0.5
    EVT_TERM_SLEEP_LONG  = 2

    DRBD_KMOD_INFO_FILE = "/proc/drbd"

    LOGGING_FORMAT = "drbdmanaged[%(process)d]: %(levelname)-10s %(message)s"

    KEY_STOR_NAME      = "storage-plugin"
    KEY_DEPLOYER_NAME  = "deployer-plugin"
    KEY_MAX_NODE_ID    = "max-node-id"
    KEY_MAX_PEERS      = "max-peers"
    KEY_MIN_MINOR_NR   = "min-minor-nr"
    KEY_MIN_PORT_NR    = "min-port-nr"
    KEY_MAX_PORT_NR    = "max-port-nr"
    KEY_MAX_FAIL_COUNT = "max-fail-count"

    KEY_EXTEND_PATH    = "extend-path"
    KEY_DRBD_CONFPATH  = "drbd-conf-path"

    KEY_DEBUG_OUT_FILE = "debug-out-file"

    DEFAULT_MAX_NODE_ID  =   31
    DEFAULT_MAX_PEERS    =    7
    DEFAULT_MIN_MINOR_NR =  100
    DEFAULT_MIN_PORT_NR  = 7000
    DEFAULT_MAX_PORT_NR  = 7999
    DEFAULT_MAX_FAIL_COUNT = 3

    # defaults
    CONF_DEFAULTS = {
        KEY_STOR_NAME      : "drbdmanage.storage.lvm.Lvm",
        KEY_DEPLOYER_NAME  : "drbdmanage.deployers.BalancedDeployer",
        KEY_MAX_NODE_ID    : str(DEFAULT_MAX_NODE_ID),
        KEY_MAX_PEERS      : str(DEFAULT_MAX_PEERS),
        KEY_MIN_MINOR_NR   : str(DEFAULT_MIN_MINOR_NR),
        KEY_MIN_PORT_NR    : str(DEFAULT_MIN_PORT_NR),
        KEY_MAX_PORT_NR    : str(DEFAULT_MAX_PORT_NR),
        KEY_MAX_FAIL_COUNT : str(DEFAULT_MAX_FAIL_COUNT),
        KEY_EXTEND_PATH    : "/sbin:/usr/sbin:/bin:/usr/bin",
        KEY_DRBD_CONFPATH  : "/var/lib/drbd.d",
        KEY_DRBDCTRL_VG    : DEFAULT_VG,
        KEY_DEBUG_OUT_FILE : "/dev/stderr",
        KEY_LOGLEVEL       : "INFO",
        KEY_SAT_CFG_TCP_KEEPIDLE: str(DEFAULT_SAT_CFG_TCP_KEEPIDLE),
        KEY_SAT_CFG_TCP_KEEPINTVL: str(DEFAULT_SAT_CFG_TCP_KEEPINTVL),
        KEY_SAT_CFG_TCP_KEEPCNT: str(DEFAULT_SAT_CFG_TCP_KEEPCNT),
    }

    # config stages
    KEY_FROM_FILE = 'from-file'
    KEY_FROM_CTRL_VOL = 'from-ctrl-vol'

    CONF_STAGE = {
        KEY_FROM_FILE: 0,
        KEY_FROM_CTRL_VOL: 1,
    }

    # Container for the server's objects directory
    _objects_root = None

    # Serial numbers generator
    _serial_gen = None

    # Factory instance for creating signal objects
    _signal_factory = None

    # Persistence layer object
    _persist   = None
    # Currently open/locked persistence object
    _locked_persist = None

    # BlockDevice manager
    _bd_mgr    = None
    # Configuration objects maps
    _nodes     = None
    _resources = None
    # Events log pipe
    _evt_file  = None
    # RegEx pattern for events parsing
    _evt_pat   = re.compile(r'(?P<type>\w+) (?P<source>[\w-]+)(?P<attrs>.*)')
    # Subprocess handle for the events log source
    _proc_evt  = None
    # Reader for the events log
    _reader    = None
    # Event handler for incoming data
    _evt_in_h  = None
    # Event handler for the hangup event on the subprocess pipe
    _evt_hup_h = None
    # Flag indicating whether run_changes() has been scheduled or not
    _run_changes_scheduled = False
    # Flag indicating whether to poke other cluster nodes from run_changes()
    _poke_cluster = False

    # The name of the node this server is running on
    _instance_node_name = None

    # The hash of the currently loaded configuration
    _conf_hash = None

    # Server configuration (local cache)
    _conf      = None

    # Plugin configuration
    _plugin_conf = None

    # Common DRBD options
    _common    = None

    _path = None

    # Logging
    _root_logger = None
    DM_LOGLEVELS = {
      "CRITICAL" : logging.CRITICAL,
      "ERROR"    : logging.ERROR,
      "WARNING"  : logging.WARNING,
      "INFO"     : logging.INFO,
      "DEBUG"    : logging.DEBUG
    }

    _debug_out = sys.stderr

    # Global drbdmanage cluster configuration
    _cluster_conf         = None

    # Change generation flag; controls updates of the serial number
    _change_open  = False

    # Quorum management
    _quorum = None

    # DEBUGGING FLAGS
    dbg_events = False

    _pluginmgr = None

    _proxy = None

    _is_satellite = False
    _forced_is_satellite = False
    # state according to the control volume
    sat_state_ctrlvol = {}

    def no_satellite(f):
        KEY_NOTHING = "nothing"
        # currently not necessary, but maybe we want to wrap functions that return (nested) types
        # in the future...
        wrapped_returns = {
            'assign': KEY_NOTHING,
            'attach': KEY_NOTHING,
            'auto_deploy': KEY_NOTHING,
            'auto_undeploy': KEY_NOTHING,
            'connect': KEY_NOTHING,
            'create_node': KEY_NOTHING,
            'create_resource': KEY_NOTHING,
            'create_snapshot': KEY_NOTHING,
            'create_volume': KEY_NOTHING,
            'dbus_load_conf': KEY_NOTHING,
            'dbus_save_conf': KEY_NOTHING,
            'detach': KEY_NOTHING,
            'disconnect': KEY_NOTHING,
            'get_ctrlvol': KEY_NOTHING,
            'init_node': KEY_NOTHING,
            'join_node': KEY_NOTHING,
            'modify_resource': KEY_NOTHING,
            'modify_state': KEY_NOTHING,
            'modify_volume': KEY_NOTHING,
            'poke': KEY_NOTHING,
            'quorum_control': KEY_NOTHING,
            'remove_node': KEY_NOTHING,
            'remove_resource': KEY_NOTHING,
            'remove_snapshot': KEY_NOTHING,
            'remove_snapshot_assignment': KEY_NOTHING,
            'remove_volume': KEY_NOTHING,
            'resize_volume': KEY_NOTHING,
            'restore_snapshot': KEY_NOTHING,
            'run_external_plugin': KEY_NOTHING,
            'set_ctrlvol': KEY_NOTHING,
            'set_drbdsetup_props': KEY_NOTHING,
            'unassign': KEY_NOTHING,
            'update_pool': KEY_NOTHING,
            'update_pool_check': KEY_NOTHING,
            'assign_satellite': KEY_NOTHING,
        }

        @wraps(f)
        def wrapper(self, *args, **kwds):
            if self._is_satellite == SAT_SATELLITE:
                fn_rc = []
                add_rc_entry(fn_rc, DM_ESATELLITE, dm_exc_text(DM_ESATELLITE))
                if f.__name__ in wrapped_returns:
                    if wrapped_returns[f.__name__] == KEY_NOTHING:
                        return fn_rc
                    else:
                        return fn_rc, wrapped_returns[f.__name__]
                else:
                    # hope for the best...
                    return fn_rc
            else:
                return f(self, *args, **kwds)
        return wrapper

    def __init__(self, signal_factory):
        """
        Initialize and start up the drbdmanage server
        """

        # ========================================
        # BEGIN -- Server initialization
        # ========================================

        self._pluginmgr = PluginManager(self)

        # Determine the current node's name
        #
        # The "(unknown)" node name never matches, because brackets are not
        # allowed characters in node names
        self._instance_node_name = "(unknown)"
        if len(sys.argv) >= 2:
            self._instance_node_name = sys.argv[1]
        else:
            try:
                uname = os.uname()
                if len(uname) >= 2:
                    self._instance_node_name = uname[1]
            except Exception:
                pass

        # Initialize logging
        self.init_logging()
        logging.info("DRBDmanage server, version %s"
                     " -- initializing on node '%s'"
                     % (DM_VERSION, self._instance_node_name))

        # Set original path variable
        try:
            self._path = os.environ["PATH"]
        except KeyError:
            self._path = ""

        # Initialize the server's objects / datastructures
        self._init_objects()

        # load the server configuration file
        self.load_server_conf(self.CONF_STAGE[self.KEY_FROM_FILE])

        # Reset the loglevel to the one specified in the configuration file
        self.set_loglevel()

        # Setup the PATH environment variable
        extend_path(self._path, self.get_conf_value(self.KEY_EXTEND_PATH))

        # Create drbdmanage objects

        # Create the proxy object (server only started on satellite)
        # control node needs the proxy object to send data
        self._proxy = DrbdManageProxy(self)

        # DRBD manager (manages DRBD resources using drbdadm etc.)
        self._drbd_mgr = DrbdManager(self)

        # Start up the drbdmanage control volume
        # call internal metric
        _, self._is_satellite = self._drbd_mgr.adjust_drbdctrl()

        # it might be the case that reading drbdmanaged.cfg overwrote what we "guessed"
        if self._forced_is_satellite is not False:
            self._is_satellite = self._forced_is_satellite

        if self._is_satellite == SAT_SATELLITE:
            self._quorum = drbdmanage.quorum.IgnoredQuorum(self)
            logging.info('DRBDManage starting as satellite node')
        elif self._is_satellite == SAT_CONTROL_NODE:
            logging.info('DRBDManage starting as control node')
            self._quorum = drbdmanage.quorum.Quorum(self)

        # Initialize the signal objects source
        if signal_factory is not None:
            self._signal_factory = signal_factory
        else:
            logging.warning("Server created without passing a signal factory, "
                            "signals are disabled")

        # ========================================
        # END -- Server initialization
        # ========================================


    def _init_objects(self):
        """
        Initializes the server's objects and the objects directory
        """
        self._objects_root = {}

        self._objects_root[DrbdManageServer.OBJ_NODES_NAME]     = {}
        self._objects_root[DrbdManageServer.OBJ_RESOURCES_NAME] = {}
        self._objects_root[DrbdManageServer.OBJ_SCONF_NAME]     = {}
        self._objects_root[DrbdManageServer.OBJ_PCONF_NAME]     = {}

        cluster_conf = PropsContainer(None, 1, None)
        self._objects_root[DrbdManageServer.OBJ_CCONF_NAME]     = cluster_conf
        self._objects_root[DrbdManageServer.OBJ_SGEN_NAME]      = cluster_conf.new_serial_gen()

        self._objects_root[KEY_SERVER_INSTANCE] = self

        self._objects_root[DrbdManageServer.OBJ_COMMON_NAME] = (
            DrbdCommon(self.get_serial, cluster_conf.get_prop(SERIAL), None)
        )

        self._objects_root[DrbdManageServer.OBJ_PERSIST_NAME] = None

        self.update_objects()


    def update_objects(self):
        """
        Updates the server's cached references into the objects directory

        The server keeps direct references to objects in the objects
        directory for quick access. These references must be updated
        whenever the objects in the objects directory may have been
        reloaded (exchanged)
        """
        srv = DrbdManageServer
        self._nodes        = self._objects_root[srv.OBJ_NODES_NAME]
        self._resources    = self._objects_root[srv.OBJ_RESOURCES_NAME]
        self._cluster_conf = self._objects_root[srv.OBJ_CCONF_NAME]
        self._serial_gen   = self._objects_root[srv.OBJ_SGEN_NAME]
        self._conf         = self._objects_root[srv.OBJ_SCONF_NAME]
        self._common       = self._objects_root[srv.OBJ_COMMON_NAME]
        self._plugin_conf  = self._objects_root[srv.OBJ_PCONF_NAME]
        self._persist      = self._objects_root[srv.OBJ_PERSIST_NAME]


    def _init_persist(self):
        """
        Initializes the persistence layer
        """

        if self._is_satellite == SAT_SATELLITE:
            persist = drbdmanage.drbd.persistence.create_satellite_persistence(self)
        else:
            persist = drbdmanage.drbd.persistence.create_server_persistence(self)

        self._objects_root[DrbdManageServer.OBJ_PERSIST_NAME] = persist
        self.update_objects()

    def run_config(self):
        # Load the drbdmanage database from the control volume
        self.load_conf()

        self.load_server_conf(self.CONF_STAGE[self.KEY_FROM_CTRL_VOL])

        # Set the full member count for quorum tracking
        self._quorum.readjust_full_member_count()

        # Create drbdmanage objects
        #
        # Block devices manager (manages backend storage devices)
        self._bd_mgr = BlockDeviceManager(self._conf[self.KEY_STOR_NAME], self._pluginmgr)

        # Start up the resources deployed by drbdmanage on the current node
        self._drbd_mgr.initial_up()

        # Initialize events tracking
        try:
            self.init_events()
        except (OSError, IOError):
            logging.critical("failed to initialize drbdsetup events tracing, "
                             "aborting startup")
            exit(1)

        # Wait for drbdsetup to finish reporting the initial DRBD status
        # Read the status report and setup quorum
        if self._is_satellite == SAT_CONTROL_NODE:
            self._drbd_event_initial_status()
            persist = None
            try:
                persist = self.begin_modify_conf()
                if persist is not None:
                    self.update_satellite_states()
                    self._persist.json_export(self._objects_root)
                    _, need_save = self.send_init_satellites()
                    if need_save:
                        self.save_conf_data(persist)
                else:
                    raise PersistenceException
            except PersistenceException:
                logging.error("Cannot save updated satellite properties")
            except QuorumException:
                logging.error("Cannot initialize satellite properties, partition does not have a quorum")
            except Exception as exc:
                # Log any otherwise unhandled exceptions, but attempt to
                # continue startup anyway
                self.catch_internal_error(exc)
            finally:
                self.end_modify_conf(persist)

        # Update storage pool information if it is unknown
        inst_node = self.get_instance_node()
        if inst_node is not None:
            if is_set(inst_node.get_state(), DrbdNode.FLAG_STORAGE):
                poolsize = inst_node.get_poolsize()
                poolfree = inst_node.get_poolfree()
                if poolsize == -1 or poolfree == -1:
                    self.update_pool([])

    def run(self):
        """
        Finishes the server's startup and runs the main loop

        Waits for client requests or events generated by "drbdsetup events".
        """
        self._init_persist()

        self.run_config()
        if self._is_satellite == SAT_SATELLITE:
            self._proxy.start()

        # gobject.MainLoop().run()

        # http://www.jejik.com/articles/2007/01/python-gstreamer_threading_and_the_main_loop/
        loop = gobject.MainLoop()
        gobject.threads_init()
        context = loop.get_context()
        while True:
            context.iteration(True)


    def init_events(self):
        """
        Initialize callbacks for events generated by "drbdsetup events"

        Starts "drbdsetup events" as a child process with drbdsetup's standard
        output piped back to the drbdmanage server. A GMainLoop controlled
        callback is set up, so the drbdmanage server can react to log entries
        generated by drbdsetup.

        The callback functions are:
            drbd_event        whenever data becomes readable on the pipe
            restart_events    when the pipe needs to be reopened
        """
        # Stop an existing subprocess before spawning a new one
        self.uninit_events()

        # Initialize a new events subprocess
        self._proc_evt = subprocess.Popen(
            [self.EVT_UTIL, "events2", "all"], 0,
            self.EVT_UTIL, stdout=subprocess.PIPE,
            close_fds=True
        )
        self._evt_file = self._proc_evt.stdout
        fcntl.fcntl(self._evt_file.fileno(),
                    fcntl.F_SETFL,
                    fcntl.F_GETFL | os.O_NONBLOCK)
        self._reader = NioLineReader(self._evt_file)

        # TODO: wait for the "exists -" line from drbdsetup events2,
        #       probably either here or somewhere in run();
        #       Lines can be requested from NioLineReader in a loop,
        #       if no more lines are available before the "exists -"
        #       line has been read, the file descriptor must be
        #       polled for the availability of more data before
        #       continuing to read from the NioLineReader.

        # detect readable data on the pipe
        self._evt_in_h = gobject.io_add_watch(
            self._evt_file.fileno(),
            gobject.IO_IN, self.drbd_event
        )
        # detect broken pipe
        self._evt_hup_h = gobject.io_add_watch(
            self._evt_file.fileno(),
            gobject.IO_HUP, self.restart_events
        )


    def uninit_events(self):
        """
        Stops "drbdsetup events" processing and the associated child process
        """
        # Unregister the input handler
        if self._evt_in_h is not None:
            gobject.source_remove(self._evt_in_h)
        self._evt_in_h = None

        # Unregister the hangup handler
        if self._evt_hup_h is not None:
            gobject.source_remove(self._evt_hup_h)
        self._evt_hup_h = None

        # If there is an existing events subprocess, attempt to terminate it
        # first. Subprocess termination is attempted in multiple stages:
        # 1. Attempt to close drbdmanage's receiver pipe, so the subprocess
        #    should notice that and exit, then give the subprocess a short
        #    period of time to exit
        # 2. If the subprocess is still there, send SIGTERM and give the
        #    subprocess another short period of time to exit
        # 3. ... wait a bit longer
        # 4. send SIGKILL and wait a short period of time for the subprocess
        #    to be killed
        # 5. ... wait a bit longer
        # 6. If the subprocess has ended, its remaining zombie process slot
        #    will be cleaned up, otherwise, drbdmanage stops polling for the
        #    subprocess at that point, because it is unclear whether the
        #    process will ever end. If it does, it will probably remain in the
        #    OS's process table as a zombie process.
        # The multiple stages are there to make subprocess termination as
        # reliable as possible, but as fast as reasonably possible at the same
        # time. Commonly, the subprocess should have exited after stage 1
        # or stage 2. Stage 6 should not be reached during normal operation.
        try:
            if self._proc_evt is not None:
                term_stage_end = 7
                term_stage     = 1
                while term_stage < term_stage_end:
                    if term_stage == 1:
                        # Stage 1: Close drbdmanage's receiver pipe
                        #          (that's the subprocess' stdout pipe)
                        stdout_pipe = self._proc_evt.stdout
                        if stdout_pipe is not None:
                            stdout_pipe.close()
                            time.sleep(self.EVT_TERM_SLEEP_SHORT)
                    elif term_stage >= 2:
                        # Check whether the process is still running
                        self._proc_evt.poll()
                        if self._proc_evt.returncode is None:
                            if term_stage == 2:
                                # Stage 2: Send SIGTERM and wait
                                self._term_events(self._proc_evt,
                                                  signal.SIGTERM)
                                time.sleep(self.EVT_TERM_SLEEP_SHORT)
                            elif term_stage == 3:
                                # Stage 3: wait longer
                                time.sleep(self.EVT_TERM_SLEEP_LONG)
                            elif term_stage == 4:
                                # Stage 4: send SIGKILL and wait
                                self._term_events(self._proc_evt,
                                                  signal.SIGKILL)
                                time.sleep(self.EVT_TERM_SLEEP_SHORT)
                            elif term_stage == 5:
                                # Stage 5: wait longer
                                time.sleep(self.EVT_TERM_SLEEP_LONG)
                            # Stage 6: no-op; runs through the poll() to
                            #          clean up the zombie process if the
                            #          OS killed the subprocess
                        else:
                            # If the process is not running anymore,
                            # leave the termination loop
                            term_stage = term_stage_end
                    # Enter the next stage
                    if term_stage < term_stage_end:
                        term_stage += 1
                # Forget the process handle; the variable will be reused
                # for a newly created events subprocess
                self._proc_evt = None
        except (OSError, IOError):
            pass


    def restart_events(self, evt_fd, condition):
        """
        Detects broken pipe, killed drbdsetup process, etc. and reinitialize
        the event callbacks
        """
        # unregister any existing event handlers for the events log
        log_error = True
        logging.error("drbdsetup events tracing has failed, restarting")

        retry = True
        while retry:
            try:
                self.init_events()
                retry = False
            except (OSError, IOError):
                if log_error:
                    logging.critical(
                        "cannot restart drbdsetup events tracing, "
                        "this node is inoperational"
                    )
                    logging.critical(
                        "retrying restart of drbdsetup events "
                        "tracing every 30 seconds"
                    )
                    log_error = False
                time.sleep(30)
        logging.info("drbdsetup events tracing reestablished")
        self._drbd_mgr.run(False, False)
        # Unregister this event handler, init_events has registered a new one
        # for the new events pipe
        return False

    def schedule_shutdown(self):
        gobject.timeout_add(0, self.shutdown)

    def schedule_run_changes(self):
        """
        Schedules execution of run_changes() from the GMainLoop

        run_changes() executes DrbdManager.run()
        """
        if not self._run_changes_scheduled:
            gobject.timeout_add(0, self.run_changes)
            self._run_changes_scheduled = True

    def schedule_poke(self):
        """
        Schedules a local DrbdManager run including poking other cluster nodes
        """
        self._poke_cluster = True
        self.schedule_run_changes()

    def run_changes(self):
        """
        Performs DrbdManager.run(), thereby applying pending changes locally
        """
        self._drbd_mgr.run(True, self._poke_cluster)
        self._run_changes_scheduled = False
        self._poke_cluster = False
        return False


    def drbd_event(self, evt_fd, condition):
        """
        Receives log entries from the "drbdsetup events" child process

        Detect state changes by reading the drbdsetup events log. If another
        node modifies the configuration on the drbdmanage control volume,
        this becomes visible in the event log as a remote role change on the
        drbdmanage control volume. In this case, the DRBD resource manager is
        invoked to check, whether any changes are required on this node.
        """
        changed = False
        while True:
            line = self._reader.readline()
            if line is not None:
                line = line.strip()
                if self.dbg_events:
                    logging.debug("received event line: %s" % line)
                sys.stderr.flush()
                match = self._evt_pat.match(line)
                if match is not None:
                    # try to parse args
                    line_data, evt_type, evt_source = self._drbd_event_split(match)

                    # Detect potential changes of the data on the
                    # control volume
                    if self._drbd_event_change_trigger(evt_type, evt_source, line_data):
                        changed = True
            else:
                break
        if changed:
            self._drbd_mgr.run(False, False)
        # True = GMainLoop shall not unregister this event handler
        return True


    def _drbd_event_initial_status(self):
        """
        Reads the initial DRBD status
        """
        logging.info("Reading initial DRBD control volume status")
        try:
            while True:
                line = self._reader.readline()
                if line is not None:
                    if line.startswith("exists -"):
                        break
                    else:
                        match = self._evt_pat.match(line)
                        if match is not None:
                            # try to parse args
                            line_data, evt_type, evt_source = self._drbd_event_split(match)

                            # Detect Quorum changes, etc.
                            self._drbd_event_change_trigger(evt_type, evt_source, line_data)
                else:
                    # poll for more data
                    poll = select.poll()
                    poll.register(self._reader.get_file().fileno())
                    fd_list = poll.poll()
                    if len(fd_list) >= 1:
                        fd, event = fd_list[0]
                        if ((event & select.POLLERR) != 0 or (event & select.POLLNVAL) != 0 or
                            (event & select.POLLHUP) != 0):
                            logging.warning("failed to determine the current status "
                                            "of the control volume")
                            break
        except IOError:
            logging.warning("drbdsetup events tracing failed while reading the current "
                            "status of the control volume")
        logging.info("Finished reading initial DRBD control volume status")


    def _drbd_event_split(self, match):
        """
        Returns fields, event type and event source of a drbd event line
        """
        # TODO: maybe this pattern can be pre-compiled, too?
        line_data = dict(re.findall('([\w-]+):(\S+)', match.group('attrs')))
        evt_type   = match.group('type')
        evt_source = match.group('source')
        return line_data, evt_type, evt_source


    def _drbd_event_change_trigger(self, evt_type, evt_source, line_data):
        changed = False
        try:
            if line_data[DrbdManageServer.EVT_ARG_NAME] == DRBDCTRL_RES_NAME:
                if (evt_type == self.EVT_TYPE_CHANGE and
                    evt_source == self.EVT_SRC_CON):
                    # Check: role change to Secondary
                    try:
                        if line_data[self.EVT_ARG_ROLE] == self.EVT_ROLE_SECONDARY:
                            changed = True
                            if self.dbg_events:
                                logging.debug(
                                    "event change trigger role:Secondary"
                                )
                    except KeyError:
                        # Ignore: Not a role change
                        pass

                if evt_source == DrbdManageServer.EVT_SRC_PEERDEV:
                    # Check: replication mode SyncTarget
                    try:
                        replication = line_data[self.EVT_ARG_REPL]
                        if replication == self.EVT_REPL_SYNCTARGET:
                            changed = True
                            if self.dbg_events:
                                logging.debug(
                                    "event change trigger "
                                    "replication:SyncTarget"
                                )
                        elif replication == self.EVT_REPL_OFF:
                            # Quorum: May have lost a node
                            node_name = line_data[self.EVT_CONN_NAME]
                            self._quorum.node_left(node_name)
                            self._quorum.readjust_full_member_count()
                        elif replication == self.EVT_REPL_ON:
                            # Quorum: Node may have joined
                            node_name = line_data[self.EVT_CONN_NAME]
                            change_quorum = self._quorum.node_joined(node_name)
                            if change_quorum:
                                persist = None
                                try:
                                    persist = self.begin_modify_conf()
                                    if persist is not None:
                                        # Unset QIGNORE status on connected nodes
                                        self._quorum.readjust_qignore_flags()
                                        self._quorum.readjust_full_member_count()
                                        self.get_serial()
                                        self.save_conf_data(persist)
                                    else:
                                        logging.warning(
                                            "Attempt to save updated quorum membership information to "
                                            "the control volume failed"
                                        )
                                except QuorumException:
                                    # This node does not have a quorum, skip saving
                                    pass
                                except PersistenceException:
                                    logging.warning(
                                        "Attempt to save updated quorum membership information to "
                                        "the control volume failed"
                                    )
                                finally:
                                    self.end_modify_conf(persist)
                    except KeyError:
                        # Ignore: Not a replication change or
                        #         replication on/off change without a conn-name argument
                        pass
        except KeyError:
            # Ignore lines with missing fields (line_data keys)
            pass
        return changed


    def _term_events(self, proc_evt, signal):
        """
        Sends signals to a subprocess
        """
        if proc_evt.pid is not None:
            os.kill(proc_evt.pid, signal)


    def init_logging(self):
        """
        Initialize global logging
        """
        self._root_logger = logging.getLogger("")
        try:
            syslog_h = logging.handlers.SysLogHandler(address="/dev/log")
        except:
            syslog_h = logging.NullHandler()

        syslog_f = logging.Formatter(fmt=self.LOGGING_FORMAT)
        syslog_h.setFormatter(syslog_f)
        self._root_logger.addHandler(syslog_h)
        self._root_logger.setLevel(logging.INFO)


    def set_loglevel(self):
        """
        Adjust the loglevel to the one specified in the server's configuration
        """
        try:
            loglevel_conf = str.upper(self._conf[KEY_LOGLEVEL])
            loglevel_id   = self.DM_LOGLEVELS[loglevel_conf]
            self._root_logger.setLevel(loglevel_id)
        except KeyError:
            pass


    def create_signal(self, path):
        """
        Create a drbdmanage signal using the server's signal factory
        """
        signal = None
        if self._signal_factory is not None:
            try:
                signal = self._signal_factory.create_signal(path)
            except DrbdManageException as dm_exc:
                logging.error("Signal creation failed: %s"
                              % (str(dm_exc)))
            except Exception:
                logging.error("Signal creation failed, "
                              "unhandled exception encountered")
        return signal

    def load_server_conf(self, stage):
        """
        Loads the server configuration

        Setting up the configuration is a two step process. In the first
        stage we load the configuration from a file (usually
        /etc/drbdmanaged.conf). In this file we store the minimal
        configuration settings (e.g. to find the control volume). In stage
        2 we then load the complete configuration from the control volume.

        The server configuration is loaded and is then unified with any
        existing default values.
        Values from the configuration override default configuration values.
        Values not specified in the configuration file are inherited from
        the default configuration. Any values specified in the configuration
        file that are not known in the default configuration are discarded.
        """
        sconf_key = DrbdManageServer.OBJ_SCONF_NAME
        pconf_key = DrbdManageServer.OBJ_PCONF_NAME

        # ## storage plugin configuration (generic part)
        # always start with a copy of default values
        plugin_configs = self._pluginmgr.get_plugin_default_config()  # [] of dicts

        final_plugin_config = {}
        for plugin in plugin_configs:
            p = plugin.copy()
            p_name = p['name']
            filter_prohibited(p, ('name',))
            final_plugin_config[p_name] = p
        plugin_vg_overwrite = dict([(plugin['name'], False) for plugin in plugin_configs])

        # ## general configuration
        # always start with a copy of default values
        final_config = self.CONF_DEFAULTS.copy()

        if stage == self.CONF_STAGE[self.KEY_FROM_CTRL_VOL]:
            # here we assume we have access to the control volume
            final_config = self.CONF_DEFAULTS.copy()

            # cluster wide settings
            _, props = self._get_cluster_props()
            if props:
                for k in props:
                    if k in final_config:
                        final_config[k] = props[k]

            # node specific general settings
            try:
                node_props = self.get_instance_node().get_props().get_all_props(
                    PropsContainer.NAMESPACES[PropsContainer.KEY_DMCONFIG])
            except:
                node_props = {}

            if KEY_SITE in node_props:
                site_props = self._get_site_props(node_props[KEY_SITE])
                for k, v in site_props.items():
                    if k in final_config:
                        final_config[k] = v

            for k in node_props:
                if k in final_config:
                    final_config[k] = node_props[k]

            # node specific plugin settings
            for plugin_name in self._plugin_conf.keys():
                try:
                    props = self.get_instance_node().get_props().get_all_props(
                        PropsContainer.NAMESPACES['plugins'] + plugin_name)
                except:
                    props = {}

                filter_allowed(props, self._plugin_conf[plugin_name].keys())
                for k in props:
                    final_plugin_config[plugin_name][k] = props[k]
                    if k == KEY_VG_NAME:
                        plugin_vg_overwrite[plugin_name] = True

            # IMPORTANT: even in this stage we still want that the config file overwrites everything else.
            stage = self.CONF_STAGE[self.KEY_FROM_FILE]

        if stage == self.CONF_STAGE[self.KEY_FROM_FILE]:
            cfg = load_server_conf_file()

            for k, v in cfg['local'].items():
                if k in final_config:
                    final_config[k] = v
                # the role is only used once at startup it does not make sense to add it to the
                # general config options because it makes no sense to write this to the control volume
                # only makes sense in drbdmanaged.cfg
                elif k == KEY_SAT_CFG_ROLE:
                    if v == KEY_SAT_CFG_SATELLITE:
                        self._forced_is_satellite = SAT_SATELLITE
                    elif v == KEY_SAT_CFG_CONTROL_NODE:
                        self._forced_is_satellite = SAT_CONTROL_NODE

            for plugin_name in cfg['plugins']:
                for k, v in cfg['plugins'][plugin_name].items():
                    final_plugin_config[plugin_name][k] = v
                    if k == KEY_VG_NAME:
                        plugin_vg_overwrite[plugin_name] = True

        # check where ctrlvol != default and plugin vg has no special settings
        drbdctrl_vg = final_config[KEY_DRBDCTRL_VG]
        if drbdctrl_vg != self.CONF_DEFAULTS[KEY_DRBDCTRL_VG]:
            for plugin_name in [k for k, v in plugin_vg_overwrite.items() if not v]:
                final_plugin_config[plugin_name][KEY_VG_NAME] = drbdctrl_vg

        self._objects_root[sconf_key] = final_config
        self._objects_root[pconf_key] = final_plugin_config

        cur_storage_plugin = self._conf.get(self.KEY_STOR_NAME, None)
        new_storage_plugin = final_config[self.KEY_STOR_NAME]
        if cur_storage_plugin and cur_storage_plugin != new_storage_plugin:
            self._bd_mgr = BlockDeviceManager(new_storage_plugin, self._pluginmgr)

        self.update_objects()  # which sets self._conf and self._plugin_conf from _objects_root

        self._reload_plugins()
        # print "Stage_END", self._conf, self._plugin_conf

    def get_conf_value(self, key):
        """
        Returns a configuration value.

        All configuration values are stored as strings. If another type is
        required, any function that retrieves the configuration value
        should attempt to convert the value to the required type. If that
        conversion fails, the configuration value from the default
        configuration (CONF_DEFAULTS) should be used instead.

        @param   key: the name (key) of the configuration value
        @return: configuration value
        @rtype:  str
        """
        return self._conf.get(key)


    def get_cluster_conf_value(self, key):
        """
        Retrieves a value from the replicated cluster configuration
        """
        return self._cluster_conf.get_prop(key)

    @no_satellite
    def run_external_plugin(self, plugin_name, props):
        fn_rc = []
        ret = None
        try:
            plugin = self._pluginmgr.get_plugin_instance(plugin_name)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            add_rc_entry(fn_rc, DM_EPLUGIN,
                         "Error loading plugin: %(msg)s, in %(backtrace)s",
                         [["msg", str(e)],
                          ["repr", repr(e)],
                          ["backtrace", "".join(traceback.format_tb(exc_traceback))],
                          ])
            return (fn_rc, {})

        try:
            self._pluginmgr.set_plugin_config(plugin_name, props)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            add_rc_entry(fn_rc, DM_EINVAL,
                         "Error configuring plugin: %(msg)s, in %(backtrace)s",
                         [["msg", str(e)],
                          ["repr", repr(e)],
                          ["backtrace", "".join(traceback.format_tb(exc_traceback))],
                          ])
            return (fn_rc, {})

        try:
            ret = plugin.run()

            if ret and len(ret) == 2:
                return (ret[0], ret[1])

            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))

            if isinstance(ret, dict):
                data = ret
            else:
                data = {}

            return (fn_rc, data)

        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            add_rc_entry(fn_rc, DM_EINVAL,
                         "Error running plugin: %(msg)s, in %(backtrace)s",
                         [["msg", str(e)],
                          ["repr", repr(e)],
                          ["backtrace", "".join(traceback.format_tb(exc_traceback))],
                          ])
            return (fn_rc, {})

        # Error (-in-error) case

        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_EPLUGIN, "error running plugin")
        return (fn_rc, {})

    def peek_serial(self):
        """
        Returns the current serial number without changing it

        Returns the current serial number, whether or not it is still in use
        for changes.
        """
        serial = 0
        serial_str = self._cluster_conf.get_prop(SERIAL)
        try:
            serial = int(serial_str)
        except TypeError:
            # FIXME: a better solution would be to find the greatest
            #        serial number set on any object in the
            #        configuration, and then to increase that number
            #        and use it as the new serial number of the cluster
            #        configuration.
            #        Another possibility would be to reset the serial
            #        number on
            #        all objects to 0 and then set a serial of 1 here.
            #        The current workaround merely keeps the system
            #        running, but the serial numbers are totally messed
            #        up if this happens.
            logging.error(
                "Unparseable serial number in the cluster "
                "configuration, setting serial=0 to recover"
            )
        return serial


    def get_serial(self):
        """
        Returns a serial number for configuration changes

        Upon the first call of this function in a sequence of changes, a
        new serial number is generated and returned. Upon subsequent calls,
        the same serial number is returned until the change generation is
        closed by calling close_serial().
        """
        return self._cluster_conf.new_serial()


    def close_serial(self):
        """
        Closes the current generation of configuration changes

        After a generation of configuration changes has been closed,
        the next call of get_serial() will open a new change generation and
        will return a new serial number.
        """
        self._serial_gen.close_serial()


    def get_quorum(self):
        """
        Returns the quorum tracking instance
        """
        return self._quorum


    def get_drbd_mgr(self):
        """
        Returns the DRBD devices manager instance
        """
        return self._drbd_mgr


    def get_bd_mgr(self):
        """
        Returns the block device manager instance
        """
        return self._bd_mgr


    def iterate_nodes(self):
        """
        Returns an iterator over all registered nodes
        """
        return self._nodes.itervalues()


    def iterate_resources(self):
        """
        Returns an iterator over all registered resources
        """
        return self._resources.itervalues()


    def get_node(self, name):
        """
        Retrieves a node by its name

        @return: the named node object or None if no object with the specified
                 name exists
        """
        node = None
        try:
            node = self._nodes.get(name)
        except Exception as exc:
            self.catch_internal_error(exc)
        return node


    def get_resource(self, name):
        """
        Retrieves a resource by its name

        @return: the named resource object or None if no object with the
                 specified name exists
        """
        resource = None
        try:
            resource = self._resources.get(name)
        except Exception as exc:
            self.catch_internal_error(exc)
        return resource


    def get_volume(self, name, vol_id):
        """
        Retrieves a volume by its name

        @return: the volume object specified by the name of the resource it is
                 contained in and by its volume id or None if no object with
                 the specified name exists
        """
        volume = None
        try:
            resource = self._resources.get(name)
            if resource is not None:
                volume = resource.get_volume(vol_id)
        except Exception as exc:
            self.catch_internal_error(exc)
        return volume

    def get_common(self):
        """
        Retrieves the common object

        @return: the common object, or None on failure
        """
        common = None
        try:
            common = self._common
        except Exception as exc:
            self.catch_internal_error(exc)
        return common

    # Get the node this server is running on
    def get_instance_node(self):
        """
        Retrieves the node that represents the host this instance of
        drbdmanage is currently running on.

        @return: the node object this instance of drbdmanage is running on
                 or None if no node object is registered for this host
        """
        node = None
        try:
            node = self._nodes[self._instance_node_name]
        except KeyError:
            pass
        except Exception as exc:
            self.catch_internal_error(exc)
        return node


    # Get the name of the node this server is running on
    def get_instance_node_name(self):
        """
        Returns the name used by the drbdmanage server to look for a node
        object that represents the hosts this drbdmanage server is currently
        running on

        @return: name of the node object this drbdmanage server is running on
        """
        return self._instance_node_name


    def _cluster_nodes_update(self):
        """
        Flags other nodes for reconfiguration of the control volume
        """
        inst_node = self.get_instance_node()
        for peer_node in self._nodes.itervalues():
            if peer_node != inst_node:
                state = peer_node.get_state()
                if is_set(state, DrbdNode.FLAG_DRBDCTRL):
                    peer_node.set_state(state | DrbdNode.FLAG_UPDATE)

    @no_satellite
    def poke(self):
        """
        Causes cluster nodes to perform pending actions by changing the serial

        Changes the serial number, thereby changing the hash value of the
        cluster configuration and causing all connected nodes to perform
        pending actions

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc   = []
        try:
            # Schedule a DrbdManager run that overrides the hash check
            # and changes the serial number, so as to cause all other
            # cluster nodes to run any scheduled changes too
            self.schedule_poke()
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    def get_config_keys(self):
        """
        All the configuration options drbdmanage knows about
        """
        fn_rc = []
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return (fn_rc, self.CONF_DEFAULTS)

    def get_plugin_default_config(self):
        """
        All the config options for known plugins
        returns a [{}, {}]
        """
        fn_rc = []
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))

        cfg = self._pluginmgr.get_plugin_default_config()
        return (fn_rc, cfg)


    def _get_cluster_props(self):
        fn_rc = []
        ret = {}
        try:
            common = self.get_common()
            if common is not None:
                props_cont = common.get_props()
                ns = PropsContainer.NAMESPACES["dmconfig"] + "cluster/"
                ret = props_cont.get_all_props(ns)
            else:
                # The common object should always be present, if it is not,
                # indicate a programming error
                raise DebugException
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return (fn_rc, ret)


    def get_cluster_config(self):
        """
        All the config options in the global section. These are options that are also valid per node
        """
        return self._get_cluster_props()

    def get_selected_config_values(self, keys):
        fn_rc = []
        ret = dict([(k, v) for k, v in self._conf.items() if k in keys])
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return (fn_rc, ret)

    def _get_all_sites(self, props_cont):
        ns = PropsContainer.NAMESPACES["dmconfig"] + "site/"
        sites = props_cont.get_all_props(ns)
        return set([k.partition('/')[0] for k in sites.keys()])

    def _get_site_props(self, site):
        props_cont = None
        common = self.get_common()
        site_props = {}

        if common is not None:
            props_cont = common.get_props()

        if props_cont is not None:
            ns = PropsContainer.NAMESPACES["dmconfig"] + "site/" + site
            site_props = props_cont.get_all_props(ns)

        return site_props

    def get_site_config(self):
        fn_rc = []
        ret = []
        props_cont = None
        common = self.get_common()

        if common is not None:
            props_cont = common.get_props()

        if props_cont is not None:
            sites = self._get_all_sites(props_cont)
            for s in sites:
                site_props = self._get_site_props(s)
                site_props['name'] = s
                ret.append(site_props)

        add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return (fn_rc, ret)

    def update_satellite_states(self):
        fn_rc = []
        states = {}

        try:
            ns = PropsContainer.NAMESPACES[PropsContainer.KEY_SATELLITES]
            node_props = self.get_instance_node().get_props().get_all_props(ns)
            for k, v in node_props.items():
                name, key = k.split('/')
                if key == KEY_ISSATELLITE:
                    states[name] = int(v)
        except:
            pass

        self.sat_state_ctrlvol = filter_allowed(self.sat_state_ctrlvol, states.keys())

        for k, v in states.items():
            self.sat_state_ctrlvol[k] = v

        add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return (fn_rc, states.keys())

    def send_init_satellites(self):
        ns = PropsContainer.NAMESPACES[PropsContainer.KEY_SATELLITES]
        self._persist.json_export(self._objects_root)

        needs_change = False
        needs_save = False

        wanted = [s for s, state in self.sat_state_ctrlvol.items()
                  if state == SAT_CON_STARTUP or state == SAT_CON_ESTABLISHED]

        instance_node = self.get_instance_node()
        for satellite in [s for s in wanted if s not in self._proxy.get_established()]:
            skip = False
            for node in self.iterate_nodes():
                # node != instance_node: can never send init to myself
                if (node is not instance_node and
                    node.get_props().get_prop(KEY_ISSATELLITE, os.path.join(ns, satellite)) is not None):
                    skip = True  # somebody else is responsible for that satellite
                    needs_change = True  # give it a hint to shutdown
                    break
            if not skip:
                opcode, length, data = self._proxy.send_cmd(satellite, KEY_S_CMD_INIT)
                if opcode == self._proxy.opcodes[KEY_S_ANS_OK]:
                    self.sat_state_ctrlvol[satellite] = SAT_CON_ESTABLISHED
                    node_props = self.get_instance_node().get_props()
                    node_props.set_prop(KEY_ISSATELLITE, SAT_CON_ESTABLISHED, os.path.join(ns, satellite))
                    needs_change = True
                    needs_save = True

        return needs_change, needs_save

    def _remove_satellites(self, satellite_names, node_names=[]):
        # persistence has to be opened by caller
        # is save to be called with [ctrlnode, ...]
        for satellite in satellite_names:
            self._proxy.shutdown_connection(satellite)

        if node_names:
            wanted_nodes = dict([(k, v) for k, v in self._nodes.items() if k in node_names])
        else:
            wanted_nodes = self._nodes

        ns = PropsContainer.NAMESPACES[PropsContainer.KEY_SATELLITES]
        for node in [v for k, v in wanted_nodes.items() if k not in satellite_names]:
            node_props = node.get_props()
            to_delete = [k for k in node_props.get_all_props(ns).keys() if k.split('/')[0] in satellite_names]
            node_props.remove_selected_props(to_delete, ns)

    def is_satellite(self, node_name):
        is_satellite = self.get_node(node_name).get_props().get_prop(KEY_ISSATELLITE)
        is_satellite = True if is_satellite is not None else False
        return is_satellite

    def get_satellite_names(self, node):
        satellite_names = set()
        if self.is_satellite(node.get_name()):
            return satellite_names

        ns = PropsContainer.NAMESPACES[PropsContainer.KEY_SATELLITES]
        node_props = node.get_props()
        satellite_names = set([k.split('/')[0] for k in node_props.get_all_props(ns).keys()])

        return satellite_names

    def get_ctrl_node(self, satellite_name):
        control_node = None
        if not self.is_satellite(satellite_name):
            return None
        else:
            control_node_name = self.get_node(satellite_name).get_props().get_prop(KEY_ISSATELLITE)
            control_node = self.get_node(control_node_name)

        return control_node

    @no_satellite
    def assign_satellite(self, props):
        fn_rc = []
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                ns = PropsContainer.NAMESPACES[PropsContainer.KEY_SATELLITES]
                new_control_node_name = props[NODE_CONTROL_NODE]
                satellite_name = props[NODE_SATELLITE_NODE]

                # santiy checks
                # do the nodes exist?
                if self.get_node(new_control_node_name) is None or self.get_node(satellite_name) is None:
                    raise KeyError

                # is the satellite_name a satellite?
                if not self.is_satellite(satellite_name):
                    raise KeyError

                # is the control node really a control node (i.e., does not have ISSATELLITE)
                if self.is_satellite(new_control_node_name):
                    raise KeyError

                # set shutdown on old node
                old_ctrl_node = self.get_ctrl_node(satellite_name)
                # could be None if old ctrlnode got removed and this is a fresh assignment
                # if there is an old ctrtlnode it is a reassignment
                if old_ctrl_node is not None:
                    old_ctrl_node.get_props().set_prop(KEY_ISSATELLITE, SAT_CON_SHUTDOWN,
                                                       os.path.join(ns, satellite_name))
                # set startup on new node
                new_control_node = self.get_node(new_control_node_name)
                new_control_node.get_props().set_prop(KEY_ISSATELLITE, SAT_CON_STARTUP,
                                                      os.path.join(ns, satellite_name))

                # set new control node on satellite
                satellite_node = self.get_node(satellite_name)
                satellite_node.get_props().set_prop(KEY_ISSATELLITE, new_control_node_name)

                self.get_serial()
            else:
                raise PersistenceException
        except KeyError:
            add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
            self.schedule_run_changes()

        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))

        return fn_rc

    @no_satellite
    def set_cluster_config(self, cfgdict):
        # for persistence see create_node
        fn_rc = []
        persist = None
        prohibited_settings = ('drbdctrl-vg',)
        cfgtype = cfgdict['type'][0]['type']

        def _set_del_rest(props, cfg, ns):
            """
            merge options from cfg und delete other settings that were set from ns
            """
            props.merge_props(cfg, ns)
            set_props = props.get_all_props(ns)
            # we got all currently set properties in set_props
            # delete all from dict which are still valid and then delete
            # these items that are no more present.
            for k in cfg:
                del(set_props[k])
            props.remove_selected_props(set_props, ns)

        try:
            persist = self.begin_modify_conf()
            if persist:
                # ## CLUSTER wide configuration ([GLOBAL]) ##
                # always a single {} (maybe empty), in a []
                glob_dict = cfgdict['globals'][0]
                glob_dict = filter_allowed(glob_dict, self.CONF_DEFAULTS.keys())
                glob_dict = filter_prohibited(glob_dict, prohibited_settings)

                ns = PropsContainer.NAMESPACES["dmconfig"] + "cluster/"
                common = self.get_common()
                props = None
                if common:
                    props = common.get_props()
                    if props:
                        _set_del_rest(props, glob_dict, ns)

                # ## SITE specific configuration ([Site:xyz]) ##
                ns = PropsContainer.NAMESPACES["dmconfig"] + "site/"
                all_sites = self._get_all_sites(props)
                cfg_sites = []
                for s in cfgdict['sites']:
                    c = s.copy()
                    site_name = c['name']
                    cfg_sites.append(site_name)
                    c = filter_allowed(c, self.CONF_DEFAULTS.keys())
                    c = filter_prohibited(c, prohibited_settings)
                    sns = ns + site_name
                    # here we can recycle props, as it uses the same props as GLOBAL
                    _set_del_rest(props, c, sns)

                # sites without a config
                cfgless_sites = [s for s in all_sites if s not in cfg_sites]
                for site_name in cfgless_sites:
                    sns = ns + site_name
                    props.remove_selected_props(props.get_all_props(sns), sns)

                # ## NODE specific configuration ([Node:xyz]) ##
                all_nodes = [n for n in self._nodes.iterkeys()]
                cfg_nodes = []
                cfg_plugins = []
                allowed_node_props = self.CONF_DEFAULTS.keys() + [KEY_SITE]
                for n in cfgdict['nodes']:
                    # we always get the name, but if only name, we are done
                    node_name = n['name']
                    node = self._nodes.get(node_name)
                    if node:
                        if len(n) >= 1:
                            cfg_nodes.append(node_name)
                        props = node.get_props()

                        # options allowed in global section (but here node specific)
                        p = n.copy()
                        p = filter_allowed(p, allowed_node_props)
                        p = filter_prohibited(p, prohibited_settings)
                        ns = PropsContainer.NAMESPACES['dmconfig']

                        old_site = props.get_prop('site', ns)

                        props.merge_props(p, ns)
                        pgone = [k for k in allowed_node_props if k not in p]
                        props.remove_selected_props(pgone, ns)

                        new_site = props.get_prop('site', ns)
                        if old_site != new_site:
                            self._set_updflag(node)

                            inform_sites = []
                            if old_site is None and new_site:
                                inform_sites = [new_site]
                            elif old_site and new_site is None:
                                inform_sites = [old_site]
                            elif old_site and new_site:
                                inform_sites = [old_site, new_site]

                            for n_iter in self._nodes.itervalues():
                                node_props_cont = n_iter.get_props()
                                if node_props_cont:
                                    ns = PropsContainer.NAMESPACES[PropsContainer.KEY_DMCONFIG]
                                    node_site = node_props_cont.get_prop('site', ns)
                                    if node_site in inform_sites:
                                        self._set_updflag(n_iter)

                        # plugin settings (only allowed node specific)
                        if cfgtype == CONF_NODE:
                            pns = PropsContainer.NAMESPACES['plugins']
                            for plugin in cfgdict['plugins']:
                                p = plugin.copy()
                                plugin_name = p['name']
                                cfg_plugins.append(plugin_name)
                                ns = pns + plugin_name
                                p = filter_allowed(p, self._plugin_conf[plugin_name].keys())
                                props.merge_props(p, ns)
                                pgone = [k for k in self._plugin_conf[plugin_name].keys() if k not in p]
                                props.remove_selected_props(pgone, ns)

                # nodes where config sections got deleted
                if cfgtype == CONF_GLOBAL:
                    # remove all their CONF_DEFAULTS props
                    cfgless_nodes = [n for n in all_nodes if n not in cfg_nodes]
                    for name in cfgless_nodes:
                        node = self._nodes.get(name)
                        if node:
                            props = node.get_props()
                            props.remove_selected_props(allowed_node_props, ns)
                elif cfgtype == CONF_NODE:
                    cfgless_plugins = [pl for pl in self._plugin_conf.keys() if pl not in cfg_plugins]
                    for plugin in cfgless_plugins:
                        pns = PropsContainer.NAMESPACES['plugins'] + plugin
                        props.remove_selected_props(props.get_all_props(pns), pns)

                self.save_conf_data(persist)
                self.schedule_run_changes()

            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)

        if len(fn_rc) == 0:
            # at this point the cluster wide config is set and the nodes config
            # propagate these changes to _conf:
            self.load_server_conf(self.CONF_STAGE[self.KEY_FROM_CTRL_VOL])
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))

        return fn_rc

    @no_satellite
    def create_node(self, node_name, props):
        """
        Registers a DRBD cluster node

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc   = []
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                sub_rc = self._create_node(False, node_name, props, None, None, None)
                if NODE_CONTROL_NODE in props and sub_rc == DM_SUCCESS:  # satellite node
                    control_node_name = props[NODE_CONTROL_NODE]
                    control_node = self._nodes.get(control_node_name)
                    if control_node:
                        props = control_node.get_props()
                        ns = PropsContainer.NAMESPACES[PropsContainer.KEY_SATELLITES]
                        props.set_prop(KEY_ISSATELLITE, SAT_CON_STARTUP, os.path.join(ns, node_name))
                        # take a shortcut to add the new node to the satellites array without
                        # update_satellite_states()
                        if control_node_name == self._instance_node_name:
                            self.sat_state_ctrlvol[node_name] = SAT_CON_STARTUP
                            self.send_init_satellites()

                if sub_rc == DM_SUCCESS or sub_rc == DM_ECTRLVOL:
                    self.save_conf_data(persist)
                else:
                    add_rc_entry(fn_rc, sub_rc, dm_exc_text(sub_rc))
            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc


    def _create_node(self, initial, node_name, props, bdev_0, bdev_1, port):
        """
        Register DRBD cluster nodes and update control volume configuration

        Used by create_node() and init_node()
        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc   = DM_EPERSIST
        node    = None
        try:
            if self._nodes.get(node_name) is not None:
                fn_rc = DM_EEXIST
            else:
                addr    = None
                addrfam = DrbdNode.AF_IPV4
                try:
                    addr     = props[NODE_ADDR]
                except KeyError:
                    pass
                try:
                    af_label = props[NODE_AF]
                    if af_label == DrbdNode.AF_IPV4_LABEL:
                        addrfam = DrbdNode.AF_IPV4
                    elif af_label == DrbdNode.AF_IPV6_LABEL:
                        addrfam = DrbdNode.AF_IPV6
                except KeyError:
                    pass
                # Default state for new nodes:
                # Node has a control volume and local storage
                node_drbdctrl = True
                try:
                    node_drbdctrl = string_to_bool(props[FLAG_DRBDCTRL])
                except (KeyError, ValueError):
                    pass
                node_storage = True
                try:
                    node_storage = string_to_bool(props[FLAG_STORAGE])
                except (KeyError, ValueError):
                    pass
                node_external = False
                try:
                    node_external = string_to_bool(props[FLAG_EXTERNAL])
                except (KeyError, ValueError):
                    pass
                node_standby = False
                try:
                    node_standby = string_to_bool(props[FLAG_STANDBY])
                except (KeyError, ValueError):
                    pass
                control_node_name = None
                try:
                    control_node_name = props[NODE_CONTROL_NODE]
                except (KeyError, ValueError):
                    pass
                node_state = 0
                if node_drbdctrl:
                    node_state |= DrbdNode.FLAG_DRBDCTRL
                if node_storage:
                    node_state |= DrbdNode.FLAG_STORAGE
                if node_standby:
                    node_state |= DrbdNode.FLAG_STANDBY
                # Ignore the quorum vote of newly added nodes until
                # the nodes join for the first time, unless this is
                # the first node initializing the drbdmanage cluster
                if not initial:
                    node_state |= DrbdNode.FLAG_QIGNORE
                try:
                    if addr is not None and addrfam is not None:
                        node = None
                        node_id = DrbdNode.NODE_ID_NONE
                        if node_drbdctrl:
                            node_id = self.get_free_drbdctrl_node_id()
                        if ((node_id != DrbdNode.NODE_ID_NONE) or
                            (not node_drbdctrl)):
                            # Initialize the node object
                            poolsize = 0
                            poolfree = 0
                            if node_storage:
                                poolsize = -1
                                poolfree = -1
                            node = DrbdNode(
                                node_name, addr, addrfam, node_id,
                                node_state, poolsize, poolfree,
                                self.get_serial, None, None
                            )
                            # Merge only auxiliary properties into the
                            # DrbdNode's properties container
                            aux_props = aux_props_selector(props)
                            node.get_props().merge_gen(aux_props)
                            self._nodes[node.get_name()] = node
                            if node_drbdctrl:
                                self._cluster_nodes_update()
                                # create or update the drbdctrl.res file
                                check_configure = self._configure_drbdctrl(
                                    initial,
                                    None, bdev_0, bdev_1, port
                                )
                                if check_configure == 0:
                                    self._drbd_mgr.adjust_drbdctrl()
                                    fn_rc = DM_SUCCESS
                                else:
                                    fn_rc = DM_ECTRLVOL
                            else:
                                if not node_external:  # a satellite
                                    node.get_props().set_prop(KEY_ISSATELLITE, control_node_name)
                                fn_rc = DM_SUCCESS
                        else:
                            # Attempted to create a node with a control volume,
                            # but could not assign a node id
                            fn_rc = DM_ENODEID
                    else:
                        fn_rc = DM_EINVAL
                except InvalidNameException:
                    fn_rc = DM_ENAME
        except PersistenceException as pexc:
            raise pexc
        except Exception as exc:
            self.catch_internal_error(exc)
            fn_rc = DM_DEBUG
        return fn_rc

    @no_satellite
    def remove_node(self, node_name, force):
        """
        Marks a node for removal from the DRBD cluster
        * Orders the node to undeploy all volumes
        * Orders all other nodes to disconnect from the node

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc   = []
        persist = None
        node    = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node = self._nodes[node_name]
                drbdctrl_flag = False
                ns = PropsContainer.NAMESPACES[PropsContainer.KEY_SATELLITES]

                if self.is_satellite(node_name):
                    ctrl_node = self.get_ctrl_node(node_name)
                    if ctrl_node:
                        ctrl_node_props = ctrl_node.get_props()
                        ctrl_node_props.set_prop(KEY_ISSATELLITE, SAT_CON_SHUTDOWN,
                                                 os.path.join(ns, node_name))
                else:
                    ctrl_node = self._nodes[node_name]
                    ctrl_node_props = ctrl_node.get_props()
                    for satellite_name in self.get_satellite_names(ctrl_node):
                        ctrl_node_props.set_prop(KEY_ISSATELLITE, SAT_CON_SHUTDOWN,
                                                 os.path.join(ns, satellite_name))

                if (not force) and (node.has_assignments() or self.get_satellite_names(node)):
                    for assignment in node.iterate_assignments():
                        assignment.undeploy()
                        resource = assignment.get_resource()
                        for peer_assg in resource.iterate_assignments():
                            peer_assg.update_connections()
                    node.remove()
                    self.schedule_run_changes()
                else:
                    if is_set(node.get_state(), DrbdNode.FLAG_DRBDCTRL):
                        drbdctrl_flag = True
                    # drop all associated assignments
                    for assignment in node.iterate_assignments():
                        resource = assignment.get_resource()
                        resource.remove_assignment(assignment)
                        # tell the remaining nodes that have this resource to
                        # drop the connection to the deleted node
                        for peer_assg in resource.iterate_assignments():
                            peer_assg.update_connections()
                    del self._nodes[node_name]
                    if drbdctrl_flag:
                        self._cluster_nodes_update()
                self.get_serial()
                self.save_conf_data(persist)
                if drbdctrl_flag:
                    self.reconfigure_drbdctrl()
            else:
                raise PersistenceException
        except KeyError:
            add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT),
                         [ [ NODE_NAME, node_name ] ])
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc


    @no_satellite
    def create_resource(self, res_name, props):
        """
        Registers a new resource that can be deployed to DRBD cluster nodes

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc = []
        resource = None
        persist  = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                resource = self._create_resource(res_name, props, fn_rc)
                if resource is not None:
                    self._resources[resource.get_name()] = resource
                    self.save_conf_data(persist)
            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc


    def _create_resource(self, res_name, props, fn_rc):
        resource = None
        try:
            resource = self._resources.get(res_name)
            if resource is not None:
                add_rc_entry(fn_rc, DM_EEXIST, dm_exc_text(DM_EEXIST),
                             [ [RES_NAME, resource.get_name()] ])
            else:
                port = RES_PORT_NR_AUTO
                secret = generate_secret()
                if secret is not None:
                    try:
                        port = int(props[RES_PORT])
                    except KeyError:
                        pass
                    if port == RES_PORT_NR_AUTO:
                        port = self.get_free_port_nr()
                    if port < 1 or port > 65535:
                        add_rc_entry(fn_rc, DM_EPORT, dm_exc_text(DM_EPORT),
                                     [ [ RES_PORT, str(port) ] ])
                    else:
                        resource = DrbdResource(
                            res_name,
                            port, secret, 0, None,
                            self.get_serial, None, None
                        )
                        # Merge only auxiliary properties into the
                        # DrbdResource's properties container
                        aux_props = aux_props_selector(props)
                        resource.get_props().merge_gen(aux_props)
                else:
                    add_rc_entry(fn_rc, DM_ESECRETG,
                                 dm_exc_text(DM_ESECRETG))
        except ValueError:
            add_rc_entry(fn_rc, DM_EINVAL, dm_exc_text(DM_EINVAL),
                         [ [ RES_PORT, port ] ])
        except InvalidNameException:
            add_rc_entry(fn_rc, DM_ENAME, dm_exc_text(DM_ENAME),
                         [ [ RES_NAME, res_name ] ])
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
            # Discard the resource if something went awry unexpectedly
            resource = None
        return resource

    @no_satellite
    def modify_node(self, node_name, serial, props):
        """
        Modifies node properties

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc = []
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node = self.get_node(node_name)
                if node is not None:
                    modified = False
                    addr_changed = False

                    # Network address change
                    try:
                        addr = props[NODE_ADDR]
                        node.set_addr(addr)
                        modified = True
                        addr_changed = True
                    except KeyError:
                        pass

                    # Address family change
                    try:
                        af_label = props[NODE_AF]
                        if af_label == DrbdNode.AF_IPV4_LABEL:
                            addrfam = DrbdNode.AF_IPV4
                        elif af_label == DrbdNode.AF_IPV6_LABEL:
                            addrfam = DrbdNode.AF_IPV6
                        node.set_addrfam(addrfam)
                        modified = True
                        addr_changed = True
                    except KeyError:
                        pass
                    except InvalidAddrFamException:
                        add_rc_entry(fn_rc, DM_EINVAL, dm_exc_text(DM_EINVAL))

                    # Storage flag change
                    try:
                        node_storage = string_to_bool(props[FLAG_STORAGE])
                        if node_storage:
                            node.set_state_flags(DrbdNode.FLAG_STORAGE)
                        else:
                            node.clear_state_flags(DrbdNode.FLAG_STORAGE)
                        modified = True
                    except (KeyError, ValueError):
                        pass

                    # Standby flag change
                    try:
                        node_standby = string_to_bool(props[FLAG_STANDBY])
                        if node_standby:
                            node.set_state_flags(DrbdNode.FLAG_STANDBY)
                        else:
                            node.clear_state_flags(DrbdNode.FLAG_STANDBY)
                        modified = True
                    except (KeyError, ValueError):
                        pass

                    if modified:
                        for assg in node.iterate_assignments():
                            assg.update_config()
                        if addr_changed:
                            for dm_node in self._nodes.itervalues():
                                dm_node_state = dm_node.get_state()
                                if is_set(dm_node_state, DrbdNode.FLAG_DRBDCTRL):
                                    dm_node.set_state_flags(DrbdNode.FLAG_UPDATE)
                        self.schedule_run_changes()
                    self.save_conf_data(persist)
                else:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except ValueError:
            # Return code set earlier
            pass
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def modify_resource(self, res_name, serial, props):
        """
        Modifies resource properties

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc = []
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                resource = self.get_resource(res_name)
                if resource is not None:
                    try:
                        port = int(props[RES_PORT])
                        if self.is_free_port_nr(port):
                            try:
                                resource.set_port(port)
                                for assg in resource.iterate_assignments():
                                    assg.update_config()
                                self.schedule_run_changes()
                            except ValueError as val_err:
                                add_rc_entry(fn_rc, DM_EINVAL, dm_exc_text(DM_EINVAL))
                                raise val_err
                        else:
                            add_rc_entry(fn_rc, DM_EEXIST, dm_exc_text(DM_EEXIST))
                            raise ValueError
                    except KeyError:
                        pass
                    # Merge only auxiliary properties into the
                    # DrbdResource's properties container
                    aux_props = aux_props_selector(props)
                    resource.get_props().merge_gen(aux_props)
                    self.save_conf_data(persist)
                else:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except ValueError:
            # Return code set earlier
            pass
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def modify_volume(self, res_name, vol_id, serial, props):
        """
        Modifies volume properties

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc = []
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                volume = None
                resource = self.get_resource(res_name)
                if resource is not None:
                    volume = resource.get_volume(vol_id)
                if volume is not None:
                    try:
                        minor_nr = int(props[VOL_MINOR])
                        if self.is_free_minor_nr(minor_nr):
                            try:
                                minor = MinorNr(minor_nr)
                                volume.set_minor(minor)
                                for assg in resource.iterate_assignments():
                                    assg.update_config()
                                self.schedule_run_changes()
                            except InvalidMinorNrException:
                                add_rc_entry(fn_rc, DM_EINVAL, dm_exc_text(DM_EINVAL))
                                raise ValueError
                        else:
                            add_rc_entry(fn_rc, DM_EEXIST, dm_exc_text(DM_EEXIST))
                            raise ValueError
                    except KeyError:
                        pass
                    # Merge only auxiliary properties into the
                    # DrbdVolume's properties container
                    aux_props = aux_props_selector(props)
                    volume.get_props().merge_gen(aux_props)
                    self.save_conf_data(persist)
                else:
                    # Volume not found
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except ValueError:
            # Return code set earlier
            pass
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def modify_assignment(self, res_name, node_name, serial, props):
        """
        Modifies assignment properties

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc = []
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                state_float = 0
                state_set   = 1
                state_unset = -1

                overwrite = state_float
                discard   = state_float

                resource = None
                try:
                    resource = self._resources[res_name]
                except ValueError as not_found:
                    add_rc_entry(fn_rc, DM_ENOENT, "Resource '%(resource)' not found", [["resource", res_name]])
                    raise not_found

                node = None
                try:
                    node = self._nodes[node_name]
                except ValueError as not_found:
                    add_rc_entry(fn_rc, DM_ENOENT, "Node '%(node)' not found", [["node", node_name]])
                    raise not_found

                assignment = node.get_assignment(resource.get_name())
                if assignment is None:
                    add_rc_entry(fn_rc, DM_ENOENT, "Resource '%(resource)' is not assigned to node '%(node)'",
                                 [["resource", res_name], ["node", node_name]])
                    raise ValueError

                for key, value in props.iteritems():
                    # currently, all values are booleans
                    flag = self._get_props_bool(key, value, fn_rc)
                    if key == FLAG_OVERWRITE:
                        if flag:
                            overwrite = state_set
                        else:
                            overwrite = state_unset
                    elif key == FLAG_DISCARD:
                        if flag:
                            discard = state_set
                        else:
                            discard = state_unset
                    else:
                        add_rc_entry(fn_rc, DM_EINVAL, "Invalid property name '%(key)' ignored", [["key", key]])

                    if overwrite == state_set:
                        if discard == state_set:
                            add_rc_entry(fn_rc, DM_EINVAL,
                                         "Conflicting flags '" + FLAG_OVERWRITE + "' and '" +
                                         FLAG_DISCARD + "'")
                            raise ValueError
                        assignment.set_tstate_flags(Assignment.FLAG_OVERWRITE)
                    elif overwrite == state_unset or discard == state_set:
                        assignment.clear_tstate_flags(Assignment.FLAG_OVERWRITE)

                    if discard == state_set:
                        assignment.set_tstate_flags(Assignment.FLAG_DISCARD)
                    elif discard == state_unset or overwrite == state_set:
                        assignment.clear_tstate_flags(Assignment.FLAG_DISCARD)

                    self.save_conf_data(persist)
            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except ValueError:
            # Return code set earlier
            pass
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    def _get_props_bool(self, key, value, fn_rc):
        flag = False
        try:
            flag = string_to_bool(value)
        except ValueError as value_error:
            add_rc_entry(fn_rc, DM_EINVAL, "Invalid value '%(value)' for boolean type argument '%(key)'",
                         [["value", value], ["key", key]])
            raise value_error
        return flag

    @no_satellite
    def resize_volume(self, res_name, vol_id, serial, size_kiB, delta_kiB):
        """
        Resizes a volume

        @return: standard return code defined in drbdmanage.exceptions
        """
        # FIXME: The volume should probably have a property that indicates a resize
        #        operation that is in progress, so that it locks out new resize
        #        commands while a previous one is still in progress
        #        That property would have to be cleaned up as soon as the resize
        #        operations are finished, or if the assignments that had the pending
        #        resize operation are removed
        fn_rc = []
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                resource = self._resources[res_name]
                volume = resource.get_volume(vol_id)

                resource_assigned = resource.has_assignments()

                if volume is None:
                    raise KeyError
                if size_kiB == 0 and delta_kiB == 0:
                    add_rc_entry(fn_rc, DM_EINVAL, "Either size_kiB or delta_kiB must be non-zero")
                    raise ValueError
                if size_kiB < 0:
                    add_rc_entry(fn_rc, DM_EINVAL, "The value of size_kiB must be a positive number or zero")
                    raise ValueError
                if delta_kiB < 0:
                    add_rc_entry(fn_rc, DM_EINVAL, "The value of delta_kiB must be a positive number or zero")
                    raise ValueError
                if size_kiB > 0 and delta_kiB > 0:
                    add_rc_entry(
                        fn_rc, DM_EINVAL,
                        "Only one of size_kiB and delta_kiB may be set to a non-zero value"
                    )
                    raise ValueError
                cur_size_kiB = volume.get_size_kiB()
                if size_kiB > 0:
                    if size_kiB <= cur_size_kiB and resource_assigned:
                        add_rc_entry(
                            fn_rc, DM_EINVAL,
                            "The new size of the volume must be greater than its current size"
                        )
                        raise ValueError
                if delta_kiB > 0:
                    size_kiB = cur_size_kiB + delta_kiB

                if resource_assigned:
                    # Check space on all nodes that have the resource assigned
                    # Calculate the gross size of the space required for resizing
                    # to the specified net size
                    max_peers = self.DEFAULT_MAX_PEERS
                    try:
                        max_peers = int(
                            self.get_conf_value(self.KEY_MAX_PEERS)
                        )
                    except ValueError:
                        # Unparseable configuration entry;
                        # no-op: use default value instead
                        pass
                    gross_size_kiB = md.MetaData.get_gross_kiB(
                        size_kiB, max_peers,
                        md.MetaData.DEFAULT_AL_STRIPES,
                        md.MetaData.DEFAULT_AL_kiB
                    )
                    for assignment in resource.iterate_assignments():
                        if is_unset(assignment.get_cstate(), Assignment.FLAG_DISKLESS):
                            node = assignment.get_node()
                            poolfree = node.get_poolfree()
                            if poolfree >= 0:
                                if gross_size_kiB > poolfree:
                                    add_rc_entry(fn_rc, DM_ENOSPC, dm_exc_text(DM_ENOSPC))
                                    raise ValueError

                    # Set the resize parameters
                    resource.begin_resize(vol_id, size_kiB)
                else:
                    # If the resource has no assignments, just change the volume size
                    volume.set_size_kiB(size_kiB)
                self.save_conf_data(persist)
                self.schedule_run_changes()
            else:
                raise PersistenceException
        except KeyError:
            add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
        except ValueError:
            # Error code set by caller
            pass
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def remove_resource(self, res_name, force):
        """
        Marks a resource for removal from the DRBD cluster
        * Orders all nodes to undeploy all volume of this resource

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc = []
        persist  = None
        resource = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                resource = self._resources[res_name]
                if (not force) and resource.has_assignments():
                    for assg in resource.iterate_assignments():
                        assg.undeploy()
                    resource.remove()
                    self.schedule_run_changes()
                else:
                    for assg in resource.iterate_assignments():
                        node = assg.get_node()
                        node.remove_assignment(assg)
                    del self._resources[resource.get_name()]
                self.get_serial()
                self.save_conf_data(persist)
            else:
                raise PersistenceException
        except KeyError:
            add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def create_volume(self, res_name, size_kiB, props):
        """
        Adds a volume to a resource

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc   = []
        fn_info = []
        volume  = None
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                resource = self._resources.get(res_name)
                if resource is None:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
                else:
                    minor = MinorNr.MINOR_NR_AUTO
                    try:
                        minor = int(props[VOL_MINOR])
                    except KeyError:
                        pass
                    except ValueError:
                        raise InvalidMinorNrException
                    if minor == MinorNr.MINOR_NR_AUTO:
                        occupied_minor_nrs = self.get_occupied_minor_nrs()
                        if occupied_minor_nrs is not None:
                            minor = self.get_free_minor_nr(occupied_minor_nrs)
                        else:
                            raise InvalidMinorNrException
                    if minor == MinorNr.MINOR_NR_ERROR:
                        raise InvalidMinorNrException
                    vol_id = self.get_free_volume_id(resource)
                    if vol_id == -1:
                        add_rc_entry(fn_rc, DM_EVOLID, dm_exc_text(DM_EVOLID))
                    else:
                        chg_serial = self.get_serial()
                        volume = None
                        try:
                            volume = DrbdVolume(vol_id, size_kiB, MinorNr(minor),
                                                0, self.get_serial, None, None)
                        except ValueError:
                            # Maximum number of volumes per resource exceeded
                            add_rc_entry(fn_rc, DM_EVOLID, dm_exc_text(DM_EVOLID))
                        if volume is not None:
                            # Merge only auxiliary properties into the
                            # DrbdVolume's properties container
                            aux_props = aux_props_selector(props)
                            volume.get_props().merge_gen(aux_props)
                            resource.add_volume(volume)
                            for assg in resource.iterate_assignments():
                                assg.update_volume_states(chg_serial)
                                vol_st = assg.get_volume_state(volume.get_id())
                                if vol_st is not None:
                                    vol_st.deploy()
                                    vol_st.attach()
                            self.schedule_run_changes()
                            add_rc_entry(fn_info, DM_INFO, 'create_volume',
                                         [[VOL_MINOR, str(minor)],
                                          [VOL_ID, str(vol_id)],
                                          [SERIAL, str(chg_serial)]])
                        self.save_conf_data(persist)
            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc + fn_info

    @no_satellite
    def remove_volume(self, res_name, vol_id, force):
        """
        Marks a volume for removal from the DRBD cluster
        * Orders all nodes to undeploy the volume

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc   = []
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                resource = self._resources[res_name]
                volume   = resource.get_volume(vol_id)
                if volume is None:
                    raise KeyError
                else:
                    if (not force) and resource.has_assignments():
                        for assg in resource.iterate_assignments():
                            peer_vol_st = assg.get_volume_state(vol_id)
                            if peer_vol_st is not None:
                                peer_vol_st.undeploy()
                        volume.remove()
                        self.schedule_run_changes()
                    else:
                        resource.remove_volume(vol_id)
                        for assg in resource.iterate_assignments():
                            assg.remove_volume_state(vol_id)
                    self.get_serial()
                    self.save_conf_data(persist)
            else:
                raise PersistenceException
        except KeyError:
            add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def assign(self, node_name, res_name, props):
        """
        Assigns a resource to a node
        * Orders all participating nodes to deploy all volumes of
          resource

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc   = []
        persist = None
        try:
            tstate = Assignment.FLAG_DEPLOY
            cstate = 0

            # Set flags from props
            flag_overwrite = False
            flag_diskless  = False
            flag_connect   = True
            flag_discard   = False
            try:
                flag_overwrite = string_to_bool(props[FLAG_OVERWRITE])
            except (KeyError, TypeError):
                pass
            try:
                flag_diskless  = string_to_bool(props[FLAG_DISKLESS])
            except (KeyError, TypeError):
                pass
            try:
                flag_connect   = string_to_bool(props[FLAG_CONNECT])
            except (KeyError, TypeError):
                pass
            try:
                flag_discard   = string_to_bool(props[FLAG_DISCARD])
            except (KeyError, TypeError):
                pass

            persist = self.begin_modify_conf()
            if persist is not None:
                node     = self._nodes.get(node_name)
                resource = self._resources.get(res_name)
                if node is None or resource is None:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
                else:
                    assignment = node.get_assignment(resource.get_name())
                    if assignment is not None:
                        add_rc_entry(fn_rc, DM_EEXIST, dm_exc_text(DM_EEXIST))
                    else:
                        # check conflicting flags
                        if (flag_overwrite and flag_diskless):
                            add_rc_entry(
                                fn_rc, DM_EINVAL, dm_exc_text(DM_EINVAL)
                            )
                        elif (flag_overwrite and flag_discard):
                            add_rc_entry(
                                fn_rc, DM_EINVAL, dm_exc_text(DM_EINVAL)
                            )
                        else:
                            # If the overwrite flag is set on this
                            # assignment, turn it off on all the assignments
                            # to other nodes
                            if flag_overwrite:
                                for assg in resource.iterate_assignments():
                                    assg.clear_tstate_flags(
                                        Assignment.FLAG_OVERWRITE
                                    )
                            tstate = (
                                tstate |
                                (Assignment.FLAG_OVERWRITE if flag_overwrite
                                    else 0) |
                                (Assignment.FLAG_DISCARD   if flag_discard
                                    else 0) |
                                (Assignment.FLAG_CONNECT   if flag_connect
                                    else 0) |
                                (Assignment.FLAG_DISKLESS  if flag_diskless
                                    else 0)
                            )
                            assign_rc = (
                                self._assign(node, resource, cstate, tstate,
                                             DrbdNode.NODE_ID_NONE)
                            )
                            if assign_rc == DM_SUCCESS:
                                assignment = node.get_assignment(
                                    resource.get_name()
                                )
                                aux_props = aux_props_selector(props)
                                assignment.get_props().merge_gen(aux_props)
                                self.save_conf_data(persist)
                                self.schedule_run_changes()
                            else:
                                add_rc_entry(
                                    fn_rc, assign_rc, dm_exc_text(assign_rc)
                                )
            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except ValueError:
            add_rc_entry(fn_rc, DM_EINVAL, dm_exc_text(DM_EINVAL))
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def unassign(self, node_name, res_name, force):
        """
        Removes the assignment of a resource to a node
        * Orders the node to undeploy all volumes of the resource

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc   = []
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node = self._nodes.get(node_name)
                if node is not None:
                    if res_name.lower() == RES_ALL_KEYWORD:
                        # Build a temporary list of assignments
                        assg_list = []
                        for assignment in node.iterate_assignments():
                            assg_list.append(assignment)
                        # Iterate over the temporary list of assignments,
                        # because the node's assignment map may change during
                        # iteration if the 'force' flag is set
                        for assignment in assg_list:
                            sub_rc = self._unassign(assignment, force)
                            if sub_rc != DM_SUCCESS:
                                resource = assignment.get_resource()
                                add_rc_entry(fn_rc, sub_rc, dm_exc_text(sub_rc),
                                             [["resource", resource.get_name()]])
                        self.save_conf_data(persist)
                        self.schedule_run_changes()
                    else:
                        resource = self._resources.get(res_name)
                        if resource is not None:
                            assignment = node.get_assignment(resource.get_name())
                            if assignment is None:
                                add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
                            else:
                                sub_rc = self._unassign(assignment, force)
                                if sub_rc != DM_SUCCESS:
                                    add_rc_entry(fn_rc, sub_rc, dm_exc_text(sub_rc))
                                self.save_conf_data(persist)
                                self.schedule_run_changes()
                        else:
                            add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT),
                                         [ [ RES_NAME, res_name ] ])
                else:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT),
                                 [ [ NODE_NAME, node_name ] ])
            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc


    def _assign(self, node, resource, cstate, tstate, node_id):
        """
        Implementation - see assign()

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc = DM_DEBUG
        try:
            self.get_serial()

            # If no node id is selected for this assignment, then attempt
            # to find a free one
            if node_id == DrbdNode.NODE_ID_NONE:
                node_id = self.get_free_node_id(resource)

            if node_id == DrbdNode.NODE_ID_NONE:
                # no free node ids
                fn_rc = DM_ENODEID
            else:

                # If that node does not have its own storage,
                # deploy a DRBD client (diskless)
                if is_unset(node.get_state(), DrbdNode.FLAG_STORAGE):
                    tstate = tstate | Assignment.FLAG_DISKLESS

                # Create the assignment object
                assignment = Assignment(node, resource, node_id,
                                        cstate, tstate, 0, None,
                                        self.get_serial, None, None)
                # Create the signal for this assignment
                assg_signal = self.create_signal(
                    "assignments/" + node.get_name() +
                    "/" + resource.get_name()
                )
                assignment.set_signal(assg_signal)
                for vol_state in assignment.iterate_volume_states():
                    vol_state.deploy()
                    if is_unset(tstate, Assignment.FLAG_DISKLESS):
                        vol_state.attach()
                node.add_assignment(assignment)
                resource.add_assignment(assignment)

                # If any of the volumes need to be resized, mark
                # the volume state for resizing
                vol_id_list = resource.get_resizing_vol_id_list()
                for vol_id in vol_id_list:
                    vol_state = assignment.get_volume_state(vol_id)
                    vol_state.begin_resize()

                # Flag all existing assignments for an update of the
                # assignment's network connections
                for assignment in resource.iterate_assignments():
                    if assignment.is_deployed():
                        assignment.update_connections()

                fn_rc = DM_SUCCESS
        except Exception as exc:
            self.catch_internal_error(exc)
        return fn_rc


    def _unassign(self, assignment, force):
        """
        Implementation - see unassign()

        @return: standard return code defined in drbdmanage.exceptions
        """
        try:
            self.get_serial()
            node = assignment.get_node()
            resource = assignment.get_resource()
            if (not force) and assignment.is_deployed():
                assignment.disconnect()
                assignment.undeploy()
            else:
                assignment.notify_removed()
                assignment.remove()
            for assignment in resource.iterate_assignments():
                if (assignment.get_node() != node and
                    assignment.is_deployed()):
                        assignment.update_connections()
            self.cleanup()
        except Exception as exc:
            self.catch_internal_error(exc)
            return DM_DEBUG
        return DM_SUCCESS


    def cluster_free_query(self, redundancy):
        """
        Determines the maximum size of an n-times redundantly deployed volume
        """
        fn_rc = []
        # Default of 0 if the free space is unknown
        # TODO: There should be a distinction between the two cases:
        #       1) there is no free space
        #       2) there are too few nodes that have a known poolfree size
        #          to determine whether there is any free space
        free_space = 0
        total_space = reduce(lambda x, y: x+y,
                             map(lambda n: max(0, n.get_poolsize()),
                                 self._nodes.values()),
                             0)
        try:
            if redundancy >= 1:
                if redundancy <= len(self._nodes):
                    # Select nodes where the amount of free space on
                    # that node is known
                    selected = []
                    for node in self._nodes.itervalues():
                        if is_set(node.get_state(), DrbdNode.FLAG_STORAGE):
                            poolfree = node.get_poolfree()
                            if poolfree != -1:
                                selected.append(node)

                    # Sort by free space
                    selected = sorted(
                        selected,
                        key=lambda node: node.get_poolfree(), reverse=True
                    )
                    if len(selected) >= redundancy:
                        node = selected[redundancy - 1]
                        gross_free = node.get_poolfree()
                        max_peers = self.DEFAULT_MAX_PEERS
                        try:
                            max_peers = int(
                                self.get_conf_value(self.KEY_MAX_PEERS)
                            )
                        except ValueError:
                            # Unparseable configuration value;
                            # no-op: keep default value
                            pass
                        try:
                            free_space = md.MetaData.get_net_kiB(
                                gross_free, max_peers,
                                md.MetaData.DEFAULT_AL_STRIPES,
                                md.MetaData.DEFAULT_AL_kiB
                            )
                        except md.MetaDataException as md_exc:
                            add_rc_entry(
                                fn_rc, DM_EINVAL,
                                md_exc.message
                            )
                            logging.debug("cluster_free_query(): MetaDataException: " + md_exc.message)
                else:
                    # requested redundancy exceeds the
                    # number of nodes in the cluster
                    add_rc_entry(fn_rc, DM_ENODECNT, dm_exc_text(DM_ENODECNT))
            else:
                # requested a redundancy of less than 1, which is an invalid
                # number of nodes
                add_rc_entry(fn_rc, DM_EINVAL, dm_exc_text(DM_EINVAL))
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc, free_space, total_space

    @no_satellite
    def auto_deploy(self, res_name, count, delta, site_clients):
        """
        Deploys a resource to a number of nodes

        The selected resource is deployed to a number of nodes, either by
        initially deploying the resource, or by deploying the resource
        on additional nodes or undeploying the resource from nodes where it
        is currently deployed, until the number of nodes where the resource
        is deployed either:
            - matches count, if the supplied count value is non-zero
        or
            - has been changed by delta, if the supplied delta value
              is non-zero
        If both supplied values are non-zero, then the operation is aborted
        due to potentially conflicting information, and DM_EINVAL is added to
        the list of return codes.

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc   = []
        persist = None
        try:
            save_changes = False
            if ((count == 0 and delta == 0) or count < 0):
                add_rc_entry(
                    fn_rc, DM_EINVAL,
                    "auto_deploy: Count (%(c)d) must be positive, or if 0, then delta (%(d)d) must be != 0",
                    [["c", count], ["d", delta]]
                )
                raise ValueError
            elif (count != 0 and delta != 0):
                add_rc_entry(
                    fn_rc, DM_EINVAL,
                    "auto_deploy: Only one of count (%(c)d) or delta (%(d)d) may be set to a non-zero value",
                    [["c", count], ["d", delta]]
                )
                raise ValueError
            else:
                deployer = self._pluginmgr.get_plugin_instance(
                    self.get_conf_value(self.KEY_DEPLOYER_NAME)
                )
                if deployer is None:
                    raise PluginException

                persist  = self.begin_modify_conf()
                if persist is None:
                    raise PersistenceException

                maxnodes = self.DEFAULT_MAX_NODE_ID
                try:
                    maxnodes = int(self._conf[self.KEY_MAX_NODE_ID]) + 1
                except ValueError:
                    pass
                crtnodes = len(self._nodes)
                maxcount = maxnodes if maxnodes < crtnodes else crtnodes
                resource = self._resources[res_name]
                assigned_count = resource.assigned_count()

                # Calculate target node count
                if delta != 0:
                    final_count = assigned_count + delta
                    if final_count <= 0:
                        add_rc_entry(
                            fn_rc, DM_EINVAL,
                            "auto_deploy: Final count %(fin)d less than 1: assigned %(ass)d + delta %(delta)d",
                            [["ass", assigned_count], ["delta", delta], ["fin", final_count]]
                        )
                        raise ValueError
                else:
                    final_count = count


                # Try to achieve it
                if final_count > maxcount:
                    add_rc_entry(fn_rc, DM_ENODECNT, dm_exc_text(DM_ENODECNT))
                    raise ValueError

                elif final_count <= 0:
                    add_rc_entry(
                        fn_rc, DM_EINVAL,
                        "auto_deploy: Final count %(fin)d <= 0",
                        [["fin", final_count]]
                    )
                    raise ValueError

                elif final_count > assigned_count:
                    # ========================================
                    # DEPLOY / EXTEND
                    # ========================================
                    # FIXME: extend does nothing for some unknown reason,
                    #        but succeeds (exit code = 0)
                    """
                    calculate the amount of memory required to deploy all
                    volumes of the resource
                    """
                    size_sum = 0
                    max_peers = self.DEFAULT_MAX_PEERS
                    try:
                        max_peers = int(
                            self.get_conf_value(self.KEY_MAX_PEERS)
                        )
                    except ValueError:
                        # Unparseable configuration entry;
                        # no-op: use default value instead
                        pass
                    for vol in resource.iterate_volumes():
                        # Calculate required gross space for a volume
                        # with the specified net space
                        try:
                            size_sum += md.MetaData.get_gross_kiB(
                                vol.get_size_kiB(), max_peers,
                                md.MetaData.DEFAULT_AL_STRIPES,
                                md.MetaData.DEFAULT_AL_kiB
                            )
                        except md.MetaDataException as md_exc:
                            add_rc_entry(
                                fn_rc, DM_EINVAL,
                                md_exc.message
                            )
                            logging.debug("auto_deploy(): MetaDataException: " + md_exc.message)
                            raise ValueError
                    """
                    filter nodes that do not have the resource deployed yet
                    """
                    undeployed = {}
                    for node in self._nodes.itervalues():
                        # skip nodes, where:
                        #   - resource is deployed already
                        #   - resource is being deployed
                        #   - resource is being undeployed
                        #   - node does not have its own storage
                        #     (diskless/client assignments only)
                        if (is_set(node.get_state(), DrbdNode.FLAG_STORAGE) and
                            resource.get_assignment(node.get_name()) is None):
                            # Node has its own storage, but the resource is not
                            # deployed on it; add it to the list of candidates
                            undeployed[node.get_name()] = node
                    """
                    Call the deployer plugin to select nodes for deploying
                    the resource
                    """
                    diff = final_count - assigned_count
                    selected = []
                    sub_rc = deployer.deploy_select(
                        undeployed, selected,
                        diff, size_sum, True
                    )
                    if sub_rc == DM_SUCCESS:
                        for node in selected:
                            self._assign(
                                node, resource,
                                0,
                                Assignment.FLAG_DEPLOY |
                                Assignment.FLAG_CONNECT,
                                DrbdNode.NODE_ID_NONE
                            )
                        save_changes = True
                    else:
                        add_rc_entry(fn_rc, sub_rc, dm_exc_text(sub_rc))
                        raise DeployerException

                elif final_count < assigned_count:
                    # ========================================
                    # REDUCE
                    # ========================================
                    ctr = assigned_count
                    # If there are assignments that are waiting for
                    # deployment, but do not have the resource deployed
                    # yet, undeploy those first
                    if ctr > final_count:
                        for assg in resource.iterate_assignments():
                            if (is_set(assg.get_tstate(),
                                Assignment.FLAG_DEPLOY) and
                                is_unset(assg.get_cstate(),
                                Assignment.FLAG_DEPLOY)):
                                    assg.undeploy()
                                    ctr -= 1
                            if not ctr > final_count:
                                break
                    if ctr > final_count:
                        # Undeploy from nodes that have the
                        # resource deployed, or should have the resource
                        # deployed (target state)
                        # Collect nodes where the resource is deployed
                        deployed = {}
                        for assg in resource.iterate_assignments():
                            if (is_set(
                                    assg.get_tstate(),
                                    Assignment.FLAG_DEPLOY
                                ) and
                                is_unset(
                                    assg.get_tstate(),
                                    Assignment.FLAG_DISKLESS
                                )):
                                    node = assg.get_node()
                                    deployed[node.get_name()] = node
                        """
                        Call the deployer plugin to select nodes for
                        undeployment of the resource
                        """
                        diff = ctr - final_count
                        selected = []
                        deployer.undeploy_select(
                            deployed, selected,
                            diff, True
                        )
                        for node in selected:
                            assg = node.get_assignment(resource.get_name())
                            if site_clients:
                                # turn the node into a client
                                assg.deploy_client()
                            else:
                                self._unassign(assg, False)
                    save_changes = True

            # condition (final_count == assigned_count) is successful, too

            if site_clients:
                # turn all remaining nodes into clients
                if self._site_clients(resource, None):
                    save_changes = True

            if save_changes:
                self.save_conf_data(persist)
                self.schedule_run_changes()
        except KeyError:
            add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
        except (ValueError, DeployerException):
            # add_rc_entry() error message set by exception generator
            pass
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def auto_undeploy(self, res_name, force):
        """
        Undeploys a resource from all nodes

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc   = []
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is None:
                raise PersistenceException

            resource = self._resources[res_name]
            removable = []
            for assg in resource.iterate_assignments():
                if (not force) and assg.is_deployed():
                    assg.disconnect()
                    assg.undeploy()
                else:
                    removable.append(assg)
            for assg in removable:
                assg.remove()
            self.save_conf_data(persist)
            self.schedule_run_changes()
        except KeyError:
            add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc


    def _site_clients(self, resource, site):
        """
        Turn all nodes that do replicate a resource into clients
        """
        assg_changed = False
        for node in self._nodes.itervalues():
            assg = node.get_assignment(resource.get_name())
            if assg is None:
                assg_changed = True
                self._assign(
                    node, resource,
                    0,
                    Assignment.FLAG_DEPLOY | Assignment.FLAG_CONNECT |
                    Assignment.FLAG_DISKLESS,
                    DrbdNode.NODE_ID_NONE
                )
            else:
                tstate = assg.get_tstate()
                if is_unset(tstate, Assignment.FLAG_DEPLOY):
                    assg_changed = True
                    assg.deploy_client()
        return assg_changed

    @no_satellite
    def modify_state(self, node_name, res_name,
      cstate_clear_mask, cstate_set_mask, tstate_clear_mask, tstate_set_mask):
        """
        Modifies the tstate (target state) of an assignment

        @return: standard return code defined in drbdmanage.exceptions
        """
        # FIXME: function is now modify_assignment, new signature
        fn_rc   = []
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node = self._nodes.get(node_name)
                if node is None:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
                else:
                    assg = node.get_assignment(res_name)
                    if assg is None:
                        add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
                    else:
                        # OVERWRITE overrides DISCARD
                        if is_set(tstate_set_mask, Assignment.FLAG_OVERWRITE):
                            tstate_clear_mask |= Assignment.FLAG_DISCARD
                            tstate_set_mask = (
                                (tstate_set_mask | Assignment.FLAG_DISCARD) ^
                                Assignment.FLAG_DISCARD
                            )
                        elif is_set(tstate_set_mask, Assignment.FLAG_DISCARD):
                            tstate_clear_mask |= Assignment.FLAG_OVERWRITE
                        assg.clear_cstate_flags(cstate_clear_mask)
                        assg.set_cstate_flags(cstate_set_mask)
                        assg.clear_tstate_flags(tstate_clear_mask)
                        assg.set_tstate_flags(tstate_set_mask)
                        # Upon setting the OVERWRITE flag on this assignment,
                        # clear it on all other assignments
                        if is_set(tstate_set_mask, Assignment.FLAG_OVERWRITE):
                            resource = assg.get_resource()
                            for peer_assg in resource.iterate_assignments():
                                if peer_assg != assg:
                                    peer_assg.clear_tstate_flags(
                                        Assignment.FLAG_OVERWRITE
                                    )
                        self.save_conf_data(persist)
                        self.schedule_run_changes()
            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc


    # TODO: should possibly specify connections between specific nodes
    @no_satellite
    def connect(self, node_name, res_name, reconnect):
        """
        Sets the CONNECT or RECONNECT flag on a resource's target state

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc    = []
        node     = None
        resource = None
        persist  = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node     = self._nodes.get(node_name)
                resource = self._resources.get(res_name)
                if node is None or resource is None:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
                else:
                    assignment = node.get_assignment(resource.get_name())
                    if assignment is None:
                        add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
                    else:
                        if reconnect:
                            assignment.reconnect()
                        else:
                            assignment.connect()
                        self.save_conf_data(persist)
                        self.schedule_run_changes()
            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def disconnect(self, node_name, res_name, force):
        """
        Clears the CONNECT flag on a resource's target state

        @return: standard return code defined in drbdmanage.exceptions
        """
        # FIXME: what does 'force' do?
        fn_rc    = []
        node     = None
        resource = None
        persist  = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node     = self._nodes.get(node_name)
                resource = self._resources.get(res_name)
                if node is None or resource is None:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
                else:
                    assignment = node.get_assignment(resource.get_name())
                    if assignment is None:
                        add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
                    else:
                        assignment.disconnect()
                        self.save_conf_data(persist)
                        self.schedule_run_changes()
            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def attach(self, node_name, res_name, vol_id):
        """
        Sets the ATTACH flag on a volume's target state

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc     = []
        node      = None
        resource  = None
        vol_state = None
        persist   = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node     = self._nodes.get(node_name)
                resource = self._resources.get(res_name)
                if node is None or resource is None:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
                else:
                    assignment = node.get_assignment(resource.get_name())
                    if assignment is None:
                        add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
                    else:
                        vol_state = assignment.get_volume_state(vol_id)
                        if vol_state is None:
                            add_rc_entry(fn_rc, DM_ENOENT,
                                         dm_exc_text(DM_ENOENT))
                        else:
                            vol_state.attach()
                            self.save_conf_data(persist)
                            self.schedule_run_changes()
                            add_rc_entry(fn_rc, DM_SUCCESS,
                                         dm_exc_text(DM_SUCCESS))
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc ) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def detach(self, node_name, res_name, vol_id):
        """
        Clears the ATTACH flag on a volume's target state

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc     = []
        node      = None
        resource  = None
        vol_state = None
        persist   = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                node     = self._nodes.get(node_name)
                resource = self._resources.get(res_name)
                if node is None or resource is None:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
                else:
                    assignment = node.get_assignment(resource.get_name())
                    if assignment is None:
                        add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
                    else:
                        vol_state = assignment.get_volume_state(vol_id)
                        if vol_state is None:
                            add_rc_entry(fn_rc, DM_ENOENT,
                                         dm_exc_text(DM_ENOENT))
                        else:
                            vol_state.detach()
                            self.save_conf_data(persist)
                            self.schedule_run_changes()
            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def update_pool_check(self):
        """
        Checks storage pool data and if necessary, updates the data
        """
        fn_rc = []
        try:
            inst_node = self.get_instance_node()
            if inst_node is not None:
                if is_set(inst_node.get_state(), DrbdNode.FLAG_STORAGE):
                    (stor_rc, poolsize, poolfree) = (
                        self._bd_mgr.update_pool(inst_node)
                    )
                    if stor_rc == DM_SUCCESS:
                        poolfree = self._pool_free_correction(
                            inst_node, poolfree
                        )
                        if (inst_node.get_poolsize() != poolsize or
                            inst_node.get_poolfree() != poolfree):
                                fn_rc = self.update_pool(
                                    [ inst_node.get_name() ]
                                )
                    else:
                        add_rc_entry(fn_rc, DM_ESTORAGE, dm_exc_text(DM_ESTORAGE))
            else:
                add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
        except Exception as exc:
            self.catch_internal_error(exc)
            add_rc_entry(fn_rc, DM_DEBUG, dm_exc_text(DM_DEBUG))
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def update_pool(self, node_names):
        """
        Updates information about the current node's storage pool

        @return: standard return code defined in drbdmanage.exceptions
        free space
        """
        fn_rc = []
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                logging.info("updating storage pool information")
                sub_rc = self.update_pool_data()
                if sub_rc == DM_SUCCESS:
                    self.cleanup()
                    self.save_conf_data(persist)
                else:
                    add_rc_entry(fn_rc, sub_rc, dm_exc_text(sub_rc))
            else:
                raise PersistenceException
        except PersistenceException:
            logging.error("cannot save updated storage pool information")
            add_rc_entry(fn_rc, DM_EPERSIST, dm_exc_text(DM_EPERSIST))
        except QuorumException:
            add_rc_entry(fn_rc, DM_EQUORUM, dm_exc_text(DM_EQUORUM))
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc


    def update_pool_data(self):
        """
        Updates information about the current node's storage pools

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc = DM_ESTORAGE
        try:
            inst_node = self.get_instance_node()
            if inst_node is not None:
                if is_set(inst_node.get_state(), DrbdNode.FLAG_STORAGE):
                    (stor_rc, poolsize, poolfree) = (
                        self._bd_mgr.update_pool(inst_node)
                    )
                    if stor_rc == DM_SUCCESS:
                        poolfree = self._pool_free_correction(
                            inst_node, poolfree
                        )
                        inst_node.set_pool(poolsize, poolfree)
                    fn_rc = DM_SUCCESS
                else:
                    # Node without storage
                    inst_node.set_pool(0, 0)
                    fn_rc = DM_SUCCESS
            else:
                fn_rc = DM_ENOENT
        except Exception as exc:
            self.catch_internal_error(exc)
            fn_rc = DM_DEBUG
        return fn_rc


    def _pool_free_correction(self, node, poolfree_in):
        """
        Predicts remaining free storage space
        """
        max_peers = self.DEFAULT_MAX_PEERS
        try:
            max_peers = int(
                self.get_conf_value(self.KEY_MAX_PEERS)
            )
        except ValueError:
            # Unparseable configuration value;
            # no-op: keep default value
            pass
        size_sum = 0
        for assignment in node.iterate_assignments():
            size_sum += assignment.get_gross_size_kiB_correction(
                max_peers
            )
        poolfree_out = poolfree_in - size_sum
        # If something is seriously wrong with the storage sizes,
        # (e.g. more storage required for deploying all resources
        #  than there is available), the pool is considered full
        if poolfree_out < 0:
            poolfree_out = 0
        return poolfree_out


    def cleanup(self):
        """
        Removes entries of undeployed nodes, resources, volumes or their
        supporting data structures (volume state and assignment entries)

        @return: standard return code defined in drbdmanage.exceptions
        """
        try:
            S_FLAG_DEPLOY  = DrbdSnapshotAssignment.FLAG_DEPLOY
            VS_FLAG_DEPLOY = DrbdVolumeState.FLAG_DEPLOY
            A_FLAG_DEPLOY  = Assignment.FLAG_DEPLOY

            # Flag indicating whether to update the serial number or not
            update_serial = False

            for node in self._nodes.itervalues():
                for assg in node.iterate_assignments():
                    assg_tstate = assg.get_tstate()
                    removable = []
                    # delete snapshot assignments that have been undeployed
                    for snaps_assg in assg.iterate_snaps_assgs():
		        # check for existing block devices
                        #
                        # turn the DEPLOY flag on again for those snapshot
                        # volume states and snapshot assignments, that
                        # still have a block device
                        snaps_assg_bd_exists = False
                        for snaps_vol_state in snaps_assg.iterate_snaps_vol_states():
                            bd_exists = False
                            if snaps_vol_state.get_bd_name() is not None:
                                bd_exists = True
                                snaps_assg_bd_exists = True
                                snaps_vol_state.set_cstate_flags(
                                    DrbdSnapshotVolumeState.FLAG_DEPLOY
                                )
                        if snaps_assg_bd_exists:
                            snaps_assg.set_cstate_flags(S_FLAG_DEPLOY)
                        # collect snapshot assignments that can be removed
                        sa_cstate = snaps_assg.get_cstate()
                        sa_tstate = snaps_assg.get_tstate()
                        if (is_unset(sa_cstate, S_FLAG_DEPLOY) and
                            (is_unset(sa_tstate, S_FLAG_DEPLOY) or
                             is_unset(assg_tstate, A_FLAG_DEPLOY)) and
                             (not snaps_assg_bd_exists)):
                                removable.append(snaps_assg)
                    for snaps_assg in removable:
                        snaps_assg.notify_removed()
                        snaps_assg.remove()
                    if len(removable) > 0:
                        update_serial = True
                    # delete volume states of volumes that have been undeployed
                    # and cleanup extended actions (xact)
                    removable = []
                    assg_bd_exists = False
                    for vol_state in assg.iterate_volume_states():
                        vol_cstate = vol_state.get_cstate()
                        vol_tstate = vol_state.get_tstate()
                        # check for existing block devices
                        bd_exists = False
                        if vol_state.get_bd_name() is not None:
                            bd_exists = True
                            assg_bd_exists = True
                            vol_state.set_cstate_flags(VS_FLAG_DEPLOY)
                        # collect volume states that can be removed
                        if (is_unset(vol_cstate, VS_FLAG_DEPLOY) and
                            (is_unset(vol_tstate, VS_FLAG_DEPLOY) or
                             is_unset(assg_tstate, A_FLAG_DEPLOY)) and
                             (not bd_exists)):
                                removable.append(vol_state)
                        else:
                            # Cleanup the XACT flag if no more
                            # extended actions are pending
                            if is_set(vol_tstate, DrbdVolumeState.FLAG_XACT):
                                vol_state.cleanup_xact()
                    if assg_bd_exists:
                        assg.set_cstate_flags(A_FLAG_DEPLOY)
                    for vol_state in removable:
                        assg.remove_volume_state(vol_state.get_id())
                    if len(removable) > 0:
                        update_serial = True

            # remove snapshot registrations for non-existent snapshots
            # (those that do not have snapshot assignments anymore)
            removable = []
            for resource in self._resources.itervalues():
                for snapshot in resource.iterate_snapshots():
                    if not snapshot.has_snaps_assgs():
                        removable.append(snapshot)
            for snapshot in removable:
                snapshot.remove()
            if len(removable) > 0:
                update_serial = True

            # delete assignments that have been undeployed
            removable = []
            for node in self._nodes.itervalues():
                for assg in node.iterate_assignments():
                    tstate = assg.get_tstate()
                    cstate = assg.get_cstate()
                    if (is_unset(cstate, A_FLAG_DEPLOY) and
                        is_unset(tstate, A_FLAG_DEPLOY)):
                            if ((not assg.has_snapshots()) and
                                (not assg.has_volume_states())):
                                removable.append(assg)
            for assg in removable:
                assg.notify_removed()
                assg.remove()
            if len(removable) > 0:
                update_serial = True

            # delete nodes that are marked for removal and that do not
            # have assignments anymore
            removable = []
            drbdctrl_flag = False
            for node in self._nodes.itervalues():
                node_state = node.get_state()
                if is_set(node_state, DrbdNode.FLAG_REMOVE):
                    if not node.has_assignments() and not self.get_satellite_names(node):
                        removable.append(node)
                        if is_set(node_state, DrbdNode.FLAG_DRBDCTRL):
                            drbdctrl_flag = True
            for node in removable:
                del self._nodes[node.get_name()]
            if len(removable) > 0:
                update_serial = True
            # if nodes with a control volume have been removed, reconfigure the control volume
            if drbdctrl_flag:
                try:
                    self._configure_drbdctrl(False, None, None, None, None)
                    self._drbd_mgr.adjust_drbdctrl()
                except (IOError, OSError) as reconf_err:
                    logging.error(
                        "Cannot reconfigure the control volume, "
                        "error description is: %s"
                        % str(reconf_err)
                    )
                self._cluster_nodes_update()

            # delete resources that are marked for removal and that do not
            # have assignments any more
            removable = []
            for resource in self._resources.itervalues():
                res_state = resource.get_state()
                if is_set(res_state, DrbdResource.FLAG_REMOVE):
                    if not resource.has_assignments():
                        removable.append(resource)
            for resource in removable:
                del self._resources[resource.get_name()]
            if len(removable) > 0:
                update_serial = True

            # delete volumes that are marked for removal and that are not
            # deployed on any node
            for resource in self._resources.itervalues():
                removable = []
                # collect volumes marked for removal
                for volume in resource.iterate_volumes():
                    if is_set(volume.get_state(), DrbdVolume.FLAG_REMOVE):
                        has_vol_state = False
                        for assg in resource.iterate_assignments():
                            vol_state = assg.get_volume_state(volume.get_id())
                            if vol_state is not None:
                                has_vol_state = True
                                break
                        if not has_vol_state:
                            removable.append(volume)
                for volume in removable:
                    resource.remove_volume(volume.get_id())
                if len(removable) > 0:
                    update_serial = True

            if update_serial:
                self.get_serial()

        except Exception as exc:
            self.catch_internal_error(exc)
            return DM_DEBUG
        return DM_SUCCESS

    def _set_updflag(self, item):
        for assg in item.iterate_assignments():
            assg.set_tstate_flags(Assignment.FLAG_UPD_CONFIG)

    @no_satellite
    def set_drbdsetup_props(self, props):
        fn_rc = []
        item = None
        props_cont = None
        persist = None

        try:
            persist = self.begin_modify_conf()

            target = props.pop("target")
            otype = props.pop("type")
            target_name = props.pop(target, False)

            if target == "common" or target == "sites":
                item = self.get_common()
            elif target == "node":
                item = self.get_node(target_name)
            elif target == "resource":
                item = self.get_resource(target_name)
            elif target == "volume":
                res_name, vol_id = target_name.split('/')
                item = self.get_volume(res_name, int(vol_id))
                item_res = self.get_resource(res_name)

            if item is not None:
                props_cont = item.get_props()

            if props_cont is not None and persist is not None:
                for k, v in props.iteritems():
                    if target == "sites":
                        ns = PropsContainer.NAMESPACES[PropsContainer.KEY_SITES]
                        ns += "%s/%s/" % (target_name, otype)
                    else:
                        ns = PropsContainer.NAMESPACES[PropsContainer.KEY_SETUPOPT] + "%s/" % (otype)
                    if k.startswith('unset'):
                        props_cont.remove_prop(k[len('unset-'):], ns)
                    else:
                        props_cont.set_prop(k, v, ns)

                if target == "volume":
                    # props got set for 'item', which was the volume.
                    # now switch item to item_res, as the flag should be
                    # set on the resource.
                    item = item_res

                if target == "common":
                    for node in self._nodes.itervalues():
                        self._set_updflag(node)
                elif target == "sites":
                    sites = target_name.split(':')
                    for node in self._nodes.itervalues():
                        node_props_cont = node.get_props()
                        if node_props_cont:
                            ns = PropsContainer.NAMESPACES[PropsContainer.KEY_DMCONFIG]
                            site = node_props_cont.get_prop('site', ns)
                            if site in sites:
                                self._set_updflag(node)
                else:
                    self._set_updflag(item)
                    # assg.get_props().new_serial()

                self.save_conf_data(persist)
                self.schedule_run_changes()

            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
            return fn_rc
        except PersistenceException:
            logging.error("cannot save updated drdb setup options")
            add_rc_entry(fn_rc, DM_EPERSIST, dm_exc_text(DM_EPERSIST))
        except QuorumException:
            add_rc_entry(fn_rc, DM_EQUORUM, dm_exc_text(DM_EQUORUM))
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)

        return fn_rc

    def list_nodes(self, node_names, serial, filter_props, req_props):
        """
        Generates a list of node views suitable for serialized transfer

        Used by the drbdmanage client to display the node list
        """
        fn_rc = []

        def node_filter():
            for node_name in node_names:
                node = self._nodes.get(node_name)
                if node is None:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT),
                                 [ [ NODE_NAME, node_name ] ])
                else:
                    yield node

        try:
            node_list = []
            if node_names is not None and len(node_names) > 0:
                selected_nodes = node_filter()
            else:
                selected_nodes = self._nodes.itervalues()
            if serial > 0:
                selected_nodes = serial_filter(serial, selected_nodes)

            if filter_props is not None and len(filter_props) > 0:
                selected_nodes = props_filter(selected_nodes, filter_props)

            instance_node = self.get_instance_node()
            for node in selected_nodes:
                node_props = node.get_properties(req_props)
                # Indicate if a node with a control volume is not connected/replicating

                if node is not instance_node and is_set(node.get_state(), DrbdNode.FLAG_DRBDCTRL):
                    if not self._quorum.is_active_member_node(node.get_name()):
                        node_props[IND_NODE_OFFLINE] = BOOL_TRUE
                node_entry = [
                    node.get_name(),
                    node_props
                ]
                node_list.append(node_entry)
                add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
            return fn_rc, node_list
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)

        return fn_rc, None


    def list_resources(self, res_names, serial, filter_props, req_props):
        """
        Generates a list of resources views suitable for serialized transfer

        Used by the drbdmanage client to display the resources/volumes list
        """
        fn_rc = []

        def resource_filter(res_names):
            for res_name in res_names:
                res = self._resources.get(res_name)
                if res is None:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT),
                                 [ [ RES_NAME, res_name ] ])
                else:
                    yield res

        try:
            res_list = []
            if res_names is not None and len(res_names) > 0:
                selected_res = resource_filter(res_names)
            else:
                selected_res = self._resources.itervalues()
            if serial > 0:
                selected_res = serial_filter(serial, selected_res)

            if filter_props is not None and len(filter_props) > 0:
                selected_res = props_filter(selected_res, filter_props)

            for res in selected_res:
                res_entry = [
                    res.get_name(),
                    res.get_properties(req_props)
                ]
                res_list.append(res_entry)
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
            return fn_rc, res_list
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)

        return fn_rc, None


    def list_volumes(self, res_names, serial, filter_props, req_props):
        """
        Generates a list of resources views suitable for serialized transfer

        Used by the drbdmanage client to display the resources/volumes list
        """
        fn_rc = []

        def resource_filter(res_names):
            for res_name in res_names:
                res = self._resources.get(res_name)
                if res is None:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT),
                                 [ [ RES_NAME, res_name ] ])
                else:
                    yield res

        try:
            selected_res = self._resources.itervalues()
            if res_names is not None and len(res_names) > 0:
                selected_res = resource_filter(res_names)
            # TODO: serial filter on vols? or serial bubbled "up", so on res as a perf opt?
            if serial > 0:
                selected_res = serial_filter(serial, selected_res)

            props_filter_flag = True if filter_props is not None and len(filter_props) > 0 else False
            res_list = []
            for res in selected_res:
                selected_vol = res.iterate_volumes()
                if props_filter_flag:
                    selected_vol = props_filter(
                        selected_vol, filter_props
                    )
                if serial > 0:
                    selected_vol = serial_filter(serial, selected_vol)

                vol_list = []
                for vol in selected_vol:
                    vol_entry = [ vol.get_id(), vol.get_properties(req_props) ]
                    vol_list.append(vol_entry)
                if (not props_filter_flag) or len(vol_list) > 0:
                    res_entry = [
                        res.get_name(),
                        res.get_properties(req_props), vol_list
                    ]
                    res_list.append(res_entry)
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
            return fn_rc, res_list
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)

        return fn_rc, None


    def list_assignments(self, node_names, res_names, serial,
                         filter_props, req_props):
        """
        Generates a list of assignment views suitable for serialized transfer

        Used by the drbdmanage client to display the assignments list
        """
        fn_rc = []

        def assg_filter(selected_nodes, selected_res):
            for node in selected_nodes.itervalues():
                for res in selected_res.itervalues():
                    assg = node.get_assignment(res.get_name())
                    if assg is not None:
                        yield assg

        try:
            if node_names is not None and len(node_names) > 0:
                selected_nodes = {}
                for node_name in node_names:
                    node = self._nodes.get(node_name)
                    if node is None:
                        add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT),
                                     [ [ NODE_NAME, node_name ] ])
                    else:
                        selected_nodes[node.get_name()] = node
            else:
                selected_nodes = self._nodes

            if res_names is not None and len(res_names) > 0:
                selected_res = {}
                for res_name in res_names:
                    res = self._resources.get(res_name)
                    if res is None:
                        add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT),
                                     [ [ RES_NAME, res_name ] ])
                    else:
                        selected_res[res.get_name()] = res
            else:
                selected_res = self._resources

            selected_assg = assg_filter(selected_nodes, selected_res)
            if serial > 0:
                selected_assg = serial_filter(serial, selected_assg)

            if filter_props is not None and len(filter_props) > 0:
                selected_assg = props_filter(selected_assg, filter_props)

            assg_list = []
            for assg in selected_assg:
                vol_state_list = []
                for vol_state in assg.iterate_volume_states():
                    vol_state_entry = [
                        vol_state.get_id(),
                        # FIXME: req_props, filter_props, nothing?
                        vol_state.get_properties(None)
                    ]
                    vol_state_list.append(vol_state_entry)
                assg_entry = [
                    assg.get_node().get_name(),
                    assg.get_resource().get_name(),
                    # FIXME: req_props, filter_props, nothing?
                    assg.get_properties(None),
                    vol_state_list
                ]
                assg_list.append(assg_entry)
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
            return fn_rc, assg_list
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)

        return fn_rc, None

    @no_satellite
    def create_snapshot(self, res_name, snaps_name, node_names, props):
        """
        Create a snapshot of a resource's volumes on a number of nodes

        Creates a snapshot registration and snapshot assignments for the
        specified nodes
        """
        # Work in progress...
        #
        # create_snapshot(res, name, node[], prop[])
        # |- for each node
        # |  '- check assignment of res on node
        # '- create snapshot in res with name and prop[]
        #    '- for each node
        #       |- create snapshot assignment, link to res and assignment
        #       '- for each volume in res
        #          '- create snapshot volume state and mark as 'deploy'
        fn_rc   = []
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is None:
                raise PersistenceException

            # Build a list of the selected nodes and ensure
            # that all of the specified nodes actually exist
            node_list = []
            for node_name in node_names:
                node = self._nodes.get(node_name)
                if node is not None:
                    node_list.append(node)
                else:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
                    raise AbortException
            if not len(node_list) > 0:
                instance_node = self._nodes.get(self._instance_node_name)
                if instance_node is not None:
                    node_list.append(instance_node)
                else:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
                    raise AbortException

            # Ensure that the specified resource exists
            resource = self._resources.get(res_name)
            if resource is None:
                add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
                raise AbortException

            # Ensure that the specified resource is assigned to all
            # selected nodes
            for node in node_list:
                assg = node.get_assignment(res_name)
                if assg is None:
                    add_rc_entry(fn_rc, DM_EINVAL, dm_exc_text(DM_EINVAL))
                    raise AbortException
                elif is_set(assg.get_tstate(), Assignment.FLAG_DISKLESS):
                    # Diskless node (DRBD9 client) selected as a target node
                    # for creating a snapshot
                    add_rc_entry(fn_rc, DM_EINVAL, dm_exc_text(DM_EINVAL))
                    raise AbortException

            # Avoid a name collision with an existing snapshot
            if resource.get_snapshot(snaps_name) is not None:
                add_rc_entry(fn_rc, DM_EEXIST, dm_exc_text(DM_EEXIST))
                raise AbortException

            # Register a new snapshot of the selected resource
            snapshot = DrbdSnapshot(
                snaps_name, resource,
                self.get_serial, None, None
            )
            resource.add_snapshot(snapshot)
            # Merge only auxiliary properties into the
            # Snapshot's properties container
            aux_props = aux_props_selector(props)
            snapshot.get_props().merge_gen(aux_props)
            # Register the snapshot assignments
            for node in node_list:
                assignment = node.get_assignment(res_name)
                snaps_assg = DrbdSnapshotAssignment(
                    snapshot, assignment,
                    0, DrbdSnapshotAssignment.FLAG_DEPLOY,
                    self.get_serial, None, None
                )
                node_name = node.get_name()
                res_name  = resource.get_name()
                snaps_assg_signal = self.create_signal(
                    "snapshots/" + node_name + "/" + res_name + "/" + snaps_name
                )
                snaps_assg.set_signal(snaps_assg_signal)
                # Create snapshot volume state objects
                for vol_state in assignment.iterate_volume_states():
                    cstate = vol_state.get_cstate()
                    tstate = vol_state.get_tstate()
                    # Snapshot volumes that are currently deployed
                    if (is_set(cstate, DrbdVolumeState.FLAG_DEPLOY) and
                        is_set(tstate, DrbdVolumeState.FLAG_DEPLOY)):
                            volume = vol_state.get_volume()
                            snaps_vol_state = DrbdSnapshotVolumeState(
                                vol_state.get_id(), volume.get_size_kiB(),
                                0, DrbdSnapshotVolumeState.FLAG_DEPLOY,
                                None, None,
                                self.get_serial, None, None
                            )
                            snaps_assg.add_snaps_vol_state(snaps_vol_state)
                # Set the snapshot assignment to deploy
                snaps_assg.set_tstate_flags(DrbdSnapshotAssignment.FLAG_DEPLOY)
                # register the snapshot assignment
                snapshot.add_snaps_assg(snaps_assg)
                assignment.add_snaps_assg(snaps_assg)
            self.save_conf_data(persist)
            self.schedule_run_changes()
        except AbortException:
            pass
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc


    def list_snapshots(self, res_names, snaps_names, serial,
                       filter_props, req_props):
        """
        List the available snapshots of a resource
        """
        fn_rc = []
        res_list = None
        def resource_filter(res_names):
            for res_name in res_names:
                res = self._resources.get(res_name)
                if res is None:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT),
                                 [ [ RES_NAME, res_name ] ])
                else:
                    yield res

        def snaps_filter(resource, snaps_names):
            for name in snaps_names:
                snaps = resource.get_snapshot(name)
                if snaps is not None:
                    yield snaps

        try:
            selected_res = self._resources.itervalues()
            if res_names is not None and len(res_names) > 0:
                selected_res = resource_filter(res_names)
            #if serial > 0:
            #    selected_res = serial_filter(serial, selected_res)

            res_list = []
            for res in selected_res:
                if snaps_names is not None and len(snaps_names) > 0:
                    selected_sn = snaps_filter(res, snaps_names)
                else:
                    selected_sn = res.iterate_snapshots()
                if filter_props is not None and len(filter_props) > 0:
                    selected_sn = props_filter(
                        selected_sn, filter_props
                    )

                sn_list = []
                for sn in selected_sn:
                     # TODO: was get_id()
                     # TODO: sn.get_properties(req_props)
                    sn_entry = [ sn.get_name(), sn.get_properties(req_props) ]
                    sn_list.append(sn_entry)
                if len(sn_list) > 0:
                    res_entry = [
                        res.get_name(),
			sn_list
                    ]
                    res_list.append(res_entry)
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        return fn_rc, res_list


    def list_snapshot_assignments(self, res_names, snaps_names, node_names,
                                  serial, filter_props, req_props):
        """
        List the available snapshots of a resource on specific nodes
        """
        fn_rc = []
        assg_list = None

        # TODO: should this function report nonexistent resource/node names
        #       in the query filter?

        # TODO: filter_props are not implemented yet

        def res_filter(res_names):
            for name in res_names:
                res = self._resources.get(name)
                if res is not None:
                    yield res

        def snaps_filter(resource, snaps_names):
            for name in snaps_names:
                snaps = resource.get_snapshot(name)
                if snaps is not None:
                    yield snaps

        try:
            if res_names is not None and len(res_names) > 0:
                selected_res = res_filter(res_names)
            else:
                selected_res = self._resources.itervalues()

            assg_list = []
            for res in selected_res:
                if snaps_names is not None and len(snaps_names) > 0:
                    selected_snaps = snaps_filter(res, snaps_names)
                else:
                    selected_snaps = res.iterate_snapshots()

                for snaps in selected_snaps:
                    snaps_assg_list = []
                    if node_names is not None and len(node_names) > 0:
                        name_map = {}
                        for name in node_names:
                            name_map[name] = None
                        for snaps_assg in snaps.iterate_snaps_assgs():
                            assg = snaps_assg.get_assignment()
                            node_name = assg.get_node().get_name()
                            if name_map.get(node_name) is not None:
                                snaps_entry = [
                                    node_name,
                                    snaps_assg.get_properties(req_props)
                                ]
                                snaps_assg_list.append(snaps_entry)
                    else:
                        for snaps_assg in snaps.iterate_snaps_assgs():
                            assg = snaps_assg.get_assignment()
                            node_name = assg.get_node().get_name()
                            snaps_entry = [
                                node_name,
                                snaps_assg.get_properties(req_props)
                            ]
                            snaps_assg_list.append(snaps_entry)
                    if len(snaps_assg_list) > 0:
                        snaps_list_entry = [
                            res.get_name(), snaps.get_name(),
                            snaps_assg_list
                        ]
                        assg_list.append(snaps_list_entry)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        return fn_rc, assg_list


    def restore_snapshot(self, res_name, snaps_res_name, snaps_name,
                         res_props, vols_props):
        """
        Restore a snapshot
        """
        fn_rc = []
        persist  = None
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                resource = self._create_resource(
                    res_name, dict(res_props), fn_rc
                )
                if resource is not None:
                    snaps_res = self._resources[snaps_res_name]
                    snapshot = snaps_res.get_snapshot(snaps_name)
                    if snapshot is not None:
                        occupied_minor_nrs = self.get_occupied_minor_nrs()
                        for snaps_assg in snapshot.iterate_snaps_assgs():
                            # Build the new resource's volume list from the
                            # first snapshot assignment's volume list
                            # FIXME: check whether the snapshot assignment was
                            #        actually ever deployed
                            # FIXME: abort if none of the assignments has ever
                            #        been deployed, because then there is no
                            #        snapshot that could be restored
                            vols_props_map = dict(vols_props)
                            sv_iter =  snaps_assg.iterate_snaps_vol_states()
                            for sv_state in sv_iter:
                                vol_id = sv_state.get_id()
                                v_props = vols_props_map.get(vol_id)
                                # Get a minor number for each volume
                                minor = MinorNr.MINOR_NR_AUTO
                                if v_props is not None:
                                    try:
                                        minor = int(v_props[VOL_MINOR])
                                    except KeyError:
                                        pass
                                    except ValueError:
                                        raise InvalidMinorNrException
                                if minor == MinorNr.MINOR_NR_AUTO:
                                    if occupied_minor_nrs is None:
                                        raise InvalidMinorNrException
                                    minor = self.get_free_minor_nr(
                                        occupied_minor_nrs
                                    )
                                if minor == MinorNr.MINOR_NR_ERROR:
                                    raise InvalidMinorNrException
                                volume = DrbdVolume(
                                    vol_id, sv_state.get_size_kiB(),
                                    MinorNr(minor), 0, self.get_serial,
                                    None, None
                                )
                                occupied_minor_nrs.append(minor)
                                if v_props is not None:
                                    # Merge only auxiliary properties into the
                                    # DrbdVolume's properties container
                                    aux_props = aux_props_selector(v_props)
                                    volume.get_props().merge_gen(aux_props)
                                resource.add_volume(volume)
                            # Break out of the loop after processing all
                            # snapshot volume states of the first
                            # snapshot assignment
                            # FIXME: corner case: this does not cover the case
                            #        where different snapshot assignments do
                            #        not have the same volumes
                            break
                        # FIXME: If the following assignment fails (although,
                        #        actually, it should never fail), saving the
                        #        resource definition should probably be
                        #        rolled back
                        self._resources[resource.get_name()] = resource
                        # Assign the newly created resource to each node that
                        # the snapshot resource was assigned to
                        # (unless that assignment is currently
                        #  being undeployed)
                        for assg in snaps_res.iterate_assignments():
                            assg_tstate_mask = (
                                Assignment.FLAG_DEPLOY |
                                Assignment.FLAG_DISKLESS
                            )
                            tstate = (assg.get_tstate() & assg_tstate_mask)
                            if is_set(tstate, Assignment.FLAG_DEPLOY):
                                node = assg.get_node()
                                cstate = 0
                                tstate |= Assignment.FLAG_CONNECT
                                assign_rc = self._assign(
                                    node, resource, cstate, tstate,
                                    assg.get_node_id()
                                )
                                if assign_rc != DM_SUCCESS:
                                    # Should not be reached, cause the only
                                    # error would be 'node not found', but
                                    # it should be there, since it should
                                    # be a reference to one of the nodes
                                    # in the server's node list;
                                    # therefore, reaching this statement
                                    # indicates an implementation error
                                    raise DebugException
                        # Set the snapshot source volumes on those nodes
                        # that have a snapshot
                        self._set_snapshot_sources(resource, snapshot)
                        self.save_conf_data(persist)
                        self.schedule_run_changes()
                    else:
                        add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except KeyError:
            add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def remove_snapshot_assignment(self, res_name, snaps_name, node_name,
                                   force):
        """
        Discard a resource's snapshot on a specific node
        """
        fn_rc   = []
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is None:
                raise PersistenceException

            try:
                resource = self._resources[res_name]
                node     = self._nodes[node_name]
                snapshot = resource.get_snapshot(snaps_name)
                if snapshot is None:
                    raise KeyError
                snaps_assg = snapshot.get_snaps_assg(node.get_name())
                if snaps_assg is None:
                    raise KeyError
                if (not force) and snaps_assg.is_deployed():
                    snaps_assg.undeploy()
                else:
                    snaps_assg.notify_removed()
                    snaps_assg.remove()
            except KeyError:
                add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
            self.cleanup()
            self.get_serial()
            self.save_conf_data(persist)
            self.schedule_run_changes()
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def remove_snapshot(self, res_name, snaps_name, force):
        """
        Discard all instances of a resource's snapshot
        """
        fn_rc   = []
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is None:
                raise PersistenceException

            try:
                resource = self._resources[res_name]
                snapshot = resource.get_snapshot(snaps_name)
                if snapshot is None:
                    raise KeyError
                if (not force) and snapshot.is_deployed():
                    for snaps_assg in snapshot.iterate_snaps_assgs():
                        snaps_assg.undeploy()
                    self.schedule_run_changes()
                else:
                    # the notify_removed signal is triggered in the
                    # DrbdSnapshot object's remove() method
                    snapshot.remove()
            except KeyError:
                add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
            self.cleanup()
            self.get_serial()
            self.save_conf_data(persist)
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc


    def _set_snapshot_sources(self, dest_res, snapshot):
        """
        Sets the source volumes for creation of a resource from a snapshot

        When a resource is created from a snapshot, set the snapshot
        source volume on the DrbdVolumeState object if a snapshot
        is available on the respective node
        """
        snaps_name = snapshot.get_name()
        source_res = snapshot.get_resource()
        for dest_assg in dest_res.iterate_assignments():
            node = dest_assg.get_node()
            source_assg = source_res.get_assignment(node.get_name())
            if source_assg is not None:
                source_snaps_assg = source_assg.get_snaps_assg(snaps_name)
                if source_snaps_assg is not None:
                    for dest_vol_state in dest_assg.iterate_volume_states():
                        dest_vol_id = dest_vol_state.get_id()
                        src_vol_state = (
                            source_snaps_assg.get_snaps_vol_state(dest_vol_id)
                        )
                        if src_vol_state is not None:
                            src_bd_name = src_vol_state.get_bd_name()
                            if src_bd_name is not None:
                                # Set the snapshot source volume property
                                # on the destination volume
                                dest_vol_props = dest_vol_state.get_props()
                                dest_vol_props.set_prop(
                                    SNAPS_SRC_BLOCKDEV, src_bd_name
                                )

    @no_satellite
    def resume(self, node_name, res_name):
        fn_rc = []
        add_rc_entry(fn_rc, DM_ENOTIMPL, dm_exc_text(DM_ENOTIMPL))
        return fn_rc

    @no_satellite
    def resume_all(self):
        fn_rc = []
        fn_rc   = []
        persist = None
        try:
            persist = self.begin_modify_conf()
            if persist is None:
                raise PersistenceException

            for node in self._nodes.itervalues():
                for assignment in node.iterate_assignments():
                    assignment.clear_fail_count()

            self.schedule_run_changes()
            self.cleanup()
            self.get_serial()
            self.save_conf_data(persist)
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc
        add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def dbus_save_conf(self):
        return self.save_conf()

    def save_conf(self):
        """
        Saves the current configuration to the drbdmanage control volume

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc = []
        persist = None
        try:
            if self._locked_persist is not None:
                self.save_conf_data(self._locked_persist)
            else:
                if self._persist is not None:
                    persist = self._persist
                    if persist.open(True):
                        self.save_conf_data(persist)
                    else:
                        raise PersistenceException
                else:
                    raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def dbus_load_conf(self):
        return self.load_conf()

    def load_conf(self):
        """
        Loads the current configuration from the drbdmanage control volume

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc = []
        persist = None
        try:
            if self._locked_persist is not None:
                self.load_conf_data(self._locked_persist)
            else:
                if self._persist is not None:
                    persist = self._persist
                    if persist.open(False):
                        self.load_conf_data(persist)
                    else:
                        raise PersistenceException
                else:
                    raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    def _reload_plugins(self):
        # make sure the current storage and deployer plugins are loaded
        # if the plugin is already loaded, this is a NOP
        self._pluginmgr.get_plugin_instance(self._conf[self.KEY_DEPLOYER_NAME])
        self._pluginmgr.get_plugin_instance(self._conf[self.KEY_STOR_NAME])

        loaded_plugins = self._pluginmgr.get_loaded_plugins()
        for (plugin_path, plugin_name) in loaded_plugins:
            try:
                # loaded plugins might also contain plugins that don't have a configuration in the objects root
                # currently these are "external" plugins
                # only known plugins like lvm, zfs, deployer have a config in self._plugin_conf
                # self._plugin_conf[plugin_name] might therefore raise a KeyError
                config = self._plugin_conf[plugin_name]
            except KeyError:
                continue
            self._pluginmgr.set_plugin_config(plugin_path, config)

    def load_conf_data(self, persist):
        """
        Loads the current configuration from the supplied persistence object

        Used by the drbdmanage server to load the configuration after the
        persistence layer had already opened it before

        @return: standard return code defined in drbdmanage.exceptions
        """
        persist.load(self._objects_root)
        self._conf_hash = persist.get_stored_hash()
        self.load_server_conf(self.CONF_STAGE[self.KEY_FROM_CTRL_VOL])

    def save_conf_data(self, persist):
        """
        Saves the current configuration to the supplied persistence object

        Used by the drbdmanage server to save the configuration after the
        persistence layer had already opened and locked it before

        @return: standard return code defined in drbdmanage.exceptions
        """
        hash_obj = None
        persist.save(self._objects_root)
        hash_obj = persist.get_hash_obj()
        if hash_obj is not None:
            self._conf_hash = hash_obj.get_hex_hash()


    def open_conf(self):
        """
        Opens the configuration on persistent storage for reading
        This function is only there because drbdcore cannot import anything
        from persistence, so the code for creating a PersistenceImpl object
        has to be somwhere else.
        Returns a PersistenceImpl object on success, or None if the operation
        fails due to errors in the persistence layer

        @return: persistence layer object
        """
        ret_persist = None

        try:
            if self._persist.open(False):
                ret_persist = self._persist
        except Exception as exc:
            # DEBUG
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logging.error(
                "cannot open control volume, unhandled exception: %s"
                % str(exc)
            )
            logging.debug("Stack trace:\n%s" % str(exc_tb))
            self._persist.close()
        return ret_persist


    def begin_modify_conf(self, override_quorum=False):
        """
        Opens the configuration on persistent storage for writing,
        implicitly locking out all other nodes, and reloads the configuration
        if it has changed.
        Returns a PersistenceImpl object on success, or None if the operation
        fails due to errors in the persistence layer

        @return: persistence layer object
        """
        ret_persist = None
        if self._locked_persist is not None:
            ret_persist = self._locked_persist
        else:
            if self._persist is not None:
                accessible = False
                modifiable = self._quorum.is_present() or override_quorum
                try:
                    accessible = self._persist.open(modifiable)
                except Exception as exc:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    access_mode = "modification" if modifiable else "reading"
                    logging.error(
                        "Cannot open the control volume for %s, "
                        "unhandled exception: %s"
                        % (access_mode, str(exc))
                    )
                    logging.debug("Stack trace:\n%s" % str(exc_tb))
                    self._persist.close()
                if accessible:
                    try:
                        if not self.hashes_match(self._persist.get_stored_hash()):
                            self.load_conf_data(self._persist)
                        if modifiable:
                            # if the configuration was opened for modification,
                            # return the persistence object
                            self._locked_persist = self._persist
                            ret_persist = self._persist
                        else:
                            # if the configuration was opened only for reading,
                            # no modifiable persistence object can be returned,
                            # and the configuration reload is finished;
                            # close the persistence object and fail
                            self._persist.close()
                    except Exception as exc:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        logging.error(
                            "Cannot read data from the control volume "
                            "unhandled exception: %s"
                            % (str(exc))
                        )
                        logging.debug("Stack trace:\n%s" % str(exc_tb))
                        self._persist.close()
                    if not modifiable:
                        raise QuorumException
        return ret_persist


    def end_modify_conf(self, persist):
        """
        Closes the persistence layer object

        @param   persist: persistence layer object to close
        """
        try:
            if persist is not None:
                if persist is self._locked_persist:
                    self._locked_persist = None
                persist.close()
            self.close_serial()
        except Exception:
            pass

    def cond_end_modify_conf(self, persist):
        """
        If run_changes() has not been scheduled, closes the persistence layer object

        @param   persist: persistence layer object to close
        """
        if not self._run_changes_scheduled:
            self.end_modify_conf(persist)

    @no_satellite
    def quorum_control(self, node_name, props, override_quorum_flag):
        """
        Sets quorum parameters on drbdmanage nodes
        """
        fn_rc   = []
        persist = None
        try:
            persist = self.begin_modify_conf(override_quorum=override_quorum_flag)
            if persist is not None:
                node = self.get_node(node_name)
                if node is not None:
                    try:
                        qignore_field = props[FLAG_QIGNORE]
                        qignore_flag = string_to_bool(qignore_field)
                        if qignore_flag:
                            node.set_state_flags(DrbdNode.FLAG_QIGNORE)
                        else:
                            node.clear_state_flags(DrbdNode.FLAG_QIGNORE)
                        self._quorum.readjust_full_member_count()
                        self.get_serial()
                        self.save_conf_data(persist)
                    except (KeyError, ValueError):
                        pass
                else:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc


    # TODO: more precise error handling
    def export_conf(self, res_name):
        """
        For a named resource, exports a configuration file for drbdadm

        Exports a configuration file for drbdadm generated from the current
        configuration of a resource managed by the drbdmanage server on the
        current host.
        If the resource name is "*", configuration files for all resources
        currently deployed on the current host are generated.

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc = []
        node = self.get_instance_node()
        if node is not None:
            if res_name is None:
                res_name = ""
            if len(res_name) > 0 and res_name != "*":
                assg = node.get_assignment(res_name)
                if assg is not None:
                    if self.export_assignment_conf(assg) != 0:
                        add_rc_entry(fn_rc, DM_DEBUG, dm_exc_text(DM_DEBUG))
                else:
                    add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT),
                                 [ [ RES_NAME, res_name ] ])
            else:
                for assg in node.iterate_assignments():
                    if self.export_assignment_conf(assg) != 0:
                        add_rc_entry(fn_rc, DM_DEBUG, dm_exc_text(DM_DEBUG))
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc


    # TODO: move over existing file instead of directly overwriting an
    #       existing file
    def export_assignment_conf(self, assignment):
        """
        From an assignment object, exports a configuration file for drbdadm

        Exports a configuration file for drbdadm generated from the current
        configuration of an assignment object managed by the drbdmanage server

        The drbdmanage server uses this function to generate temporary
        configuration files for drbdadm callbacks by the DRBD kernel module
        as well.

        @return: 0 on success, 1 on error
        """
        fn_rc = 0
        resource = assignment.get_resource()
        file_path = os.path.join(self._conf[self.KEY_DRBD_CONFPATH],
                                 "drbdmanage_" + resource.get_name() + ".res")

        global_path = os.path.join(self._conf[self.KEY_DRBD_CONFPATH],
                                   FILE_GLOBAL_COMMON_CONF)
        assg_conf = None
        global_conf = None
        try:
            assg_conf = open(file_path, "w")
            global_conf = open(global_path, "w")
            writer = DrbdAdmConf(self._objects_root)
            writer.write(assg_conf, assignment, False, global_conf)
        except IOError as ioerr:
            logging.error(
                "cannot write to configuration file '%s' or '%s', error "
                "returned by the OS is: %s"
                % (file_path, global_path, ioerr.strerror)
            )
            fn_rc = 1
        finally:
            if assg_conf is not None:
                assg_conf.close()
            if global_conf is not None:
                global_conf.close()
        return fn_rc


    def remove_assignment_conf(self, resource_name):
        """
        Removes (unlinks) a drbdadm configuration file

        The drbdmanage server uses this function to remove configuration files
        of resources that become undeployed on the current host.

        @return: 0 on success, 1 on error
        """
        fn_rc = 0
        file_path = os.path.join(self._conf[self.KEY_DRBD_CONFPATH],
                                 "drbdmanage_" + resource_name + ".res")
        try:
            os.unlink(file_path)
        except OSError as oserr:
            if oserr.errno != errno.ENOENT:
                logging.error(
                    "cannot remove configuration file '%s', "
                    "error returned by the OS is: %s"
                    % (file_path, oserr.strerror)
                )
                fn_rc = 1
        return fn_rc


    def get_conf_hash(self):
        """
        Retrieves the hash code of the currently loaded configuration

        @return: hash code of the currently loaded configuration
        @rtype:  str
        """
        return self._conf_hash


    def hashes_match(self, cmp_hash):
        """
        Checks whether the currently known hash matches the supplied hash

        Configuration changes on the drbdmanage control volume are detected
        by checking whether the hash has changed. This is done by comparing
        the hash of the currently known configuration to the hash stored on
        the control volume whenever the data on the control volume may have
        changed.

        @return: True if the hashes match, False otherwise
        @rtype:  bool
        """
        if self._conf_hash is not None and cmp_hash is not None:
            if self._conf_hash == cmp_hash:
                return True
        return False


    def reconfigure(self):
        """
        Reconfigures the server

        @return: standard return code defined in drbdmanage.exceptions
        """
        fn_rc = []
        try:
            self.load_server_conf(self.CONF_STAGE[self.KEY_FROM_FILE])
            self.set_loglevel()
            extend_path(self._path, self.get_conf_value(self.KEY_EXTEND_PATH))
            fn_rc = self.load_conf()
            self.load_server_conf(self.CONF_STAGE[self.KEY_FROM_CTRL_VOL])
            self._drbd_mgr.reconfigure()
            self._bd_mgr = BlockDeviceManager(self._conf[self.KEY_STOR_NAME], self._pluginmgr)
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def init_node(self, name, props):
        """
        Server part of initializing a new drbdmanage cluster
        """
        fn_rc   = []
        persist = None
        try:
            persist = self.begin_modify_conf(override_quorum=True)

            if persist is not None:

                # clear the configuration
                srv = DrbdManageServer
                self._objects_root[srv.OBJ_NODES_NAME] = {}
                self._objects_root[srv.OBJ_RESOURCES_NAME] = {}
                self.update_objects()

                bdev_0 = None
                bdev_1 = None
                port   = None
                try:
                    bdev_0 = props[NODE_VOL_0]
                    bdev_1 = props[NODE_VOL_1]
                    port = int(props[NODE_PORT])
                except (KeyError, ValueError, TypeError):
                    pass

                if bdev_0 is not None and bdev_1 is not None and port is not None:
                    sub_rc = self._create_node(True, name, props, bdev_0, bdev_1, port)
                    if sub_rc == DM_SUCCESS or sub_rc == DM_ECTRLVOL:
                        # attempt to determine the amount of total and free
                        # storage space on the local node; if that fails, total
                        # and free space will be determined later, either when
                        # volumes are created or when the drbdmanage server
                        # is restarted
                        # therefore, the return code is intentionally ignored
                        self.update_pool_data()
                        # save the changes to the control volume
                        self.save_conf_data(persist)
                    else:
                        add_rc_entry(fn_rc, sub_rc, dm_exc_text(sub_rc))
                else:
                    add_rc_entry(fn_rc, DM_EINVAL, dm_exc_text(DM_EINVAL))
            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    @no_satellite
    def join_node(self, props):
        """
        Server part of integrating a node into an existing drbdmanage cluster
        """
        fn_rc   = []
        persist = None
        try:
            persist = self.begin_modify_conf(override_quorum=True)

            if persist is not None:
                # TODO: there should probably be library functions for evaluating
                #       return code lists
                conf_drbdctrl = self._configure_drbdctrl
                fn_rc = self.load_conf()
                load_ok = False
                for rc_entry in fn_rc:
                    if rc_entry[0] == DM_SUCCESS:
                        load_ok = True
                    else:
                        load_ok = False
                        break
                # empty the return codes list
                del fn_rc[:]

                if load_ok:
                    check_node = self._nodes.get(self._instance_node_name)
                    if check_node is not None:
                        bdev_0 = None
                        bdev_1 = None
                        port   = None
                        secret = None

                        try:
                            bdev_0 = props[NODE_VOL_0]
                            bdev_1 = props[NODE_VOL_1]
                            port   = int(props[NODE_PORT])
                            secret = props[NODE_SECRET]
                        except (KeyError, ValueError, TypeError):
                            pass

                        if (bdev_0 is not None and bdev_1 is not None and
                            port is not None and secret is not None):
                            if (conf_drbdctrl(True, secret, bdev_0, bdev_1, port) == 0):
                                # Establish connections to the other
                                # drbdmanage nodes
                                self._drbd_mgr.adjust_drbdctrl()
                                # Clear the update flag on the joining node
                                state = check_node.get_state()
                                state = ((state | DrbdNode.FLAG_UPDATE) ^ DrbdNode.FLAG_UPDATE)
                                check_node.set_state(state)
                                # Attempt to update the node's storage pool data
                                # If it fails now, it will run again later anyway,
                                # therefore the return code is ignored
                                self.update_pool_data()
                                # Save changes
                                self.save_conf_data(persist)
                            else:
                                add_rc_entry(fn_rc, DM_ECTRLVOL, dm_exc_text(DM_ECTRLVOL))
                        else:
                            add_rc_entry(fn_rc, DM_EINVAL, dm_exc_text(DM_EINVAL))
                    else:
                        add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
                else:
                    # load_conf() failed
                    raise PersistenceException
            else:
                raise PersistenceException
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.end_modify_conf(persist)
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc


    def TQ_joinc(self, node_name=None):
        if node_name is None or len(node_name) == 0:
            return [("Error: joinc query without a node name argument")]

        fields    = None
        secret    = None
        port      = None
        l_addr    = None
        l_node_id = None
        r_addr    = None
        r_node_id = None
        r_name    = None

        drbdctrl_res = None

        if self.is_satellite(node_name):
            return [('Error: Node "%s" is a satellite node!\nJust make sure drbdmanaged is running on %s'
                     % (node_name, node_name))]

        conffile = DrbdAdmConf(self._objects_root)
        try:
            drbdctrl_res = open(
                build_path(DRBDCTRL_RES_PATH, DRBDCTRL_RES_FILE),
                "r"
            )
            fields = conffile.read_drbdctrl_params(drbdctrl_res)
        except (IOError, OSError):
            pass
        finally:
            if drbdctrl_res is not None:
                try:
                    drbdctrl_res.close()
                except (IOError, OSError):
                    pass

        try:
            address = fields[NODE_ADDRESS]
            idx = address.rfind(":")
            if idx != -1:
                port = address[idx + 1:]
            else:
                raise ValueError
            secret = fields[NODE_SECRET]
        except (KeyError, ValueError):
            pass

        inst_node = self.get_instance_node()
        rem_node  = self._nodes.get(node_name)

        if inst_node is not None and rem_node is not None:
            r_addr    = inst_node.get_addr()
            r_node_id = str(inst_node.get_node_id())
            r_name    = inst_node.get_name()
            l_addr    = rem_node.get_addr()
            l_node_id = str(rem_node.get_node_id())

        if (all([secret, port, r_addr, r_node_id, r_name, l_addr, l_node_id])):
            return ["drbdmanage", "join", "-p",
                    port, l_addr, l_node_id, r_name, r_addr,
                    r_node_id, secret]
        else:
            return [("Error: Generation of the join command failed")]


    def TQ_version(self):
        """
        Returns version information about various subsystems
        """
        drbd_kernel_version  = "<unknown>"
        drbd_kernel_git_hash = "<unknown>"
        drbd_utils_version   = "<unknown>"
        drbd_utils_git_hash  = "<unknown>"

        # Retrieve the DRBD kernel module's version and GIT hash
        proc_drbd_file = None
        try:
            proc_drbd_file = open(DrbdManageServer.DRBD_KMOD_INFO_FILE, "r")
            drbd_kernel_version = proc_drbd_file.readline().rstrip("\n")
            drbd_kernel_git_hash = proc_drbd_file.readline().rstrip("\n")
        except IOError:
            logging.debug("Cannot retrieve DRBD kernel module version information from '%s'"
                          % (DrbdManageServer.DRBD_KMOD_INFO_FILE))
        finally:
            if proc_drbd_file is not None:
                try:
                    proc_drbd_file.close()
                except IOError:
                    pass

        # TODO: retrieve the drbd-utils' version

        version_info = [
            key_value_string(KEY_SERVER_VERSION, DM_VERSION),
            key_value_string(KEY_SERVER_GITHASH, DM_GITHASH),
            key_value_string(KEY_DRBD_KERNEL_VERSION, drbd_kernel_version),
            key_value_string(KEY_DRBD_KERNEL_GIT_HASH, drbd_kernel_git_hash),
            key_value_string(KEY_DRBD_UTILS_VERSION, drbd_utils_version),
            key_value_string(KEY_DRBD_UTILS_GIT_HASH, drbd_utils_git_hash)
        ]
        return version_info


    def TQ_get_path(self, res_name, vol_id_arg="0"):
        """ Get path of device node.
            res_name is needed, vol_id is optional. """
        # TODO: can this be per-node specific?
        resource = self._resources.get(res_name)
        # TODO: throw exceptions?
        if resource is None:
            return ["Resource not found"]
        vol_id = int(vol_id_arg)
        volume = resource.get_volume(vol_id)
        if volume is None:
            return ["Invalid volume id"]
        return [volume.get_path()]


    def TQ_export_conf_split_up(self, node_name, res_name):
        """
        Export the configuration file for a DRBD resource on a specified node,
        with some values split out into a dictionary.
        """
        response = []
        values   = {}
        node     = self._nodes.get(node_name)
        resource = self._resources.get(res_name)
        if not resource:
            response = ["Error: Resource not found"]

        values['shared-secret'] = resource.get_secret()
        # should use a write-template function instead
        # MUST NOT SAVE WRONG VALUE
        resource.set_secret("%(shared-secret)s")

        if node is not None and resource is not None:
            assignment = node.get_assignment(res_name)
            if assignment is not None:
                conf_buffer = StringIO.StringIO()
                writer = DrbdAdmConf(self._objects_root, target_node=node)
                writer.write(conf_buffer, assignment, False)
                val_list = list(reduce(lambda x, y: x + y, values.items()))
                response = [conf_buffer.getvalue()] + val_list
                conf_buffer.close()
            else:
                response = [
                    "Error: Resource %s is not assigned to node %s"
                    % (res_name, node_name)
                ]
        else:
            if node is None:
                response = ["Error: Node %s not found" % (node_name)]
            else:
                response = ["Error: Resource %s not found" % (res_name)]

        # is that needed, or will local data be removed anyway?
        self.load_conf()
        return response


    def TQ_export_conf(self, node_name, res_name):
        """
        Export the configuration file for a DRBD resource on a specified node
        """
        response = []
        node     = self._nodes.get(node_name)
        resource = self._resources.get(res_name)
        if not resource:
            response = ["Error: Resource not found"]
        if node is not None and resource is not None:
            assignment = node.get_assignment(res_name)
            if assignment is not None:
                conf_buffer = StringIO.StringIO()
                writer = DrbdAdmConf(self._objects_root, target_node=node)
                writer.write(conf_buffer, assignment, False)
                response = [conf_buffer.getvalue()]
                conf_buffer.close()
            else:
                response = [
                    "Error: Resource %s is not assigned to node %s"
                    % (res_name, node_name)
                ]
        else:
            if node is None:
                response = ["Error: Node %s not found" % (node_name)]
            else:
                response = ["Error: Resource %s not found" % (res_name)]
        return response


    def text_query(self, command):
        """
        Query text strings from the server

        @param   command: query command and argument list
        @type    command: list of str
        @return: list of answer texts to the query
        @rtype:  list of str
        """
        fn_rc = []
        result_text = []
        try:
            if len(command) < 1:
                add_rc_entry(fn_rc, DM_EINVAL, dm_exc_text(DM_EINVAL))
                return (
                    fn_rc,
                    [
                        "Error: empty argument list sent to the "
                        "drbdmanage server"
                    ]
                )

            func_name = "TQ_" + command.pop(0)
            text_query_func = getattr(self, func_name)
            if text_query_func is None:
                result_text = ["Error: unknown command"]
            else:
                # optional arguments are those that have default values
                # specified in the function declaration
                (mandatory_args, _, _, optional_args) = (
                    inspect.getargspec(text_query_func)
                )
                # remove the "self" argument
                mandatory_args.pop(0)

                mandatory_args_len = len(mandatory_args)
                optional_args_len  = (0 if optional_args is None
                                      else len(optional_args))
                command_len        = len(command)
                # TODO: varargs
                if command_len > mandatory_args_len:
                    result_text = ["Error: too many arguments."]
                elif command_len + optional_args_len < mandatory_args_len:
                    result_text = ["Error: too few arguments."]
                else:
                    result_text = text_query_func(*command)
        except Exception as exc:
            # FIXME: useful error messages required here
            logging.error("text_query() command failed: %s" % str(exc))
            add_rc_entry(fn_rc, DM_DEBUG, dm_exc_text(DM_DEBUG))
            return (
                fn_rc,
                [
                    "Error: Text query command failed on the "
                    "drbdmanage server"
                ]
            )
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc, result_text


    def reconfigure_drbdctrl(self):
        """
        Updates the current node's control volume configuration
        """
        self._configure_drbdctrl(False, None, None, None, None)
        self._drbd_mgr.adjust_drbdctrl()


    def _configure_drbdctrl(self, initial, secret, bdev_0, bdev_1, port):
        """
        Creates or updates the drbdctrl resource configuration file
        """
        # if values are missing, try to get those values from an existing
        # configuration file; if no configuration file can be read,
        # use default values
        fn_rc        = 1
        drbdctrl_res = None
        conffile     = DrbdAdmConf(self._objects_root)
        update       = False

        if (bdev_0 is not None and bdev_1 is not None and
            port is not None and secret is not None):
            update = True
        else:
            # Load values from an existing configuation unless all values are
            # specified or an initial configuration is requested
            if not initial:
                fields = None

                # Try to open an existing configuration file
                try:
                    drbdctrl_res = open(
                        build_path(DRBDCTRL_RES_PATH, DRBDCTRL_RES_FILE),
                        "r"
                    )
                except (IOError, OSError):
                    # if the drbdctrl.res file cannot be opened, assume
                    # that it does not exist and create a new one
                    update = True

                # If an existing configuration file can be read, try to extract
                # values from the configuration file
                if not update:
                    try:
                        fields = conffile.read_drbdctrl_params(drbdctrl_res)
                    except (IOError, OSError):
                        pass
                    finally:
                        if drbdctrl_res is not None:
                            try:
                                drbdctrl_res.close()
                            except (IOError, OSError):
                                pass
                    if fields is not None:
                        try:
                            if port is None:
                                address = fields[NODE_ADDRESS]
                                idx = address.rfind(":")
                                if idx != -1:
                                    port = address[idx + 1:]
                                else:
                                    raise ValueError
                            if bdev_0 is None:
                                bdev_0 = fields[NODE_VOL_0]
                            if bdev_1 is None:
                                bdev_1 = fields[NODE_VOL_1]
                            if secret is None:
                                secret = fields[NODE_SECRET]
                            update = True
                        except (KeyError, ValueError):
                            pass

        # if an existing configuration has been read successfully,
        # or an initial configuration file should be created,
        # write the drbdctrl.res file
        if initial or update:
            try:
                # use defaults for anything that is still unset
                if port is None:
                    port = str(DRBDCTRL_DEFAULT_PORT)
                if bdev_0 is None:
                    bdev_0 = os.path.join("/dev/", self.get_conf_value(KEY_DRBDCTRL_VG),
                                          DRBDCTRL_LV_NAME_0)
                if bdev_1 is None:
                    bdev_1 = os.path.join("/dev/", self.get_conf_value(KEY_DRBDCTRL_VG),
                                          DRBDCTRL_LV_NAME_1)
                if secret is None:
                    secret = generate_secret()

                drbdctrl_res = open(
                    build_path(DRBDCTRL_RES_PATH, DRBDCTRL_RES_FILE), "w"
                )
                # Collect all nodes that have a drbdmanage control volume
                drbdctrl_nodes = {}
                for node in self._nodes.itervalues():
                    node_state = node.get_state()
                    if is_set(node_state, DrbdNode.FLAG_DRBDCTRL):
                        drbdctrl_nodes[node.get_name()] = node
                # Generate the drbdmanage control volume configuration file
                conffile.write_drbdctrl(drbdctrl_res, drbdctrl_nodes,
                                        bdev_0, bdev_1, port, secret)
                drbdctrl_res.close()
                fn_rc = 0
            except (IOError, OSError):
                pass
            finally:
                if drbdctrl_res is not None:
                    try:
                        drbdctrl_res.close()
                    except (IOError, OSError):
                        pass
        return fn_rc

    @no_satellite
    def get_ctrlvol(self):
        # basically would be allowed on satellites, but:
        # we send ctrlvol data to satellites only if the need it (e.g., new volume to deploy)
        # this avoids unneccessary network traffic, but ctrlvol on satellites might be "outdated"
        fn_rc = []
        self._persist.json_export(self._objects_root)
        jsonblob = self._persist.get_json_data()
        add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return (fn_rc, jsonblob)

    @no_satellite
    def set_ctrlvol(self, jsonblob):
        fn_rc = []
        try:
            persist = self.begin_modify_conf()
            if persist is not None:
                self._persist.set_json_data(jsonblob)
                self._persist.json_import(self._objects_root)
                self.save_conf_data(persist)
                self.reconfigure_drbdctrl()
            else:
                raise PersistenceException
        except KeyError:
            add_rc_entry(fn_rc, DM_ENOENT, dm_exc_text(DM_ENOENT))
        except DrbdManageException as server_exc:
            server_exc.add_rc_entry(fn_rc)
        except Exception as exc:
            self.catch_and_append_internal_error(fn_rc, exc)
        finally:
            self.cond_end_modify_conf(persist)
            self.schedule_run_changes()
        if len(fn_rc) == 0:
            add_rc_entry(fn_rc, DM_SUCCESS, dm_exc_text(DM_SUCCESS))
        return fn_rc

    def debug_console(self, cmdline):
        """
        Set debugging options
        """
        fn_rc = 127
        sync = False
        persist = None
        try:
            args = cmdline.split()
            command = args.pop(0)
            if command == "sync":
                sync = True
                persist = self.begin_modify_conf(False)
                if persist is None:
                    raise PersistenceException
                command = args.pop(0)
            if command == "sync/ovrd":
                sync = True
                persist = self.begin_modify_conf(True)
                if persist is None:
                    raise PersistenceException
                command = args.pop(0)
            if command == "set":
                try:
                    subcommand = args.pop(0)
                    if subcommand == "n":
                        fn_rc = self._debug_set_node(args)
                    elif subcommand == "r":
                        fn_rc = self._debug_set_resource(args)
                    elif subcommand == "v":
                        fn_rc = self._debug_set_volume(args)
                    elif subcommand == "a":
                        fn_rc = self._debug_set_assignment(args)
                    elif subcommand == "s":
                        fn_rc = self._debug_set_snapshot(args)
                    elif subcommand == "s/a":
                        fn_rc = self._debug_set_snapshot_assignment(args)
                    else:
                        key, val = self._debug_keyval_split(subcommand)
                        if key == "dbg_events":
                            self.dbg_events = self._debug_parse_flag(val)
                            fn_rc = 0
                        elif key == "loglevel":
                            loglevel = self._debug_parse_loglevel(val)
                            self._root_logger.setLevel(loglevel)
                            fn_rc = 0
                        elif key == "dbgout":
                            fn_rc = self._debug_set_debug_out(val)
                except (AttributeError, IndexError):
                    fn_rc = 1
            elif command == "cancel-actions":
                subcommand = args.pop(0)
                if subcommand == "r":
                    res_name = args.pop(0)
                    resource = self._debug_get_resource(res_name)
                    state = resource.get_state()
                    state = (state | DrbdResource.FLAG_REMOVE) ^ DrbdResource.FLAG_REMOVE
                    resource.set_state(0)
                    fn_rc = 0
                elif subcommand == "v":
                    res_name = args.pop(0)
                    vol_id_str = args.pop(0)
                    volume = self._debug_get_volume(res_name, vol_id_str)
                    state = volume.get_state()
                    state = (state | DrbdVolume.FLAG_REMOVE) ^ DrbdVolume.FLAG_REMOVE
                    volume.set_state(state)
                    fn_rc = 0
                elif subcommand == "n":
                    node_name = args.pop(0)
                    node = self._debug_get_node(node_name)
                    state = node.get_state()
                    clear = DrbdNode.FLAG_UPD_POOL | DrbdNode.FLAG_UPDATE | DrbdNode.FLAG_REMOVE
                    state = (state | clear) ^ clear
                    node.set_state(state)
                    fn_rc = 0
                elif subcommand == "a":
                    assg_path = args.pop(0)
                    assignment = self._debug_get_assignment(assg_path)
                    assignment.set_tstate(assignment.get_cstate())
                    for vol_state in assignment.iterate_volume_states():
                        vol_state.set_tstate(vol_state.get_cstate())
                    fn_rc = 0
            elif command == "run":
                try:
                    item = args.pop(0)
                    if item == "cleanup":
                        self.cleanup()
                        fn_rc = 0
                    elif item == "DrbdManager":
                        # override the hash check, but do not poke
                        # the cluster
                        self._drbd_mgr.run(True, False)
                        fn_rc = 0
                    elif item == "initial_up":
                        self._drbd_mgr.initial_up()
                        fn_rc = 0
                    elif item == "adjust_drbdctrl":
                        self._drbd_mgr.adjust_drbdctrl()
                        fn_rc = 0
                    elif item == "down_drbdctrl":
                        self._drbd_mgr.down_drbdctrl()
                        fn_rc = 0
                    elif item == "init_events":
                        self.init_events()
                        fn_rc = 0
                    elif item == "uninit_events":
                        self.uninit_events()
                        fn_rc = 0
                except (AttributeError, IndexError):
                    pass
            elif command == "restart":
                try:
                    item = args.pop(0)
                    if item == "events":
                        self.restart_events(None, None)
                        fn_rc = 0
                except (AttributeError, IndexError):
                    pass
            elif command == "test":
                try:
                    item = args.pop(0)
                    if item == "stdout":
                        sys.stdout.write("(test stdout)\n")
                        fn_rc = 0
                    elif item == "stderr":
                        sys.stderr.write("(test stderr)\n")
                        fn_rc = 0
                    elif item == "dbgout":
                        self._debug_out.write("(test dbgout)\n")
                        fn_rc = 0
                except (AttributeError, IndexError):
                    pass
            elif command == "list":
                try:
                    item = args.pop(0)
                    if item == "n":
                        fn_rc = self._debug_list_nodes(args)
                    elif item == "r":
                        fn_rc = self._debug_list_resources(args)
                    elif item == "v":
                        fn_rc = self._debug_list_volumes(args)
                    elif item == "a":
                        fn_rc = self._debug_list_assignments(args)
                    elif item == "s":
                        fn_rc = self._debug_list_snapshots(args)
                    elif item == "s/a":
                        fn_rc = self._debug_list_snapshot_assignments(args)
                    elif item == "conf/server":
                        fn_rc = self._debug_list_server_conf(args)
                    elif item == "conf/cluster":
                        fn_rc = self._debug_list_cluster_conf(args)
                    elif item == "props":
                        fn_rc = self._debug_list_props(args)
                except (AttributeError, IndexError):
                    pass
            elif command == "gen":
                try:
                    item = args.pop(0)
                    if item == "drbdctrl":
                        fn_rc = self._debug_gen_drbdctrl(args)
                except (AttributeError, IndexError):
                    pass
            elif command == "mod":
                try:
                    item = args.pop(0)
                    if item == "drbdctrl":
                        fn_rc = self._debug_mod_drbdctrl(args)
                except (AttributeError, IndexError):
                    pass
            elif command == "invalidate":
                self._conf_hash = None
                fn_rc = 0
            elif command == "show":
                try:
                    subcommand = args.pop(0)
                    if subcommand == "hash":
                        if self._conf_hash is None:
                            self._debug_out.write("unset/invalid\n")
                        else:
                            self._debug_out.write(
                                "%s\n" % (self._conf_hash)
                            )
                        fn_rc = 0
                except (AttributeError, IndexError):
                    pass
            elif command == "exit":
                try:
                    exit_code_str = args.pop(0)
                    exit_code     = int(exit_code_str)
                    exit_msg = ("server shutdown (debug command): exit %d"
                                % (exit_code))
                    self._debug_out.write(exit_msg + "\n")
                    self._debug_out.flush()
                    logging.debug(exit_msg)
                    exit(exit_code)
                except (ValueError, AttributeError, IndexError):
                    pass
        except IndexError:
            # args.pop(0) from an empty list
            self._debug_out.write("Incomplete command line")
            self._debug_out.flush()
        except ValueError:
            # Generated by _debug_get_*() functions
            pass
        except PersistenceException:
            self._debug_out.write("Unable to access the persistent configuration\n")
            self._debug_out.flush()
        except Exception as exc:
            self.catch_internal_error(exc)
            fn_rc = DM_DEBUG
        finally:
            if sync:
                try:
                    self.save_conf_data(persist)
                except:
                    pass
                self.end_modify_conf(persist)
            try:
                self._debug_out.flush()
            except (IOError, OSError, AttributeError):
                pass
        return fn_rc


    def _debug_get_node(self, node_name):
        if node_name is None:
            self._debug_out.write("Missing node argument\n")
            self._debug_out.flush()
            raise ValueError
        node = self.get_node(node_name)
        if node is None:
            self._debug_out.write("Node '%s' not found\n" % (node_name))
            self._debug_out.flush()
            raise ValueError
        return node


    def _debug_get_resource(self, res_name):
        if res_name is None:
            self._debug_out.write("Missing resource argument\n")
            self._debug_out.flush()
            raise ValueError
        resource = self.get_resource(res_name)
        if resource is None:
            self._debug_out.write("Resource '%s' not found\n" % (res_name))
            self._debug_out.flush()
            raise ValueError
        return resource


    def _debug_get_assignment(self, assg_path):
        if assg_path is None:
            self._debug_out.write("Missing assignment path\n")
            self._debug_out.flush()
            raise ValueError
        idx = assg_path.find("/")
        if idx == -1:
            self._debug_out.write("Invalid assignment path\n")
            self._debug_out.flush()
            raise ValueError
        node_name = assg_path[:idx]
        res_name = assg_path[idx + 1:]
        node = self._debug_get_node(node_name)
        resource = self._debug_get_resource(res_name)
        assignment = node.get_assignment(resource.get_name())
        if assignment is None:
            self._debug_out.write("Assignment '%s/%s' not found\n" % (node.get_name(), resource.get_name()))
            self._debug_out.flush()
            raise ValueError
        return assignment


    def _debug_get_volume(self, res_name, vol_id_str):
        resource = self._debug_get_resource(res_name)
        if vol_id_str is None:
            self._debug_out.write("Missing volume id argument\n")
            self._debug_out.flush()
            raise ValueError
        vol_id = 0
        try:
            vol_id = int(vol_id_str)
        except ValueError:
            self._debug_out.write("Invalid volume id argument\n")
            self._debug_out.flush()
            raise ValueError
        volume = resource.get_volume(vol_id)
        if volume is None:
            self._debug_out.write("Volume '%s/%d' not found\n" % (resource.get_name(), vol_id))
            self._debug_out.flush()
            raise ValueError
        return volume


    def _debug_gen_drbdctrl(self, args):
        fn_rc = 1
        secret = args.pop(0)
        port   = args.pop(0)
        bdev_0 = args.pop(0)
        bdev_1 = args.pop(0)
        fn_rc  = self._configure_drbdctrl(True, secret, bdev_0, bdev_1, port)
        return fn_rc


    def _debug_mod_drbdctrl(self, args):
        fn_rc = 1
        secret = args.pop(0)
        port   = args.pop(0)
        bdev_0 = args.pop(0)
        bdev_1 = args.pop(0)
        fn_rc  = self._configure_drbdctrl(False, secret, bdev_0, bdev_1, port)
        return fn_rc


    def _debug_list_nodes(self, args):
        fn_rc = 1
        title = "list: nodes"
        nodename = None
        try:
            nodename = args.pop(0)
        except IndexError:
            pass
        if nodename is not None:
            node = self._nodes.get(nodename)
            if node is not None:
                self._debug_section_begin(title)
                self._debug_dump_node(node)
                self._debug_section_end(title)
                fn_rc = 0
            else:
                self._debug_out.write("Node '%s' not found\n" % (nodename))
        else:
            self._debug_section_begin(title)
            for node in self._nodes.itervalues():
                self._debug_dump_node(node)
            self._debug_section_end(title)
            fn_rc = 0
        return fn_rc


    def _debug_list_resources(self, args):
        fn_rc = 1
        title = "list: resources"
        resname = None
        try:
            resname = args.pop(0)
        except IndexError:
            pass
        if resname is not None:
            resource = self._resources.get(resname)
            if resource is not None:
                self._debug_section_begin(title)
                self._debug_dump_resource(resource)
                self._debug_section_end(title)
                fn_rc = 0
            else:
                self._debug_out.write("Resource '%s' not found\n" % (resname))
        else:
            self._debug_section_begin(title)
            for resource in self._resources.itervalues():
                self._debug_dump_resource(resource)
            self._debug_section_end(title)
            fn_rc = 0
        return fn_rc


    def _debug_list_volumes(self, args):
        fn_rc = 1
        title = "list: resources"
        resname = None
        try:
            resname = args.pop(0)
        except IndexError:
            pass
        if resname is not None:
            resource = self._resources.get(resname)
            if resource is not None:
                self._debug_section_begin(title)
                self._debug_dump_volumes(resource)
                self._debug_section_end(title)
                fn_rc = 0
            else:
                self._debug_out.write("Resource '%s' not found\n" % (resname))
        else:
            self._debug_section_begin(title)
            for resource in self._resources.itervalues():
                self._debug_dump_volumes(resource)
            self._debug_section_end(title)
            fn_rc = 0
        return fn_rc


    def _debug_list_assignments(self, args):
        fn_rc = 1
        title = "list: assignments"
        objname = None
        try:
            objname = args.pop(0)
        except IndexError:
            pass
        if objname is not None:
            if objname.find("@") == 0:
                nodename = objname[1:]
                node = self._nodes.get(nodename)
                if node is not None:
                    self._debug_section_begin(title)
                    for assg in node.iterate_assignments():
                        self._debug_dump_assignment(assg)
                    self._debug_section_end(title)
                    fn_rc = 0
                else:
                    self._debug_out.write(
                        "Node '%s' not found\n" % (nodename)
                    )
            else:
                resource = self._resources.get(objname)
                if resource is not None:
                    self._debug_section_begin(title)
                    for assg in resource.iterate_assignments():
                        self._debug_dump_assignment(assg)
                    self._debug_section_end(title)
                    fn_rc = 0
                else:
                    self._debug_out.write(
                        "Resource '%s' not found\n" % (objname)
                    )
        else:
            self._debug_section_begin(title)
            for node in self._nodes.itervalues():
                for assg in node.iterate_assignments():
                    self._debug_dump_assignment(assg)
            self._debug_section_end(title)
            fn_rc = 0
        return fn_rc


    def _debug_list_snapshots(self, args):
        fn_rc = 1
        title = "list: snapshots"
        resname = None
        try:
            resname = args.pop(0)
        except IndexError:
            pass
        if resname is not None:
            resource = self._resources.get(resname)
            if resource is not None:
                self._debug_section_begin(title)
                for snapshot in resource.iterate_snapshots:
                    self._debug_dump_snapshot(snapshot)
                self._debug_section_end(title)
                fn_rc = 0
            else:
                self._debug_out.write("Resource '%s' not found\n" % (resname))
        else:
            self._debug_section_begin(title)
            for resource in self._resources.itervalues():
                for snapshot in resource.iterate_snapshots():
                    self._debug_dump_snapshot(snapshot)
            self._debug_section_end(title)
            fn_rc = 0
        return fn_rc


    def _debug_list_snapshot_assignments(self, args):
        fn_rc = 1
        title = "list: snapshot assignments"
        resname = None
        try:
            resname   = args.pop(0)
        except IndexError:
            pass
        snapsname = None
        try:
            snapsname = args.pop(0)
        except IndexError:
            pass
        if resname is not None and snapsname is not None:
            resource = self._resources.get(resname)
            if resource is not None:
                snapshot = resource.get_snapshot(snapsname)
                if snapshot is not None:
                    self._debug_section_begin(title)
                    for snaps_assg in snapshot.iterate_snaps_assgs():
                        self._debug_dump_snapshot_assignment(snaps_assg)
                    self._debug_section_end(title)
                    fn_rc = 0
                else:
                    self._debug_out.write(
                        "Snapshot '%s' not found\n" % (snapsname)
                    )
            else:
                self._debug_out.write("Resource '%s' not found\n" % (resname))
        else:
            self._debug_section_begin(title)
            for resource in self._resources.itervalues():
                for snapshot in resource.iterate_snapshots():
                    for snaps_assg in snapshot.iterate_snaps_assgs():
                        self._debug_dump_snapshot_assignment(snaps_assg)
            self._debug_section_end(title)
            fn_rc = 0
        return fn_rc


    def _debug_list_props(self, args):
        fn_rc        = 1
        title        = "list: object properties"
        props_format = "%-30s = %s\n"
        obj_class = None
        try:
            obj_class    = args.pop(0)
        except IndexError:
            pass
        obj_name = None
        try:
            obj_name     = args.pop(0)
        except IndexError:
            pass
        prop_key = None
        try:
            prop_key     = args.pop(0)
        except IndexError:
            pass
        props        = None
        if obj_class == "n":
            node = self._nodes.get(obj_name)
            if node is not None:
                props = node.get_props()
            else:
                self._debug_out.write("Node '%s' not found\n" % (obj_name))
        elif obj_class == "r":
            resource = self._resources.get(obj_name)
            if resource is not None:
                props = resource.get_props()
            else:
                self._debug_out.write("Resource '%s' not found\n" % (obj_name))
        elif obj_class == "v":
            split_idx = obj_name.find("/")
            if split_idx != -1:
                resname  = obj_name[:split_idx]
                resource = self._resources.get(resname)
                if resource is not None:
                    vol_nr = obj_name[split_idx + 1:]
                    try:
                        vol_id   = int(vol_nr)
                        volume   = resource.get_volume(vol_id)
                        if volume is not None:
                            props = volume.get_props()
                        else:
                            self._debug_out.write(
                                "Resource '%s' has no volume %d\n"
                                % (resname, vol_id)
                            )
                    except ValueError:
                        self._debug_out.write(
                            "Invalid volume id '%s'\n" % (vol_id)
                        )
                else:
                    self._debug_out.write(
                        "Resource '%s' not found\n" % (resname)
                    )
            else:
                resource = self._resources.get(obj_name)
                if resource is not None:
                    props = resource.get_props()
                else:
                    self._debug_out.write(
                        "Resource '%s' not found\n" % (obj_name)
                    )
        elif obj_class == "a":
            split_idx = obj_name.find("/")
            if split_idx != -1:
                nodename = obj_name[:split_idx]
                resname  = obj_name[split_idx + 1:]
                vol_nr   = None
                split_idx = resname.find("/")
                if split_idx != -1:
                    vol_nr  = resname[split_idx + 1:]
                    resname = resname[:split_idx]
                node     = self._nodes.get(nodename)
                resource = self._resources.get(resname)
                if node is not None and resource is not None:
                    assg = node.get_assignment(resource.get_name())
                    if assg is not None:
                        if vol_nr is not None:
                            try:
                                vol_id    = int(vol_nr)
                                vol_state = assg.get_volume_state(vol_id)
                                if vol_state is not None:
                                    props = vol_state.get_props()
                                else:
                                    self._debug_out.write(
                                        "Assignment '%s/%s' has no state for "
                                        "volume %d\n"
                                        % (node.get_name(),
                                           resource.get_name(),
                                           vol_id)
                                    )
                            except ValueError:
                                self._debug_out.write(
                                    "Invalid volume id '%s'\n"
                                    % (vol_nr)
                                )
                        else:
                            props = assg.get_props()
                    else:
                        self._debug_out.write(
                            "Assignment '%s/%s' not found\n"
                            % (node.get_name(), resource.get_name())
                        )
                else:
                    if resource is None:
                        self._debug_out.write(
                            "Resource '%s' not found\n"
                            % (resname)
                        )
                    if node is None:
                        self._debug_out.write(
                            "Node '%s' not found\n"
                            % (nodename)
                        )
        else:
            self._debug_out.write("Unknown object class '%s'\n" % (obj_class))
        if props is not None:
            if prop_key is None:
                self._debug_section_begin(title)
                for (prop_key, props_val) in props.iteritems():
                    self._debug_out.write(props_format % (prop_key, props_val))
                self._debug_section_end(title)
                fn_rc = 0
            else:
                props_val = props.get_prop(prop_key)
                if props_val is not None:
                    self._debug_section_begin(title)
                    self._debug_out.write(props_format % (prop_key, props_val))
                    self._debug_section_end(title)
                    fn_rc = 0
                else:
                    self._debug_out.write(
                        "Property '%s' not found\n" % prop_key
                    )
        return fn_rc


    def _debug_list_server_conf(self, args):
        title = "list: server configuration"
        self._debug_section_begin(title)
        self._debug_list_conf(args, self._conf)
        self._debug_section_end(title)
        return 0


    def _debug_list_cluster_conf(self, args):
        title = "list: cluster configuration"
        self._debug_section_begin(title)
        self._debug_list_props_container(args, self._cluster_conf)
        self._debug_section_end(title)
        return 0


    def _debug_list_conf(self, args, conf):
        keyval_format    = "%-30s = %s\n"
        key_unset_format = "Key '%s' not found\n"
        val_unset_format = "%-30s is unset\n"
        key = None
        try:
            key = args.pop(0)
        except IndexError:
            pass
        if key is not None:
            try:
                val = conf[key]
                if val is not None:
                    self._debug_out.write(keyval_format % (key, val))
                else:
                    self._debug_out.write(val_unset_format % (key))
            except KeyError:
                self._debug_out.write(key_unset_format % (key))
        else:
            for (key, val) in conf.iteritems():
                if val is not None:
                    self._debug_out.write(keyval_format % (key, val))
                else:
                    self._debug_out.write(val_unset_format % (key))


    def _debug_list_props_container(self, args, conf):
        keyval_format    = "%-30s = %s\n"
        key_unset_format = "Key '%s' not found\n"
        key = None
        try:
            key = args.pop(0)
        except IndexError:
            pass
        if key is not None:
            val = conf.get_prop(key)
            if val is not None:
                self._debug_out.write(keyval_format % (key, val))
            else:
                self._debug_out.write(key_unset_format % (key))
        else:
            for (key, val) in conf.iteritems():
                self._debug_out.write(keyval_format % (key, val))


    def _debug_dump_node(self, node):
        self._debug_out.write(
            "  ID:%-18s NID:%2d AF:%-2u ADDR:%-16s S:0x%.16x\n"
            % (node.get_name(), node.get_node_id(),
               node.get_addrfam(), node.get_addr(), node.get_state())
        )


    def _debug_dump_resource(self, resource):
        self._debug_out.write(
            "  ID:%-18s P:%.5u S:0x%.16x\n"
            % (resource.get_name(), int(resource.get_port()),
               resource.get_state())
        )


    def _debug_dump_volumes(self, resource):
        self._debug_out.write(
            "  R/ID:%-18s\n"
            % (resource.get_name())
        )
        for volume in resource.iterate_volumes():
            vol_size_kiB = volume.get_size_kiB()
            self._debug_out.write(
                "  * V/ID:%.5u M:%.7u SIZE:%.13u S:0x%.16x\n"
                % (volume.get_id(), volume.get_minor().get_value(),
                   vol_size_kiB, volume.get_state())
            )


    def _debug_dump_assignment(self, assg):
        node     = assg.get_node()
        resource = assg.get_resource()
        self._debug_out.write(
            "  N/ID:%-18s R/ID:%-18s\n"
            % (node.get_name(), resource.get_name())
        )
        self._debug_out.write(
            "  '- S/C:0x%.16x S/T:0x%.16x\n"
            % (assg.get_cstate(),
               assg.get_tstate())
        )
        for vol_state in assg.iterate_volume_states():
            vol_bdev_path = vol_state.get_bd_path()
            if vol_bdev_path is None:
                vol_bdev_path = "(unset)"
            self._debug_out.write(
                "  * V/ID:%.5u S/C:0x%.16x S/T:0x%.16x\n"
                % (vol_state.get_id(),
                   vol_state.get_cstate(),
                   vol_state.get_tstate())
            )
            self._debug_out.write(
                "  '- BD:%s\n" % (vol_bdev_path)
            )
        for snaps_assg in assg.iterate_snaps_assgs():
            self._debug_dump_snapshot_assignment(snaps_assg)


    def _debug_dump_snapshot(self, snapshot):
        resource = snapshot.get_resource()
        self._debug_out.write(
            "  R/ID:%-18s S/ID:%-18s\n"
            % (resource.get_name(), snapshot.get_name())
        )


    def _debug_dump_snapshot_assignment(self, snaps_assg):
        assg     = snaps_assg.get_assignment()
        snapshot = snaps_assg.get_snapshot()
        node     = assg.get_node()
        self._debug_dump_snapshot(snapshot)
        self._debug_out.write(
            "  '- N/ID: %s\n"
            % (node.get_name())
        )
        self._debug_out.write(
            "     '- S/C:0x%.16x S/T:0x%.16x\n"
            % (snaps_assg.get_cstate(), snaps_assg.get_tstate())
        )
        for snaps_vol_state in snaps_assg.iterate_snaps_vol_states():
            self._debug_out.write(
                "     * V/ID:%.5u S/C:0x%.16x S/T:0x%.16x\n"
                % (snaps_vol_state.get_id(), snaps_vol_state.get_cstate(),
                   snaps_vol_state.get_tstate())
            )


    def _debug_set_node(self, args):
        fn_rc = 1
        nodename = None
        try:
            nodename = args.pop(0)
        except IndexError:
            pass
        if nodename is not None:
            node = self._nodes.get(nodename)
            if node is not None:
                keyval = None
                try:
                    keyval = args.pop(0)
                    key, val = self._debug_keyval_split(keyval)
                    if key == "state":
                        try:
                            state_update = long(val)
                            node.set_state(state_update)
                            fn_rc = 0
                        except ValueError:
                            pass
                except IndexError:
                    self._debug_out.write("Missing argument\n")
            else:
                self._debug_out.write("Node '%s' not found\n" % (nodename))
        return fn_rc


    def _debug_set_resource(self, args):
        fn_rc = 1
        resname = None
        try:
            resname = args.pop(0)
        except IndexError:
            pass
        if resname is not None:
            resource = self._resources.get(resname)
            if resource is not None:
                keyval = None
                try:
                    keyval = args.pop(0)
                    key, val = self._debug_keyval_split(keyval)
                    if key == "state":
                        try:
                            state_update = long(val)
                            resource.set_state(state_update)
                            fn_rc = 0
                        except ValueError:
                            pass
                except IndexError:
                    self._debug_out.write("Missing argument\n")
            else:
                self._debug_out.write("Resource '%s' not found\n" % (resname))
        return fn_rc


    def _debug_set_volume(self, args):
        fn_rc = 1
        resname = None
        try:
            resname    = args.pop(0)
        except IndexError:
            pass
        if resname is not None:
            vol_id_str = None
            split_idx  = resname.find("/")
            if split_idx != -1:
                vol_id_str = resname[split_idx + 1:]
                resname    = resname[:split_idx]
            resource = self._resources.get(resname)
            if resource is not None and vol_id_str is not None:
                try:
                    vol_id = int(vol_id_str)
                    volume = resource.get_volume(vol_id)
                    if volume is not None:
                        try:
                            keyval = args.pop(0)
                            key, val = self._debug_keyval_split(keyval)
                            if key == "state":
                                state_update = long(val)
                                volume.set_state(state_update)
                                fn_rc = 0
                        except IndexError:
                            self._debug_out.write("Missing argument\n")
                    else:
                        self._debug_out.write(
                            "Invalid volume index %u for resource '%s'\n"
                            % (vol_id, resource.get_name())
                        )
                except ValueError:
                    pass
            else:
                self._debug_out.write("Resource '%s' not found\n" % (resname))
        return fn_rc


    def _debug_set_assignment(self, args):
        fn_rc = 1
        nodename = None
        resname = None
        vol_flag = False

        try:
            nodename = args.pop(0)
        except IndexError:
            pass
        split_idx = nodename.find("/")
        if split_idx != -1:
            resname = nodename[split_idx + 1:]
            nodename = nodename[:split_idx]

        vol_id_str = None
        split_idx = resname.find("/")
        if split_idx != -1:
            vol_flag = True
            vol_id_str = resname[split_idx + 1:]
            resname = resname[:split_idx]

        vol_id = -1
        if vol_flag:
            try:
                vol_id = int(vol_id_str)
            except (TypeError, ValueError):
                self._debug_out.write("Invalid volume id\n")

        if nodename is not None and resname is not None and (vol_id >= 0 or not vol_flag):
            node     = self._nodes.get(nodename)
            resource = self._resources.get(resname)
            if node is not None and resource is not None:
                assg = node.get_assignment(resource.get_name())
                if assg is not None:
                    if not vol_flag:
                        try:
                            keyval = args.pop(0)
                            key, val = self._debug_keyval_split(keyval)
                            if key == "cstate":
                                state_update = long(val)
                                assg.set_cstate(state_update)
                                fn_rc = 0
                            elif key == "tstate":
                                state_update = long(val)
                                assg.set_tstate(state_update)
                                fn_rc = 0
                        except ValueError:
                            self._debug_out.write("Invalid value\n")
                        except IndexError:
                            self._debug_out.write("Missing argument\n")
                    else:
                        vol_state = assg.get_volume_state(vol_id)
                        if vol_state is not None:
                            keyval = args.pop(0)
                            key, val = self._debug_keyval_split(keyval)
                            if key == "cstate":
                                state_update = long(val)
                                vol_state.set_cstate(state_update)
                                fn_rc = 0
                            elif key == "tstate":
                                state_update = long(val)
                                vol_state.set_tstate(state_update)
                                fn_rc = 0
                        else:
                            self._debug_out.write(
                                "Assignment '%s/%s' has no volume id %d\n"
                                % (node.get_name(), resource.get_name(), vol_id)
                            )
                else:
                    self._debug_out.write(
                        "Resource '%s' is not assigned to node '%s'\n"
                        % (resource.get_name(), node.get_name())
                    )
            else:
                if node is None:
                    self._debug_out.write("Node '%s' not found\n" % (nodename))
                if resource is None:
                    self._debug_out.write("Resource '%s' not found\n" % (resname))
        return fn_rc


    def _debug_set_snapshot(self, args):
        return 1


    def _debug_set_snapshot_assignment(self, args):
        return 1


    def _debug_parse_flag(self, val):
        """
        Convert a string argument to boolean values
        """
        if val == "1":
            flag = True
        elif val == "0":
            flag = False
        else:
            raise SyntaxException
        return flag


    def _debug_parse_loglevel(self, val):
        """
        Convert a string argument to a standard log level
        """
        loglevel = None
        try:
            loglevel = self.DM_LOGLEVELS[val.upper()]
        except KeyError:
            raise SyntaxException
        return loglevel


    def _debug_set_debug_out(self, val):
        """
        Connects the debug output channel to an output stream
        """
        fn_rc = 1
        try:
            if (self._debug_out is not sys.stdout and
                self._debug_out is not sys.stderr):
                    try:
                        self._debug_out.close()
                    except (IOError, OSError, AttributeError):
                        pass
            if val == "stdout":
                self._debug_out = sys.stdout
                fn_rc = 0
            elif val == "stderr":
                self._debug_out = sys.stderr
                fn_rc = 0
            elif val == "file":
                self._debug_out = None
                out_file = self.get_conf_value(self.KEY_DEBUG_OUT_FILE)
                if out_file is not None:
                    self._debug_out = open(out_file, "a+")
                    fn_rc = 0
                else:
                    logging.error(
                        "The configuration entry '%s' is missing, "
                        "debug output redirected to stderr\n"
                        % (self.KEY_DEBUG_OUT_FILE)
                    )
            else:
                raise SyntaxException
        except (IOError, OSError):
            pass
        finally:
            if self._debug_out is None:
                self._debug_out = sys.stderr
        return fn_rc


    def _debug_keyval_split(self, keyval):
        split_idx = keyval.find("=")
        key = keyval[:split_idx].lower()
        val = keyval[split_idx + 1:]
        return (key, val)


    def _debug_section_begin(self, title):
        self._debug_section_generic("BEGIN:", title)


    def _debug_section_end(self, title):
        self._debug_section_generic("END:", title)


    def _debug_section_generic(self, prefix, title):
        # the prefix should not be longer than 6 characters
        section_ruler = "== DEBUG == %-6s %s ==" % (prefix, title)
        title_len = len(title)
        # extend the "=" line up to a total length of 75
        # characters (added up with the text prefix, that's
        # the magic '53' remaining characters here)
        repeat = 53 - title_len if title_len <= 53 else 0
        section_ruler += ("=" * repeat) + "\n"
        self._debug_out.write(section_ruler)


    def shutdown(self, props={}):
        """
        Stops this drbdmanage server instance
        """
        logging.info("server shutdown (requested by shutdown API call)")
        try:
            shutdown_sat_str = props.get(KEY_S_CMD_SHUTDOWN)
            if shutdown_sat_str is not None:
                shutdown_sat = string_to_bool(shutdown_sat_str)
                if shutdown_sat:
                    logging.info("shutting down satellites")
                    for satellite in [sat for sat, state in self.sat_state_ctrlvol.items()
                                      if state == SAT_CON_ESTABLISHED or state == SAT_CON_SHUTDOWN]:
                        self._proxy.send_cmd(satellite, KEY_S_CMD_SHUTDOWN)
        except ValueError:
            # raised if string_to_bool() is supplied with an invalid value
            pass

        try:
            shutdown_res_str = props.get(KEY_SHUTDOWN_RES)
            if shutdown_res_str is not None:
                shutdown_res = string_to_bool(shutdown_res_str)
                if shutdown_res:
                    logging.info("shutting down drbdmanage-controlled resources")
                    self._drbd_mgr.final_down()
        except ValueError:
            # string_to_bool() again
            pass

        logging.info("shutting down the TCPServer")
        if self._proxy:
            self._proxy.shutdown()

        logging.info("shutting down the control volume")
        try:
            self._drbd_mgr.down_drbdctrl()
        except:
            pass

        logging.info("shutting down DRBD events processing")
        # Shutdown events processing and the associated child process
        try:
            self.uninit_events()
        except:
            pass

        logging.info("server shutdown complete, exiting")
        exit(0)


    def get_occupied_minor_nrs(self):
        """
        Retrieves a list of occupied (in-use) minor numbers

        @return list of minor numbers that are currently in use
        """
        minor_list = []
        try:
            min_nr = int(self._conf[self.KEY_MIN_MINOR_NR])
            for resource in self._resources.itervalues():
                for vol in resource.iterate_volumes():
                    minor_obj = vol.get_minor()
                    nr_item = minor_obj.get_value()
                    if nr_item >= min_nr and nr_item <= MinorNr.MINOR_NR_MAX:
                        minor_list.append(nr_item)
        except ValueError:
            minor_list = None
        return minor_list


    def get_free_minor_nr(self, minor_list):
        """
        Retrieves a free (unused) minor number

        Minor numbers are allocated in the range from the configuration value
        KEY_MIN_MINOR_NR to the constant MinorNr.MINOR_NR_MAX. A minor number
        that is unique across the drbdmanage cluster is allocated for each
        volume.

        @return: next free minor number; or -1 on error
        """
        try:
            min_nr = int(self._conf[self.KEY_MIN_MINOR_NR])
            minor_nr = get_free_number(min_nr, MinorNr.MINOR_NR_MAX,
                                       minor_list)
            if minor_nr == -1:
                raise ValueError
        except ValueError:
            minor_nr = MinorNr.MINOR_NR_ERROR
        return minor_nr


    def is_free_port_nr(self, port):
        """
        Checks whether a specified port number is allocated or not

        @return: True if the specified port number is unallocated, False otherwise
        """
        is_free = True
        for resource in self._resources.itervalues():
            used_port = resource.get_port()
            if port == used_port:
                is_free = False
        return is_free


    def is_free_minor_nr(self, minor):
        """
        Checks whether a specified minor number is allocated or not

        @return: True if the specified minor number is unallocated, False otherwise
        """
        used_minors = self.get_occupied_minor_nrs()
        is_free = minor not in used_minors
        return is_free


    def get_free_port_nr(self):
        """
        Retrieves a free (unused) network port number

        Port numbers are allocated in the range of the configuration values
        KEY_MIN_PORT_NR..KEY_MAX_PORT_NR. A port number that is unique
        across the drbdmanage cluster is allocated for each resource.

        @return: next free network port number; or -1 on error
        """
        min_nr = int(self._conf[self.KEY_MIN_PORT_NR])
        max_nr = int(self._conf[self.KEY_MAX_PORT_NR])

        port_list = []
        for resource in self._resources.itervalues():
            nr_item = resource.get_port()
            if nr_item >= min_nr and nr_item <= max_nr:
                port_list.append(nr_item)
        port = get_free_number(min_nr, max_nr, port_list)
        if port == -1:
            port = RES_PORT_NR_ERROR
        return port


    def get_free_node_id(self, resource):
        """
        Retrieves a free (unused) node id number

        Node IDs range from 0 to the configuration value of KEY_MAX_NODE_ID
        and are allocated per resource (the node IDs of the same nodes can
        differ from one assigned resource to another)

        @return: next free node id number; or DrbdNode.NODE_ID_NONE on error
        """
        max_node_id = int(self._conf[self.KEY_MAX_NODE_ID])
        id_list = []
        for assg in resource.iterate_assignments():
            id_item = assg.get_node_id()
            if id_item >= 0 and id_item <= int(max_node_id):
                id_list.append(id_item)
        node_id = get_free_number(0, int(max_node_id), id_list)
        if node_id == -1:
            node_id = DrbdNode.NODE_ID_NONE
        return node_id


    def get_free_drbdctrl_node_id(self):
        """
        Retrieves a free (unused) node id number

        Node IDs range from 0 to the configuration value of KEY_MAX_NODE_ID
        and are allocated per resource (the node IDs of the same nodes can
        differ from one assigned resource to another)

        @return: next free node id number; or DrbdNode.NODE_ID_NONE on error
        """
        max_node_id = int(self._conf[self.KEY_MAX_NODE_ID])

        id_list = []
        for node in self._nodes.itervalues():
            id_item = node.get_node_id()
            if id_item >= 0 and id_item <= max_node_id:
                id_list.append(id_item)
        node_id = get_free_number(0, max_node_id, id_list)
        if node_id == -1:
            node_id = DrbdNode.NODE_ID_NONE
        return node_id


    def get_free_volume_id(self, resource):
        """
        Retrieves a free (unused) volume id number

        Volume IDs range from 0 to MAX_RES_VOLS and are allocated per resource

        @return: next free volume id number; or -1 on error
        """
        id_list = []
        for vol in resource.iterate_volumes():
            id_item = vol.get_id()
            if id_item >= 0 and id_item <= DrbdResource.MAX_RES_VOLS:
                id_list.append(id_item)
        vol_id = get_free_number(0, DrbdResource.MAX_RES_VOLS, id_list)
        return vol_id


    def catch_internal_error(self, exc):
        # http://stackoverflow.com/questions/5736080/
        # sys-exc-info1-type-and-format-in-python-2-71
        #
        # (obviously, you have to remove the newline from the link above)
        expl = "Internal error (error traceback failed)"
        args = {}
        try:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            exc_text = (traceback.format_exception_only(exc_type, exc_obj))[0]
            tb = traceback.extract_tb(exc_tb, 3)
            # Everything passed as string, to make dbus happy
            args =  {
                "file1": tb[0][0],
                "line1": str(tb[0][1]),
                'exc':   exc_text.strip()
            }
            expl = "Internal error: In %(file1)s@%(line1)s: %(exc)s"
            if len(tb) > 1:
                args["file2"] = tb[1][0]
                args["line2"] = str(tb[1][1])
                expl += "; called from %(file2)s@%(line2)s"
            logging.critical(expl % args)
            logging.debug("--- start stack trace")
            for tb_entry in traceback.format_tb(exc_tb):
                logging.debug(tb_entry)
            logging.debug("--- end stack trace")
            self.end_modify_conf(self._locked_persist)
        except Exception:
            pass
        fn_rc_args = []
        for key, value in args.iteritems():
            fn_rc_args.append([key, value])
        return (expl, fn_rc_args)


    def catch_and_append_internal_error(self, fn_rc, exc):
        msg, args = self.catch_internal_error(exc)
        add_rc_entry(fn_rc, DM_DEBUG, msg, args)
        add_rc_entry(
            fn_rc, DM_DEBUG, "%(versioninfo)s",
            [
                ["versioninfo", DM_VERSION + '; ' + DM_GITHASH]
            ]
        )


"""
Tracing - may be used for debugging
"""
def traceit(frame, event, arg):
    if event == "line":
        lineno = frame.f_lineno
        print(frame.f_code.co_filename, ":", "line", lineno)
    return traceit

"""
Uncomment the statement below to enable tracing
"""
#sys.settrace(traceit)
