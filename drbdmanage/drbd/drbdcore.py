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

import sys
import logging
import drbdmanage.consts as consts
import drbdmanage.conf.conffile
import drbdmanage.snapshots.snapshots as snapshots
import drbdmanage.exceptions as dmexc
import drbdmanage.drbd.commands as drbdcmd

"""
WARNING!
  do not import anything from drbdmanage.drbd.persistence
"""
from drbdmanage.storage.storagecommon import GenericStorage
from drbdmanage.drbd.drbdcommon import GenericDrbdObject
from drbdmanage.exceptions import (
    InvalidAddrFamException, VolSizeRangeException, PersistenceException
)
from drbdmanage.exceptions import DM_SUCCESS
from drbdmanage.utils import MetaData
from drbdmanage.utils import Selector, bool_to_string, is_set, is_unset


class DrbdManager(object):

    """
    Manages deployment/undeployment of DRBD resources
    """

    _server  = None
    _drbdadm = None
    _resconf = None

    # Used as a return code to indicate that drbdadm could not be executed
    DRBDADM_EXEC_FAILED = 127

    # Used as a return code to indicate that undeploying volumes failed
    STOR_UNDEPLOY_FAILED = 126


    def __init__(self, ref_server):
        logging.debug("DrbdManager: Enter function __init__()")
        self._server  = ref_server
        self._resconf = drbdmanage.conf.conffile.DrbdAdmConf()
        self.reconfigure()
        logging.debug("DrbdManager: Exit function __init__()")


    # FIXME
    # TODO: currently, if the configuration can not be loaded (due to
    #       errors in the data structures on the disk), the server's
    #       hash is not updated, so the DrbdManager can begin to loop,
    #       trying to load the new configuration. If DrbdManager fails
    #       to load the configuration, it should probably rather stop
    #       than loop at some point.
    def run(self, override_hash_check, poke_cluster):
        """
        Performs actions to reflect changes to the drbdmanage configuration

        If the configuration on the drbdmanage control volume has changed,
        this function attempts to reach the target state that is set for
        each resource in the configuration.
        According to the target state of each resource, resources/volumes
        are deployed/undeployed/attached/detached/connected/disconnected/etc...

        If override_hash_check is set, then the hash comparison that
        determines whether the configuration has changed is skipped,
        and the function continues as if the configuration has changed, even
        if it actually has not changed.

        If poke_cluster is set and a check for changes requested for the local
        node is carried out, then an update of the serial number is forced.
        This will implicitly change the configuration, thereby notifying
        all cluster nodes to perform any requested changes.

        Flags state summary:
            override_hash_check == False and poke_cluster == False:
                If the hash has changed, check for and run local changes
            override_hash_check == False and poke_cluster == True:
                If the hash has changed, check for and run local changes and
                force an update of the serial number
            override_hash_check == True and poke_cluster == False:
                Always check for and run local changes
            override_hash_check == True and poke_cluster == True:
                Always check for and run local changes and force an update
                of the serial number

        @type:  override_hash_check: bool
        @type:  poke_cluster:        bool
        """
        logging.debug("DrbdManager: Enter function run()")
        persist = None
        logging.debug("DrbdManager: invoked")
        try:
            # Always perform changes requested for the local node if the
            # hash check is overridden
            run_changes = override_hash_check
            if override_hash_check == False:
                # check whether the configuration hash has changed
                persist = self._server.open_conf()
                if persist is not None:
                    logging.debug("DrbdManager: hash check: %s"
                                  % persist.get_stored_hash())
                    # if the configuration changed, enable performing
                    # changes requested for the local node
                    if self._server.hashes_match(persist.get_stored_hash()):
                        logging.debug("DrbdManager: hash unchanged")
                    else:
                        logging.debug("DrbdManager: hash changed")
                        run_changes = True
                    persist.close()
                else:
                    logging.debug("DrbdManager: cannot open the "
                                  "control volume (read-only)")

            if run_changes == True:
                # close the read-only stream, then lock and open the
                # configuration for reading and writing
                persist = self._server.begin_modify_conf()
                if persist is not None:
                    old_serial = self._server.peek_serial()
                    changed    = self.perform_changes()
                    new_serial = self._server.peek_serial()
                    if (poke_cluster == True and new_serial == old_serial):
                        # increase the serial number, implicitly changing the
                        # hash and thereby running requested changes on all
                        # cluster nodes
                        self._server.get_serial()
                        changed = True
                    if changed == True:
                        logging.debug("DrbdManager: state changed, "
                                      "saving control volume data")
                        self._server.save_conf_data(persist)
                    else:
                        logging.debug("DrbdManager: state unchanged")
                else:
                    logging.debug("DrbdManager: cannot open the "
                                  "control volume (read-write)")
        except PersistenceException:
            exc_type, exc_obj, exc_trace = sys.exc_info()
            logging.debug("DrbdManager: caught PersistenceException:")
            logging.debug("Stack trace:\n%s" % (str(exc_trace)))
        except Exception as exc:
            exc_type, exc_obj, exc_trace = sys.exc_info()
            logging.debug("DrbdManager: abort, unhandled exception: %s"
                          % (str(exc)))
            logging.debug("Stack trace:\n%s" % (str(exc_trace)))
        finally:
            # end_modify_conf() also works for both, read-only and
            # read-write streams
            self._server.end_modify_conf(persist)
            logging.debug("DrbdManager: finished")
        logging.debug("DrbdManager: Exit function run()")


    # FIXME
    # - still requires more error handling
    # - external configuration file (for drbdadm before-resync-target, etc.)
    #   probably needs to be updated in some more locations
    # - some things, like deploying a resource, still need to become more
    #   robust, because sometimes meta data fails to initialize, etc., and
    #   there needs to be correction code for those cases
    def perform_changes(self):
        """
        Calls worker functions for required resource state changes

        Determines which state changes are required for each resource and
        calls functions to attempt to reach the desired target state.

        @return: True if the state of any resource has changed, False otherwise
        @rtype:  bool
        """
        logging.debug("DrbdManager: Enter function perform_changes()")
        state_changed = False
        pool_changed  = False

        """
        Check whether the system the drbdmanaged server is running on is
        a registered node in the configuration
        """
        node = self._server.get_instance_node()
        if node is None:
            logging.warning(
                "DrbdManager: abort, this node ('%s') has no entry in "
                "the data tables"
                % (self._server.get_instance_node_name())
            )
            return False


        """
        Check for changes of the cluster configuration (node members)
        """
        node_state = node.get_state()
        if is_set(node_state, DrbdNode.FLAG_UPDATE):
            state_changed = True
            self._server.reconfigure_drbdctrl()
            node.set_state(node_state ^ DrbdNode.FLAG_UPDATE)

        """
        Check all assignments and snapshots for changes
        """
        for assg in node.iterate_assignments():
            # Assignment changes
            (set_state_changed, set_pool_changed) = (
                self._assignment_actions(assg)
            )
            if set_state_changed:
                state_changed = True
            if set_pool_changed:
                pool_changed = True
            # Snapshot changes
            (set_state_changed, set_pool_changed) = (
                self._snapshot_actions(assg)
            )
            if set_state_changed:
                state_changed = True
            if set_pool_changed:
                pool_changed = True

        """
        Cleanup the server's data structures
        (remove entries that are no longer required)
        """
        self._server.cleanup()

        if pool_changed:
            self._server.update_pool_data()

        logging.debug("DrbdManager: Exit function perform_changes()")
        return state_changed


    def _assignment_actions(self, assg):
        """
        ============================================================
        Actions for assignments
        (concerning all volumes of a resource)
        ============================================================
        """
        logging.debug("DrbdManager: Enter function _assignment_actions()")
        state_changed  = False
        pool_changed   = False
        failed_actions = False
        act_flag = assg.requires_action()
        if act_flag and assg.is_empty():
            if assg.requires_undeploy():
                # Assignment has no volumes deployed and is effectively
                # disabled; Nothing to do, except for setting the correct
                # state, so the assignment can be cleaned up
                assg.set_tstate(0)
                assg.undeploy_adjust_cstate()
                state_changed = True
        elif act_flag:
            logging.debug(
                "assigned resource %s cstate(%x)->tstate(%x)"
                % (assg.get_resource().get_name(),
                   assg.get_cstate(), assg.get_tstate())
            )
            state_changed = True

            """
            Undeploy an assignment/resource and all of its volumes
            """
            if assg.requires_undeploy():
                pool_changed = True
                fn_rc = self._undeploy_assignment(assg)
                assg.set_rc(fn_rc)
                if fn_rc != 0:
                    failed_actions = True
            else:
                """
                Disconnect an assignment/resource
                """
                if assg.requires_disconnect():
                    fn_rc = self._disconnect(assg)
                    assg.set_rc(fn_rc)
                    if fn_rc != 0:
                        failed_actions = True

                """
                Update connections
                """
                assg_actions = assg.get_tstate()
                if (not failed_actions):
                    if is_set(assg_actions, Assignment.FLAG_UPD_CON):
                        fn_rc = self._update_connections(assg)
                        assg.set_rc(fn_rc)
                        if fn_rc != 0:
                            failed_actions = True
                    if is_set(assg_actions, Assignment.FLAG_RECONNECT):
                        fn_rc = self._reconnect(assg)
                        assg.set_rc(fn_rc)
                        if fn_rc != 0:
                            failed_actions = True

                """
                ============================================================
                Per-Volume actions
                (actions that concern a single volume of a resource)
                ============================================================
                """
                for vol_state in assg.iterate_volume_states():
                    (set_pool_changed, set_failed_actions) = (
                        self._volume_actions(assg, vol_state)
                    )
                    if set_pool_changed:
                        pool_changed = True
                    if set_failed_actions:
                        failed_actions = True

                """
                ============================================================
                Actions for assignments (continuation)
                (concerning all volumes of a resource)
                ============================================================
                """

                # FIXME: this should probably be changed so that the other
                #        actions (deploy/connect/etc.) do not operate on the
                #        assignment any more if it has no active volumes;
                #        for now, this just skips every other action and
                #        sets the assignment's current state to 0 again to
                #        make sure that it can be cleaned up as soon as its
                #        target state changes to 0 (e.g., unassign/undeploy).
                if assg.is_empty():
                    if assg.get_cstate() != 0:
                        assg.undeploy_adjust_cstate()
                        state_changed = True
                else:
                    """
                    Deploy an assignment (finish deploying)
                    Volumes have already been deployed by the per-volume
                    actions at this point. Only if all volumes that should be
                    deployed have been deployed (current state vs.
                    target state), then mark the assignment as deployed, too.
                    """
                    if (not failed_actions) and assg.requires_deploy():
                        pool_changed = True
                        fn_rc = self._deploy_assignment(assg)
                        assg.set_rc(fn_rc)
                        if fn_rc != 0:
                            failed_actions = True

                    if (not failed_actions) and assg.requires_connect():
                        fn_rc = self._connect(assg)
                        assg.set_rc(fn_rc)
                        if fn_rc != 0:
                            failed_actions = True

                    # TODO: Check whether all volumes are actually diskless
                    #       (e.g., bd_name/bd_path == NULL)
                    if is_set(assg.get_tstate(), Assignment.FLAG_DISKLESS):
                        assg.set_cstate_flags(Assignment.FLAG_DISKLESS)

        logging.debug("DrbdManager: Exit function _assignment_actions()")
        return (state_changed, pool_changed)


    def _volume_actions(self, assg, vol_state):
        """
        Deploy or undeploy a volume
        """
        logging.debug("DrbdManager: Enter function _volume_actions()")
        pool_changed   = False
        failed_actions = False

        if vol_state.requires_undeploy():
            pool_changed = True
            fn_rc = self._undeploy_volume(assg, vol_state)
            assg.set_rc(fn_rc)
            if fn_rc != 0:
                failed_actions = True
        else:
            if vol_state.requires_deploy():
                pool_changed = True
                fn_rc = self._deploy_volume_actions(assg, vol_state)
                assg.set_rc(fn_rc)
                if fn_rc != 0:
                    failed_actions = True
            """
            Attach a volume to or detach a volume from local storage
            """
            if (not failed_actions):
                if vol_state.requires_attach():
                    fn_rc = self._attach(assg, vol_state)
                    assg.set_rc(fn_rc)
                    if fn_rc != 0:
                        failed_actions = True
                elif vol_state.requires_detach():
                    fn_rc = self._detach(assg, vol_state)
                    assg.set_rc(fn_rc)
                    if fn_rc != 0:
                        failed_actions = True

        logging.debug("DrbdManager: Exit function _volume_actions()")
        return (pool_changed, failed_actions)


    def _snapshot_actions(self, assg):
        logging.debug("DrbdManager: Enter function _snapshot_actions()")
        state_changed  = False
        failed_actions = False
        pool_changed   = False
        # Operate only on deployed assignments
        for snaps_assg in assg.iterate_snaps_assgs():
            assg_tstate = assg.get_tstate()
            snaps_name = snaps_assg.get_snapshot().get_resource().get_name()
            if snaps_assg.requires_deploy() or snaps_assg.requires_undeploy():
                logging.debug(
                    "snapshot %s/%s cstate(%x)->tstate(%x)"
                    % (snaps_assg.get_snapshot().get_resource().get_name(),
                       snaps_assg.get_snapshot().get_name(),
                       snaps_assg.get_cstate(), snaps_assg.get_tstate())
                )
            if snaps_assg.requires_deploy():
                assg_cstate    = assg.get_cstate()
                assg_tstate    = assg.get_tstate()
                if (is_set(assg_cstate, Assignment.FLAG_DEPLOY) and
                    is_set(assg_tstate, Assignment.FLAG_DEPLOY)):
                    error_code = snaps_assg.get_error_code()
                    if error_code == 0:
                        state_changed   = True
                        snaps_vol_iter = snaps_assg.iterate_snaps_vol_states()
                        for snaps_vol_state in snaps_vol_iter:
                            (set_pool_changed, set_failed_actions) = (
                                self._snaps_deploy_volume(
                                    snaps_assg, snaps_vol_state
                                )
                            )
                            if set_pool_changed:
                                pool_changed = True
                            if set_failed_actions:
                                failed_actions = True
                        if not failed_actions:
                            snaps_assg.set_cstate_flags(
                                snapshots.DrbdSnapshotAssignment.FLAG_DEPLOY
                            )
                    else:
                        logging.debug(
                            "snapshot assignment %s/%s is marked FAILED, "
                            "error code = %d"
                            % (snaps_name,
                               snaps_assg.get_snapshot().get_name(),
                               error_code)
                        )
                else:
                    logging.info(
                        "Cannot create snapshot %s/%s, "
                        "source assignment is not deployed"
                        % (snaps_name, snaps_assg.get_snapshot().get_name())
                    )
                    failed_actions = True
            elif (snaps_assg.requires_undeploy() or
                  is_unset(assg_tstate, Assignment.FLAG_DEPLOY)):
                state_changed = True
                for snaps_vol_state in snaps_assg.iterate_snaps_vol_states():
                    (set_pool_changed, set_failed_actions) = (
                        self._snaps_undeploy_volume(
                            snaps_assg, snaps_vol_state
                        )
                    )
                    if set_pool_changed:
                        pool_changed = True
                    if set_failed_actions:
                        failed_actions = True
                if not failed_actions:
                    snaps_assg.set_cstate(0)
                    snaps_assg.set_tstate(0)
            else:
                for snaps_vol_state in snaps_assg.iterate_snaps_vol_states():
                    vol_cstate = snaps_vol_state.get_cstate()
                    vol_tstate = snaps_vol_state.get_tstate()
                    if vol_tstate != vol_cstate:
                        logging.debug(
                            "snapshot %s/%s #%u cstate(%x)->tstate(%x)"
                            % (snaps_name,
                               snaps_assg.get_snapshot().get_name(),
                               snaps_vol_state.get_id(),
                               vol_cstate, vol_tstate)
                        )
                        (set_pool_changed, set_failed_actions) = (
                            self._snaps_volume_actions(
                                snaps_assg, snaps_vol_state
                            )
                        )
                        if set_pool_changed:
                            pool_changed = True
                        if set_failed_actions:
                            failed_actions = True
        logging.debug("DrbdManager: Exit function _snapshot_actions()")
        return (state_changed, pool_changed)


    def _snaps_volume_actions(self, snaps_assg, snaps_vol_state):
        logging.debug("DrbdManager: Enter function _snaps_volume_actions()")
        pool_changed   = False
        failed_actions = False
        snaps          = snaps_assg.get_snapshot()
        resource       = snaps.get_resource()
        logging.debug(
            "_snaps_volume_actions(): snapshot volume %s/%s #%d "
            "cstate(%x)->tstate(%x)"
            % (resource.get_name(), snaps.get_name(),
               snaps_vol_state.get_id(),
               snaps_vol_state.get_cstate(), snaps_vol_state.get_tstate())
        )
        if snaps_vol_state.requires_deploy():
            # Deploy snapshots
            pool_changed, failed_actions = (
                self._snaps_deploy_volume(snaps_assg, snaps_vol_state)
            )
        elif snaps_vol_state.requires_undeploy():
            # Undeploy snapshots
            pool_changed, failed_actions = (
                self._snaps_undeploy_volume(snaps_assg, snaps_vol_state)
            )
        logging.debug("DrbdManager: Exit function _snaps_volume_actions()")
        return (pool_changed, failed_actions)


    def _snaps_deploy_volume(self, snaps_assg, snaps_vol_state):
        logging.debug("DrbdManager: Enter function _snaps_deploy_volume()")
        pool_changed   = False
        failed_actions = False
        blockdev       = None
        assg           = snaps_assg.get_assignment()
        snaps          = snaps_assg.get_snapshot()
        resource       = snaps.get_resource()
        bd_mgr = self._server.get_bd_mgr()
        snaps_name   = snaps.get_name()
        snaps_vol_id = snaps_vol_state.get_id()
        src_vol_state = assg.get_volume_state(snaps_vol_id)
        if src_vol_state is not None:
            src_cstate = src_vol_state.get_cstate()
            src_tstate = src_vol_state.get_tstate()
            # Operate only on deployed volumes
            if (is_set(src_cstate, DrbdVolumeState.FLAG_DEPLOY) and
                is_set(src_tstate, DrbdVolumeState.FLAG_DEPLOY)):
                # Create the snapshot
                src_bd_name = src_vol_state.get_bd_name()
                pool_changed = True
                blockdev = bd_mgr.create_snapshot(
                    snaps_name, snaps_vol_id, src_bd_name
                )
                if blockdev is not None:
                    snaps_vol_state.set_bd(
                        blockdev.get_name(), blockdev.get_path()
                    )
                    snaps_vol_state.set_cstate_flags(
                        snapshots.DrbdSnapshotVolumeState.FLAG_DEPLOY
                    )
                else:
                    logging.error(
                        "Failed to create snapshot %s/%s #%u "
                        "of source volume %s"
                         % (resource.get_name(), snaps_name, snaps_vol_id,
                            src_bd_name)
                    )
                    failed_actions = True
            else:
                logging.error(
                    "Cannot create snapshot %s/%s #%u, "
                    "source volume is not deployed"
                     % (resource.get_name(), snaps_name, snaps_vol_id)
                )
                failed_actions = True
        else:
            logging.error(
                "Snapshot %s/%s references non-existent volume id %d of "
                "its source resource"
                % (resource.get_name(), snaps.get_name(), snaps_vol_id)
            )
            failed_actions = True
        if failed_actions:
            snaps_assg.set_error_code(dmexc.DM_ESTORAGE)
        logging.debug("DrbdManager: Exit function _snaps_deploy_volume()")
        return (pool_changed, failed_actions)


    def _snaps_undeploy_volume(self, snaps_assg, snaps_vol_state):
        logging.debug("DrbdManager: Enter function _snaps_undeploy_volume()")
        pool_changed   = False
        failed_actions = False
        snaps          = snaps_assg.get_snapshot()
        snaps_name     = snaps.get_name()
        snaps_vol_id   = snaps_vol_state.get_id()

        bd_name = snaps_vol_state.get_bd_name()
        if bd_name is not None:
            pool_changed = True
            bd_mgr = self._server.get_bd_mgr()
            fn_rc = bd_mgr.remove_snapshot(
                bd_name
            )
        if fn_rc == DM_SUCCESS or bd_name is None:
            snaps_vol_state.set_bd(None, None)
            snaps_vol_state.set_cstate(0)
            snaps_vol_state.set_tstate(0)
        else:
            logging.error(
                "Failed to remove snapshot %s #%u block device '%s'"
                 % (snaps_name, snaps_vol_id, bd_name)
            )
            failed_actions = True
        logging.debug("DrbdManager: Exit function _snaps_undeploy_volume()")
        return (pool_changed, failed_actions)


    def adjust_drbdctrl(self):
        logging.debug("DrbdManager: Enter function adjust_drbdctrl()")
        # call drbdadm to bring up the control volume
        drbd_proc = self._drbdadm.ext_conf_adjust(consts.DRBDCTRL_RES_NAME)
        if drbd_proc is not None:
            fn_rc = drbd_proc.wait()
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED
        logging.debug("DrbdManager: Exit function adjust_drbdctrl()")
        return fn_rc


    def down_drbdctrl(self):
        logging.debug("DrbdManager: Enter function down_drbdctrl()")
        # call drbdadm to stop the control volume
        drbd_proc = self._drbdadm.ext_conf_down(consts.DRBDCTRL_RES_NAME)
        if drbd_proc is not None:
            fn_rc = drbd_proc.wait()
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED
        logging.debug("DrbdManager: Exit function down_drbdctrl()")
        return fn_rc


    def initial_up(self):
        """
        Attempts to bring up all deployed resources.
        Used when the drbdmanage server starts up.
        """
        logging.debug("DrbdManager: Enter function initial_up()")
        node = self._server.get_instance_node()
        if node is not None:
            for assg in node.iterate_assignments():
                cstate = assg.get_cstate()
                tstate = assg.get_tstate()
                if (is_set(cstate, Assignment.FLAG_DEPLOY) and
                    is_set(tstate, Assignment.FLAG_DEPLOY)):
                        try:
                            self._up_resource(assg)
                        except Exception as exc:
                            logging.debug(
                                "failed to start resource '%s', "
                                "unhandled exception: %s"
                                % (assg.get_resource().get_name(), str(exc))
                            )
        logging.debug("DrbdManager: Exit function initial_up()")


    def _up_resource(self, assignment):
        """
        Brings up DRBD resources
        """
        logging.debug("DrbdManager: Enter function _up_resource()")
        resource = assignment.get_resource()
        res_name = resource.get_name()

        if assignment.is_empty():
            logging.info(
                "resource '%s' has no volumes, start skipped"
                % (resource.get_name())
            )
            fn_rc = 0
        else:
            logging.info("starting resource '%s'" % res_name)

            bd_mgr = self._server.get_bd_mgr()
            for vol_state in assignment.iterate_volume_states():
                bd_name = vol_state.get_bd_name()
                if bd_name is not None:
                    fn_rc = bd_mgr.up_blockdevice(bd_name)
                    if fn_rc != DM_SUCCESS:
                        logging.warning(
                            "resource '%s': attempt to start the backend "
                            "blockdevice '%s' failed"
                            % (res_name, bd_name)
                        )

            # call drbdadm to bring up the resource
            drbd_proc = self._drbdadm.adjust(resource.get_name())
            if drbd_proc is not None:
                self._resconf.write(drbd_proc.stdin, assignment, False)
                drbd_proc.stdin.close()
                fn_rc = drbd_proc.wait()
            else:
                fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

        logging.debug("DrbdManager: Exit function _up_resource()")
        return fn_rc


    def _down_resource(self, assignment):
        """
        Brings down DRBD resources
        """
        logging.debug("DrbdManager: Enter function _down_resource()")
        # Currently unused; bd_mgr might stop the backend volume in the future
        # bd_mgr   = self._server.get_bd_mgr()
        resource = assignment.get_resource()

        logging.info("stopping resource '%s'" % (resource.get_name()))

        # call drbdadm to bring up the resource
        drbd_proc = self._drbdadm.down(resource.get_name())
        if drbd_proc is not None:
            self._resconf.write(drbd_proc.stdin, assignment, False)
            drbd_proc.stdin.close()
            fn_rc = drbd_proc.wait()
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

        logging.debug("DrbdManager: Exit function _down_resource()")
        return fn_rc


    def _deploy_volume_actions(self, assignment, vol_state):
        """
        Deploys a volumes or restores a snapshot
        """
        logging.debug("DrbdManager: Enter function _deploy_volume_actions()")
        diskless = is_set(assignment.get_tstate(), Assignment.FLAG_DISKLESS)
        blockdev = None
        resource = assignment.get_resource()

        failed_actions = False

        max_peers = self._server.DEFAULT_MAX_PEERS
        try:
            max_peers = int(
                self._server.get_conf_value(
                    self._server.KEY_MAX_PEERS
                )
            )
        except ValueError:
            pass


        # Default to error
        fn_rc = -1

        # ============================================================
        # Block device / backing storage creation
        # ============================================================
        # Decide whether to deploy a new empty blockdevice or
        # deploy a copy of an existing snapshot
        # (snapshot restore)
        props = vol_state.get_props()
        src_bd_name = props.get_prop(consts.SNAPS_SRC_BLOCKDEV)
        if src_bd_name is not None:
            # restore snapshot
            fn_rc, blockdev = self._deploy_volume_from_snapshot(
                assignment, vol_state, src_bd_name
            )
            if fn_rc != 0 or blockdev is None:
                failed_actions = True
        else:
            if not diskless:
                fn_rc, blockdev = self._deploy_volume_blockdev(
                    assignment, vol_state, max_peers
                )
                if fn_rc != 0 or blockdev is None:
                    failed_actions = True

        # Continue if none of the previous steps failed
        if not failed_actions:
            # update configuration file, so drbdadm before-resync-target
            # can work properly
            self._server.export_assignment_conf(assignment)

            # ============================================================
            # Prepare datastructures for writing DRBD configuration files
            # ============================================================

            # add all the (peer) nodes that have or will have this
            # resource deployed
            nodes = []
            for peer_assg in resource.iterate_assignments():
                if is_set(peer_assg.get_tstate(), Assignment.FLAG_DEPLOY):
                    nodes.append(peer_assg.get_node())

            local_node       = assignment.get_node()
            local_vol_states = []
            vol_states       = {}
            deploy_flag      = DrbdVolumeState.FLAG_DEPLOY
            assg_res         = assignment.get_resource()

            # LOCAL NODE
            # - add the current volume no matter what
            #   its current state or target state is, so drbdadm
            #   can see it in the configuration and operate on it
            # - add those other volumes that are already deployed
            local_vol_states.append(vol_state)
            for vstate in assignment.iterate_volume_states():
                if ((is_set(vstate.get_tstate(), deploy_flag) and
                    is_set(vstate.get_cstate(), deploy_flag))):
                        # do not add the same volume state object
                        # twice; the volume state for the current
                        # volume has already been added
                        if vstate is not vol_state:
                            local_vol_states.append(vstate)
            vol_states[local_node.get_name()] = local_vol_states

            # OTHER NODES
            # - pretend that all volumes that the local node has are also
            #   on all other nodes
            for assg_node in nodes:
                peer_assg = assg_node.get_assignment(assg_res.get_name())
                # prevent adding the local node twice
                if peer_assg is not assignment:
                    assg_vol_states = []
                    for local_vstate in local_vol_states:
                        peer_vstate = peer_assg.get_volume_state(
                            local_vstate.get_id()
                        )
                        if peer_vstate is not None:
                            assg_vol_states.append(peer_vstate)
                        else:
                            # The volume state list should be the same for
                            # all assignments; if it is not, log an error
                            logging.error(
                                "Volume state list mismatch between multiple "
                                "assignments for resource %s"
                                % (assg_res.get_name())
                            )
                    vol_states[assg_node.get_name()] = assg_vol_states

            # ============================================================
            # Meta-data creation
            # ============================================================
            # Meta data creation for assignments that have local storage
            if (not failed_actions) and (not diskless) and src_bd_name is None:
                fn_rc = self._deploy_volume_metadata(
                    assignment, vol_state, max_peers, nodes, vol_states
                )

            # ============================================================
            # DRBD initialization
            # ============================================================
            # FIXME: this does not need to run if metadata
            #        creation failed
            # Adjust the DRBD resource to configure the volume
            drbd_proc = self._drbdadm.adjust(resource.get_name())
            if drbd_proc is not None:
                self._resconf.write_excerpt(drbd_proc.stdin, assignment,
                                            nodes, vol_states)
                drbd_proc.stdin.close()
                fn_rc = drbd_proc.wait()
                if fn_rc == 0:
                    if diskless:
                        # Successful "drbdadm adjust" is sufficient for a
                        # diskless client to become deployed
                        vol_state.set_cstate_flags(DrbdVolumeState.FLAG_DEPLOY)
                        fn_rc = 0
                    else:
                        # FIXME: if the blockdevice is missing or meta-data
                        #        creation failed, "drbdadm adjust" should have
                        #        failed, too, but it would probably be better
                        #        to check those cases explicitly
                        vol_state.set_cstate_flags(
                            DrbdVolumeState.FLAG_ATTACH |
                            DrbdVolumeState.FLAG_DEPLOY
                        )
                        fn_rc = 0
                    # "drbdadm adjust" implicitly connects the resource
                    assignment.set_cstate_flags(Assignment.FLAG_CONNECT)
            else:
                fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

        logging.debug("DrbdManager: Exit function _deploy_volume_actions()")
        return fn_rc


    def _deploy_volume_blockdev(self, assignment, vol_state, max_peers):
        """
        Creates a new empty block device for a new volume
        """
        logging.debug("DrbdManager: Enter function _deploy_volume_blockdev()")
        fn_rc    = -1
        bd_mgr   = self._server.get_bd_mgr()
        resource = assignment.get_resource()
        volume   = resource.get_volume(vol_state.get_id())

        net_size   = volume.get_size_kiB()
        gross_size = MetaData.get_gross_data_kiB(net_size, max_peers)
        blockdev = bd_mgr.create_blockdevice(
            resource.get_name(),
            volume.get_id(),
            gross_size
        )

        if blockdev is not None:
            vol_state.set_bd(
                blockdev.get_name(),
                blockdev.get_path()
            )
            # FIXME: If meta-data creation fails later, drbdmanage
            #        will not retry it, because it seems that the
            #        resource is deployed already. However, if the
            #        volume is not marked as deployed here, then
            #        if meta-data creation fails and the volume is
            #        left in an undeployed state, drbdmanage would
            #        forget to remove the backend blockdevice upon
            #        undeploying a partly-deployed resource.
            #        This must be redesigned with additional flags
            vol_state.set_cstate_flags(DrbdVolumeState.FLAG_DEPLOY)
            fn_rc = 0

        logging.debug("DrbdManager: Exit function _deploy_volume_blockdev()")
        return fn_rc, blockdev


    def _deploy_volume_from_snapshot(self, assignment, vol_state, src_bd_name):
        """
        Creates a new blockdevice from a snapshot
        """
        logging.debug(
            "DrbdManager: Enter function _deploy_volume_from_snapshot()"
        )
        fn_rc    = -1
        bd_mgr   = self._server.get_bd_mgr()
        resource = assignment.get_resource()

        blockdev = bd_mgr.restore_snapshot(
            resource.get_name(),
            vol_state.get_id(),
            src_bd_name
        )
        if blockdev is not None:
            bd_name = blockdev.get_name()
            bd_path = blockdev.get_path()
            vol_state.set_bd(bd_name, bd_path)
            bd_mgr.up_blockdevice(bd_name)
            vol_state.set_cstate_flags(DrbdVolumeState.FLAG_DEPLOY)
            fn_rc = 0

        logging.debug(
            "DrbdManager: Exit function _deploy_volume_from_snapshot()"
        )
        return fn_rc, blockdev


    def _deploy_volume_metadata(self, assignment, vol_state, max_peers,
                                nodes, vol_states):
        """
        Creates DRBD metadata on a volume
        """
        logging.debug("DrbdManager: Enter function _deploy_volume_metadata()")
        fn_rc    = -1
        resource = assignment.get_resource()

        # Initialize DRBD metadata
        drbd_proc = self._drbdadm.create_md(
            resource.get_name(), vol_state.get_id(), max_peers
        )
        if drbd_proc is not None:
            self._resconf.write_excerpt(drbd_proc.stdin, assignment,
                                        nodes, vol_states)
            drbd_proc.stdin.close()
            fn_rc = drbd_proc.wait()
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

        logging.debug("DrbdManager: Exit function _deploy_volume_metadata()")
        return fn_rc


    def _deploy_volume(self, assignment, vol_state):
        """
        Deploys a volume and updates its state values
        """
        logging.debug("DrbdManager: Enter function _deploy_volume()")
        # defaults to error
        fn_rc    = -1
        # do not create block devices for clients
        diskless = is_set(assignment.get_tstate(), Assignment.FLAG_DISKLESS)
        blockdev = None
        volume   = vol_state.get_volume()
        resource = assignment.get_resource()

        # Create the backend blockdevice for assignments
        # that have local storage
        if not diskless:
            bd_mgr   = self._server.get_bd_mgr()

            max_peers = self._server.DEFAULT_MAX_PEERS
            try:
                max_peers = int(
                    self._server.get_conf_value(
                        self._server.KEY_MAX_PEERS
                    )
                )
            except ValueError:
                pass

            net_size   = volume.get_size_kiB()
            gross_size = MetaData.get_gross_data_kiB(net_size, max_peers)
            blockdev = bd_mgr.create_blockdevice(
                resource.get_name(),
                volume.get_id(),
                gross_size
            )

            if blockdev is not None:
                vol_state.set_bd(
                    blockdev.get_name(),
                    blockdev.get_path()
                )
                # FIXME: If meta-data creation fails later, drbdmanage
                #        will not retry it, because it seems that the
                #        resource is deployed already. However, if the
                #        volume is not marked as deployed here, then
                #        if meta-data creation fails and the volume is
                #        left in an undeployed state, drbdmanage would
                #        forget to remove the backend blockdevice upon
                #        undeploying a partly-deployed resource.
                #        This must be redesigned with additional flags
                vol_state.set_cstate_flags(DrbdVolumeState.FLAG_DEPLOY)
            else:
                # block device allocation failed
                fn_rc = -1

        # Prepare the data structures for reconfiguring the DRBD resource
        # using drbdadm

        # update configuration file, so drbdadm
        # before-resync-target can work properly
        self._server.export_assignment_conf(assignment)

        # add all the (peer) nodes that have or will have this
        # resource deployed
        nodes = []
        for peer_assg in resource.iterate_assignments():
            if is_set(peer_assg.get_tstate(), Assignment.FLAG_DEPLOY):
                nodes.append(peer_assg.get_node())

        local_node       = assignment.get_node()
        local_vol_states = []
        vol_states       = {}
        deploy_flag      = DrbdVolumeState.FLAG_DEPLOY
        assg_res         = assignment.get_resource()

        # LOCAL NODE
        # - add the current volume no matter what
        #   its current state or target state is, so drbdadm
        #   can see it in the configuration and operate on it
        # - add those other volumes that are already deployed
        local_vol_states.append(vol_state)
        for vstate in assignment.iterate_volume_states():
            if ((is_set(vstate.get_tstate(), deploy_flag) and
                is_set(vstate.get_cstate(), deploy_flag))):
                    # do not add the same volume state object
                    # twice; the volume state for the current
                    # volume has already been added
                    if vstate is not vol_state:
                        local_vol_states.append(vstate)
        vol_states[local_node.get_name()] = local_vol_states

        # OTHER NODES
        # - pretend that all volumes that the local node has are also
        #   on all other nodes
        for assg_node in nodes:
            peer_assg = assg_node.get_assignment(assg_res.get_name())
            # prevent adding the local node twice
            if peer_assg is not assignment:
                assg_vol_states = []
                for local_vstate in local_vol_states:
                    peer_vstate = peer_assg.get_volume_state(
                        local_vstate.get_id()
                    )
                    if peer_vstate is not None:
                        assg_vol_states.append(peer_vstate)
                    else:
                        # The volume state list should be the same for
                        # all assignments; if it is not, log an error
                        logging.error(
                            "Volume state list mismatch between multiple "
                            "assignments for resource %s"
                            % (assg_res.get_name())
                        )
                vol_states[assg_node.get_name()] = assg_vol_states

        # Meta-data creation for assignments that have local storage
        if not diskless:
            if blockdev is not None:
                # Initialize DRBD metadata
                drbd_proc = self._drbdadm.create_md(
                    resource.get_name(), vol_state.get_id(), max_peers
                )
                if drbd_proc is not None:
                    self._resconf.write_excerpt(drbd_proc.stdin, assignment,
                                                nodes, vol_states)
                    drbd_proc.stdin.close()
                    fn_rc = drbd_proc.wait()
                else:
                    fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

        # FIXME: this does not need to run if metadata
        #        creation failed
        # Adjust the DRBD resource to configure the volume
        drbd_proc = self._drbdadm.adjust(resource.get_name())
        if drbd_proc is not None:
            self._resconf.write_excerpt(drbd_proc.stdin, assignment,
                                        nodes, vol_states)
            drbd_proc.stdin.close()
            fn_rc = drbd_proc.wait()
            if fn_rc == 0:
                if diskless:
                    # Successful "drbdadm adjust" is sufficient for a
                    # diskless client to become deployed
                    vol_state.set_cstate_flags(DrbdVolumeState.FLAG_DEPLOY)
                    fn_rc = 0
                else:
                    # FIXME: if the blockdevice is missing or meta-data
                    #        creation failed, "drbdadm adjust" should have
                    #        failed, too, but it would probably be better
                    #        to check those cases explicitly
                    vol_state.set_cstate_flags(
                        DrbdVolumeState.FLAG_ATTACH |
                        DrbdVolumeState.FLAG_DEPLOY
                    )
                    fn_rc = 0
                # "drbdadm adjust" implicitly connects the resource
                assignment.set_cstate_flags(Assignment.FLAG_CONNECT)
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

        logging.debug("DrbdManager: Exit function _deploy_volume()")
        return fn_rc


    def _undeploy_volume(self, assignment, vol_state):
        """
        Undeploys a volume, then resets the state values of the volume state
        entry, so it can be removed from the assignment by the cleanup
        function.
        """
        logging.debug("DrbdManager: Enter function _undeploy_volume()")
        bd_mgr   = self._server.get_bd_mgr()
        resource = assignment.get_resource()

        nodes = []
        for peer_assg in resource.iterate_assignments():
            if is_set(peer_assg.get_tstate(), Assignment.FLAG_DEPLOY):
                nodes.append(peer_assg.get_node())

        # if there are any deployed volumes left in the configuration,
        # remember to update the configuration file, otherwise remember to
        # delete any existing configuration file
        keep_conf   = False
        vol_states  = {}
        deploy_flag = DrbdVolumeState.FLAG_DEPLOY
        assg_res    = assignment.get_resource()
        for assg_node in nodes:
            peer_assg = assg_node.get_assignment(assg_res.get_name())
            assg_vol_states = []
            for vstate in assignment.iterate_volume_states():
                # Add only deployed volumes to the volume state list
                if (is_set(vstate.get_tstate(), deploy_flag) and
                    is_set(vstate.get_cstate(), deploy_flag)):
                        # If there are any deployed volumes left in the
                        # configuration for this node, remember to update the
                        # configuration file, otherwise, do not keep (delete)
                        # any locally existing configuration file for this
                        # assignment
                        if peer_assg is assignment:
                            keep_conf = True
                        assg_vol_states.append(vstate)
            vol_states[assg_node.get_name()] = assg_vol_states

        if keep_conf:
            # Adjust the resource to only keep those volumes running that
            # are currently deployed and not marked for becoming undeployed
            #
            # Update the configuration file
            self._server.export_assignment_conf(assignment)
            # Adjust the resource
            drbd_proc = self._drbdadm.adjust(resource.get_name())
            if drbd_proc is not None:
                self._resconf.write_excerpt(drbd_proc.stdin, assignment,
                                            nodes, vol_states)
                drbd_proc.stdin.close()
                fn_rc = drbd_proc.wait()
                if fn_rc == 0:
                    vol_state.clear_cstate_flags(DrbdVolumeState.FLAG_ATTACH)
            else:
                fn_rc = DrbdManager.DRBDADM_EXEC_FAILED
        else:
            # There are no volumes left in the resource,
            # stop the entire resource
            #
            # Stop the resource
            drbd_proc = self._drbdadm.down(resource.get_name())
            if drbd_proc is not None:
                self._resconf.write_excerpt(drbd_proc.stdin, assignment,
                                            nodes, vol_states)
                drbd_proc.stdin.close()
                fn_rc = drbd_proc.wait()
                if fn_rc == 0:
                    vol_state.clear_cstate_flags(DrbdVolumeState.FLAG_ATTACH)
            # Delete the configuration file, because it would not be valid
            # without having any volumes
            self._server.remove_assignment_conf(resource.get_name())

        if fn_rc == 0:
            fn_rc = -1
            bd_name = vol_state.get_bd_name()
            if bd_name is not None:
                fn_rc = bd_mgr.remove_blockdevice(bd_name)
            if fn_rc == DM_SUCCESS or bd_name is None:
                fn_rc = 0
                vol_state.set_bd(None, None)
                vol_state.set_cstate(0)
                vol_state.set_tstate(0)
                if not keep_conf:
                    # there are no volumes left in the resource, set the
                    # assignment's current state to 0 to enable
                    # fast cleanup of the assignment in the case that the
                    # target state changes to undeploy
                    assignment.undeploy_adjust_cstate()

        logging.debug("DrbdManager: Exit function _undeploy_volume()")
        return fn_rc


    def _deploy_assignment(self, assignment):
        """
        Finishes deployment of an assignment. The actual deployment of the
        assignment's/resource's volumes takes place in per-volume actions
        of the DrbdManager.perform_changes() function.
        """
        logging.debug("DrbdManager: Enter function _deploy_assignment()")
        fn_rc = 0
        deploy_fail = False
        resource = assignment.get_resource()
        tstate   = assignment.get_tstate()
        primary  = self.primary_deployment(assignment)
        empty    = True
        for vol_state in assignment.iterate_volume_states():
            vol_tstate = vol_state.get_tstate()
            vol_cstate = vol_state.get_cstate()

            # If the assignment does not have any more active volumes,
            # (empty == True), the assignment's current state will be
            # set to 0, otherwise (empty == False), the assignment's
            # current state may change depending on whether all the
            # volumes have been deployed or not
            if (is_set(vol_tstate, DrbdVolumeState.FLAG_DEPLOY) or
                is_set(vol_cstate, DrbdVolumeState.FLAG_DEPLOY)):
                    empty = False

            if (is_set(vol_tstate, DrbdVolumeState.FLAG_DEPLOY) and
                is_unset(vol_cstate, DrbdVolumeState.FLAG_DEPLOY)):
                    deploy_fail = True
        if not empty:
            if primary:
                drbd_proc = self._drbdadm.primary(resource.get_name(), True)
                if drbd_proc is not None:
                    self._resconf.write(drbd_proc.stdin, assignment, False)
                    drbd_proc.stdin.close()
                    fn_rc = drbd_proc.wait()
                else:
                    fn_rc = DrbdManager.DRBDADM_EXEC_FAILED
                drbd_proc = self._drbdadm.secondary(resource.get_name())
                if drbd_proc is not None:
                    self._resconf.write(drbd_proc.stdin, assignment, False)
                    drbd_proc.stdin.close()
                    fn_rc = drbd_proc.wait()
                else:
                    fn_rc = DrbdManager.DRBDADM_EXEC_FAILED
                assignment.clear_tstate_flags(Assignment.FLAG_OVERWRITE)
            elif is_set(tstate, Assignment.FLAG_DISCARD):
                fn_rc = self._reconnect(assignment)
            if deploy_fail:
                fn_rc = -1
            else:
                assignment.set_cstate_flags(Assignment.FLAG_DEPLOY)
        else:
            assignment.undeploy_adjust_cstate()
        logging.debug("DrbdManager: Exit function _deploy_assignment()")
        return fn_rc


    def _undeploy_assignment(self, assignment):
        """
        Undeploys all volumes of a resource, then reset the assignment's state
        values, so it can be removed by the cleanup function.
        """
        logging.debug("DrbdManager: Enter function _undeploy_assignment()")
        bd_mgr = self._server.get_bd_mgr()
        resource = assignment.get_resource()

        ud_errors = False
        # No actions are required for empty assignments
        if not assignment.is_empty():
            # call drbdadm to stop the DRBD on top of the blockdevice
            drbd_proc = self._drbdadm.down(resource.get_name())
            if drbd_proc is not None:
                local_node = self._server.get_instance_node()
                nodes = [ local_node ]
                vol_state_list = []
                for vol_state in assignment.iterate_volume_states():
                    vol_state_list.append(vol_state)
                node_vol_states = { local_node.get_name(): vol_state_list }
                self._resconf.write_excerpt(
                    drbd_proc.stdin, assignment, nodes, node_vol_states
                )
                drbd_proc.stdin.close()
                fn_rc = drbd_proc.wait()

                if fn_rc == 0:
                    # if the assignment was diskless, mark all
                    # volumes as undeployed
                    cstate = assignment.get_cstate()
                    if is_set(cstate, Assignment.FLAG_DISKLESS):
                        for vol_state in assignment.iterate_volume_states():
                            vol_state.set_cstate(0)
                            vol_state.set_tstate(0)
                    else:
                        # undeploy all non-diskless volumes
                        for vol_state in assignment.iterate_volume_states():
                            bd_name = vol_state.get_bd_name()
                            if bd_name is not None:
                                stor_rc = bd_mgr.remove_blockdevice(bd_name)
                                if stor_rc == DM_SUCCESS:
                                    vol_state.set_bd(None, None)
                                    vol_state.set_cstate(0)
                                    vol_state.set_tstate(0)
                                else:
                                    ud_errors = True
            else:
                fn_rc = DrbdManager.DRBDADM_EXEC_FAILED
                ud_errors = True

        if not ud_errors:
            # Remove the external configuration file
            self._server.remove_assignment_conf(resource.get_name())
            assignment.undeploy_adjust_cstate()
            assignment.set_tstate(0)

        fn_rc = (DrbdManager.STOR_UNDEPLOY_FAILED if ud_errors else DM_SUCCESS)

        logging.debug("DrbdManager: Exit function _undeploy_assignment()")
        return fn_rc


    def _connect(self, assignment):
        """
        Connects a resource on the current node to all peer nodes
        """
        logging.debug("DrbdManager: Enter function _connect()")
        resource = assignment.get_resource()
        tstate = assignment.get_tstate()
        discard_flag = is_set(tstate, Assignment.FLAG_DISCARD)
        drbd_proc = self._drbdadm.connect(resource.get_name(), discard_flag)
        if drbd_proc is not None:
            self._resconf.write(drbd_proc.stdin, assignment, False)
            drbd_proc.stdin.close()
            fn_rc = drbd_proc.wait()
            assignment.set_cstate_flags(Assignment.FLAG_CONNECT)
            assignment.clear_tstate_flags(Assignment.FLAG_DISCARD)
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

        logging.debug("DrbdManager: Exit function _connect()")
        return fn_rc


    def _disconnect(self, assignment):
        """
        Disconnects a resource on the current node from all peer nodes
        """
        logging.debug("DrbdManager: Enter function _disconnect()")
        resource = assignment.get_resource()
        drbd_proc = self._drbdadm.disconnect(resource.get_name())
        if drbd_proc is not None:
            self._resconf.write(drbd_proc.stdin, assignment, True)
            drbd_proc.stdin.close()
            fn_rc = drbd_proc.wait()
            assignment.clear_cstate_flags(Assignment.FLAG_CONNECT)
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

        logging.debug("DrbdManager: Exit function _disconnect()")
        return fn_rc


    def _update_connections(self, assignment):
        """
        Updates connections
        * Disconnect from nodes that do not have the same resource
          connected anymore
        * Connect to nodes that have newly deployed a resource
        * Leave valid existing connections untouched
        """
        logging.debug("DrbdManager: Enter function _update_connections()")
        resource = assignment.get_resource()
        # TODO:
        """
        * Finds active connections
        * ... disconnect those that do not match any
          of the assignments for that resource.
        * ... connect those where there is an assignment for that resource
          but no matching connection
        """
        # call drbdadm to update connections
        drbd_proc = self._drbdadm.adjust(resource.get_name())
        if drbd_proc is not None:
            self._resconf.write(drbd_proc.stdin, assignment, False)
            drbd_proc.stdin.close()
            fn_rc = drbd_proc.wait()
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED
        if fn_rc == 0:
            assignment.set_cstate_flags(Assignment.FLAG_CONNECT)
            assignment.clear_tstate_flags(Assignment.FLAG_UPD_CON)
            if is_unset(assignment.get_tstate(), Assignment.FLAG_DISKLESS):
                for vol_state in assignment.iterate_volume_states():
                    vol_state.set_cstate_flags(DrbdVolumeState.FLAG_ATTACH)

        logging.debug("DrbdManager: Exit function _update_connections()")
        return fn_rc


    def _reconnect(self, assignment):
        """
        Disconnects, then connects again
        """
        logging.debug("DrbdManager: Enter function _reconnect()")
        # disconnect
        self._disconnect(assignment)
        # connect
        fn_rc = self._connect(assignment)
        assignment.clear_tstate_flags(Assignment.FLAG_RECONNECT)

        logging.debug("DrbdManager: Exit function _reconnect()")
        return fn_rc


    def _attach(self, assignment, vol_state):
        """
        Attaches a volume
        """
        logging.debug("DrbdManager: Enter function _attach()")
        resource = assignment.get_resource()
        # do not attach clients, because there is no local storage on clients
        drbd_proc = self._drbdadm.attach(
            resource.get_name(),
            vol_state.get_id()
        )
        if drbd_proc is not None:
            self._resconf.write(drbd_proc.stdin, assignment, False)
            drbd_proc.stdin.close()
            fn_rc = drbd_proc.wait()
            if is_unset(assignment.get_tstate(), Assignment.FLAG_DISKLESS):
                # TODO: order drbdadm to attach the volume
                vol_state.set_cstate_flags(DrbdVolumeState.FLAG_ATTACH)
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

        logging.debug("DrbdManager: Exit function _attach()")
        return fn_rc


    def _detach(self, assignment, vol_state):
        """
        Detaches a volume
        """
        logging.debug("DrbdManager: Enter function _detach()")
        resource = assignment.get_resource()
        drbd_proc = self._drbdadm.detach(
            resource.get_name(),
            vol_state.get_id()
        )
        if drbd_proc is not None:
            self._resconf.write(drbd_proc.stdin, assignment, True)
            drbd_proc.stdin.close()
            fn_rc = drbd_proc.wait()
            vol_state.clear_cstate_flags(DrbdVolumeState.FLAG_ATTACH)
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

        logging.debug("DrbdManager: Exit function _detach()")
        return fn_rc


    def primary_deployment(self, assignment):
        """
        Checks whether this assignment should switch to the primary role
        after deployment (primary --force).

        Decision rules:
        NEVER switch to the primary role,
        * if this assignment's discard flag is set and
          the overwrite flag unset
        * if the peer assignment's overwrite flag set, its diskless flag is
          unset, and this assignment's overwrite flag is unset
        ALWAYS switch to the primary role,
        * if this assignment's overwrite flag is set
        * if the peer assignment's diskless flag is set
        OTHERWISE, switch to the primary role according to the
        truth table below.
        Key:
            L: this (local) assignment is restoring a snapshot
            P: the peer's  assignment is restoring a snapshot
            D: the peer's assignment is already deployed
            Values: 1: true, 0: false
            L P D   Role
            0 0 0   primary
            0 0 1   secondary
            0 1 0   secondary
            0 1 1   secondary
            1 0 0   primary
            1 0 1   primary
            1 1 0   primary
            1 1 1   secondary
        """
        logging.debug("DrbdManager: Enter function primary_deployment()")
        tstate = assignment.get_tstate()
        force_primary = is_set(tstate, Assignment.FLAG_OVERWRITE)
        discard = is_set(tstate, Assignment.FLAG_DISCARD)
        primary = (False if discard and not force_primary else True)
        if primary and not force_primary:
            resource = assignment.get_resource()
            local_restore = assignment.is_snapshot_restore()
            for peer_assg in resource.iterate_assignments():
                if peer_assg != assignment:
                    pa_cstate = peer_assg.get_cstate()
                    pa_tstate = peer_assg.get_tstate()
                    if is_unset(pa_cstate, Assignment.FLAG_DISKLESS):
                        pa_deployed = is_set(pa_cstate, Assignment.FLAG_DEPLOY)
                        pa_restore = peer_assg.is_snapshot_restore()
                        # See the truth table in the function
                        # description comment to figure out which
                        # cases this condition is supposed to handle
                        # (all the secondary role cases)
                        if (is_set(pa_tstate, Assignment.FLAG_OVERWRITE) or
                            (not local_restore and
                            (pa_restore or pa_deployed)) or
                            (local_restore and pa_restore and pa_deployed)):
                            # Do not assume the primary role
                            primary = False
                            break
        logging.debug("DrbdManager: Exit function primary_deployment()")
        return primary


    def reconfigure(self):
        """
        Reconfigures the DrbdManager instance

        Called by the server's reconfigure() function
        """
        logging.debug("DrbdManager: Enter function reconfigure()")
        self._drbdadm = drbdmanage.drbd.commands.DrbdAdm(
            self._server.get_conf_value(self._server.KEY_DRBDADM_PATH)
        )
        logging.debug("DrbdManager: Exit function reconfigure()")


class DrbdResource(GenericDrbdObject):

    NAME_MAXLEN  = consts.RES_NAME_MAXLEN
    # Valid characters in addition to [a-zA-Z0-9]
    NAME_VALID_CHARS      = "_"
    # Additional valid characters, but not allowed as the first character
    NAME_VALID_INNER_CHARS = "-"

    _name        = None
    _secret      = None
    _port        = None
    _state       = None
    _volumes     = None
    _assignments = None
    _snapshots   = None

    # Reference to the server's get_serial() function
    _get_serial = None

    FLAG_REMOVE  = 0x1
    FLAG_NEW     = 0x2

    # STATE_MASK must include all valid flags;
    # used to mask the value supplied to set_state() to prevent setting
    # non-existent flags
    STATE_MASK   = FLAG_REMOVE | FLAG_NEW

    # maximum volumes per resource
    MAX_RES_VOLS = 64

    def __init__(self, name, port, secret, state, init_volumes,
                 get_serial_fn, init_serial, init_props):
        super(DrbdResource, self).__init__(
            get_serial_fn, init_serial,
            init_props
        )
        self._name         = self.name_check(name)
        if secret is not None:
            self._secret   = str(secret)
        checked_state = None
        if state is not None:
            try:
                checked_state = long(state)
            except ValueError:
                pass
        if checked_state is not None:
            self._state = checked_state & self.STATE_MASK
        else:
            self._state = 0

        self._volumes = {}
        if init_volumes is not None:
            for volume in init_volumes:
                self._volumes[volume.get_id()] = volume

        checked_state = None
        if state is not None:
            try:
                checked_state = long(state)
            except ValueError:
                pass
        if checked_state is not None:
            self._state = checked_state & self.STATE_MASK
        else:
            self._state = 0

        self._port         = port
        self._assignments  = {}
        self._snapshots    = {}
        self._get_serial   = get_serial_fn


    def get_name(self):
        return self._name


    def get_port(self):
        return self._port


    def name_check(self, name):
        checked_name = GenericDrbdObject.name_check(
            name, DrbdResource.NAME_MAXLEN,
            DrbdResource.NAME_VALID_CHARS, DrbdResource.NAME_VALID_INNER_CHARS
        )
        # A resource can not be named "all", because that is a
        # keyword in the drbdsetup/drbdadm utilities
        if checked_name.lower() == drbdcmd.DrbdAdm.RES_ALL_KEYWORD:
            raise dmexc.InvalidNameException
        return checked_name


    def init_add_assignment(self, assignment):
        node = assignment.get_node()
        self._assignments[node.get_name()] = assignment


    def add_assignment(self, assignment):
        node = assignment.get_node()
        self._assignments[node.get_name()] = assignment
        self.get_props().new_serial()


    def get_assignment(self, name):
        return self._assignments.get(name)


    def remove_assignment(self, assignment):
        node = assignment.get_node()
        try:
            del self._assignments[node.get_name()]
            self.get_props().new_serial()
        except KeyError:
            pass


    def init_add_snapshot(self, snapshot):
        self._snapshots[snapshot.get_name()] = snapshot


    def add_snapshot(self, snapshot):
        self._snapshots[snapshot.get_name()] = snapshot
        self.get_props().new_serial()


    def get_snapshot(self, name):
        return self._snapshots.get(name)


    def iterate_snapshots(self):
        return self._snapshots.itervalues()


    def remove_snapshot(self, snapshot):
        try:
            del self._snapshots[snapshot.get_name()]
            self.get_props().new_serial()
        except KeyError:
            pass


    def iterate_assignments(self):
        return self._assignments.itervalues()


    def has_assignments(self):
        return len(self._assignments) > 0


    def assigned_count(self):
        """
        Return the number of nodes this resource is assigned to
        (waiting for deployment or deployed)
        """
        count = 0
        for assg in self._assignments.itervalues():
            if is_set(assg.get_tstate(), Assignment.FLAG_DEPLOY):
                count += 1
        return count


    def occupied_count(self):
        """
        Return the number of assignments that still occupy this node
        (waiting for deployment, deployed or waiting for undeployment)
        """
        return len(self._assignments)


    def deployed_count(self):
        """
        Return the number of nodes where this resource remains deployed
        (deployed and NOT waiting for undeployment)
        """
        count = 0
        for assg in self._assignments.itervalues():
            if ((assg.get_cstate() & assg.get_tstate() &
                Assignment.FLAG_DEPLOY) != 0):
                    count += 1
        return count


    def add_volume(self, volume):
        self._volumes[volume.get_id()] = volume
        self.get_props().new_serial()


    def get_volume(self, vol_id):
        return self._volumes.get(vol_id)


    def remove_volume(self, vol_id):
        volume = self._volumes.get(vol_id)
        if volume is not None:
            try:
                del self._volumes[volume.get_id()]
                self.get_props().new_serial()
            except KeyError:
                pass


    def iterate_volumes(self):
        return self._volumes.itervalues()


    def remove(self):
        if is_unset(self._state, self.FLAG_REMOVE):
            self._state |= self.FLAG_REMOVE
            self.get_props().new_serial()


    def set_secret(self, secret):
        self._secret = secret
        self.get_props().new_serial()


    def get_secret(self):
        return self._secret


    def get_state(self):
        return self._state


    def set_state(self, state):
        if state != self._state:
            self._state = state & self.STATE_MASK
            self.get_props().new_serial()


    def filter_match(self, filter_props):
        match = False
        if filter_props is None or len(filter_props) == 0:
            match = True
        else:
            match = self.properties_match(filter_props)
            if not match:
                special_props_list = {
                    consts.RES_NAME    : self._name,
                    consts.RES_SECRET  : str(self._secret),
                    consts.RES_PORT    : str(self._port),
                    consts.TSTATE_PREFIX + consts.FLAG_REMOVE :
                        bool_to_string(is_set(self._state, self.FLAG_REMOVE))
                }
                match = self.special_properties_match(
                    special_props_list, filter_props
                )
        return match


    def get_properties(self, req_props):
        properties = {}

        selector = Selector(req_props)
        if req_props is not None and len(req_props) > 0:
            selected = selector.list_selector
        else:
            selected = selector.all_selector

        if selected(consts.RES_NAME):
            properties[consts.RES_NAME] = self._name
        # Note: adding the shared secret to the properties list
        #       commented out - R. Altnoeder 2014-04-24
        # if selected(consts.RES_SECRET):
        #     properties[consts.RES_SECRET] = str(self._secret)
        if selected(consts.RES_PORT):
            properties[consts.RES_PORT] = str(self._port)
        if selected(consts.TSTATE_PREFIX + consts.FLAG_REMOVE):
            properties[consts.TSTATE_PREFIX + consts.FLAG_REMOVE] = (
                bool_to_string(is_set(self._state, self.FLAG_REMOVE))
            )
        for (key, val) in self.get_props().iteritems():
            if selected(key):
                if val is not None:
                    properties[key] = str(val)
        return properties



class DrbdVolume(GenericStorage, GenericDrbdObject):

    """
    Representation of a DRBD volume specification in drbdmanage's object model

    This class represents the specification of a DRBD volume in
    drbdmanage's configuration. DrbdVolume objects are referenced in
    DrbdResource objects.
    """

    _id          = None
    _size_kiB    = None
    _minor       = None
    _state       = None

    # Reference to the server's get_serial() function
    _get_serial = None

    FLAG_REMOVE  = 0x1

    # STATE_MASK must include all valid flags;
    # used to mask the value supplied to set_state() to prevent setting
    # non-existent flags
    STATE_MASK   = FLAG_REMOVE

    def __init__(self, vol_id, size_kiB, minor, state, get_serial_fn,
                 init_serial, init_props):
        if not size_kiB > 0:
            raise VolSizeRangeException
        super(DrbdVolume, self).__init__(size_kiB)
        GenericDrbdObject.__init__(
            self, get_serial_fn, init_serial, init_props
        )
        self._id = int(vol_id)
        if self._id < 0 or self._id >= DrbdResource.MAX_RES_VOLS:
            raise ValueError
        self._size_kiB     = size_kiB
        self._minor        = minor

        checked_state = None
        if state is not None:
            try:
                checked_state = long(state)
            except ValueError:
                pass
            if checked_state is not None:
                self._state = checked_state & self.STATE_MASK
            else:
                self._state = 0
        self._get_serial   = get_serial_fn


    def get_id(self):
        return self._id


    # returns a storagecore.MinorNr object
    def get_minor(self):
        return self._minor


    def get_path(self):
        # TODO: return "pretty" name, /dev/drbd/by-res/...
        return "/dev/drbd" + str(self.get_minor().get_value())


    def get_state(self):
        return self._state


    def set_state(self, state):
        if state != self._state:
            self._state = state & self.STATE_MASK
            self.get_props().new_serial()


    def remove(self):
        if is_unset(self._state, self.FLAG_REMOVE):
            self._state |= self.FLAG_REMOVE
            self.get_props().new_serial()


    def filter_match(self, filter_props):
        match = False
        if filter_props is None or len(filter_props) == 0:
            match = True
        else:
            match = self.properties_match(filter_props)
            if not match:
                special_props_list = {
                    consts.VOL_ID        : str(self._id),
                    consts.VOL_SIZE      : str(self._size_kiB),
                    consts.VOL_MINOR     : str(self._minor.get_value()),
                    consts.TSTATE_PREFIX + consts.FLAG_REMOVE :
                        bool_to_string(is_set(self._state, self.FLAG_REMOVE))
                }
                match = self.special_properties_match(
                    special_props_list, filter_props
                )
        return match


    def get_properties(self, req_props):
        properties = {}

        selector = Selector(req_props)
        if req_props is not None and len(req_props) > 0:
            selected = selector.list_selector
        else:
            selected = selector.all_selector

        if selected(consts.VOL_ID):
            properties[consts.VOL_ID]    = str(self._id)
        if selected(consts.VOL_SIZE):
            properties[consts.VOL_SIZE]  = str(self._size_kiB)
        if selected(consts.VOL_MINOR):
            properties[consts.VOL_MINOR] = str(self._minor.get_value())
        if selected(consts.TSTATE_PREFIX + consts.FLAG_REMOVE):
            properties[consts.TSTATE_PREFIX + consts.FLAG_REMOVE] = (
                bool_to_string(is_set(self._state, self.FLAG_REMOVE))
            )
        for (key, val) in self.get_props().iteritems():
            if selected(key):
                if val is not None:
                    properties[key] = str(val)
        return properties




class DrbdNode(GenericDrbdObject):

    """
    Represents a drbdmanage host node in drbdmanage's object model.
    """

    NAME_MAXLEN = consts.NODE_NAME_MAXLEN
    # Valid characters in addition to [a-zA-Z0-9]
    NAME_VALID_CHARS      = "_"
    # Additional valid characters, but not allowed as the first character
    NAME_VALID_INNER_CHARS = ".-"

    AF_IPV4 = 4
    AF_IPV6 = 6

    AF_IPV4_LABEL = "ipv4"
    AF_IPV6_LABEL = "ipv6"

    NODE_ID_NONE = -1

    _name     = None
    _addr     = None
    _addrfam  = None
    _node_id  = None
    _state    = None
    _poolsize = None
    _poolfree = None

    _assignments = None

    # Reference to the server's get_serial() function
    _get_serial = None

    FLAG_REMOVE   =     0x1
    FLAG_UPDATE   =     0x2
    FLAG_DRBDCTRL =     0x4
    FLAG_STORAGE  =     0x8
    FLAG_UPD_POOL = 0x10000

    # STATE_MASK must include all valid flags;
    # used to mask the value supplied to set_state() to prevent setting
    # non-existent flags
    STATE_MASK = (FLAG_REMOVE | FLAG_UPD_POOL | FLAG_UPDATE | FLAG_DRBDCTRL |
                  FLAG_STORAGE)


    def __init__(self, name, addr, addrfam, node_id, state, poolsize, poolfree,
                 get_serial_fn, init_serial, init_props):
        super(DrbdNode, self).__init__(get_serial_fn, init_serial, init_props)
        self._name    = self.name_check(name)
        # TODO: there should be sanity checks on addr
        af_n = int(addrfam)
        if af_n == self.AF_IPV4 or af_n == self.AF_IPV6:
            self._addrfam = af_n
        else:
            raise InvalidAddrFamException
        self._addr         = addr
        self._node_id      = node_id
        self._assignments  = {}

        checked_state = None
        if state is not None:
            try:
                checked_state = long(state)
            except ValueError:
                pass
        if checked_state is not None:
            self._state = checked_state & self.STATE_MASK
        else:
            self._state = 0

        self._poolfree     = poolfree
        self._poolsize     = poolsize
        self._get_serial   = get_serial_fn


    def get_name(self):
        return self._name


    def get_addr(self):
        return self._addr


    def get_addrfam(self):
        return self._addrfam


    def get_node_id(self):
        return self._node_id


    def set_node_id(self, node_id):
        self._node_id = node_id
        self.get_props().new_serial()


    def get_addrfam_label(self):
        label = "unknown"
        if self._addrfam == self.AF_IPV4:
            label = self.AF_IPV4_LABEL
        elif self._addrfam == self.AF_IPV6:
            label = self.AF_IPV6_LABEL
        return label


    def get_state(self):
        return self._state


    def set_state(self, state):
        saved_state = self._state
        self._state = state & self.STATE_MASK
        if saved_state != self._state:
            self.get_props().new_serial()


    def get_poolsize(self):
        return self._poolsize


    def get_poolfree(self):
        return self._poolfree


    def set_poolsize(self, size):
        if size != self._poolsize:
            self._poolsize = size
            self.get_props().new_serial()



    def set_poolfree(self, size):
        if size != self._poolfree:
            self._poolfree = size
            self.get_props().new_serial()


    def set_pool(self, size, free):
        if size != self._poolsize or free != self._poolfree:
            self._poolsize = size
            self._poolfree = free
            self.get_props().new_serial()


    def remove(self):
        if is_unset(self._state, self.FLAG_REMOVE):
            self._state |= self.FLAG_REMOVE
            self.get_props().new_serial()


    def upd_pool(self):
        if is_unset(self._state, self.FLAG_UPD_POOL):
            self._state |= self.FLAG_UPD_POOL
            self.get_props().new_serial()


    def name_check(self, name):
        return GenericDrbdObject.name_check(
            name, DrbdNode.NAME_MAXLEN,
            DrbdNode.NAME_VALID_CHARS, DrbdNode.NAME_VALID_INNER_CHARS
        )


    def init_add_assignment(self, assignment):
        resource = assignment.get_resource()
        self._assignments[resource.get_name()] = assignment


    def add_assignment(self, assignment):
        resource = assignment.get_resource()
        self._assignments[resource.get_name()] = assignment
        self.get_props().new_serial()


    def get_assignment(self, name):
        assignment = None
        try:
            assignment = self._assignments[name]
        except KeyError:
            pass
        return assignment


    def remove_assignment(self, assignment):
        resource = assignment.get_resource()
        try:
            del self._assignments[resource.get_name()]
            self.get_props().new_serial()
        except KeyError:
            pass


    def has_assignments(self):
        return len(self._assignments) > 0


    def iterate_assignments(self):
        return self._assignments.itervalues()


    def filter_match(self, filter_props):
        match = False
        if filter_props is None or len(filter_props) == 0:
            match = True
        else:
            match = self.properties_match(filter_props)
            if not match:
                special_props_list = {
                    consts.NODE_NAME     : self._name,
                    consts.NODE_AF       : self._addrfam,
                    consts.NODE_ADDR     : self._addr,
                    consts.NODE_ID       : self._node_id,
                    consts.NODE_POOLSIZE : self._poolsize,
                    consts.NODE_POOLFREE : self._poolfree,
                    consts.TSTATE_PREFIX + consts.FLAG_REMOVE :
                        bool_to_string(is_set(self._state, self.FLAG_REMOVE)),
                    consts.TSTATE_PREFIX + consts.FLAG_UPDATE   :
                        bool_to_string(is_set(self._state, self.FLAG_UPDATE)),
                    consts.TSTATE_PREFIX + consts.FLAG_UPD_POOL :
                        bool_to_string(is_set(self._state, self.FLAG_UPD_POOL))
                }
                match = self.special_properties_match(
                    special_props_list, filter_props
                )
        return match


    def get_properties(self, req_props):
        properties = {}

        selector = Selector(req_props)
        if req_props is not None and len(req_props) > 0:
            selected = selector.list_selector
        else:
            selected = selector.all_selector

        if selected(consts.NODE_NAME):
            properties[consts.NODE_NAME]     = self._name
        if selected(consts.NODE_AF):
            properties[consts.NODE_AF]       = str(self._addrfam)
        if selected(consts.NODE_ADDR):
            properties[consts.NODE_ADDR]     = str(self._addr)
        if selected(consts.NODE_ID):
            properties[consts.NODE_ID]       = str(self._node_id)
        if selected(consts.NODE_POOLSIZE):
            properties[consts.NODE_POOLSIZE] = str(self._poolsize)
        if selected(consts.NODE_POOLFREE):
            properties[consts.NODE_POOLFREE] = str(self._poolfree)
        if selected(consts.TSTATE_PREFIX + consts.FLAG_REMOVE):
            properties[consts.TSTATE_PREFIX + consts.FLAG_REMOVE] = (
                bool_to_string(is_set(self._state, self.FLAG_REMOVE))
            )
        if selected(consts.TSTATE_PREFIX + consts.FLAG_UPDATE):
            properties[consts.TSTATE_PREFIX + consts.FLAG_UPDATE] = (
                bool_to_string(is_set(self._state, self.FLAG_UPDATE))
            )
        if selected(consts.TSTATE_PREFIX + consts.FLAG_UPD_POOL):
            properties[consts.TSTATE_PREFIX + consts.FLAG_UPD_POOL] = (
                bool_to_string(is_set(self._state, self.FLAG_UPD_POOL))
            )
        if selected(consts.TSTATE_PREFIX + consts.FLAG_DRBDCTRL):
            properties[consts.TSTATE_PREFIX + consts.FLAG_DRBDCTRL] = (
                bool_to_string(is_set(self._state, self.FLAG_DRBDCTRL))
            )
        if selected(consts.TSTATE_PREFIX + consts.FLAG_STORAGE):
            properties[consts.TSTATE_PREFIX + consts.FLAG_STORAGE] = (
                bool_to_string(is_set(self._state, self.FLAG_STORAGE))
            )
        for (key, val) in self.get_props().iteritems():
            if selected(key):
                if val is not None:
                    properties[key] = str(val)
        return properties


class DrbdVolumeState(GenericDrbdObject):

    """
    Represents the state of a resource's volume assigned to a node

    A DrbdVolumeState object represents the state of a volume that is part
    of a resource that has been assigned to a node by means of an
    Assignment object. DrbdVolumeState objects are referenced by Assignment
    objects.
    """

    _volume  = None
    _bd_path = None
    _bd_name = None
    _cstate  = 0
    _tstate  = 0

    # Reference to the server's get_serial() function
    _get_serial = None

    FLAG_DEPLOY    = 0x1
    FLAG_ATTACH    = 0x2

    # CSTATE_MASK must include all valid current state flags;
    # used to mask the value supplied to set_cstate() to prevent setting
    # non-existent flags
    CSTATE_MASK    = FLAG_DEPLOY | FLAG_ATTACH

    # TSTATE_MASK must include all valid target state flags;
    # used to mask the value supplied to set_tstate() to prevent setting
    # non-existent flags
    TSTATE_MASK    = FLAG_DEPLOY | FLAG_ATTACH

    def __init__(self, volume, cstate, tstate, bd_name, bd_path,
                 get_serial_fn, init_serial, init_props):
        super(DrbdVolumeState, self).__init__(
            get_serial_fn, init_serial, init_props
        )
        self._volume = volume

        if bd_name is not None and bd_path is not None:
            self._bd_name = bd_name
            self._bd_path = bd_path

        checked_cstate = None
        if cstate is not None:
            try:
                checked_cstate = long(cstate)
            except ValueError:
                pass
        if checked_cstate is not None:
            self._cstate = checked_cstate & self.CSTATE_MASK
        else:
            self._cstate = 0

        checked_tstate = None
        if tstate is not None:
            try:
                checked_tstate = long(tstate)
            except ValueError:
                pass
        if checked_tstate is not None:
            self._tstate = checked_tstate & self.TSTATE_MASK
        else:
            self._tstate = self.FLAG_DEPLOY | self.FLAG_ATTACH

        self._get_serial   = get_serial_fn


    def get_volume(self):
        return self._volume


    def get_id(self):
        return self._volume.get_id()


    def get_bd_path(self):
        return self._bd_path


    def get_bd_name(self):
        return self._bd_name


    def set_bd(self, bd_name, bd_path):
        if bd_name != self._bd_name or bd_path != self._bd_path:
            self._bd_name = bd_name
            self._bd_path = bd_path
            self.get_props().new_serial()


    def requires_action(self):
        return (self._cstate != self._tstate)


    def requires_deploy(self):
        return (is_set(self._tstate, self.FLAG_DEPLOY) and
                is_unset(self._cstate, self.FLAG_DEPLOY))


    def requires_attach(self):
        return (is_set(self._tstate, self.FLAG_ATTACH) and
                is_unset(self._cstate, self.FLAG_ATTACH))


    def requires_undeploy(self):
        return (is_unset(self._tstate, self.FLAG_DEPLOY) and
                is_set(self._cstate, self.FLAG_DEPLOY))


    def requires_detach(self):
        return (is_unset(self._tstate, self.FLAG_ATTACH) and
                is_set(self._cstate, self.FLAG_ATTACH))


    def set_cstate(self, cstate):
        if cstate != self._cstate:
            self._cstate = cstate & self.CSTATE_MASK
            self.get_props().new_serial()


    def set_tstate(self, tstate):
        if tstate != self._tstate:
            self._tstate = tstate & self.TSTATE_MASK
            self.get_props().new_serial()


    def get_cstate(self):
        return self._cstate


    def get_tstate(self):
        return self._tstate


    def deploy(self):
        if is_unset(self._tstate, self.FLAG_DEPLOY):
            self._tstate = self._tstate | self.FLAG_DEPLOY
            self.get_props().new_serial()


    def undeploy(self):
        if self._tstate != 0:
            self._tstate = 0
            self.get_props().new_serial()


    def attach(self):
        if is_unset(self._tstate, self.FLAG_ATTACH):
            self._tstate = self._tstate | self.FLAG_ATTACH
            self.get_props().new_serial()


    def detach(self):
        if is_set(self._tstate, self.FLAG_ATTACH):
            self._tstate = ((self._tstate | self.FLAG_ATTACH) ^
                            self.FLAG_ATTACH)
            self.get_props().new_serial()


    def is_snapshot_restore(self):
        src_bd_name = self.get_props().get_prop(consts.SNAPS_SRC_BLOCKDEV)
        return (True if src_bd_name is not None else False)


    def set_cstate_flags(self, flags):
        saved_cstate = self._cstate
        self._cstate = (self._cstate | flags) & self.CSTATE_MASK
        if saved_cstate != self._cstate:
            self.get_props().new_serial()


    def clear_cstate_flags(self, flags):
        saved_cstate = self._cstate
        self._cstate = ((self._cstate | flags) ^ flags) & self.CSTATE_MASK
        if saved_cstate != self._cstate:
            self.get_props().new_serial()


    def set_tstate_flags(self, flags):
        saved_tstate = self._tstate
        self._tstate = (self._tstate | flags) & self.TSTATE_MASK
        if saved_tstate != self._tstate:
            self.get_props().new_serial()


    def clear_tstate_flags(self, flags):
        saved_tstate = self._tstate
        self._tstate = ((self._tstate | flags) ^ flags) & self.TSTATE_MASK
        if saved_tstate != self._tstate:
            self.get_props().new_serial()


    def filter_match(self, filter_props):
        match = False
        if filter_props is None or len(filter_props) == 0:
            match = True
        else:
            match = self.properties_match(filter_props)
            if not match:
                bdev = "" if self._bd_path is None else str(self._bd_path)
                special_props_list = {
                    consts.VOL_ID      : self._volume.get_id(),
                    consts.VOL_SIZE    : self._volume.get_size_kiB(),
                    consts.VOL_BDEV    : bdev,
                    consts.VOL_MINOR   : self._volume.get_minor().get_value(),
                    consts.TSTATE_PREFIX + consts.FLAG_DEPLOY :
                        bool_to_string(is_set(self._tstate, self.FLAG_DEPLOY)),
                    consts.TSTATE_PREFIX + consts.FLAG_ATTACH :
                        bool_to_string(is_set(self._tstate, self.FLAG_ATTACH)),
                    consts.CSTATE_PREFIX + consts.FLAG_DEPLOY :
                        bool_to_string(is_set(self._cstate, self.FLAG_DEPLOY)),
                    consts.CSTATE_PREFIX + consts.FLAG_ATTACH :
                        bool_to_string(is_set(self._cstate, self.FLAG_ATTACH))
                }
                match = self.special_properties_match(
                    special_props_list, filter_props
                )
        return match


    def get_properties(self, req_props):
        properties = {}

        selector = Selector(req_props)
        if req_props is not None and len(req_props) > 0:
            selected = selector.list_selector
        else:
            selected = selector.all_selector

        if selected(consts.VOL_ID):
            properties[consts.VOL_ID]    = str(self._volume.get_id())
        if selected(consts.VOL_SIZE):
            properties[consts.VOL_SIZE]  = str(self._volume.get_size_kiB())
        if selected(consts.VOL_BDEV):
            properties[consts.VOL_BDEV]  = (
                "" if self._bd_path is None else str(self._bd_path)
            )
        if selected(consts.VOL_MINOR):
            properties[consts.VOL_MINOR] = (
                str(self._volume.get_minor().get_value())
            )
        if selected(consts.TSTATE_PREFIX + consts.FLAG_DEPLOY):
            properties[consts.TSTATE_PREFIX + consts.FLAG_DEPLOY] = (
                bool_to_string(is_set(self._tstate, self.FLAG_DEPLOY))
            )
        if selected(consts.TSTATE_PREFIX + consts.FLAG_ATTACH):
            properties[consts.TSTATE_PREFIX + consts.FLAG_ATTACH] = (
                bool_to_string(is_set(self._tstate, self.FLAG_ATTACH))
            )
        if selected(consts.CSTATE_PREFIX + consts.FLAG_DEPLOY):
            properties[consts.CSTATE_PREFIX + consts.FLAG_DEPLOY] = (
                bool_to_string(is_set(self._cstate, self.FLAG_DEPLOY))
            )
        if selected(consts.CSTATE_PREFIX + consts.FLAG_ATTACH):
            properties[consts.CSTATE_PREFIX + consts.FLAG_ATTACH] = (
                bool_to_string(is_set(self._cstate, self.FLAG_ATTACH))
            )
        for (key, val) in self.get_props().iteritems():
            if selected(key):
                if val is not None:
                    properties[key] = str(val)
        return properties


class Assignment(GenericDrbdObject):

    """
    Representation of a resource assignment to a node

    An Assignment object represents the state of a resource that has been
    assigned to a node.
    """

    _node        = None
    _resource    = None
    _vol_states  = None
    _node_id     = None
    _snaps_assgs = None
    _cstate      = 0
    _tstate      = 0
    # return code of operations
    _rc          = 0

    # Reference to the server's get_serial() function
    _get_serial = None

    FLAG_DEPLOY    = 0x1
    FLAG_CONNECT   = 0x2
    FLAG_DISKLESS  = 0x4

    FLAG_UPD_CON   = 0x10000
    FLAG_RECONNECT = 0x20000
    # --overwrite-data-of-peer / primary --force
    FLAG_OVERWRITE = 0x40000
    # --discard-my-data upon connect / resolve split-brain
    FLAG_DISCARD   = 0x80000

    # CSTATE_MASK must include all valid current state flags;
    # used to mask the value supplied to set_cstate() to prevent setting
    # non-existent flags
    CSTATE_MASK    = FLAG_DEPLOY | FLAG_CONNECT | FLAG_DISKLESS

    # TSTATE_MASK must include all valid target state flags;
    # used to mask the value supplied to set_tstate() to prevent setting
    # non-existent flags
    TSTATE_MASK    = (FLAG_DEPLOY | FLAG_CONNECT | FLAG_DISKLESS |
                      FLAG_UPD_CON | FLAG_RECONNECT |
                      FLAG_OVERWRITE | FLAG_DISCARD)

    # Mask applied to ignore action flags on the target state
    # of an assignment.
    ACT_IGN_MASK   = (TSTATE_MASK ^ (FLAG_DISCARD | FLAG_OVERWRITE))


    def __init__(self, node, resource, node_id, cstate, tstate,
                 init_rc, vol_states,
                 get_serial_fn, init_serial, init_props):
        super(Assignment, self).__init__(
            get_serial_fn, init_serial, init_props
        )
        self._node         = node
        self._resource     = resource
        self._vol_states   = {}
        if vol_states is not None:
            for vol_state in vol_states:
                self._vol_states[vol_state.get_id()] = vol_state
        for volume in resource.iterate_volumes():
            if self._vol_states.get(volume.get_id()) is None:
                self._vol_states[volume.get_id()] = DrbdVolumeState(
                    volume,
                    0, 0, None, None,
                    get_serial_fn, None, None
                )
        self._node_id      = int(node_id)
        self._rc           = int(init_rc)
        self._snaps_assgs  = {}

        # current state
        checked_cstate = None
        if cstate is not None:
            try:
                checked_cstate = long(cstate)
            except ValueError:
                pass
        if checked_cstate is not None:
            self._cstate = checked_cstate & self.CSTATE_MASK
        else:
            self._cstate = 0

        # target state
        checked_tstate = None
        if tstate is not None:
            try:
                checked_tstate = long(tstate)
            except ValueError:
                pass
        if checked_tstate is not None:
            self._tstate = checked_tstate & self.TSTATE_MASK
        else:
            self._tstate = self.FLAG_DEPLOY

        self._tstate       = tstate
        self._rc           = 0
        self._get_serial   = get_serial_fn


    def get_node(self):
        return self._node


    def get_resource(self):
        return self._resource


    # used by AssignmentPersistence
    def add_volume_state(self, vol_state):
        self._vol_states[vol_state.get_id()] = vol_state
        self.get_props().new_serial()


    def iterate_volume_states(self):
        return self._vol_states.itervalues()


    def get_volume_state(self, vol_id):
        return self._vol_states.get(vol_id)


    def remove_volume_state(self, vol_id):
        vol_st = self._vol_states.get(vol_id)
        if vol_st is not None:
            del self._vol_states[vol_id]
            self.get_props().new_serial()


    def has_volume_states(self):
        return (True if len(self._vol_states) > 0 else False)


    def update_volume_states(self, serial):
        update_assg = False
        # create volume states for new volumes in the resource
        for volume in self._resource.iterate_volumes():
            # only process volumes that are not pending removal
            if is_unset(volume.get_state(), DrbdVolume.FLAG_REMOVE):
                vol_st = self._vol_states.get(volume.get_id())
                if vol_st is None:
                    update_assg = True
                    vol_st = DrbdVolumeState(
                        volume,
                        0, 0, None, None,
                        self._get_serial, None, None
                    )
                    self._vol_states[volume.get_id()] = vol_st
        # remove volume states for volumes that no longer exist in the resource
        for vol_st in self._vol_states.itervalues():
            volume = self._resource.get_volume(vol_st.get_id())
            if volume is None:
                update_assg = True
                del self._vol_states[vol_st.get_id()]
        if update_assg:
            self.get_props().new_serial()


    def add_snaps_assg(self, snaps_assg):
        snapshot = snaps_assg.get_snapshot()
        self._snaps_assgs[snapshot.get_name()] = snaps_assg
        self.get_props().new_serial()


    def init_add_snaps_assg(self, snaps_assg):
        self._snaps_assgs[snaps_assg.get_snapshot().get_name()] = snaps_assg


    def iterate_snaps_assgs(self):
        return self._snaps_assgs.itervalues()


    def get_snaps_assg(self, name):
        return self._snaps_assgs.get(name)


    def remove_snaps_assg(self, snaps_assg):
        try:
            snapshot = snaps_assg.get_snapshot()
            del self._snaps_assgs[snapshot.get_name()]
            self.get_props().new_serial()
        except KeyError:
            pass


    def has_snapshots(self):
        return (True if len(self._snaps_assgs) > 0 else False)


    def remove(self):
        removable = []
        for snaps_assg in self._snaps_assgs.itervalues():
            removable.append(snaps_assg)
        for snaps_assg in removable:
            snaps_assg.remove()
        self._node.remove_assignment(self)
        self._resource.remove_assignment(self)


    def get_node_id(self):
        return self._node_id


    def get_gross_size_kiB(self, peers):
        """
        Calculates the storage size occupied by this assignment
        """
        size_sum = 0
        if (is_set(self._cstate, Assignment.FLAG_DEPLOY) or
            is_set(self._tstate, Assignment.FLAG_DEPLOY)):
                for vol_state in self._vol_states.itervalues():
                    cstate = vol_state.get_cstate()
                    tstate = vol_state.get_tstate()
                    if (is_set(cstate, DrbdVolumeState.FLAG_DEPLOY) or
                        is_set(tstate, DrbdVolumeState.FLAG_DEPLOY)):
                            volume = vol_state.get_volume()
                            size_sum += MetaData.get_gross_data_kiB(
                                volume.get_size_kiB(), peers
                            )
        return size_sum


    def get_gross_size_kiB_correction(self, peers):
        """
        Calculates the storage size for not-yet-deployed assignments
        """
        size_sum = 0
        # TODO: Correct calculation for volumes that are transitioning from
        #       diskless to backing storage

        # Calculate corretions only for assignments that should be deployed
        if is_set(self._tstate, Assignment.FLAG_DEPLOY):
            # Calculate size correction, if the assignment is not deployed, but
            # should be deployed and should not be diskless:
            if (is_unset(self._cstate, Assignment.FLAG_DEPLOY) and
                is_unset(self._tstate, Assignment.FLAG_DISKLESS)):
                    size_sum += self._get_undeployed_corr(peers)
            # Calculate size correction, if the assignment is deployed, should
            # remain deployed, and is diskless, but should transition from
            # diskless to storage
            elif (is_set(self._cstate, Assignment.FLAG_DEPLOY) and
                  is_set(self._cstate, Assignment.FLAG_DISKLESS) and
                  is_unset(self._tstate, Assignment.FLAG_DISKLESS)):
                    size_sum += self._get_diskless_corr(peers)
        return size_sum


    def _get_undeployed_corr(self, peers):
        """
        Volume sizes for not-yet-deployed volumes
        """
        size_sum = 0
        for vol_state in self._vol_states.itervalues():
            cstate = vol_state.get_cstate()
            tstate = vol_state.get_tstate()
            if (is_unset(cstate, DrbdVolumeState.FLAG_DEPLOY) and
                is_set(tstate, DrbdVolumeState.FLAG_DEPLOY)):
                    volume = vol_state.get_volume()
                    size_sum += MetaData.get_gross_data_kiB(
                        volume.get_size_kiB(), peers
                    )
        return size_sum


    def _get_diskless_corr(self, peers):
        """
        Volumes sizes for volumes that transition from diskless to storage
        """
        size_sum = 0
        for vol_state in self._vol_states.itervalues():
            cstate = vol_state.get_cstate()
            tstate = vol_state.get_tstate()
            if (is_set(cstate, DrbdVolumeState.FLAG_DEPLOY) and
                is_set(tstate, DrbdVolumeState.FLAG_DEPLOY)):
                    volume = vol_state.get_volume()
                    size_sum += MetaData.get_gross_data_kiB(
                        volume.get_size_kiB(), peers
                    )
        return size_sum


    def is_snapshot_restore(self):
        """
        Returns True if any DrbdVolumeState is a snapshot restore, else False
        """
        result = False
        for vol_state in self._vol_states.itervalues():
            if vol_state.is_snapshot_restore():
                result = True
                break
        return result


    def get_cstate(self):
        return self._cstate


    def set_cstate(self, cstate):
        if cstate != self._cstate:
            self._cstate = cstate & self.CSTATE_MASK
            self.get_props().new_serial()


    def get_tstate(self):
        return self._tstate


    def set_tstate(self, tstate):
        if tstate != self._tstate:
            self._tstate = tstate & self.TSTATE_MASK
            self.get_props().new_serial()


    def deploy(self):
        """
        Sets the assignment's target state to deployed

        Used to indicate that an assignment's volumes should be deployed
        (installed) on the node
        """
        if is_unset(self._tstate, self.FLAG_DEPLOY):
            self._tstate = self._tstate | self.FLAG_DEPLOY
            self.get_props().new_serial()


    def undeploy(self):
        """
        Sets the assignment's target state to undeployed

        Used to indicate that an assignment's volumes should be undeployed
        (removed) from a node
        """
        if self._tstate != 0:
            self._tstate = 0
            self.get_props().new_serial()


    def undeploy_adjust_cstate(self):
        """
        Conditionally clears the current state of the assignment

        If the current state FLAG_DEPLOY is not set on any volume,
        none of the volumes has a block device, and the assignment's
        target state FLAG_DEPLOY is not set, then the assignment's
        current state is set to 0.
        """
        if is_unset(self._tstate, self.FLAG_DEPLOY):
            flag_clear = True
            bd_clear   = True
            for vol_state in self._vol_states.itervalues():
                vol_cstate  = vol_state.get_cstate()
                if is_set(vol_cstate, DrbdVolumeState.FLAG_DEPLOY):
                    flag_clear = False
                if vol_state.get_bd_name() is not None:
                    bd_clear = False
            if flag_clear and bd_clear:
                # Clear the assignment's current state to indicate that
                # undeploying the assignment was successful
                if self._cstate != 0:
                    self._cstate = 0
                    self.get_props().new_serial()


    def connect(self):
        """
        Sets the assignment's target state to connected

        Used to trigger a connect action on the assignment's resource
        """
        if is_unset(self._tstate, self.FLAG_CONNECT):
            self._tstate = self._tstate | self.FLAG_CONNECT
            self.get_props().new_serial()


    def reconnect(self):
        """
        Sets the reconnect action flag on the assignment's target state

        Used to trigger reconnection of the assignment's resource's network
        connections (effectively, disconnect immediately followed by connect)
        """
        if is_unset(self._tstate, self.FLAG_RECONNECT):
            self._tstate = self._tstate | self.FLAG_RECONNECT
            self.get_props().new_serial()


    def disconnect(self):
        """
        Sets the assignment's target state to disconnected

        Used to trigger a disconnect action on the assignment's resource
        """
        if is_set(self._tstate, self.FLAG_CONNECT):
            self._tstate = ((self._tstate | self.FLAG_CONNECT) ^
                            self.FLAG_CONNECT)
            self.get_props().new_serial()


    def deploy_client(self):
        """
        Sets the assignment's target state to diskless/deployed

        Used to indicate that an assignment's volumes should be connected
        on a node in DRBD9 client mode (without local storage)
        """
        if (is_unset(self._tstate, self.FLAG_DEPLOY) or
            is_unset(self._tstate, self.FLAG_DISKLESS)):
                self._tstate = (self._tstate | self.FLAG_DEPLOY |
                                self.FLAG_DISKLESS)
                self.get_props().new_serial()


    def update_connections(self):
        """
        Sets the UPDCON flag on an assignment's target state

        Used to indicate that the network connections of an assignment's
        resource have changed and should be updated. This should be achieved
        without disconnecting connections that are still valid
        (e.g., NOT disconnect/connect or reconnect)
        Commonly, drbdadm adjust should be a good candidate to update
        connections.
        """
        if (self._tstate & self.FLAG_UPD_CON) == 0:
            self._tstate = self._tstate | self.FLAG_UPD_CON
            self.get_props().new_serial()


    def set_rc(self, assg_rc):
        """
        Sets the return code of an action performed on the assignment

        Used to indicate the return code of failed actions, so the reason
        for a failed action can be queried on a remote node.
        """
        self._rc = assg_rc
        self.get_props().new_serial()


    def get_rc(self):
        """
        Retrieves the return code of an action performed on the assignment

        See set_rc.
        """
        return self._rc


    def is_deployed(self):
        """
        Returns the deployment state of an assignment.

        @return: True if the resource is currently deployed; False otherwise
        """
        return is_set(self._cstate, self.FLAG_DEPLOY)


    def is_connected(self):
        """
        Returns the connection state of an assignment.

        @return: True if the resource is currently connected; False otherwise
        """
        return is_set(self._cstate, self.FLAG_CONNECT)


    def is_empty(self):
        """
        Returns whether the assignment has any active volumes or not

        @return: True if the assignment has active volumes; False otherwise
        """
        empty = True
        if len(self._vol_states) > 0:
            for vol_state in self._vol_states.itervalues():
                if (is_set(vol_state.get_tstate(), self.FLAG_DEPLOY) or
                    is_set(vol_state.get_cstate(), self.FLAG_DEPLOY)):
                        empty = False
        return empty


    def requires_action(self):
        """
        Checks whether an assignment requires any action to be taken

        Actions can be required by either the assignment's state or by the
        state of one of the assignment's volume state objects.

        @returns: True if the assignment or a volume state requires action;
                  False otherwise
        """
        req_act = False
        for vol_state in self._vol_states.itervalues():
            if vol_state.requires_action():
                req_act = True
        return (self._tstate != self._cstate) or req_act


    def requires_deploy(self):
        """
        Returns whether the assignment needs to be deployed

        If an assignment is not currently deployed, but its target state
        requires it to be deployed, this function returns True to indicate
        that action must be taken to deploy this assignment.

        @returns: True if the assignment needs to be deployed; False otherwise
        """
        return (is_set(self._tstate, self.FLAG_DEPLOY) and
                is_unset(self._cstate, self.FLAG_DEPLOY))


    def requires_connect(self):
        """
        Returns whether the assignment requires a connect action

        Returns True only if the assignment's resource's current state is
        disconnected and its target state is connected.

        @returns: True if the assignment requires a connect action;
                  False otherwise
        """
        return (is_set(self._tstate, self.FLAG_CONNECT) and
                is_unset(self._cstate, self.FLAG_CONNECT))


    def requires_undeploy(self):
        """
        Returns whether the assignment needs to be undeployed

        If an assignment is currently deployed, but its target state
        requires it to be undeployed, this function returns True to indicate
        that action must be taken to undeploy this assignment.

        @returns: True if the assignment needs to be undeployed;
                  False otherwise
        """
        return (is_set(self._cstate, self.FLAG_DEPLOY) and
                is_unset(self._tstate, self.FLAG_DEPLOY))


    def requires_disconnect(self):
        """
        Returns whether the assignment requires a disconnect action

        Returns True only if the assignment's resource's current state is
        connected and its target state is disconnected.

        @returns: True if the assignment requires a disconnect action;
                  False otherwise
        """
        return (is_set(self._cstate, self.FLAG_CONNECT) and
                is_unset(self._tstate, self.FLAG_CONNECT))


    def set_cstate_flags(self, flags):
        saved_cstate = self._cstate
        self._cstate = (self._cstate | flags) & self.CSTATE_MASK
        if saved_cstate != self._cstate:
            self.get_props().new_serial()


    def clear_cstate_flags(self, flags):
        saved_cstate = self._cstate
        self._cstate = ((self._cstate | flags) ^ flags) & self.CSTATE_MASK
        if saved_cstate != self._cstate:
            self.get_props().new_serial()


    def set_tstate_flags(self, flags):
        saved_tstate = self._tstate
        self._tstate = (self._tstate | flags) & self.TSTATE_MASK
        if saved_tstate != self._tstate:
            self.get_props().new_serial()


    def clear_tstate_flags(self, flags):
        saved_tstate = self._tstate
        self._tstate = ((self._tstate | flags) ^ flags) & self.TSTATE_MASK
        if saved_tstate != self._tstate:
            self.get_props().new_serial()


    def filter_match(self, filter_props):
        match = False
        if filter_props is None or len(filter_props) == 0:
            match = True
        else:
            match = self.properties_match(filter_props)
            if not match:
                special_props_list = {
                    consts.NODE_ID     : str(self._node_id),
                    consts.TSTATE_PREFIX + consts.FLAG_DEPLOY :
                        bool_to_string(
                            is_set(self._tstate, self.FLAG_DEPLOY)
                        ),
                    consts.TSTATE_PREFIX + consts.FLAG_CONNECT :
                        bool_to_string(
                            is_set(self._tstate, self.FLAG_CONNECT)
                        ),
                    consts.TSTATE_PREFIX + consts.FLAG_DISKLESS :
                        bool_to_string(
                            is_set(self._tstate, self.FLAG_DISKLESS)
                        ),
                    consts.CSTATE_PREFIX + consts.FLAG_DEPLOY :
                        bool_to_string(
                            is_set(self._cstate, self.FLAG_DEPLOY)
                        ),
                    consts.CSTATE_PREFIX + consts.FLAG_CONNECT :
                        bool_to_string(
                            is_set(self._cstate, self.FLAG_CONNECT)
                        ),
                    consts.CSTATE_PREFIX + consts.FLAG_DISKLESS :
                        bool_to_string(
                            is_set(self._cstate, self.FLAG_DISKLESS)
                        ),
                }
                match = self.special_properties_match(
                    special_props_list, filter_props
                )
        return match


    def get_properties(self, req_props):
        properties = {}

        selector = Selector(req_props)
        if req_props is not None and len(req_props) > 0:
            selected = selector.list_selector
        else:
            selected = selector.all_selector

        if selected(consts.NODE_NAME):
            properties[consts.NODE_NAME] = self._node.get_name()
        if selected(consts.RES_NAME):
            properties[consts.RES_NAME]  = self._resource.get_name()
        if selected(consts.NODE_ID):
            properties[consts.NODE_ID]   = str(self._node_id)

        # target state flags
        if selected(consts.TSTATE_PREFIX + consts.FLAG_DEPLOY):
            properties[consts.TSTATE_PREFIX + consts.FLAG_DEPLOY] = (
                bool_to_string(
                    is_set(self._tstate, self.FLAG_DEPLOY)
                )
            )
        if selected(consts.TSTATE_PREFIX + consts.FLAG_CONNECT):
            properties[consts.TSTATE_PREFIX + consts.FLAG_CONNECT] = (
                bool_to_string(
                    is_set(self._tstate, self.FLAG_CONNECT)
                )
            )
        if selected(consts.TSTATE_PREFIX + consts.FLAG_DISKLESS):
            properties[consts.TSTATE_PREFIX + consts.FLAG_DISKLESS] = (
                bool_to_string(
                    is_set(self._tstate, self.FLAG_DISKLESS)
                )
            )
        # target state - action flags
        if selected(consts.TSTATE_PREFIX + consts.FLAG_UPD_CON):
            properties[consts.TSTATE_PREFIX + consts.FLAG_UPD_CON] = (
                bool_to_string(
                    is_set(self._tstate, self.FLAG_UPD_CON)
                )
            )
        if selected(consts.TSTATE_PREFIX + consts.FLAG_OVERWRITE):
            properties[consts.TSTATE_PREFIX + consts.FLAG_OVERWRITE] = (
                bool_to_string(
                    is_set(self._tstate, self.FLAG_OVERWRITE)
                )
            )
        if selected(consts.TSTATE_PREFIX + consts.FLAG_DISCARD):
            properties[consts.TSTATE_PREFIX + consts.FLAG_DISCARD] = (
                bool_to_string(
                    is_set(self._tstate, self.FLAG_DISCARD)
                )
            )
        if selected(consts.TSTATE_PREFIX + consts.FLAG_RECONNECT):
            properties[consts.TSTATE_PREFIX + consts.FLAG_RECONNECT] = (
                bool_to_string(
                    is_set(self._tstate, self.FLAG_RECONNECT)
                )
            )
        # current state flags
        if selected(consts.CSTATE_PREFIX + consts.FLAG_DEPLOY):
            properties[consts.CSTATE_PREFIX + consts.FLAG_DEPLOY] = (
                bool_to_string(
                    is_set(self._cstate, self.FLAG_DEPLOY)
                )
            )
        if selected(consts.CSTATE_PREFIX + consts.FLAG_CONNECT):
            properties[consts.CSTATE_PREFIX + consts.FLAG_CONNECT] = (
                bool_to_string(
                    is_set(self._cstate, self.FLAG_CONNECT)
                )
            )
        if selected(consts.CSTATE_PREFIX + consts.FLAG_DISKLESS):
            properties[consts.CSTATE_PREFIX + consts.FLAG_DISKLESS] = (
                bool_to_string(
                    is_set(self._cstate, self.FLAG_DISKLESS)
                )
            )

        for (key, val) in self.get_props().iteritems():
            if selected(key):
                if val is not None:
                    properties[key] = str(val)

        return properties
