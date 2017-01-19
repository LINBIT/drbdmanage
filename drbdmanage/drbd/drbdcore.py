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
import logging
import time
import subprocess
import drbdmanage.utils as utils
import drbdmanage.consts as consts
import drbdmanage.conf.conffile
import drbdmanage.snapshots.snapshots as snapshots
import drbdmanage.exceptions as dmexc
import drbdmanage.drbd.commands as drbdcmd
import drbdmanage.drbd.metadata as md
import drbdmanage.propscontainer
import drbdmanage.messagelog as msglog

# Breaks the import system; maybe one day when everything has been rewritten
# to use only 'import xy' it might work...
# Until then, just misuse self._server instead of DrbdManageServer.CONSTANT,
# because Python does not know the difference between variables, constants,
# class members and instance members anyway
# import drbdmanage.server
import drbdmanage.storage.storagecore as storcore

"""
WARNING!
  do not import anything from drbdmanage.drbd.persistence
"""
from drbdmanage.storage.storagecommon import GenericStorage
from drbdmanage.drbd.drbdcommon import GenericDrbdObject
from drbdmanage.exceptions import (
    InvalidAddrFamException, VolSizeRangeException, PersistenceException, QuorumException
)
from drbdmanage.exceptions import DM_SUCCESS, DM_ESTORAGE
from drbdmanage.utils import (
    Selector, bool_to_string, is_set, is_unset, check_node_name, generate_gi_hex_string,
    log_in_out
)


class DrbdManager(object):

    """
    Manages deployment/undeployment of DRBD resources
    """

    _server  = None
    _drbdadm = None
    _resconf = None

    # Used as a return code to indicate that undeploying volumes failed
    STOR_UNDEPLOY_FAILED = 126

    @log_in_out
    def __init__(self, ref_server):
        self._server  = ref_server
        self._resconf = drbdmanage.conf.conffile.DrbdAdmConf(self._server._objects_root)
        self.reconfigure()  # creates DrbdAdm object. THINK: why should that object ever by recreated?


    # FIXME
    # TODO: currently, if the configuration can not be loaded (due to
    #       errors in the data structures on the disk), the server's
    #       hash is not updated, so the DrbdManager can begin to loop,
    #       trying to load the new configuration. If DrbdManager fails
    #       to load the configuration, it should probably rather stop
    #       than loop at some point.
    @log_in_out
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
        self._server._sat_lock.acquire()
        persist = None
        data_changed = False
        failed_actions = False
        logging.debug("DrbdManager: invoked")
        try:
            # Always perform changes requested for the local node if the
            # hash check is overridden
            run_changes = override_hash_check
            if not override_hash_check:
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

            if run_changes:
                # self._server.export_conf('*')
                # close the read-only stream, then lock and open the
                # configuration for reading and writing
                persist = self._server.begin_modify_conf()
                if persist is not None:
                    loaded_hash = persist.get_stored_hash()
                    changed, failed_actions = self.perform_changes()
                    if poke_cluster:
                        # increase the serial number, implicitly changing the
                        # hash and thereby running requested changes on all
                        # cluster nodes
                        self._server.get_serial()
                        changed = True
                    if changed:
                        logging.debug("DrbdManager: state changed, "
                                      "saving control volume data")
                        self._server.save_conf_data(persist)
                        # Report changed data back only if the hash has changed too
                        if not self._server.hashes_match(loaded_hash):
                            data_changed = True
                    else:
                        logging.debug("DrbdManager: state unchanged")
                else:
                    logging.debug("DrbdManager: cannot open the "
                                  "control volume (read-write)")
        except PersistenceException:
            exc_type, exc_obj, exc_trace = sys.exc_info()
            logging.debug("DrbdManager: caught PersistenceException:")
            logging.debug("Stack trace:\n%s" % (str(exc_trace)))
        except QuorumException:
            log_message = (
                "DrbdManager: Check for pending actions skipped, "
                "partition does not have a quorum"
            )
            logging.warning(log_message)
            self._server.get_message_log().add_entry(msglog.MessageLog.WARN, log_message)
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
            self._server._sat_lock.release()
        return data_changed, failed_actions


    # FIXME
    # - still requires more error handling
    # - external configuration file (for drbdadm before-resync-target, etc.)
    #   probably needs to be updated in some more locations
    # - some things, like deploying a resource, still need to become more
    #   robust, because sometimes meta data fails to initialize, etc., and
    #   there needs to be correction code for those cases
    @log_in_out
    def perform_changes(self):
        """
        Calls worker functions for required resource state changes

        Determines which state changes are required for each resource and
        calls functions to attempt to reach the desired target state.

        @return: True if the state of any resource has changed, False otherwise
        @rtype:  bool
        """
        state_changed  = False
        pool_changed   = False
        failed_actions = False

        """
        Check whether the system the drbdmanaged server is running on is
        a registered node in the configuration
        """
        node = self._server.get_instance_node()
        if node is None:
            log_message = (
                "DrbdManager: abort, this node ('%s') has no entry in "
                "the data tables"
                % (self._server.get_instance_node_name())
            )
            logging.warning(log_message)
            self._server.get_message_log().add_entry(msglog.MessageLog.WARN, log_message)
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
        max_fail_count = self._get_max_fail_count()
        for assg in node.iterate_assignments():
            resource = assg.get_resource()
            managed = resource.is_managed()
            if not managed:
                log_message = (
                    "Resource '%s' is marked as unmanaged"
                    % (resource.get_name())
                )
                logging.warning(log_message)
                self._server.get_message_log().add_entry(msglog.MessageLog.WARN, log_message)
            # Assignment changes
            fail_count = assg.get_fail_count()
            if fail_count < max_fail_count:
                set_state_changed  = False
                set_pool_changed   = False
                set_failed_actions = False
                try:
                    if managed:
                        (set_state_changed, set_pool_changed, set_failed_actions) = (
                            self._assignment_actions(assg)
                        )
                    else:
                        logging.debug(
                            "Resource '%s' is marked as unmanaged, skipping _assignment_actions()"
                            % (resource.get_name())
                        )
                except dmexc.ResourceFileException as res_exc:
                    log_message = "DrbdManager: %s" % (res_exc.get_log_message())
                    logging.error(log_message)
                    assg.increase_fail_count()
                    set_failed_actions = True
                    set_state_changed  = True

                if set_state_changed:
                    state_changed = True
                if set_pool_changed:
                    pool_changed = True
                if set_failed_actions:
                    failed_actions = True
            else:
                failed_actions = True

            # Snapshot changes
            fail_count = assg.get_fail_count()
            if fail_count < max_fail_count:
                if managed:
                    (set_state_changed, set_pool_changed, set_failed_actions) = (
                        self._snapshot_actions(assg)
                    )
                else:
                    logging.debug(
                        "Resource '%s' is marked as unmanaged, skipping _snapshot_actions()"
                        % (resource.get_name())
                    )
                if set_state_changed:
                    state_changed = True
                if set_pool_changed:
                    pool_changed = True
                if set_failed_actions:
                    failed_actions = True
            else:
                failed_actions = True

            if state_changed and not failed_actions:
                # If actions were performed and none of them failed,
                # clear any previously existing fail count
                assg.clear_fail_count()

        """
        Send new ctrlvol state to satellites
        """
        if self._server._server_role == consts.SAT_LEADER_NODE:
            proxy = self._server._proxy

            self._server._persist.json_export(self._server._objects_root)
            final_ctrl_vol = None
            changed_at_all = True

            at_least_one_failed_cnt = 0
            # if everything is nice and cosy, we send aroud the ctrlvolume as long till nobody has any
            # further changes (changed_at_all). Base case how to do it.
            # but there might be progress even though old things failed, in that case send the updated
            # volume around for one round (every satellite adds its changes), but then break to avoid
            # endless loops.

            overall_loop_cnt = 0
            while changed_at_all:
                overall_loop_cnt += 1
                changed_at_all = False
                at_least_one_failed = False
                for sat_name in self._server.get_reachable_satellite_names().union(self._server._sat_proposed_shutdown):
                    opcode, length, data = proxy.send_cmd(sat_name, consts.KEY_S_CMD_UPDATE)
                    if opcode == proxy.opcodes[consts.KEY_S_ANS_E_COMM]:  # give it a second chance
                        opcode, length, data = proxy.send_cmd(sat_name, consts.KEY_S_CMD_UPDATE)

                    if opcode == proxy.opcodes[consts.KEY_S_ANS_CHANGED_FAILED]:
                        at_least_one_failed = True

                    # if the satellite has nothing more to say and is already in the proposed
                    # shutdown set(), add it to the set where it really gets a shutdown.
                    if opcode == proxy.opcodes[consts.KEY_S_ANS_UNCHANGED] and \
                       sat_name in self._server._sat_proposed_shutdown:
                        try:
                            self._server._sat_proposed_shutdown.remove(sat_name)
                        except:
                            pass
                        self._server._sat_shutdown.add(sat_name)

                    if opcode == proxy.opcodes[consts.KEY_S_ANS_CHANGED] or \
                       opcode == proxy.opcodes[consts.KEY_S_ANS_CHANGED_FAILED]:
                        changed_at_all = True
                        final_ctrl_vol = data
                        # set_json_data is required, next send_cmd() reads that value!
                        # do not remove following call:
                        self._server._persist.set_json_data(final_ctrl_vol)

                if at_least_one_failed:
                    at_least_one_failed_cnt += 1
                if at_least_one_failed_cnt == 5 or overall_loop_cnt == 7:
                    break

            if final_ctrl_vol:
                self._server._persist.set_json_data(final_ctrl_vol)
                self._server._persist.json_import(self._server._objects_root)
                state_changed = True

        """
        Cleanup the server's data structures
        (remove entries that are no longer required)
        """
        self._server.cleanup()

        if pool_changed:
            self._server.update_pool_data()

        return state_changed, failed_actions


    @log_in_out
    def _assignment_actions(self, assg):
        """
        ============================================================
        Actions for assignments
        (concerning all volumes of a resource)
        ============================================================
        """
        state_changed  = False
        pool_changed   = False
        failed_actions = False

        assg_cstate = assg.get_cstate()
        assg_tstate = assg.get_tstate()

        act_flag = assg.requires_action()
        if act_flag and assg.is_empty():
            if assg.requires_undeploy():
                # Assignment has no volumes deployed and is effectively
                # disabled; Nothing to do, except for setting the correct
                # state, so the assignment can be cleaned up
                pool_changed = True
                state_changed = True
                fn_rc = self._undeploy_assignment(assg)
                assg.set_rc(fn_rc)
                assg.undeploy_adjust_cstate()
                if fn_rc != 0:
                    failed_actions = True
        elif act_flag:
            logging.debug(
                "DrbdManager: Assigned resource '%s' cstate(%x) -> tstate(%x)"
                % (assg.get_resource().get_name(),
                   assg_cstate, assg_tstate)
            )

            """
            Undeploy an assignment/resource and all of its volumes
            """
            if assg.requires_undeploy():
                pool_changed = True
                state_changed = True
                fn_rc = self._undeploy_assignment(assg)
                assg.set_rc(fn_rc)
                if fn_rc != 0:
                    failed_actions = True
            else:
                """
                Disconnect an assignment/resource
                """
                if assg.requires_disconnect():
                    state_changed = True
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
                        state_changed = True
                        fn_rc = self._update_connections(assg)
                        assg.set_rc(fn_rc)
                        if fn_rc != 0:
                            failed_actions = True
                    if is_set(assg_actions, Assignment.FLAG_RECONNECT):
                        state_changed = True
                        fn_rc = self._reconnect(assg)
                        assg.set_rc(fn_rc)
                        if fn_rc != 0:
                            failed_actions = True

                """
                Update config (triggered by {net,disk,resource}-options command)
                """
                if (not failed_actions) and is_set(assg_actions, Assignment.FLAG_UPD_CONFIG):
                    state_changed = True
                    fn_rc = self._reconfigure_assignment(assg)
                    if fn_rc == 0:
                        assg.clear_tstate_flags(Assignment.FLAG_UPD_CONFIG)
                    else:
                        failed_actions = True

                """
                ============================================================
                Per-Volume actions
                (actions that concern a single volume of a resource)
                ============================================================
                """
                for vol_state in assg.iterate_volume_states():
                    (set_state_changed, set_pool_changed, set_failed_actions) = (
                        self._volume_actions(assg, vol_state)
                    )
                    if set_state_changed:
                        state_changed = True
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

                # Finishes transitions between server assignments and
                # diskless client assignments
                if (is_set(assg_tstate, Assignment.FLAG_DISKLESS) and
                    is_unset(assg_cstate, Assignment.FLAG_DISKLESS)):
                    # Check whether all volume states indicate diskless
                    has_disks = False
                    for vol_state in assg.iterate_volume_states():
                        if vol_state.get_bd_name() is not None:
                            has_disks = True
                            break
                    if not has_disks:
                        assg.set_cstate_flags(Assignment.FLAG_DISKLESS)
                elif (is_unset(assg_tstate, Assignment.FLAG_DISKLESS) and
                      is_set(assg_cstate, Assignment.FLAG_DISKLESS)):
                    # Check whether all volume states indicate a disk
                    has_disks = True
                    for vol_state in assg.iterate_volume_states():
                        if vol_state.get_bd_name() is None:
                            has_disks = False
                            break
                    if has_disks:
                        assg.clear_cstate_flags(Assignment.FLAG_DISKLESS)

                # FIXME: this should probably be changed so that the other
                #        actions (deploy/connect/etc.) do not operate on the
                #        assignment any more if it has no active volumes;
                #        for now, this just skips every other action and
                #        sets the assignment's current state to 0 again to
                #        make sure that it can be cleaned up as soon as its
                #        target state changes to 0 (e.g., unassign/undeploy).
                if assg.is_empty():
                    if assg_cstate != 0:
                        state_changed = True
                        assg.undeploy_adjust_cstate()
                else:
                    """
                    Deploy an assignment (finish deploying)
                    Volumes have already been deployed by the per-volume
                    actions at this point. Only if all volumes that should be
                    deployed have been deployed (current state vs.
                    target state), then mark the assignment as deployed, too.
                    """
                    if (not failed_actions) and assg.requires_deploy():
                        state_changed = True
                        pool_changed  = True
                        fn_rc = self._deploy_assignment(assg)
                        assg.set_rc(fn_rc)
                        if fn_rc != 0:
                            failed_actions = True

                    if (not failed_actions) and assg.requires_connect():
                        state_changed = True
                        fn_rc = self._connect(assg)
                        assg.set_rc(fn_rc)
                        if fn_rc != 0:
                            failed_actions = True

            if failed_actions:
                logging.debug("DrbdManager: _assignment_actions(): increasing assignment fail count")
                assg.increase_fail_count()
        if state_changed:
            assg.notify_changed()
        return (state_changed, pool_changed, failed_actions)


    @log_in_out
    def _volume_actions(self, assg, vol_state):
        """
        Deploy or undeploy a volume
        """
        state_changed  = False
        pool_changed   = False
        failed_actions = False

        assg_cstate = assg.get_cstate()
        assg_tstate = assg.get_tstate()

        max_peers = self._server.DEFAULT_MAX_PEERS
        try:
            max_peers = int(
                self._server.get_conf_value(
                    self._server.KEY_MAX_PEERS
                )
            )
        except ValueError:
            pass

        if vol_state.requires_undeploy():
            pool_changed  = True
            state_changed = True
            fn_rc = self._undeploy_volume(assg, vol_state)
            assg.set_rc(fn_rc)
            if fn_rc != 0:
                failed_actions = True
        else:
            if vol_state.requires_deploy():
                pool_changed = True
                state_changed = True
                fn_rc = self._deploy_volume_actions(assg, vol_state, max_peers)
                assg.set_rc(fn_rc)
                if fn_rc != 0:
                    failed_actions = True

            if (not failed_actions) and vol_state.requires_resize_storage():
                logging.debug(
                    "Resource '%s' Volume %d: requires_resize_storage() == True",
                    assg.get_resource().get_name(), vol_state.get_id()
                )
                pool_changed = True
                state_changed = True
                sub_rc = self._resize_volume_blockdevice(assg, vol_state, max_peers)
                if sub_rc != DM_SUCCESS:
                    failed_actions = True

                # Check immediately whether the DRBD can be resized too
                if self._is_resize_storage_finished(assg, vol_state.get_id()):
                    logging.debug(
                        "Resource '%s' Volume %d: _is_resize_storage_finished() == True",
                        assg.get_resource().get_name(), vol_state.get_id()
                    )
                    sub_rc = self._resize_volume_drbd(assg, vol_state)
                    if sub_rc != DM_SUCCESS:
                        failed_actions = True
                else:
                    logging.debug(
                        "Resource '%s' Volume %d: _is_resize_storage_finished() == False",
                        assg.get_resource().get_name(), vol_state.get_id()
                    )
            elif vol_state.requires_resize_drbd():
                logging.debug(
                    "Resource '%s' Volume %d: requires_resize_drbd() == True",
                    assg.get_resource().get_name(), vol_state.get_id()
                )
                if self._is_resize_storage_finished(assg, vol_state.get_id()):
                    logging.debug(
                        "Resource '%s' Volume %d: _is_resize_storage_finished() == True",
                        assg.get_resource().get_name(), vol_state.get_id()
                    )
                    pool_changed = True
                    state_changed = True
                    sub_rc = self._resize_volume_drbd(assg, vol_state)
                    if sub_rc != DM_SUCCESS:
                        failed_actions = True
                else:
                    logging.debug(
                        "Resource '%s' Volume %d: _is_resize_storage_finished() == False",
                        assg.get_resource().get_name(), vol_state.get_id()
                    )
            """
            Attach a volume to or detach a volume from local storage
            """
            if (not failed_actions):
                if vol_state.requires_attach():
                    state_changed = True
                    fn_rc = self._attach(assg, vol_state)
                    assg.set_rc(fn_rc)
                    if fn_rc != 0:
                        failed_actions = True
                elif (vol_state.requires_detach()):
                    state_changed = True
                    fn_rc = self._detach(assg, vol_state)
                    assg.set_rc(fn_rc)
                    if fn_rc != 0:
                        failed_actions = True

            """
            Transition from diskless client to server or vice-versa
            """
            if (not failed_actions):
                if (is_set(assg_tstate, Assignment.FLAG_DISKLESS) and
                    is_unset(assg_cstate, Assignment.FLAG_DISKLESS)):
                    # Server -> diskless client transition
                    if vol_state.get_bd_name() is not None:
                        state_changed = True
                        vol_state.clear_tstate_flags(DrbdVolumeState.FLAG_ATTACH)
                        fn_rc = self._undeploy_volume(assg, vol_state)
                        if fn_rc != 0:
                            failed_actions = True
                elif (is_unset(assg_tstate, Assignment.FLAG_DISKLESS) and
                      is_set(assg_cstate, Assignment.FLAG_DISKLESS)):
                    # Diskless client -> server transition
                    if vol_state.get_bd_name() is None:
                        state_changed = True
                        # Remove any previously set snapshot restoration properties
                        vol_state.get_props().remove_prop(consts.SNAPS_SRC_BLOCKDEV)
                        vol_state.set_cstate_flags(DrbdVolumeState.FLAG_ATTACH)
                        vol_state.set_tstate_flags(DrbdVolumeState.FLAG_ATTACH)
                        fn_rc = self._deploy_volume_actions(assg, vol_state, max_peers)
                        if fn_rc != 0:
                            failed_actions = True

        logging.debug("DrbdManager: Exit function _volume_actions()")
        return (state_changed, pool_changed, failed_actions)

    @log_in_out
    def _snapshot_actions(self, assg):
        state_changed  = False
        failed_actions = False
        pool_changed   = False
        # Operate only on deployed assignments
        for snaps_assg in assg.iterate_snaps_assgs():
            set_state_changed = False
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
                assg_cstate = assg.get_cstate()
                assg_tstate = assg.get_tstate()
                if (is_set(assg_cstate, Assignment.FLAG_DEPLOY) and
                    is_set(assg_tstate, Assignment.FLAG_DEPLOY)):
                    error_code = snaps_assg.get_error_code()
                    if error_code == 0:
                        set_state_changed = True
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
                set_state_changed = True
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
                        (vol_set_state_changed, set_pool_changed, set_failed_actions) = (
                            self._snaps_volume_actions(
                                snaps_assg, snaps_vol_state
                            )
                        )
                        if vol_set_state_changed:
                            set_state_changed = True
                        if set_pool_changed:
                            pool_changed = True
                        if set_failed_actions:
                            failed_actions = True
            if set_state_changed:
                state_changed = True
                snaps_assg.notify_changed()
        if failed_actions:
            logging.debug("DrbdManager: _snapshot_actions(): increasing assignment fail count")
            assg.increase_fail_count()
        return (state_changed, pool_changed, failed_actions)

    @log_in_out
    def _snaps_volume_actions(self, snaps_assg, snaps_vol_state):
        pool_changed   = False
        failed_actions = False
        state_changed  = False
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
            state_changed = True
            pool_changed, failed_actions = (
                self._snaps_deploy_volume(snaps_assg, snaps_vol_state)
            )
        elif snaps_vol_state.requires_undeploy():
            # Undeploy snapshots
            state_changed = True
            pool_changed, failed_actions = (
                self._snaps_undeploy_volume(snaps_assg, snaps_vol_state)
            )
        return (state_changed, pool_changed, failed_actions)

    @log_in_out
    def _snaps_deploy_volume(self, snaps_assg, snaps_vol_state):
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
                    log_message = (
                        "Failed to create snapshot %s/%s #%u "
                        "of source volume %s"
                         % (resource.get_name(), snaps_name, snaps_vol_id,
                            src_bd_name)
                    )
                    logging.error(log_message)
                    self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)
                    failed_actions = True
            else:
                log_message = (
                    "Cannot create snapshot %s/%s #%u, "
                    "source volume is not deployed"
                     % (resource.get_name(), snaps_name, snaps_vol_id)
                )
                logging.error(log_message)
                self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)
                failed_actions = True
        else:
            log_message = (
                "Snapshot %s/%s references non-existent volume id %d of "
                "its source resource"
                % (resource.get_name(), snaps.get_name(), snaps_vol_id)
            )
            logging.error(log_message)
            self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)
            failed_actions = True
        if failed_actions:
            snaps_assg.set_error_code(dmexc.DM_ESTORAGE)
        return (pool_changed, failed_actions)

    @log_in_out
    def _snaps_undeploy_volume(self, snaps_assg, snaps_vol_state):
        pool_changed   = False
        failed_actions = False
        snaps          = snaps_assg.get_snapshot()
        snaps_name     = snaps.get_name()
        snaps_vol_id   = snaps_vol_state.get_id()

        fn_rc = DM_ESTORAGE
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
            log_message = (
                "Failed to remove snapshot %s #%u block device '%s'"
                 % (snaps_name, snaps_vol_id, bd_name)
            )
            logging.error(log_message)
            self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)
            failed_actions = True
        return (pool_changed, failed_actions)

    @log_in_out
    def adjust_drbdctrl(self, was_previous_leader=False):
        sat_state = consts.SAT_POTENTIAL_LEADER_NODE
        drbdctrl_res_name = consts.DRBDCTRL_RES_NAME

        # call drbdadm to bring up the control volume
        # discard=was_previous_leader:
        # e.g. there was a 3 node cluster, leader got disconnected, other nodes elected new leader
        # old-leader runs reelection and has to discard his data

        fn_rc = self._drbdadm.adjust(drbdctrl_res_name, discard=was_previous_leader)
        if fn_rc != 0:
            res_file_name = os.path.join(consts.DRBDCTRL_RES_PATH, drbdctrl_res_name)
            res_file_exits = os.path.isfile(res_file_name)
            import subprocess
            ctrlvol_exits = True if subprocess.call(["drbdsetup", "status", ".drbdctrl"]) == 0 else False

            if res_file_exits or ctrlvol_exits:
                sat_state = consts.SAT_POTENTIAL_LEADER_NODE
            else:
                sat_state = consts.SAT_SATELLITE
        return fn_rc, sat_state

    @log_in_out
    def leader_election(self, force_win=False):
        timeout = 2
        quorum_nodes = self._server._quorum.get_full_member_count()
        wait_by_connect = False
        if quorum_nodes <= 2:
            wait_by_connect = True

        if force_win:
            logging.info('Leader election by forcing success')
            succ = True
        elif wait_by_connect:
            logging.info('Leader election by wait for connections')
            succ = self._drbdadm.wait_connect_resource(consts.DRBDCTRL_RES_NAME, timeout=timeout)
        else:
            logging.info('Leader election by quorum (%d Nodes)' % quorum_nodes)
            nr_nodes = self._server._quorum.get_active_member_count()
            succ = self._server._quorum.is_present()
            if succ:
                logging.info('Leader election: got quorum (%d/%d) nodes' % (nr_nodes, quorum_nodes))
            else:
                logging.info('Leader election: no quorum (%d/%d) nodes' % (nr_nodes, quorum_nodes))
                time.sleep(timeout)

        if not succ:
            return False, False

        fn_rc = self._drbdadm.primary(consts.DRBDCTRL_RES_NAME, force=False, with_drbdsetup=True)
        won = (fn_rc == 0)
        logging.info('Leader election: %s election' % ('won' if won else 'lost'))
        return succ, won

    @log_in_out
    def down_drbdctrl(self):
        # call drbdadm to stop the control volume
        fn_rc = self._drbdadm.down(consts.DRBDCTRL_RES_NAME)
        return fn_rc

    @log_in_out
    def secondary_drbdctrl(self):
        # call drbdadm to stop the control volume
        fn_rc = self._drbdadm.secondary(consts.DRBDCTRL_RES_NAME)
        return fn_rc

    @log_in_out
    def initial_up(self):
        """
        Attempts to bring up all deployed resources.
        Used when the drbdmanage server starts up.
        """
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
    @log_in_out
    def final_down(self):
        """
        Attempts to shut down all resources
        Used when the drbdmanage server shuts down.
        """
        node = self._server.get_instance_node()
        if node is not None:
            for assg in node.iterate_assignments():
                try:
                    self._down_resource(assg)
                except Exception as exc:
                    logging.debug(
                        "failed to shut down resource '%s', "
                        "unhandled exception: %s"
                        % (assg.get_resource().get_name(), str(exc))
                    )

    @log_in_out
    def check_res_file(self, res_name, tmp_res_file_path, res_file_path):
        ok = self._drbdadm.check_res_file(res_name, tmp_res_file_path, res_file_path)
        return ok

    @log_in_out
    def _up_resource(self, assignment):
        """
        Brings up DRBD resources
        """
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
                        log_message = (
                            "resource '%s': attempt to start the backend "
                            "blockdevice '%s' failed"
                            % (res_name, bd_name)
                        )
                        logging.warning(log_message)
                        self._server.get_message_log().add_entry(msglog.MessageLog.WARN, log_message)

            # update the configuration file
            self._server.export_assignment_conf(assignment)

            # call drbdadm to bring up the resource
            fn_rc = self._drbdadm.adjust(resource.get_name())

        return fn_rc

    @log_in_out
    def _down_resource(self, assignment):
        """
        Brings down DRBD resources
        """
        # Currently unused; bd_mgr might stop the backend volume in the future
        # bd_mgr   = self._server.get_bd_mgr()
        resource = assignment.get_resource()

        logging.info("DrbdManager: Stopping resource '%s'" % (resource.get_name()))

        # call drbdadm to bring down the resource
        self._server.export_assignment_conf(assignment)
        fn_rc = self._drbdadm.down(resource.get_name())

        return fn_rc

    @log_in_out
    def _delete_drbd_info_file(self, volume):
        """
        Deletes any old DRBD block device info file for the volume's minor number
        """
        # FIXME: The /var/lib/drbd path should probably be configurable
        minor_obj = volume.get_minor()
        minor_nr = minor_obj.get_value()
        file_name = ("/var/lib/drbd/drbd-minor-%d.lkbd" % (minor_nr))
        self._server.remove_file(file_name)

    @log_in_out
    def _deploy_volume_actions(self, assignment, vol_state, max_peers):
        """
        Deploys a volumes or restores a snapshot
        """
        # Attempt to delete any old DRBD block device info file for the new volume's minor number
        self._delete_drbd_info_file(vol_state.get_volume())

        diskless = is_set(assignment.get_tstate(), Assignment.FLAG_DISKLESS)
        blockdev = None
        resource = assignment.get_resource()

        failed_actions = False

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
                            log_message = (
                                "Volume state list mismatch between multiple "
                                "assignments for resource %s"
                                % (assg_res.get_name())
                            )
                            logging.error(log_message)
                            self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)
                    vol_states[assg_node.get_name()] = assg_vol_states

            # ============================================================
            # Meta-data creation
            # ============================================================
            # Meta data creation for assignments that have local storage
            thin_flag = self._is_thin_provisioning()
            initial_flag = self._is_initial_deployer(assignment, vol_state)
            if (not failed_actions) and (not diskless) and src_bd_name is None:
                fn_rc = self._deploy_volume_metadata(
                    assignment, vol_state, max_peers, nodes, vol_states,
                    thin_flag, initial_flag
                )
                if fn_rc != 0:
                    failed_actions = True

            if (not failed_actions):
                # ============================================================
                # DRBD initialization
                # ============================================================
                # FIXME: this does not need to run if metadata
                #        creation failed
                # Adjust the DRBD resource to configure the volume
                res_name = resource.get_name()
                assg_conf, global_conf = self._server.open_assignment_conf(res_name)
                self._resconf.write_excerpt(assg_conf, assignment,
                                            nodes, vol_states, global_conf)
                self._server.close_assignment_conf(assg_conf, global_conf)
                self._server.update_assignment_conf(res_name)

                vol_id = vol_state.get_id()
                fn_rc = self._drbdadm.adjust(res_name)
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
                        if thin_flag:
                            vol_state.get_props().set_prop(DrbdVolumeState.KEY_IS_THIN, consts.BOOL_TRUE)
                        else:
                            vol_state.get_props().remove_prop(DrbdVolumeState.KEY_IS_THIN)
                        fn_rc = 0
                    # "drbdadm adjust" implicitly connects the resource
                    assignment.set_cstate_flags(Assignment.FLAG_CONNECT)
                else:
                    log_message = (
                        "Deploying resource '%s' volume %d: DRBD adjust command failed"
                        % (res_name, vol_id)
                    )
                    logging.error(log_message)
                    self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)

                # Run new-current-uuid on initial deployment of a thinly provisioned volume
                if initial_flag and thin_flag and (not self._is_snapshot_restore(resource, vol_id)):
                    new_gi_check = self._drbdadm.new_current_uuid(res_name, vol_id)
                    if new_gi_check:
                        fn_rc = 0
                    else:
                        fn_rc = drbdcmd.DrbdAdm.DRBDUTIL_EXEC_FAILED
                        log_message = (
                            "Deploying resource '%s' volume %d: DRBD new-current-uuid command failed"
                            % (res_name, vol_id)
                        )
                        logging.error(log_message)
                        self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)

        return fn_rc

    @log_in_out
    def _deploy_volume_blockdev(self, assignment, vol_state, max_peers):
        """
        Creates a new empty block device for a new volume
        """
        fn_rc    = -1
        bd_mgr   = self._server.get_bd_mgr()
        resource = assignment.get_resource()
        volume   = resource.get_volume(vol_state.get_id())
        blockdev = None

        net_size = volume.get_size_kiB()
        is_resizing = False
        if vol_state.requires_resize_storage():
            is_resizing = True
            try:
                net_size = volume.get_resize_value()
            except ValueError:
                logging.debug(
                    "DrbdManager: _deploy_volume_blockdev(): Encountered an invalid volume resize value: "
                    "Resource '%s, Volume %d"
                    % (resource.get_name(), volume.get_id())
                )
        try:
            gross_size = md.MetaData.get_gross_kiB(
                net_size, max_peers, md.MetaData.DEFAULT_AL_STRIPES, md.MetaData.DEFAULT_AL_kiB
            )
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
                if is_resizing:
                    vol_state.finish_resize_storage()
                fn_rc = 0
            else:
                log_message = (
                    "DrbdManager: Failed to create block device for resource '%s' volume %d"
                    % (resource.get_name(), vol_state.get_id())
                )
                logging.error(log_message)
                self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)
        except md.MetaDataException as md_exc:
            logging.debug("DrbdManager: _deploy_volume_blockdev(): MetaDataException: " + md_exc.message)
            log_message = (
                "Meta data creation failed: %s"
                % (md_exc.message)
            )
            self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)

        return fn_rc, blockdev

    @log_in_out
    def _deploy_volume_from_snapshot(self, assignment, vol_state, src_bd_name):
        """
        Creates a new blockdevice from a snapshot
        """
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

            # Create DRBD info file with current device size
            volume = vol_state.get_volume()
            minor_obj = volume.get_minor()
            minor_nr = minor_obj.get_value()
            info_file_path = "/var/lib/drbd/drbd-minor-%d.lkbd" % (minor_nr)
            logging.debug("Updating DRBD block device info file '%s'" % (info_file_path))
            info_file = None
            try:
                info_file = open(info_file_path, "w")
                logging.debug("Running 'blockdev --getsize64 %s'" % (bd_path))
                proc = subprocess.Popen(["blockdev", "--getsize64", bd_path], stdout=info_file)
                proc.wait()
            except IOError as io_err:
                log_message = (
                    "Failed to update DRBD block device info file '%s', I/O error encountered"
                     % (info_file_path)
                )
                logging.error(log_message)
                self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)
            if info_file is not None:
                try:
                    info_file.close()
                except IOError:
                    pass

            # If a resize operation is required, resize the backend device
            if vol_state.requires_resize_storage():
                max_peers = self._server.DEFAULT_MAX_PEERS
                try:
                    max_peers = int(
                        self._server.get_conf_value(
                            self._server.KEY_MAX_PEERS
                        )
                    )
                except ValueError:
                    pass
                sub_rc = self._resize_volume_blockdevice(assignment, vol_state, max_peers)
                if sub_rc == DM_SUCCESS:
                    fn_rc = 0
            else:
                # Nothing more to do
                fn_rc = 0
        else:
            log_message = (
                "DrbdManager: Failed to restore snapshot for resource '%s' volume %d"
                % (resource.get_name(), vol_state.get_id())
            )
            logging.error(log_message)
            self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)

        return fn_rc, blockdev

    @log_in_out
    def _deploy_volume_metadata(self, assignment, vol_state, max_peers,
                                nodes, vol_states, thin_flag, initial_flag):
        """
        Creates DRBD metadata on a volume
        """
        fn_rc    = -1
        resource = assignment.get_resource()

        res_name = assignment.get_resource().get_name()
        assg_conf, global_conf = self._server.open_assignment_conf(res_name)
        self._resconf.write_excerpt(assg_conf, assignment,
                                    nodes, vol_states, global_conf)
        self._server.close_assignment_conf(assg_conf, global_conf)
        self._server.update_assignment_conf(res_name)

        # Initialize DRBD metadata
        fn_rc = self._drbdadm.create_md(resource.get_name(), vol_state.get_id(), max_peers)

        if fn_rc == 0:
            if initial_flag or thin_flag:
                # Set the DRBD current generation identifier if it is set on the volume
                volume = resource.get_volume(vol_state.get_id())
                if volume is not None:
                    initial_gi = volume.get_props().get_prop(DrbdVolume.KEY_CURRENT_GI)
                    if initial_gi is not None:
                        set_gi_check = False
                        volume = vol_state.get_volume()
                        node_id = assignment.get_node_id()
                        minor_nr = volume.get_minor().get_value()
                        bd_path = vol_state.get_bd_path()
                        if thin_flag:
                            # Thin provisioning deployment
                            set_gi_check = self._drbdadm.set_gi(
                                str(node_id), str(minor_nr), bd_path,
                                initial_gi
                            )
                        else:
                            # Fat provisioning initial deployment (first deployer of the volume)
                            set_gi_check = self._drbdadm.set_gi(
                                str(node_id), str(minor_nr), bd_path,
                                generate_gi_hex_string(), history_1_gi=initial_gi, set_flags=True
                            )

                        if set_gi_check:
                            fn_rc = 0
                        else:
                            fn_rc = drbdcmd.DrbdAdm.DRBDUTIL_EXEC_FAILED
                            log_message = (
                                "Deploying resource '%s' volume %d: DRBD set-gi command failed"
                                % (res_name, vol_state.get_id())
                            )
                            logging.error(log_message)
                            self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)
        else:
            log_message = (
                "DrbdManager: Failed to create meta data for resource '%s' volume %d"
                % (resource.get_name(), vol_state.get_id())
            )
            logging.error(log_message)
            self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)
        return fn_rc

    @log_in_out
    def _undeploy_volume(self, assignment, vol_state):
        """
        Undeploys a volume, then resets the state values of the volume state
        entry, so it can be removed from the assignment by the cleanup
        function.
        """
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

        # Update the configuration file
        res_name = resource.get_name()
        assg_conf, global_conf = self._server.open_assignment_conf(res_name)
        self._resconf.write_excerpt(assg_conf, assignment,
                                    nodes, vol_states, global_conf)
        self._server.close_assignment_conf(assg_conf, global_conf)
        self._server.update_assignment_conf(res_name)

        fn_rc = -1
        if keep_conf:
            # Adjust the resource to only keep those volumes running that
            # are currently deployed and not marked for becoming undeployed
            #
            # Adjust the resource
            # FIXME: sometimes drbdadm tries to resize for no reason,
            #        but only once, so try that a couple times if
            #        it fails
            retries = 0
            while retries < 3:
                fn_rc = self._drbdadm.adjust(res_name)
                if fn_rc == 0:
                    vol_state.clear_cstate_flags(DrbdVolumeState.FLAG_ATTACH)
                    break
                retries += 1
            if fn_rc != 0:
                log_message = (
                    "Undeploying resource '%s' volume %d: DRBD adjust command failed"
                    % (res_name, vol_state.get_id())
                )
                logging.error(log_message)
                self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)
        else:
            # There are no volumes left in the resource,
            # stop the entire resource
            #
            # Stop the resource
            fn_rc = self._drbdadm.down(resource.get_name())
            if fn_rc == 0:
                vol_state.clear_cstate_flags(DrbdVolumeState.FLAG_ATTACH)
            else:
                log_message = (
                    "Undeploying resource '%s' volume %d: DRBD down command failed"
                    % (res_name, vol_state.get_id())
                )
                logging.error(log_message)
                self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)

            # FIXME: can this cause a race condition?
            #
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
                # Delete the DRBD info file
                self._delete_drbd_info_file(vol_state.get_volume())
                if is_unset(vol_state.get_tstate(), DrbdVolumeState.FLAG_DEPLOY):
                    vol_state.set_cstate(0)
                    vol_state.set_tstate(0)
                if not keep_conf:
                    # there are no volumes left in the resource, set the
                    # assignment's current state to 0 to enable
                    # fast cleanup of the assignment in the case that the
                    # target state changes to undeploy
                    assignment.undeploy_adjust_cstate()
            else:
                log_message = (
                    "DrbdManager: Undeploying resource '%s' volume %d failed"
                    % (res_name(), vol_state.get_id())
                )
                logging.error(log_message)
                self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)

        return fn_rc

    @log_in_out
    def _resize_volume_blockdevice(self, assignment, vol_state, max_peers):
        """
        Resizes the block device of an existing volume
        """
        fn_rc    = -1
        bd_mgr   = self._server.get_bd_mgr()
        resource = assignment.get_resource()
        volume   = resource.get_volume(vol_state.get_id())

        gross_size = 0
        try:
            try:
                net_size = volume.get_resize_value()
                gross_size = md.MetaData.get_gross_kiB(
                    net_size, max_peers, md.MetaData.DEFAULT_AL_STRIPES, md.MetaData.DEFAULT_AL_kiB
                )
            except ValueError as v_err:
                logging.debug(
                    "DrbdManager: _deploy_volume_blockdev(): Encountered an invalid volume resize value: "
                    "Resource '%s, Volume %d"
                    % (resource.get_name(), volume.get_id())
                )
                # Re-raise the exception to skip the resize action
                raise v_err

            bd_name = vol_state.get_bd_name()
            if bd_name is not None:
                fn_rc = bd_mgr.extend_blockdevice(bd_name, gross_size)
            else:
                # If there is no blockdevice, then there is nothing to resize
                fn_rc = DM_SUCCESS

            if fn_rc == DM_SUCCESS:
                vol_state.finish_resize_storage()
        except ValueError:
            vol_state.fail_resize()

        return fn_rc

    @log_in_out
    def _resize_volume_drbd(self, assignment, vol_state):
        """
        Completes the resize of a DRBD volume
        """
        fn_rc = -1
        resource = assignment.get_resource()
        res_name = resource.get_name()
        vol_id = vol_state.get_id()
        volume = resource.get_volume(vol_id)
        if volume is not None:
            if is_unset(assignment.get_tstate(), Assignment.FLAG_DISKLESS):
                saved_vol_size = volume.get_size_kiB()
                try:
                    new_size = volume.get_resize_value()
                    volume.set_size_kiB(new_size)
                except ValueError:
                    pass

                # If this volume is thinly provisioned on all assigned nodes,
                # skip resyncing the space newly allocated by the resize operation
                assume_clean = self._is_global_thin_volume(resource, vol_id)

                # Update the resource configuration file
                self._server.export_assignment_conf(assignment)

                fn_rc = self._drbdadm.resize(res_name, vol_state.get_id(), assume_clean)
                if fn_rc == 0:
                    logging.debug(
                        "Resource '%s', Volume %d: finish_resize_drbd()"
                        % (res_name, vol_id)
                    )
                    resource.finish_resize_drbd(vol_id)
                    # The assigment of the resource on all nodes must be adjusted to update
                    # the size parameter in the configuration.
                    # For now, the assignment's UPD_CON flag actually causes a
                    # 'drbdadm adjust', therefore simply enable the flag
                    # FIXME: There should probably be an adjust flag for that
                    for peer_assg in resource.iterate_assignments():
                        if not (peer_assg is assignment):
                            peer_assg.update_config()
                else:
                    # Rollback the volume size change
                    volume.set_size_kiB(saved_vol_size)
                    log_message = (
                        "Resizing resource '%s' volume %d: DRBD resize command failed"
                        % (res_name, vol_id)
                    )
                    logging.error(log_message)
                    self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)
            else:
                # Client, do not initiate a resize on a client node
                fn_rc = DM_SUCCESS
        else:
            # There is a volume state for a non-existent volume
            assignment.update_volume_states()
        return fn_rc

    @log_in_out
    def _deploy_assignment(self, assignment):
        """
        Finishes deployment of an assignment. The actual deployment of the
        assignment's/resource's volumes takes place in per-volume actions
        of the DrbdManager.perform_changes() function.
        """
        fn_rc = 0
        deploy_fail = False
        resource = assignment.get_resource()
        tstate   = assignment.get_tstate()
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
            if deploy_fail:
                fn_rc = -1
            else:
                assignment.set_cstate_flags(Assignment.FLAG_DEPLOY)
        else:
            assignment.undeploy_adjust_cstate()
        return fn_rc

    @log_in_out
    def _undeploy_assignment(self, assignment):
        """
        Undeploys all volumes of a resource, then reset the assignment's state
        values, so it can be removed by the cleanup function.
        """
        bd_mgr = self._server.get_bd_mgr()
        resource = assignment.get_resource()
        res_name = resource.get_name()

        ud_errors = False
        # No actions are required for empty assignments
        if not assignment.is_empty():
            # Update the resource configuration file
            res_name = assignment.get_resource().get_name()
            if (self._drbdadm.fallback_down(res_name)):
                for vol_state in assignment.iterate_volume_states():
                    bd_name = vol_state.get_bd_name()
                    if bd_name is not None:
                        # volume has a block device
                        stor_rc = bd_mgr.remove_blockdevice(bd_name)
                        if stor_rc == DM_SUCCESS:
                            vol_state.set_bd(None, None)
                            vol_state.set_cstate(0)
                            vol_state.set_tstate(0)
                        else:
                            log_message = (
                                "Undeploying assignment '%s': Block device removal of volume %d failed"
                                % (res_name, vol_state.get_id())
                            )
                            logging.error(log_message)
                            self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)
                            ud_errors = True
                    else:
                        # volume has no block device, nothing to do
                        vol_state.set_cstate(0)
                        vol_state.set_tstate(0)
                        # Delete the DRBD info file
                    self._delete_drbd_info_file(vol_state.get_volume())
            else:
                log_message = (
                    "Undeploying assignment '%s': DRBD down command failed"
                    % (res_name)
                )
                logging.error(log_message)
                self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)
                ud_errors = True

        if not ud_errors:
            # Remove the external configuration file
            self._server.remove_assignment_conf(resource.get_name())
            assignment.undeploy_adjust_cstate()
            assignment.set_tstate(0)

        fn_rc = (DrbdManager.STOR_UNDEPLOY_FAILED if ud_errors else DM_SUCCESS)

        return fn_rc


    @log_in_out
    def _connect(self, assignment):
        """
        Connects a resource on the current node to all peer nodes
        """
        fn_rc = DM_SUCCESS
        resource = assignment.get_resource()
        tstate = assignment.get_tstate()
        discard_flag = is_set(tstate, Assignment.FLAG_DISCARD)

        self._server.export_assignment_conf(assignment)

        fn_rc = self._drbdadm.connect(resource.get_name(), discard_flag)
        if fn_rc == 0:
            assignment.set_cstate_flags(Assignment.FLAG_CONNECT)
            assignment.clear_tstate_flags(Assignment.FLAG_DISCARD)
        else:
            log_message = (
                "Connecting resource '%s' failed"
                % (resource.get_name())
            )
            logging.error(log_message)
            self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)

        return fn_rc

    @log_in_out
    def _disconnect(self, assignment):
        """
        Disconnects a resource on the current node from all peer nodes
        """
        fn_rc = DM_SUCCESS
        resource = assignment.get_resource()
        self._server.export_assignment_conf(assignment)

        fn_rc = self._drbdadm.disconnect(resource.get_name())
        if fn_rc == 0:
            assignment.clear_cstate_flags(Assignment.FLAG_CONNECT)
        else:
            log_message = (
                "Disconnecting resource '%s' failed"
                % (resource.get_name())
            )
            logging.error(log_message)
            self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)

        return fn_rc

    @log_in_out
    def _update_connections(self, assignment):
        """
        Updates connections
        * Disconnect from nodes that do not have the same resource
          connected anymore
        * Connect to nodes that have newly deployed a resource
        * Leave valid existing connections untouched
        """
        resource = assignment.get_resource()

        self._server.export_assignment_conf(assignment)

        # call drbdadm to update connections
        fn_rc = self._drbdadm.adjust(resource.get_name())
        if fn_rc == 0:
            assignment.set_cstate_flags(Assignment.FLAG_CONNECT)
            assignment.clear_tstate_flags(Assignment.FLAG_UPD_CON)
            if is_unset(assignment.get_tstate(), Assignment.FLAG_DISKLESS):
                for vol_state in assignment.iterate_volume_states():
                    vol_state.set_cstate_flags(DrbdVolumeState.FLAG_ATTACH)

        return fn_rc

    @log_in_out
    def _reconnect(self, assignment):
        """
        Disconnects, then connects again
        """
        # disconnect
        self._disconnect(assignment)
        # connect
        fn_rc = self._connect(assignment)
        if fn_rc == 0:
            assignment.clear_tstate_flags(Assignment.FLAG_RECONNECT)

        return fn_rc

    @log_in_out
    def _attach(self, assignment, vol_state):
        """
        Attaches a volume
        """
        fn_rc = DM_SUCCESS
        resource = assignment.get_resource()
        res_name = resource.get_name()
        # do not attach clients, because there is no local storage on clients
        if is_unset(assignment.get_tstate(), Assignment.FLAG_DISKLESS):
            self._server.export_assignment_conf(assignment)
            fn_rc = self._drbdadm.attach(
                res_name,
                vol_state.get_id()
            )
            if fn_rc == 0:
                vol_state.set_cstate_flags(DrbdVolumeState.FLAG_ATTACH)
            else:
                log_message = (
                    "Attaching resource '%s' volume %d failed"
                    % (res_name, vol_state.get_id())
                )
                logging.error(log_message)
                self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)
        else:
            vol_state.clear_tstate_flags(DrbdVolumeState.FLAG_ATTACH)

        return fn_rc

    @log_in_out
    def _detach(self, assignment, vol_state):
        """
        Detaches a volume
        """
        fn_rc = DM_SUCCESS
        resource = assignment.get_resource()
        self._server.export_assignment_conf(assignment)
        fn_rc = self._drbdadm.detach(
            resource.get_name(),
            vol_state.get_id()
        )
        if fn_rc == 0:
            vol_state.clear_cstate_flags(DrbdVolumeState.FLAG_ATTACH)
        else:
            log_message = (
                "Detaching resource '%s' volume %d failed"
                % (resource.get_name(), vol_state.get_id())
            )
            logging.error(log_message)
            self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)

        return fn_rc

    def _reconfigure_assignment(self, assignment):
        """
        Applies configuration changes (...)
        """
        self._server.export_assignment_conf(assignment)
        fn_rc = self._drbdadm.adjust(assignment.get_resource().get_name())
        if fn_rc != 0:
            resource = assignment.get_resource()
            log_message = (
                "Resource '%s': DRBD adjust command failed"
                % (resource.get_name())
            )
            logging.error(log_message)
            self._server.get_message_log().add_entry(msglog.MessageLog.ALERT, log_message)
        return fn_rc

    @log_in_out
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
        # Do not switch to the primary role if the current gi
        # property is set on all volumes
        if primary and not force_primary:
            have_gi = True
            resource = assignment.get_resource()
            for volume in resource.iterate_volumes():
                vol_props = volume.get_props()
                current_gi = vol_props.get_prop(DrbdVolume.KEY_CURRENT_GI)
                if current_gi is None:
                    have_gi = False
                    break
            if have_gi:
                primary = False
        return primary


    def _is_resize_storage_finished(self, assg, vol_id):
        flag = True
        xact = drbdmanage.propscontainer.Props.KEY_XACT
        for node_assg in assg.get_resource().iterate_assignments():
            if is_unset(node_assg.get_cstate(), Assignment.FLAG_DISKLESS):
                vol_state = node_assg.get_volume_state(vol_id)
                if vol_state is not None:
                    vs_props = vol_state.get_props()
                    resize_stage = vs_props.get_prop(DrbdVolumeState.KEY_RESIZE_STAGE, namespace=xact)
                    if resize_stage is not None:
                        if resize_stage != DrbdVolumeState.RESIZE_STAGE_DRBD:
                           flag = False
                           break
                else:
                    logging.error(
                        "Internal error: _is_resize_storage_finished(): "
                        "Assignment %s/%s is missing the DrbdVolumeState object for volume #%d"
                        % (assg.get_node().get_name(), assg.get_resource().get_name(), int(vol_id))
                    )
        return flag


    def _get_max_fail_count(self):
        # this is actually supposed to be DrbdManageServer.CONSTANT, but the import system
        # broke again when 'import drbdmanage.server' was added
        max_fail_count = self._server.DEFAULT_MAX_FAIL_COUNT
        prop_str = self._server.get_conf_value(self._server.KEY_MAX_FAIL_COUNT)
        if prop_str is not None:
            try:
                max_fail_count = int(prop_str)
            except (ValueError, TypeError):
                pass
        return max_fail_count


    def _is_initial_deployer(self, assignment, vol_state):
        """
        Indicates whether this node is the first to deploy a volume
        """
        first_flag = True
        resource = assignment.get_resource()
        vol_id = vol_state.get_id()
        # If the deploy flag is set on one of the peers' assignment's volume state,
        # then this node is not the initial deployer of the volume
        for peer_assg in resource.iterate_assignments():
            if assignment is not peer_assg:
                peer_vol_state = peer_assg.get_volume_state(vol_id)
                if is_set(peer_vol_state.get_cstate(), DrbdVolumeState.FLAG_DEPLOY):
                    first_flag = False
        return first_flag


    def _is_thin_provisioning(self):
        """
        Indicates whether the local node is using thin provisioning
        """
        thin_flag = False
        bd_mgr = self._server.get_bd_mgr()
        prov_type = bd_mgr.get_trait(storcore.StoragePlugin.KEY_PROV_TYPE)
        if prov_type is not None:
            if prov_type == storcore.StoragePlugin.PROV_TYPE_THIN:
                thin_flag = True
        return thin_flag


    def _is_global_thin_volume(self, resource, vol_id):
        """
        Indicates whether the volume is thinly provisioned on all assigned nodes
        """
        global_thin_flag = True
        for assignment in resource.iterate_assignments():
            if is_unset(assignment.get_cstate(), Assignment.FLAG_DISKLESS):
                thin_flag = False
                vol_state = assignment.get_volume_state(vol_id)
                if is_set(vol_state.get_cstate(), DrbdVolumeState.FLAG_DEPLOY):
                    thin_prop = vol_state.get_props().get_prop(DrbdVolumeState.KEY_IS_THIN)
                    if thin_prop is not None:
                        try:
                            thin_flag = utils.string_to_bool(thin_prop)
                        except ValueError:
                            pass
                    if not thin_flag:
                        global_thin_flag = False
                        break
        return global_thin_flag


    def _is_snapshot_restore(self, resource, vol_id):
        """
        Indicates whether this resource is restoring a snapshot on any nodes
        """
        restore_flag = False
        for assignment in resource.iterate_assignments():
            vol_state = assignment.get_volume_state(vol_id)
            if vol_state is not None:
                props = vol_state.get_props()
                src_bd_name = props.get_prop(consts.SNAPS_SRC_BLOCKDEV)
                if src_bd_name is not None:
                    restore_flag = True
                    break
        return restore_flag


    @log_in_out
    def reconfigure(self):
        """
        Reconfigures the DrbdManager instance

        Called by the server's reconfigure() function
        """
        conf_path = self._server._conf.get(self._server.KEY_DRBD_CONFPATH,
                                           self._server.DEFAULT_DRBD_CONFPATH)
        self._drbdadm = drbdmanage.drbd.commands.DrbdAdm(conf_path)


class DrbdCommon(GenericDrbdObject):
    """
    Holds common DRBD setup options (configuration parameters for DRBD)
    """

    def __init__(self, get_serial_fn, init_serial, init_props):
        super(DrbdCommon, self).__init__(
            get_serial_fn, init_serial,
            init_props
        )


class DrbdResource(GenericDrbdObject):

    NAME_MINLEN  = 1
    NAME_MAXLEN  = consts.RES_NAME_MAXLEN
    # Valid characters in addition to [a-zA-Z0-9]
    NAME_VALID_CHARS = consts.RES_NAME_VALID_CHARS
    # Additional valid characters, but not allowed as the first character
    NAME_VALID_INNER_CHARS = consts.RES_NAME_VALID_INNER_CHARS

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

    # STATE_MASK must include all valid flags;
    # used to mask the value supplied to set_state() to prevent setting
    # non-existent flags
    STATE_MASK   = FLAG_REMOVE

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


    def set_port(self, port):
        if port >= 1 and port <= 65535 and port != self._port:
            self._port = port
            self.get_props().new_serial()
        else:
            raise ValueError

    def get_port(self):
        return self._port


    def name_check(self, name):
        checked_name = GenericDrbdObject.name_check(
            name, DrbdResource.NAME_MINLEN, DrbdResource.NAME_MAXLEN,
            DrbdResource.NAME_VALID_CHARS, DrbdResource.NAME_VALID_INNER_CHARS
        )
        # A resource can not be named "all", because that is a
        # keyword in the drbdsetup/drbdadm utilities
        if checked_name.lower() == consts.RES_ALL_KEYWORD:
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


    def begin_resize(self, vol_id, new_size_kiB):
        xact = drbdmanage.propscontainer.Props.KEY_XACT
        for assg in self.iterate_assignments():
            assg_cstate = assg.get_cstate()
            assg_tstate = assg.get_tstate()
            if (is_unset(assg_cstate, Assignment.FLAG_DISKLESS) and
                is_unset(assg_tstate, Assignment.FLAG_DISKLESS)):
                vol_state = assg.get_volume_state(vol_id)
                if vol_state is not None:
                    vs_props = vol_state.get_props()
                    vs_props.set_prop(
                        DrbdVolumeState.KEY_RESIZE_STAGE, DrbdVolumeState.RESIZE_STAGE_STOR,
                        namespace=xact
                    )
                    vol_state.set_tstate_flags(DrbdVolumeState.FLAG_XACT)
        volume = self.get_volume(vol_id)
        vol_props = volume.get_props()
        vol_props.set_prop(DrbdVolume.KEY_RESIZE_VALUE, str(new_size_kiB), namespace=xact)


    def get_resizing_vol_id_list(self):
        id_list = []
        xact = drbdmanage.propscontainer.Props.KEY_XACT
        for volume in self.iterate_volumes():
            vol_props = volume.get_props()
            resize_value = vol_props.get_prop(DrbdVolume.KEY_RESIZE_VALUE, namespace=xact)
            if resize_value is not None:
                id_list.append(volume.get_id())
        return id_list


    def finish_resize_drbd(self, vol_id):
        xact = drbdmanage.propscontainer.Props.KEY_XACT
        for assg in self.iterate_assignments():
            vol_state = assg.get_volume_state(vol_id)
            if vol_state is not None:
                vs_props = vol_state.get_props()
                vs_props.remove_prop(DrbdVolumeState.KEY_RESIZE_STAGE, namespace=xact)
                vol_state.cleanup_xact()
        volume = self.get_volume(vol_id)
        if volume is not None:
            vol_props = volume.get_props()
            vol_props.remove_prop(DrbdVolume.KEY_RESIZE_VALUE, namespace=xact)


    def is_managed(self):
        """
        Indicates whether this resource is being managed or not

        The resource is always managed unless the value of the property
        consts.MANAGED is set to consts.BOOL_FALSE
        """
        managed = True
        managed_prop = self.get_props().get_prop(consts.MANAGED)
        if managed_prop is not None:
            try:
                managed = utils.string_to_bool(managed_prop)
            except ValueError:
                pass
        return managed


    def get_state(self):
        return self._state


    def set_state(self, state):
        if state != self._state:
            self._state = state & self.STATE_MASK
            self.get_props().new_serial()


    def set_state_flags(self, flags):
        saved_state = self._state
        self._state = (self._state | flags) & self.STATE_MASK
        if saved_state != self._state:
            self.get_props().new_serial()


    def clear_state_flags(self, flags):
        saved_state = self._state
        self._state = ((self._state | flags) ^ flags) & self.STATE_MASK
        if saved_state != self._state:
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

    # Target size of a resize action
    KEY_RESIZE_VALUE = "resize-value"

    # Current generation identifier - PropsContainer key
    KEY_CURRENT_GI = "current-gi"

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


    def set_minor(self, minor):
        if minor.get_value() != self._minor.get_value():
            self._minor = minor
            self.get_props().new_serial()


    def get_path(self):
        # TODO: return "pretty" name, /dev/drbd/by-res/...
        return "/dev/drbd" + str(self.get_minor().get_value())


    def set_size_kiB(self, new_size_kiB):
        if new_size_kiB != self._size_kiB:
            self.get_props().new_serial()
        self.common_set_size_kiB(new_size_kiB)
        self._size_kiB = long(new_size_kiB)


    def get_state(self):
        return self._state


    def is_resizing(self):
        """
        Indicates whether this volume is being resized
        """
        flag = False
        xact = drbdmanage.propscontainer.Props.KEY_XACT
        vol_props = self.get_props()
        if vol_props.get_prop(DrbdVolume.KEY_RESIZE_VALUE, namespace=xact) is not None:
            flag = True
        return flag


    def get_resize_value(self):
        """
        Returns the requested volume size for resizing, or generates a ValueError
        """
        xact = drbdmanage.propscontainer.Props.KEY_XACT
        vol_props = self.get_props()
        resize_value_str = vol_props.get_prop(DrbdVolume.KEY_RESIZE_VALUE, namespace=xact)
        resize_value = 0
        if resize_value_str is not None:
            try:
                resize_value = long(resize_value_str)
            except (ValueError, TypeError):
                raise ValueError
        else:
            raise ValueError
        return resize_value


    def set_state(self, state):
        if state != self._state:
            self._state = state & self.STATE_MASK
            self.get_props().new_serial()


    def set_state_flags(self, flags):
        saved_state = self._state
        self._state = (self._state | flags) & self.STATE_MASK
        if saved_state != self._state:
            self.get_props().new_serial()


    def clear_state_flags(self, flags):
        saved_state = self._state
        self._state = ((self._state | flags) ^ flags) & self.STATE_MASK
        if saved_state != self._state:
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

    NAME_MINLEN = consts.NODE_NAME_MINLEN
    NAME_MAXLEN = consts.NODE_NAME_MAXLEN

    AF_IPV4 = consts.AF_IPV4
    AF_IPV6 = consts.AF_IPV6

    AF_IPV4_LABEL = consts.AF_IPV4_LABEL
    AF_IPV6_LABEL = consts.AF_IPV6_LABEL

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
    FLAG_STANDBY  =    0x10
    FLAG_QIGNORE  =    0x20
    FLAG_UPD_POOL = 0x10000

    # STATE_MASK must include all valid flags;
    # used to mask the value supplied to set_state() to prevent setting
    # non-existent flags
    STATE_MASK = (FLAG_REMOVE | FLAG_UPD_POOL | FLAG_UPDATE | FLAG_DRBDCTRL |
                  FLAG_STORAGE | FLAG_STANDBY | FLAG_QIGNORE)


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


    def set_addr(self, addr):
        if addr != self._addr:
            self._addr = addr
            self.get_props().new_serial()


    def get_addrfam(self):
        return self._addrfam


    def set_addrfam(self, addrfam):
        if addrfam != self._addrfam:
            if addrfam == self.AF_IPV4 or addrfam == self.AF_IPV6:
                self._addrfam = addrfam
                self.get_props().new_serial()
            else:
                raise InvalidAddrFamException


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


    def set_state_flags(self, flags):
        saved_state = self._state
        self._state = (self._state | flags) & self.STATE_MASK
        if saved_state != self._state:
            self.get_props().new_serial()


    def clear_state_flags(self, flags):
        saved_state = self._state
        self._state = ((self._state | flags) ^ flags) & self.STATE_MASK
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
        return check_node_name(name)


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
                    consts.NODE_AF       : str(self._addrfam),
                    consts.NODE_ADDR     : self._addr,
                    consts.NODE_ID       : str(self._node_id),
                    consts.NODE_POOLSIZE : str(self._poolsize),
                    consts.NODE_POOLFREE : str(self._poolfree),
                    consts.TSTATE_PREFIX + consts.FLAG_REMOVE :
                        bool_to_string(is_set(self._state, self.FLAG_REMOVE)),
                    consts.TSTATE_PREFIX + consts.FLAG_UPDATE   :
                        bool_to_string(is_set(self._state, self.FLAG_UPDATE)),
                    consts.TSTATE_PREFIX + consts.FLAG_UPD_POOL :
                        bool_to_string(is_set(self._state, self.FLAG_UPD_POOL)),
                    consts.TSTATE_PREFIX + consts.FLAG_DRBDCTRL :
                        bool_to_string(is_set(self._state, self.FLAG_DRBDCTRL)),
                    consts.TSTATE_PREFIX + consts.FLAG_STORAGE :
                        bool_to_string(is_set(self._state, self.FLAG_STORAGE)),
                    consts.TSTATE_PREFIX + consts.FLAG_STANDBY :
                        bool_to_string(is_set(self._state, self.FLAG_STANDBY)),
                    consts.TSTATE_PREFIX + consts.FLAG_QIGNORE :
                        bool_to_string(is_set(self._state, self.FLAG_QIGNORE))
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
        if selected(consts.TSTATE_PREFIX + consts.FLAG_STANDBY):
            properties[consts.TSTATE_PREFIX + consts.FLAG_STANDBY] = (
                bool_to_string(is_set(self._state, self.FLAG_STANDBY))
            )
        if selected(consts.TSTATE_PREFIX + consts.FLAG_QIGNORE):
            properties[consts.TSTATE_PREFIX + consts.FLAG_QIGNORE] = (
                bool_to_string(is_set(self._state, self.FLAG_QIGNORE))
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
    # Pending extended actions controlled by properties (resize, ...)
    FLAG_XACT      = 0x10000

    # CSTATE_MASK must include all valid current state flags;
    # used to mask the value supplied to set_cstate() to prevent setting
    # non-existent flags
    CSTATE_MASK    = FLAG_DEPLOY | FLAG_ATTACH

    # TSTATE_MASK must include all valid target state flags;
    # used to mask the value supplied to set_tstate() to prevent setting
    # non-existent flags
    TSTATE_MASK    = FLAG_DEPLOY | FLAG_ATTACH | FLAG_XACT

    # Current stage of a resize action
    KEY_RESIZE_STAGE = "resize-stage"

    # Waiting for resize of the backend storage
    RESIZE_STAGE_STOR = "storage"
    # Waiting for resize of the DRBD device
    RESIZE_STAGE_DRBD = "drbd"
    # Indicates that resizing failed
    RESIZE_STAGE_FAILED = "failed"

    KEY_IS_THIN = "is-thin-volume"

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


    def begin_resize(self):
        xact = drbdmanage.propscontainer.Props.KEY_XACT
        vs_props = self.get_props()
        vs_props.set_prop(
            DrbdVolumeState.KEY_RESIZE_STAGE, DrbdVolumeState.RESIZE_STAGE_STOR,
            namespace=xact
        )
        self.set_tstate_flags(DrbdVolumeState.FLAG_XACT)


    def requires_resize_storage(self):
        return self.resize_stage_matches(DrbdVolumeState.RESIZE_STAGE_STOR)


    def requires_resize_drbd(self):
        return self.resize_stage_matches(DrbdVolumeState.RESIZE_STAGE_DRBD)


    def resize_stage_matches(self, value):
        flag = False
        xact = drbdmanage.propscontainer.Props.KEY_XACT
        vs_props = self.get_props()
        resize_stage = vs_props.get_prop(DrbdVolumeState.KEY_RESIZE_STAGE, namespace=xact)
        if resize_stage is not None:
            if resize_stage == value:
                flag = True
        return flag


    def finish_resize_storage(self):
        xact = drbdmanage.propscontainer.Props.KEY_XACT
        vs_props = self.get_props()
        vs_props.set_prop(
            DrbdVolumeState.KEY_RESIZE_STAGE, DrbdVolumeState.RESIZE_STAGE_DRBD,
            namespace=xact
        )
        self.set_tstate_flags(DrbdVolumeState.FLAG_XACT)


    def fail_resize(self):
        xact = drbdmanage.propscontainer.Props.KEY_XACT
        vs_props = self.get_props()
        vs_props.set_prop(
            DrbdVolumeState.KEY_RESIZE_STAGE, DrbdVolumeState.RESIZE_STAGE_FAILED,
            namespace=xact
        )


    def cleanup_xact(self):
        xact = drbdmanage.propscontainer.Props.KEY_XACT
        vs_props = self.get_props()
        xact_props = vs_props.get_all_props(namespace=xact)
        if len(xact_props) == 0:
            self.clear_tstate_flags(DrbdVolumeState.FLAG_XACT)


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
                    consts.VOL_ID      : str(self._volume.get_id()),
                    consts.VOL_SIZE    : str(self._volume.get_size_kiB()),
                    consts.VOL_BDEV    : bdev,
                    consts.VOL_MINOR   : str(self._volume.get_minor().get_value()),
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

    # Signal for status change notifications
    _signal     = None

    FLAG_DEPLOY    = 0x1
    FLAG_CONNECT   = 0x2
    FLAG_DISKLESS  = 0x4

    FLAG_UPD_CON   = 0x10000
    FLAG_RECONNECT = 0x20000
    # --overwrite-data-of-peer / primary --force
    FLAG_OVERWRITE = 0x40000
    # --discard-my-data upon connect / resolve split-brain
    FLAG_DISCARD   = 0x80000
    FLAG_UPD_CONFIG = 0x100000

    FAIL_COUNT_HARD_LIMIT = 99

    # CSTATE_MASK must include all valid current state flags;
    # used to mask the value supplied to set_cstate() to prevent setting
    # non-existent flags
    CSTATE_MASK    = FLAG_DEPLOY | FLAG_CONNECT | FLAG_DISKLESS

    # TSTATE_MASK must include all valid target state flags;
    # used to mask the value supplied to set_tstate() to prevent setting
    # non-existent flags
    TSTATE_MASK    = (FLAG_DEPLOY | FLAG_CONNECT | FLAG_DISKLESS |
                      FLAG_UPD_CON | FLAG_RECONNECT |
                      FLAG_OVERWRITE | FLAG_DISCARD | FLAG_UPD_CONFIG)

    # Mask applied to ignore action flags on the target state
    # of an assignment.
    ACT_IGN_MASK   = (TSTATE_MASK ^
                      (FLAG_DISCARD | FLAG_OVERWRITE | FLAG_UPD_CONFIG))


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
                    if volume.is_resizing():
                        vol_st.begin_resize()
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
        # FIXME: peers may not be the number of peers the volume were created with
        size_sum = 0
        try:
            if (is_set(self._cstate, Assignment.FLAG_DEPLOY) or
                is_set(self._tstate, Assignment.FLAG_DEPLOY)):
                    for vol_state in self._vol_states.itervalues():
                        cstate = vol_state.get_cstate()
                        tstate = vol_state.get_tstate()
                        if (is_set(cstate, DrbdVolumeState.FLAG_DEPLOY) or
                            is_set(tstate, DrbdVolumeState.FLAG_DEPLOY)):
                                volume = vol_state.get_volume()
                                size_sum += md.MetaData.get_gross_kiB(
                                    volume.get_size_kiB(), peers,
                                    md.MetaData.DEFAULT_AL_STRIPES,
                                    md.MetaData.DEFAULT_AL_kiB
                                )
        except md.MetaDataException as md_exc:
            logging.debug("Assignment.get_gross_size_kiB(): MetaDataException: " + md_exc.message)
            size_sum = 0
        return size_sum


    def get_gross_size_kiB_correction(self, peers):
        """
        Calculates the storage size for not-yet-deployed assignments
        """
        # FIXME: peers may not be the number of peers the volumes were created with
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

            size_sum += self._get_resize_corr(peers)
        return size_sum


    def _get_resize_corr(self, peers):
        """
        Correction for volumes that are being resized
        """
        diff_size_kiB = 0
        resource = self.get_resource()
        for volume in resource.iterate_volumes():
            try:
                net_size_kiB = volume.get_resize_value()
                gross_size_kiB = md.MetaData.get_gross_kiB(
                    net_size_kiB, peers,
                    md.MetaData.DEFAULT_AL_STRIPES,
                    md.MetaData.DEFAULT_AL_kiB
                )
                cur_size_kiB = md.MetaData.get_gross_kiB(
                    volume.get_size_kiB(), peers,
                    md.MetaData.DEFAULT_AL_STRIPES,
                    md.MetaData.DEFAULT_AL_kiB
                )
                diff_size_kiB += gross_size_kiB - cur_size_kiB
            except (ValueError, TypeError):
                pass
        return diff_size_kiB


    def _get_undeployed_corr(self, peers):
        """
        Volume sizes for not-yet-deployed volumes
        """
        size_sum = 0
        try:
            for vol_state in self._vol_states.itervalues():
                cstate = vol_state.get_cstate()
                tstate = vol_state.get_tstate()
                if (is_unset(cstate, DrbdVolumeState.FLAG_DEPLOY) and
                    is_set(tstate, DrbdVolumeState.FLAG_DEPLOY)):
                        volume = vol_state.get_volume()
                        size_sum += md.MetaData.get_gross_kiB(
                            volume.get_size_kiB(), peers,
                            md.MetaData.DEFAULT_AL_STRIPES,
                            md.MetaData.DEFAULT_AL_kiB
                        )
        except md.MetaDataException as md_exc:
            logging.debug("Assignment._get_undeployed_corr(): MetaDataException: " + md_exc.message)
            size_sum = 0
        return size_sum


    def _get_diskless_corr(self, peers):
        """
        Volumes sizes for volumes that transition from diskless to storage
        """
        size_sum = 0
        try:
            for vol_state in self._vol_states.itervalues():
                cstate = vol_state.get_cstate()
                tstate = vol_state.get_tstate()
                if (is_set(cstate, DrbdVolumeState.FLAG_DEPLOY) and
                    is_set(tstate, DrbdVolumeState.FLAG_DEPLOY)):
                        volume = vol_state.get_volume()
                        size_sum += md.MetaData.get_gross_kiB(
                            volume.get_size_kiB(), peers,
                            md.MetaData.DEFAULT_AL_STRIPES,
                            md.MetaData.DEFAULT_AL_kiB
                        )
        except md.MetaDataException as md_exc:
            logging.debug("Assignment._get_diskless_corr(): MetaDataException: " + md_exc.message)
            size_sum = 0
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
        # Always pretend the assignment is deployed before trying to undeploy,
        # so it will not be cleaned up if a previous attempt to deploy the
        # assignment failed, but its DRBD resource configuration file is still
        # present on some node and therefore requires the node to run its
        # undeploy operations
        self._cstate |= self.FLAG_DEPLOY
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
        Sets the UPD_CON flag on an assignment's target state

        Used to indicate that the network connections of an assignment's
        resource have changed and should be updated. This should be achieved
        without disconnecting connections that are still valid
        (e.g., NOT disconnect/connect or reconnect)
        Commonly, drbdadm adjust should be a good candidate to update
        connections.
        """
        if (self._tstate & Assignment.FLAG_UPD_CON) == 0:
            self._tstate = self._tstate | Assignment.FLAG_UPD_CON
            self.get_props().new_serial()


    def update_config(self):
        """
        Sets the UPD_CONFIG flag on an assignment's target state

        Used to indicate that the assignment needs reconfiguration
        (e.g., drbdadm adjust)
        """
        if (self._tstate & Assignment.FLAG_UPD_CONFIG) == 0:
            self._tstate = self._tstate | Assignment.FLAG_UPD_CONFIG
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


    def get_fail_count(self):
        """
        Returns the fail count of this assignment
        """
        props = self.get_props()
        fail_count = props.get_int_or_default(consts.FAIL_COUNT, 0)
        return fail_count if fail_count >= 0 else 0


    def increase_fail_count(self):
        """
        Increases the fail count of this assignment
        """
        props = self.get_props()
        fail_count = props.get_int_or_default(consts.FAIL_COUNT, 0)
        if fail_count < Assignment.FAIL_COUNT_HARD_LIMIT:
            fail_count += 1
            props.set_prop(consts.FAIL_COUNT, str(fail_count))


    def clear_fail_count(self):
        """
        Clears (removes) the fail count property of this assignment
        """
        props = self.get_props()
        props.remove_prop(consts.FAIL_COUNT)


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


    def set_signal(self, signal):
        """
        Assigns the signal instance for client notifications
        """
        self._signal = signal


    def get_signal(self):
        """
        Returns the signal instance for client notifications
        """
        return self._signal


    def notify_changed(self):
        """
        Sends a signal to notify clients of a status change
        """
        if self._signal is not None:
            try:
                self._signal.notify_changed()
            except dmexc.DrbdManageException as dm_exc:
                logging.warning("Cannot send change notification signal: %s"
                                % (str(dm_exc)))
            except Exception:
                logging.warning("Cannot send change notification signal, "
                                "unhandled exception encountered")


    def notify_removed(self):
        """
        Removes the assignment's signal

        This method should be called when the assignment is removed
        """
        if self._signal is not None:
            try:
                self._signal.destroy()
                self._signal = None
            except dmexc.DrbdManageException as dm_exc:
                logging.warning("Cannot send removal notification signal: %s"
                                % (str(dm_exc)))
            except Exception:
                logging.warning("Cannot send removal notification signal, "
                                "unhandled exception encountered")


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
        if selected(consts.TSTATE_PREFIX + consts.FLAG_UPD_CONFIG):
            properties[consts.TSTATE_PREFIX + consts.FLAG_UPD_CONFIG] = (
                bool_to_string(
                    is_set(self._tstate, self.FLAG_UPD_CONFIG)
                )
            )

        for (key, val) in self.get_props().iteritems():
            if selected(key):
                if val is not None:
                    properties[key] = str(val)

        return properties
