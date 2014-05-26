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
import drbdmanage.drbd.commands

"""
WARNING!
  do not import anything from drbdmanage.drbd.persistence
"""
from drbdmanage.storage.storagecore import GenericStorage
from drbdmanage.exceptions import (IncompatibleDataException,
    InvalidAddrFamException, InvalidNameException, VolSizeRangeException)
from drbdmanage.exceptions import DM_SUCCESS
from drbdmanage.utils import (Selector, bool_to_string)


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


    def __init__(self, server):
        self._server  = server
        self._resconf = drbdmanage.conf.conffile.DrbdAdmConf()
        self.reconfigure()


    # FIXME
    # TODO: currently, if the configuration can not be loaded (due to
    #       errors in the data structures on the disk), the server's
    #       hash is not updated, so the DrbdManager can begin to loop,
    #       trying to load the new configuration. If DrbdManager fails
    #       to load the configuration, it should probably rather stop
    #       than loop at some point.
    def run(self):
        """
        Performs actions to reflect changes to the drbdmanage configuration

        If the configuration on the drbdmanage control volume has changed,
        this function attempts to reach the target state that is set for
        each resource in the configuration.
        According to the target state of each resource, resources/volumes
        are deployed/undeployed/attached/detached/connected/disconnected/etc...
        """
        persist = None
        logging.debug("DrbdManager: invoked")
        try:
            # check whether the configuration hash has changed
            persist = self._server.open_conf()
            if persist is not None:
                logging.debug("DrbdManager: hash check: %s"
                  % persist.get_stored_hash())
                if self._server.hashes_match(persist.get_stored_hash()):
                    # configuration did not change, bail out
                    logging.debug("DrbdManager: hash unchanged, "
                      "finished")
                    return
            # configuration hash has changed
            # lock and reload the configuration
            persist.close()
            persist = self._server.begin_modify_conf()
            if persist is not None:
                if self.perform_changes():
                    logging.debug("DrbdManager: state changed, "
                      "saving data tables")
                    self._server.save_conf_data(persist)
                else:
                    logging.debug("DrbdManager: state unchanged")
            else:
                logging.debug("DrbdManager: cannot load data tables")
            logging.debug("DrbdManager: finished")
        except Exception as exc:
            # FIXME PersistenceException should at least be ignored here
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logging.error("DrbdManager: abort, unhandled exception: %s"
              % str(exc))
            logging.debug("Stack trace:\n%s" % str(exc_tb))
        finally:
            # this also works for read access
            self._server.end_modify_conf(persist)


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
        state_changed = False
        pool_changed  = False

        """
        Check whether the system the drbdmanaged server is running on is
        a registered node in the configuration
        """
        node = self._server.get_instance_node()
        if node is None:
            logging.warning("DrbdManager: abort, this node ('%s') has "
              "no entry in the data tables"
              % self._server.get_instance_node_name())
            return False


        """
        Check for changes of the cluster configuration (node members)
        """
        node_state = node.get_state()
        if node_state & DrbdNode.FLAG_UPDATE != 0:
            state_changed = True
            self._server.reconfigure_drbdctrl()
            node.set_state(node_state ^ DrbdNode.FLAG_UPDATE)

        """
        Check all assignments for changes
        """
        assignments = node.iterate_assignments()
        for assg in assignments:

            if assg.requires_action():
                logging.debug("assigned resource %s cstate(%x)->tstate(%x)"
                  % (assg.get_resource().get_name(),
                  assg.get_cstate(), assg.get_tstate()))

                state_changed  = True
                failed_actions = False

                """
                ============================================================
                Actions for assignments
                (concerning all volumes of a resource)
                ============================================================
                """

                """
                Undeploy an assignment/resource and all of its volumes
                """
                if (not failed_actions) and assg.requires_undeploy():
                    pool_changed = True
                    fn_rc = self._undeploy_assignment(assg)
                    assg.set_rc(fn_rc)
                    if fn_rc != 0:
                        failed_actions = True
                    # ignore other actions for the same assignment
                    # after undeploy
                    continue

                """
                Disconnect an assignment/resource
                """
                if (not failed_actions) and assg.requires_disconnect():
                    fn_rc = self._disconnect(assg)
                    assg.set_rc(fn_rc)
                    if fn_rc != 0:
                        failed_actions = True

                """
                Update connections
                """
                assg_actions = assg.get_tstate()
                if (not failed_actions):
                    if (assg_actions & Assignment.FLAG_UPD_CON) != 0:
                        fn_rc = self._update_connections(assg)
                        assg.set_rc(fn_rc)
                        if fn_rc != 0:
                            failed_actions = True
                    if (assg_actions & Assignment.FLAG_RECONNECT) != 0:
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

                    """
                    Deploy or undeploy a volume
                    """
                    if (not failed_actions):
                        if vol_state.requires_deploy():
                            pool_changed = True
                            fn_rc = self._deploy_volume(assg, vol_state)
                            assg.set_rc(fn_rc)
                            if fn_rc != 0:
                                failed_actions = True
                        elif vol_state.requires_undeploy():
                            pool_changed = True
                            fn_rc = self._undeploy_volume(assg, vol_state)
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


                """
                ============================================================
                Actions for assignments (continuation)
                (concerning all volumes of a resource)
                ============================================================
                """

                """
                Deploy an assignment (finish deploying)
                Volumes have already been deployed by the per-volume actions
                at this point. Only if all volumes that should be deployed have
                been deployed (current state vs. target state), then mark
                the assignment as deployed, too.
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

                if (assg.get_tstate() & Assignment.FLAG_DISKLESS) != 0:
                    assg.set_cstate_flags(Assignment.FLAG_DISKLESS)

        """
        Cleanup the server's data structures
        (remove entries that are no longer required)
        """
        self._server.cleanup()

        if pool_changed:
            self._server.update_pool_data()

        return state_changed


    def adjust_drbdctrl(self):
        # call drbdadm to bring up the resource
        drbd_proc = self._drbdadm.ext_conf_adjust(
          consts.DRBDCTRL_RES_NAME)
        if drbd_proc is not None:
            fn_rc = drbd_proc.wait()
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED
        return fn_rc


    def initial_up(self):
        """
        Attempts to bring up all deployed resources.
        Used when the drbdmanage server starts up.
        """
        node = self._server.get_instance_node()
        if node is not None:
            for assg in node.iterate_assignments():
                if assg.is_deployed():
                    try:
                        self._up_resource(assg)
                    except Exception as exc:
                        logging.debug("failed to start resource '%s', "
                        "unhandled exception: %s"
                        % (assg.get_resource().get_name(), str(exc)))


    def _up_resource(self, assignment):
        """
        Brings up DRBD resources
        """
        resource = assignment.get_resource()

        logging.info("starting resource '%s'"
          % resource.get_name())

        # call drbdadm to bring up the resource
        drbd_proc = self._drbdadm.adjust(resource.get_name())
        if drbd_proc is not None:
            self._resconf.write(drbd_proc.stdin, assignment, False)
            drbd_proc.stdin.close()
            fn_rc = drbd_proc.wait()
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

        return fn_rc


    def _down_resource(self, assignment):
        """
        Brings down DRBD resources
        """
        bd_mgr   = self._server.get_bd_mgr()
        resource = assignment.get_resource()

        logging.info("stopping resource '%s'"
          % resource.get_name())

        # call drbdadm to bring up the resource
        drbd_proc = self._drbdadm.down(resource.get_name())
        if drbd_proc is not None:
            self._resconf.write(drbd_proc.stdin, assignment, False)
            drbd_proc.stdin.close()
            fn_rc = drbd_proc.wait()
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

        return fn_rc


    def _deploy_volume(self, assignment, vol_state):
        """
        Deploys a volume and update its state values
        """
        # do not create block devices for clients
        if not assignment.get_tstate() & Assignment.FLAG_DISKLESS != 0:
            bd_mgr   = self._server.get_bd_mgr()
            resource = assignment.get_resource()
            volume   = vol_state.get_volume()

            blockdev = bd_mgr.create_blockdevice(resource.get_name(),
              volume.get_id(), volume.get_size_kiB())

            if blockdev is not None:
                vol_state.set_blockdevice(blockdev.get_name(),
                    blockdev.get_path())
                vol_state.set_cstate_flags(DrbdVolumeState.FLAG_DEPLOY)

                max_node_id = 0
                try:
                    max_node_id = int(self._server.get_conf_value(
                      self._server.KEY_MAX_NODE_ID))
                except ValueError:
                    pass
                finally:
                    if max_node_id < 1:
                        max_node_id = self._server.DEFAULT_MAX_NODE_ID

                max_peers = 0
                try:
                    max_peers = int(self._server.get_conf_value(
                      self._server.KEY_MAX_PEERS))
                except ValueError:
                    pass
                finally:
                    if max_peers < 1 or max_peers > max_node_id:
                        max_peers = self._server.DEFAULT_MAX_NODE_ID

                # update configuration file, so drbdadm
                # before-resync-target can work properly
                self._server.export_assignment_conf(assignment)

                nodes      = []
                vol_states = []
                # add all the (peer) nodes that have or will have this
                # resource deployed
                for peer_assg in resource.iterate_assignments():
                    if (peer_assg.get_tstate() & Assignment.FLAG_DEPLOY) != 0:
                        nodes.append(peer_assg.get_node())
                # add the current volume state object no matter what its
                # current state or target state is, so drbdadm can see
                # it in the configuration and operate on it
                vol_states.append(vol_state)
                for vstate in assignment.iterate_volume_states():
                    if ((vstate.get_tstate() & DrbdVolumeState.FLAG_DEPLOY)
                      != 0 and
                      (vstate.get_cstate() & DrbdVolumeState.FLAG_DEPLOY)
                      != 0):
                        # ensure that the current volume state object is
                        # not added twice
                        if vstate != vol_state:
                            vol_states.append(vstate)

                # Initialize DRBD metadata
                drbd_proc = self._drbdadm.create_md(resource.get_name(),
                  vol_state.get_id(), max_peers)
                if drbd_proc is not None:
                    self._resconf.write_excerpt(drbd_proc.stdin,
                      assignment, nodes, vol_states)
                    drbd_proc.stdin.close()
                    fn_rc = drbd_proc.wait()
                else:
                    fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

                # FIXME: this does not need to run if metadata
                #        creation failed
                # Adjust the DRBD resource to configure the volume
                drbd_proc = self._drbdadm.adjust(resource.get_name())
                if drbd_proc is not None:
                    self._resconf.write_excerpt(drbd_proc.stdin,
                        assignment, nodes, vol_states)
                    drbd_proc.stdin.close()
                    fn_rc = drbd_proc.wait()
                    if fn_rc == 0:
                        vol_state.set_cstate_flags(DrbdVolumeState.FLAG_ATTACH |
                            DrbdVolumeState.FLAG_DEPLOY)
                        # drbdadm adjust implicitly connects the resource
                        assignment.set_cstate_flags(Assignment.FLAG_CONNECT)
                else:
                    fn_rc = DrbdManager.DRBDADM_EXEC_FAILED
            else:
                # block device allocation failed
                fn_rc = -1
        else:
            vol_state.set_cstate_flags(DrbdVolumeState.FLAG_DEPLOY)
            fn_rc = 0

        return fn_rc


    def _undeploy_volume(self, assignment, vol_state):
        """
        Undeploys a volume, then reset the state values of the volume state
        entry, so it can be removed from the assignment by the cleanup
        function.
        """
        bd_mgr   = self._server.get_bd_mgr()
        resource = assignment.get_resource()

        nodes      = []
        vol_states = []
        for peer_assg in resource.iterate_assignments():
            if (peer_assg.get_tstate() & Assignment.FLAG_DEPLOY) != 0:
                nodes.append(peer_assg.get_node())
        for vstate in assignment.iterate_volume_states():
            if ((vstate.get_tstate() & DrbdVolumeState.FLAG_DEPLOY) != 0 and
              (vstate.get_cstate() & DrbdVolumeState.FLAG_DEPLOY) != 0):
                vol_states.append(vstate)
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

        if fn_rc == 0:
            fn_rc = -1
            tstate = assignment.get_tstate()
            if (tstate & Assignment.FLAG_DISKLESS) == 0:
                fn_rc = bd_mgr.remove_blockdevice(resource.get_name(),
                  vol_state.get_id())
            if fn_rc == DM_SUCCESS or (tstate & Assignment.FLAG_DISKLESS) != 0:
                fn_rc = 0
                vol_state.set_cstate(0)
                vol_state.set_tstate(0)

        # if there are any deployed volumes left in the configuration, write
        # a new configuration file, otherwise delete the configuration file
        keep_conf = False
        for remaining in assignment.iterate_volume_states():
            if ((remaining.get_tstate() & DrbdVolumeState.FLAG_DEPLOY) != 0
              and (remaining.get_cstate() & DrbdVolumeState.FLAG_DEPLOY) != 0):
                keep_conf = True
                break
        if keep_conf:
            # update configuration file
            self._server.export_assignment_conf(assignment)
        else:
            # delete configuration file
            self._server.remove_assignment_conf(assignment)

        return fn_rc


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
        primary  = self.primary_deployment(assignment)
        for vol_state in assignment.iterate_volume_states():
            if (vol_state.get_tstate() & DrbdVolumeState.FLAG_DEPLOY != 0
              and vol_state.get_cstate() & DrbdVolumeState.FLAG_DEPLOY == 0):
                deploy_fail = True
        if primary:
            drbd_proc = self._drbdadm.primary(resource.get_name(), True)
            if drbd_proc is not None:
                self._resconf.write(drbd_proc.stdin, assignment, False)
                drbd_proc.stdin.close()
                fn_rc = drbd_proc.wait()
            else:
                fn_rc = DrbdManager.DRBDADM_EXEC_FAILED
            assignment.clear_tstate_flags(Assignment.FLAG_OVERWRITE)
        elif tstate & Assignment.FLAG_DISCARD != 0:
            fn_rc = self._reconnect(assignment)
        if deploy_fail:
            fn_rc = -1
        else:
            assignment.set_cstate_flags(Assignment.FLAG_DEPLOY)
        return fn_rc


    # FIXME: fails to undeploy, because undeploying the last volume
    # gets an error from drbdadm, and then does not remove the
    # blockdevice, but this function removes the entire assignment
    # anyway.
    # Should first perform drbdadm down, then remove the blockdevices,
    # and only if everything worked as intended, remove the assignment;
    # otherwise, single volumes may be removed after their blockdevice
    # has been undeployed successfully
    def _undeploy_assignment(self, assignment):
        """
        Undeploys all volumes of a resource, then reset the assignment's state
        values, so it can be removed by the cleanup function.
        """
        bd_mgr = self._server.get_bd_mgr()
        resource = assignment.get_resource()
        # call drbdadm to stop the DRBD on top of the blockdevice
        drbd_proc = self._drbdadm.secondary(resource.get_name())
        if drbd_proc is not None:
            self._resconf.write(drbd_proc.stdin, assignment, True)
            drbd_proc.stdin.close()
            fn_rc = drbd_proc.wait()
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

        if fn_rc == 0:
            # call drbdadm to stop the DRBD on top of the blockdevice
            drbd_proc = self._drbdadm.down(resource.get_name())
            if drbd_proc is not None:
                self._resconf.write(drbd_proc.stdin, assignment, True)
                drbd_proc.stdin.close()
                fn_rc = drbd_proc.wait()
            else:
                DrbdManager.DRBDADM_EXEC_FAILED

            if fn_rc == 0:
                # undeploy all volumes
                ud_errors = False
                cstate = assignment.get_cstate()
                if (cstate & Assignment.FLAG_DISKLESS) == 0:
                    for vol_state in assignment.iterate_volume_states():
                        stor_rc = bd_mgr.remove_blockdevice(
                            resource.get_name(), vol_state.get_id()
                        )
                        if stor_rc == DM_SUCCESS:
                            vol_state.set_cstate(0)
                            vol_state.set_tstate(0)
                        else:
                            ud_errors = True
                else:
                    # if the assignment is diskless...
                    for vol_state in assignment.iterate_volume_states():
                        vol_state.set_cstate(0)
                        vol_state.set_tstate(0)

                if not ud_errors:
                    # Remove the external configuration file
                    self._server.remove_assignment_conf(resource.get_name())
                    assignment.set_cstate(0)
                    assignment.set_tstate(0)

                fn_rc = (DrbdManager.STOR_UNDEPLOY_FAILED if ud_errors
                    else DM_SUCCESS)

        return fn_rc


    def _connect(self, assignment):
        """
        Connects a resource on the current node to all peer nodes
        """
        resource = assignment.get_resource()
        discard_flag = True if ((assignment.get_tstate()
          & Assignment.FLAG_DISCARD) != 0) else False
        drbd_proc = self._drbdadm.connect(resource.get_name(), discard_flag)
        if drbd_proc is not None:
            self._resconf.write(drbd_proc.stdin, assignment, False)
            drbd_proc.stdin.close()
            fn_rc = drbd_proc.wait()
            assignment.set_cstate_flags(Assignment.FLAG_CONNECT)
            assignment.clear_tstate_flags(Assignment.FLAG_DISCARD)
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

        return fn_rc


    def _disconnect(self, assignment):
        """
        Disconnects a resource on the current node from all peer nodes
        """
        resource = assignment.get_resource()
        drbd_proc = self._drbdadm.disconnect(resource.get_name())
        if drbd_proc is not None:
            self._resconf.write(drbd_proc.stdin, assignment, True)
            drbd_proc.stdin.close()
            fn_rc = drbd_proc.wait()
            assignment.clear_cstate_flags(Assignment.FLAG_CONNECT)
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

        return fn_rc


    def _update_connections(self, assignment):
        """
        Updates connections
        * Disconnect from nodes that do not have the same resource
          connected anymore
        * Connect to nodes that have newly deployed a resource
        * Leave valid existing connections untouched
        """
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
            for vol_state in assignment.iterate_volume_states():
                vol_state.set_cstate_flags(DrbdVolumeState.FLAG_ATTACH)

        return fn_rc


    def _reconnect(self, assignment):
        """
        Disconnects, then connects again
        """
        # disconnect
        self._disconnect(assignment)
        # connect
        fn_rc = self._connect(assignment)
        assignment.clear_tstate_flags(Assignment.FLAG_RECONNECT)

        return fn_rc


    def _attach(self, assignment, vol_state):
        """
        Attaches a volume
        """
        resource = assignment.get_resource()
        # do not attach clients, because there is no local storage on clients
        drbd_proc = self._drbdadm.attach(resource.get_name(),
          vol_state.get_id())
        if drbd_proc is not None:
            self._resconf.write(drbd_proc.stdin, assignment, False)
            drbd_proc.stdin.close()
            fn_rc = drbd_proc.wait()
            if not assignment.get_tstate() & Assignment.FLAG_DISKLESS != 0:
                # TODO: order drbdadm to attach the volume
                vol_state.set_cstate_flags(DrbdVolumeState.FLAG_ATTACH)
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

        return fn_rc


    def _detach(self, assignment, vol_state):
        """
        Detaches a volume
        """
        resource = assignment.get_resource()
        drbd_proc = self._drbdadm.detach(resource.get_name(),
          vol_state.get_id())
        if drbd_proc is not None:
            self._resconf.write(drbd_proc.stdin, assignment, True)
            drbd_proc.stdin.close()
            fn_rc = drbd_proc.wait()
            vol_state.clear_cstate_flags(DrbdVolumeState.FLAG_ATTACH)
        else:
            fn_rc = DrbdManager.DRBDADM_EXEC_FAILED

        return fn_rc


    def primary_deployment(self, assignment):
        """
        Checks whether this assignment should switch to the primary role
        after deployment (primary --force).

        Decision rules:
        DO NOT switch to the primary role,
        * if there are other nodes that have the resource deployed already;
          instead, replicate the data from those other nodes, even if the
          resource is marked for undeployment on those other nodes
        * if there is another node that has the overwrite flag set
          on this resource
        * if this assignment has the discard flag set
        DO switch to the primary role,
        * if this assignment is the first one to deploy the resource on a node
        * if this assignment has the overwrite flag set
          (overrides the discard flag, too, although it should not be
          possible to set both at the same time, anyway)
        """
        primary = True
        tstate = assignment.get_tstate()
        resource = assignment.get_resource()
        if (tstate & Assignment.FLAG_OVERWRITE) == 0:
            if (tstate & Assignment.FLAG_DISCARD) == 0:
                for peer_assg in resource.iterate_assignments():
                    if peer_assg == assignment:
                        continue
                    if ((peer_assg.get_cstate() & Assignment.FLAG_DEPLOY) != 0
                      or (peer_assg.get_tstate() & Assignment.FLAG_OVERWRITE)
                      != 0):
                        primary = False
                        break
            else:
                primary = False
        return primary


    def reconfigure(self):
        """
        Reconfigures the DrbdManager instance

        Called by the server's reconfigure() function
        """
        self._drbdadm = drbdmanage.drbd.commands.DrbdAdm(
          self._server.get_conf_value(self._server.KEY_DRBDADM_PATH))


    @staticmethod
    def name_check(name, length):
        """
        Check the validity of a string for use as a name for
        objects like nodes or volumes.
        A valid name must match these conditions:
          * must at least be 1 byte long
          * must not be longer than specified by the caller
          * contains a-z, A-Z, 0-9, - and _ characters only
          * contains at least one alpha character (a-z, A-Z)
          * must not start with a numeric character
        """
        if name == None or length == None:
            raise TypeError
        name_b   = bytearray(str(name), "utf-8")
        name_len = len(name_b)
        if name_len < 1 or name_len > length:
            raise InvalidNameException
        alpha = False
        idx = 0
        while idx < name_len:
            item = name_b[idx]
            if item >= ord('a') and item <= ord('z'):
                alpha = True
            elif item >= ord('A') and item <= ord('Z'):
                alpha = True
            else:
                if not ((item >= ord('0') and item <= ord('9') and idx >= 1)
                  or item == ord("-") or item == ord("_")):
                    raise InvalidNameException
            idx += 1
        if not alpha:
            raise InvalidNameException
        return str(name_b)


class GenericDrbdObject(object):

    """
    Super class of Drbd* objects with a property list
    """

    props = None


    def __init__(self):
        self.props = {}


    def properties_match(self, filter_props):
        """
        Returns True if any of the criteria match the object's properties

        If any of the key/value pair entries in filter_props match the
        corresponding entry in an object's properties map (props), this
        function returns True; otherwise it returns False.
        """
        match = False
        for key, val in filter_props.iteritems():
            prop_val = self.props.get(key)
            if prop_val is not None:
                if prop_val == val:
                    match = True
                    break
        return match


    def special_properties_match(self, special_props_list, filter_props):
        """
        Returns True if any of the criteria match the list properties

        If any of the key/value pair entries in filter_props match the
        corresponding entry in the supplied list of an object's special
        properties, this function returns True; otherwise it returns False
        """
        match = False
        for key, val in special_props_list.iteritems():
            filter_props_val = filter_props.get(key)
            if (filter_props_val is not None
                and val == filter_props_val):
                    match = True
                    break
        return match


class DrbdResource(GenericDrbdObject):
    NAME_MAXLEN   = consts.RES_NAME_MAXLEN

    _name        = None
    _secret      = None
    _port        = None
    _state       = None
    _volumes     = None
    _assignments = None

    FLAG_REMOVE  = 0x1
    FLAG_NEW     = 0x2

    # STATE_MASK must include all valid flags;
    # used to mask the value supplied to set_state() to prevent setting
    # non-existent flags
    STATE_MASK   = FLAG_REMOVE | FLAG_NEW

    # maximum volumes per resource
    MAX_RES_VOLS = 64

    def __init__(self, name, port):
        super(DrbdResource, self).__init__()
        self._name         = self.name_check(name)
        self._secret       = "default"
        self._port         = port
        self._state        = 0
        self._volumes      = {}
        self._assignments  = {}
        self.props[consts.SERIAL] = 1


    def get_name(self):
        return self._name


    def get_port(self):
        return self._port


    def name_check(self, name):
        return DrbdManager.name_check(name, self.NAME_MAXLEN)


    def add_assignment(self, assignment):
        node = assignment.get_node()
        self._assignments[node.get_name()] = assignment


    def get_assignment(self, name):
        return self._assignments.get(name)


    def remove_assignment(self, assignment):
        node = assignment.get_node()
        del self._assignments[node.get_name()]


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
            if (assg.get_tstate() & Assignment.FLAG_DEPLOY != 0):
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
            if (assg.get_cstate() & assg.get_tstate()
              & Assignment.FLAG_DEPLOY != 0):
                count += 1
        return count


    def add_volume(self, volume):
        self._volumes[volume.get_id()] = volume


    def get_volume(self, vol_id):
        return self._volumes.get(vol_id)


    def remove_volume(self, vol_id):
        volume = self._volumes.get(vol_id)
        if volume is not None:
            del self._volumes[volume.get_id()]


    def iterate_volumes(self):
        return self._volumes.itervalues()


    def remove(self):
        self._state |= self.FLAG_REMOVE


    def set_secret(self, secret):
        self._secret = secret


    def get_secret(self):
        return self._secret


    def get_state(self):
        return self._state


    def set_state(self, state):
        self._state = state & self.STATE_MASK


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
                        bool_to_string(self._state & self.FLAG_REMOVE)
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
            properties[consts.RES_NAME]   = self._name
        # Note: adding the shared secret to the properties list
        #       commented out - R. Altnoeder 2014-04-24
        # if selected(consts.RES_SECRET):
        #     properties[consts.RES_SECRET] = str(self._secret)
        if selected(consts.RES_PORT):
            properties[consts.RES_PORT]   = str(self._port)
        if selected(consts.TSTATE_PREFIX + consts.FLAG_REMOVE):
            properties[consts.TSTATE_PREFIX + consts.FLAG_REMOVE] = (
                bool_to_string(self._state & self.FLAG_REMOVE))
        for key in self.props.iterkeys():
            if selected(key):
                val = self.props.get(key)
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

    FLAG_REMOVE  = 0x1

    # STATE_MASK must include all valid flags;
    # used to mask the value supplied to set_state() to prevent setting
    # non-existent flags
    STATE_MASK   = FLAG_REMOVE

    def __init__(self, vol_id, size_kiB, minor):
        if not size_kiB > 0:
            raise VolSizeRangeException
        super(DrbdVolume, self).__init__(size_kiB)
        GenericDrbdObject.__init__(self)
        self._id = int(vol_id)
        if self._id < 0 or self._id >= DrbdResource.MAX_RES_VOLS:
            raise ValueError
        self._size_kiB     = size_kiB
        self._minor        = minor
        self._state        = 0
        self.props[consts.SERIAL] = 1


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
        self._state = state & self.STATE_MASK


    def remove(self):
        self._state |= self.FLAG_REMOVE


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
                        bool_to_string(self._state & self.FLAG_REMOVE)
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
                bool_to_string(self._state & self.FLAG_REMOVE))
        for key in self.props.iterkeys():
            if selected(key):
                val = self.props.get(key)
                if val is not None:
                    properties[key] = str(val)
        return properties




class DrbdNode(GenericDrbdObject):

    """
    Represents a drbdmanage host node in drbdmanage's object model.
    """

    NAME_MAXLEN = consts.NODE_NAME_MAXLEN

    AF_IPV4 = 4
    AF_IPV6 = 6

    AF_IPV4_LABEL = "ipv4"
    AF_IPV6_LABEL = "ipv6"

    _name     = None
    _addr     = None
    _addrfam  = None
    _node_id  = None
    _state    = None
    _poolsize = None
    _poolfree = None

    _assignments = None

    FLAG_REMOVE   =     0x1
    FLAG_UPDATE   =     0x2
    FLAG_UPD_POOL = 0x10000

    # STATE_MASK must include all valid flags;
    # used to mask the value supplied to set_state() to prevent setting
    # non-existent flags
    STATE_MASK = FLAG_REMOVE | FLAG_UPD_POOL | FLAG_UPDATE


    def __init__(self, name, addr, addrfam, node_id):
        super(DrbdNode, self).__init__()
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
        self._state        = 0
        self._poolfree     = -1
        self._poolsize     = -1
        self.props[consts.SERIAL] = 1


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
        self._state = state & self.STATE_MASK


    def get_poolsize(self):
        return self._poolsize


    def get_poolfree(self):
        return self._poolfree


    def set_poolsize(self, size):
        self._poolsize = size


    def set_poolfree(self, size):
        self._poolfree = size


    def set_pool(self, size, free):
        self._poolsize = size
        self._poolfree = free


    def remove(self):
        self._state |= self.FLAG_REMOVE


    def upd_pool(self):
        self._state |= self.FLAG_UPD_POOL


    def name_check(self, name):
        return DrbdManager.name_check(name, self.NAME_MAXLEN)


    def add_assignment(self, assignment):
        resource = assignment.get_resource()
        self._assignments[resource.get_name()] = assignment


    def get_assignment(self, name):
        assignment = None
        try:
            assignment = self._assignments[name]
        except KeyError:
            pass
        return assignment


    def remove_assignment(self, assignment):
        resource = assignment.get_resource()
        del self._assignments[resource.get_name()]


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
                        bool_to_string(self._state & self.FLAG_REMOVE),
                    consts.TSTATE_PREFIX + consts.FLAG_UPDATE   :
                        bool_to_string(self._state & self.FLAG_UPDATE),
                    consts.TSTATE_PREFIX + consts.FLAG_UPD_POOL :
                        bool_to_string(self._state & self.FLAG_UPD_POOL)
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
                bool_to_string(self._state & self.FLAG_REMOVE))
        if selected(consts.TSTATE_PREFIX + consts.FLAG_UPDATE):
            properties[consts.TSTATE_PREFIX + consts.FLAG_UPDATE] = (
                bool_to_string(self._state & self.FLAG_UPDATE))
        if selected(consts.TSTATE_PREFIX + consts.FLAG_UPD_POOL):
            properties[consts.TSTATE_PREFIX + consts.FLAG_UPD_POOL] = (
                bool_to_string(self._state & self.FLAG_UPD_POOL))
        for key in self.props.iterkeys():
            if selected(key):
                val = self.props.get(key)
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

    _volume      = None
    _bd_path     = None
    _blockdevice = None
    _cstate      = 0
    _tstate      = 0

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

    def __init__(self, volume):
        super(DrbdVolumeState, self).__init__()
        self._volume       = volume
        self._cstate       = 0
        self._tstate       = 0
        self.props[consts.SERIAL] = 1


    def get_volume(self):
        return self._volume


    def get_id(self):
        return self._volume.get_id()


    def get_bd_path(self):
        return self._bd_path


    def get_blockdevice(self):
        return self._blockdevice


    def set_blockdevice(self, blockdevice, bd_path):
        self._blockdevice = blockdevice
        self._bd_path     = bd_path


    def requires_action(self):
        return (self._cstate != self._tstate)


    def requires_deploy(self):
        return ((self._tstate & self.FLAG_DEPLOY == self.FLAG_DEPLOY)
          and (self._cstate & self.FLAG_DEPLOY == 0))


    def requires_attach(self):
        return ((self._tstate & self.FLAG_ATTACH == self.FLAG_ATTACH)
          and (self._cstate & self.FLAG_ATTACH == 0))


    def requires_undeploy(self):
        return ((self._tstate & self.FLAG_DEPLOY == 0)
          and (self._cstate & self.FLAG_DEPLOY != 0))


    def requires_detach(self):
        return ((self._tstate & self.FLAG_ATTACH == 0)
          and (self._cstate & self.FLAG_ATTACH != 0))


    def set_cstate(self, cstate):
        self._cstate = cstate & self.CSTATE_MASK


    def set_tstate(self, tstate):
        self._tstate = tstate & self.TSTATE_MASK


    def get_cstate(self):
        return self._cstate


    def get_tstate(self):
        return self._tstate


    def deploy(self):
        self._tstate = self._tstate | self.FLAG_DEPLOY


    def undeploy(self):
        self._tstate = 0


    def attach(self):
        self._tstate = self._tstate | self.FLAG_ATTACH


    def detach(self):
        self._tstate = (self._tstate | self.FLAG_ATTACH) ^ self.FLAG_ATTACH


    def set_cstate_flags(self, flags):
        self._cstate = (self._cstate | flags) & self.CSTATE_MASK


    def clear_cstate_flags(self, flags):
        self._cstate = ((self._cstate | flags) ^ flags) & self.CSTATE_MASK


    def set_tstate_flags(self, flags):
        self._tstate = (self._tstate | flags) & self.TSTATE_MASK


    def clear_tstate_flags(self, flags):
        self._tstate = ((self._tstate | flags) ^ flags) & self.TSTATE_MASK


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
                        bool_to_string(self._tstate & self.FLAG_DEPLOY),
                    consts.TSTATE_PREFIX + consts.FLAG_ATTACH :
                        bool_to_string(self._tstate & self.FLAG_ATTACH),
                    consts.CSTATE_PREFIX + consts.FLAG_DEPLOY :
                        bool_to_string(self._cstate & self.FLAG_DEPLOY),
                    consts.CSTATE_PREFIX + consts.FLAG_ATTACH :
                        bool_to_string(self._cstate & self.FLAG_ATTACH)
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
                "" if self._bd_path is None else str(self._bd_path))
        if selected(consts.VOL_MINOR):
            properties[consts.VOL_MINOR] = (
                str(self._volume.get_minor().get_value()))
        if selected(consts.TSTATE_PREFIX + consts.FLAG_DEPLOY):
            properties[consts.TSTATE_PREFIX + consts.FLAG_DEPLOY] = (
                bool_to_string(self._tstate & self.FLAG_DEPLOY))
        if selected(consts.TSTATE_PREFIX + consts.FLAG_ATTACH):
            properties[consts.TSTATE_PREFIX + consts.FLAG_ATTACH] = (
                bool_to_string(self._tstate & self.FLAG_ATTACH))
        if selected(consts.CSTATE_PREFIX + consts.FLAG_DEPLOY):
            properties[consts.CSTATE_PREFIX + consts.FLAG_DEPLOY] = (
                bool_to_string(self._cstate & self.FLAG_DEPLOY))
        if selected(consts.CSTATE_PREFIX + consts.FLAG_ATTACH):
            properties[consts.CSTATE_PREFIX + consts.FLAG_ATTACH] = (
                bool_to_string(self._cstate & self.FLAG_ATTACH))
        for key in self.props.iterkeys():
            if selected(key):
                val = self.props.get(key)
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
    _cstate      = 0
    _tstate      = 0
    # return code of operations
    _rc          = 0

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
    TSTATE_MASK    = (FLAG_DEPLOY | FLAG_CONNECT | FLAG_DISKLESS
                       | FLAG_UPD_CON | FLAG_RECONNECT
                       | FLAG_OVERWRITE | FLAG_DISCARD)

    # Mask applied to ignore action flags on the target state
    # of an assignment.
    ACT_IGN_MASK   = (TSTATE_MASK ^ (FLAG_DISCARD | FLAG_OVERWRITE))


    def __init__(self, node, resource, node_id, cstate, tstate):
        super(Assignment, self).__init__()
        self._node         = node
        self._resource     = resource
        self._vol_states   = {}
        for volume in resource.iterate_volumes():
            self._vol_states[volume.get_id()] = DrbdVolumeState(volume)
        self._node_id      = int(node_id)
        self.props[consts.SERIAL] = 1
        # current state
        self._cstate       = cstate
        # target state
        self._tstate       = tstate
        self._rc           = 0


    def get_node(self):
        return self._node


    def get_resource(self):
        return self._resource


    # used by AssignmentPersistence
    def add_volume_state(self, vol_state):
        self._vol_states[vol_state.get_id()] = vol_state


    def iterate_volume_states(self):
        return self._vol_states.itervalues()


    def get_volume_state(self, vol_id):
        return self._vol_states.get(vol_id)


    def remove_volume_state(self, vol_id):
        vol_st = self._vol_states.get(vol_id)
        if vol_st is not None:
            del self._vol_states[vol_id]


    def update_volume_states(self, serial):
        update_assg = False
        # create volume states for new volumes in the resource
        for volume in self._resource.iterate_volumes():
            # skip volumes that are pending removal
            if volume.get_state() & DrbdVolume.FLAG_REMOVE != 0:
                continue
            vol_st = self._vol_states.get(volume.get_id())
            if vol_st is None:
                update_assg = True
                vol_st = DrbdVolumeState(volume)
                vol_st.props[consts.SERIAL] = serial
                self._vol_states[volume.get_id()] = vol_st
        # remove volume states for volumes that no longer exist in the resource
        for vol_st in self._vol_states.itervalues():
            volume = self._resource.get_volume(vol_st.get_id())
            if volume is None:
                update_assg = True
                del self._vol_states[vol_st.get_id()]
        if update_assg:
            self.props[consts.SERIAL] = serial


    def remove(self):
        self._node.remove_assignment(self)
        self._resource.remove_assignment(self)


    def get_node_id(self):
        return self._node_id


    def get_cstate(self):
        return self._cstate


    def set_cstate(self, cstate):
        self._cstate = cstate & self.CSTATE_MASK


    def get_tstate(self):
        return self._tstate


    def set_tstate(self, tstate):
        self._tstate = tstate & self.TSTATE_MASK


    def deploy(self):
        """
        Sets the assignment's target state to deployed

        Used to indicate that an assignment's volumes should be deployed
        (installed) on the node
        """
        self._tstate = self._tstate | self.FLAG_DEPLOY


    def undeploy(self):
        """
        Sets the assignment's target state to undeployed

        Used to indicate that an assignment's volumes should be undeployed
        (removed) from a node
        """
        self._tstate = 0


    def connect(self):
        """
        Sets the assignment's target state to connected

        Used to trigger a connect action on the assignment's resource
        """
        self._tstate = self._tstate | self.FLAG_CONNECT


    def reconnect(self):
        """
        Sets the reconnect action flag on the assignment's target state

        Used to trigger reconnection of the assignment's resource's network
        connections (effectively, disconnect immediately followed by connect)
        """
        self._tstate = self._tstate | self.FLAG_RECONNECT


    def disconnect(self):
        """
        Sets the assignment's target state to disconnected

        Used to trigger a disconnect action on the assignment's resource
        """
        self._tstate = (self._tstate | self.FLAG_CONNECT) ^ self.FLAG_CONNECT


    def deploy_client(self):
        """
        Sets the assignment's target state to diskless/deployed

        Used to indicate that an assignment's volumes should be connected
        on a node in DRBD9 client mode (without local storage)
        """
        self._tstate = self._tstate | self.FLAG_DEPLOY | self.FLAG_DISKLESS


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
        self._tstate = self._tstate | self.FLAG_UPD_CON


    def set_rc(self, assg_rc):
        """
        Sets the return code of an action performed on the assignment

        Used to indicate the return code of failed actions, so the reason
        for a failed action can be queried on a remote node.
        """
        self._rc = assg_rc


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
        return (self._cstate & self.FLAG_DEPLOY) != 0


    def is_connected(self):
        """
        Returns the connection state of an assignment.

        @return: True if the resource is currently connected; False otherwise
        """
        return (self._cstate & self.FLAG_CONNECT) != 0


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
        return ((self._tstate & self.ACT_IGN_MASK) != self._cstate) or req_act


    def requires_deploy(self):
        """
        Returns whether the assignment needs to be deployed

        If an assignment is not currently deployed, but its target state
        requires it to be deployed, this function returns True to indicate
        that action must be taken to deploy this assignment.

        @returns: True if the assignment needs to be deployed; False otherwise
        """
        return ((self._tstate & self.FLAG_DEPLOY == self.FLAG_DEPLOY)
          and (self._cstate & self.FLAG_DEPLOY == 0))


    def requires_connect(self):
        """
        Returns whether the assignment requires a connect action

        Returns True only if the assignment's resource's current state is
        disconnected and its target state is connected.

        @returns: True if the assignment requires a connect action;
                  False otherwise
        """
        return ((self._tstate & self.FLAG_CONNECT == self.FLAG_CONNECT)
          and (self._cstate & self.FLAG_CONNECT == 0))


    def requires_undeploy(self):
        """
        Returns whether the assignment needs to be undeployed

        If an assignment is currently deployed, but its target state
        requires it to be undeployed, this function returns True to indicate
        that action must be taken to undeploy this assignment.

        @returns: True if the assignment needs to be undeployed;
                  False otherwise
        """
        return ((self._cstate & self.FLAG_DEPLOY == self.FLAG_DEPLOY)
          and (self._tstate & self.FLAG_DEPLOY == 0))


    def requires_disconnect(self):
        """
        Returns whether the assignment requires a disconnect action

        Returns True only if the assignment's resource's current state is
        connected and its target state is disconnected.

        @returns: True if the assignment requires a disconnect action;
                  False otherwise
        """
        return ((self._cstate & self.FLAG_CONNECT == self.FLAG_CONNECT)
          and (self._tstate & self.FLAG_CONNECT == 0))


    def set_cstate_flags(self, flags):
        self._cstate = (self._cstate | flags) & self.CSTATE_MASK


    def clear_cstate_flags(self, flags):
        self._cstate = ((self._cstate | flags) ^ flags) & self.CSTATE_MASK


    def set_tstate_flags(self, flags):
        self._tstate = (self._tstate | flags) & self.TSTATE_MASK


    def clear_tstate_flags(self, flags):
        self._tstate = ((self._tstate | flags) ^ flags) & self.TSTATE_MASK


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
                        bool_to_string(self._tstate & self.FLAG_DEPLOY),
                    consts.TSTATE_PREFIX + consts.FLAG_CONNECT :
                        bool_to_string(self._tstate & self.FLAG_CONNECT),
                    consts.TSTATE_PREFIX + consts.FLAG_DISKLESS :
                        bool_to_string(self._tstate & self.FLAG_DISKLESS),
                    consts.CSTATE_PREFIX + consts.FLAG_DEPLOY :
                        bool_to_string(self._cstate & self.FLAG_DEPLOY),
                    consts.CSTATE_PREFIX + consts.FLAG_CONNECT :
                        bool_to_string(self._cstate & self.FLAG_CONNECT),
                    consts.CSTATE_PREFIX + consts.FLAG_DISKLESS :
                        bool_to_string(self._cstate & self.FLAG_DISKLESS),
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
                bool_to_string(self._tstate & self.FLAG_DEPLOY))
        if selected(consts.TSTATE_PREFIX + consts.FLAG_CONNECT):
            properties[consts.TSTATE_PREFIX + consts.FLAG_CONNECT] = (
                bool_to_string(self._tstate & self.FLAG_CONNECT))
        if selected(consts.TSTATE_PREFIX + consts.FLAG_DISKLESS):
            properties[consts.TSTATE_PREFIX + consts.FLAG_DISKLESS] = (
                bool_to_string(self._tstate & self.FLAG_DISKLESS))
        # target state - action flags
        if selected(consts.TSTATE_PREFIX + consts.FLAG_UPD_CON):
            properties[consts.TSTATE_PREFIX + consts.FLAG_UPD_CON] = (
                bool_to_string(self._tstate & self.FLAG_UPD_CON))
        if selected(consts.TSTATE_PREFIX + consts.FLAG_OVERWRITE):
            properties[consts.TSTATE_PREFIX + consts.FLAG_OVERWRITE] = (
                bool_to_string(self._tstate & self.FLAG_OVERWRITE))
        if selected(consts.TSTATE_PREFIX + consts.FLAG_DISCARD):
            properties[consts.TSTATE_PREFIX + consts.FLAG_DISCARD] = (
                bool_to_string(self._tstate & self.FLAG_DISCARD))
        if selected(consts.TSTATE_PREFIX + consts.FLAG_RECONNECT):
            properties[consts.TSTATE_PREFIX + consts.FLAG_RECONNECT] = (
                bool_to_string(self._tstate & self.FLAG_RECONNECT))
        # current state flags
        if selected(consts.CSTATE_PREFIX + consts.FLAG_DEPLOY):
            properties[consts.CSTATE_PREFIX + consts.FLAG_DEPLOY] = (
                bool_to_string(self._cstate & self.FLAG_DEPLOY))
        if selected(consts.CSTATE_PREFIX + consts.FLAG_CONNECT):
            properties[consts.CSTATE_PREFIX + consts.FLAG_CONNECT] = (
                bool_to_string(self._cstate & self.FLAG_CONNECT))
        if selected(consts.CSTATE_PREFIX + consts.FLAG_DISKLESS):
            properties[consts.CSTATE_PREFIX + consts.FLAG_DISKLESS] = (
                bool_to_string(self._cstate & self.FLAG_DISKLESS))

        for key in self.props.iterkeys():
            if selected(key):
                val = self.props.get(key)
                if val is not None:
                    properties[key] = str(val)
        return properties
