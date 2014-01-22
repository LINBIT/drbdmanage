#!/usr/bin/python

__author__="raltnoeder"
__date__ ="$Sep 12, 2013 10:43:21 AM$"

import subprocess
import drbdmanage.conf.conffile
import drbdmanage.drbd.commands

"""
WARNING!
  do not import anything from drbdmanage.drbd.persistence
"""
from drbdmanage.storage.storagecore import GenericStorage
from drbdmanage.storage.storagecore import BlockDevice
from drbdmanage.exceptions import *
from drbdmanage.utils import *


class DrbdManager(object):
    
    """
    Manages deployment/undeployment of DRBD resources
    """
    
    _server  = None
    _drbdadm = None
    _resconf = None
    
    # Used as a return code to indicate that drbdadm could not be executed
    DRBDADM_EXEC_FAILED = 127
    
    
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
        sys.stdout.write("%sDEBUG: DrbdManager invoked%s\n"
          % (COLOR_YELLOW, COLOR_NONE))
        try:
            # check whether the configuration hash has changed
            persist = self._server.open_conf()
            if persist is not None:
                sys.stderr.write("%sDEBUG: drbdcore check/hash: %s%s\n"
                  % (COLOR_DARKPINK, persist.get_stored_hash(),
                  COLOR_NONE))
                if self._server.hashes_match(persist.get_stored_hash()):
                    # configuration did not change, bail out
                    sys.stdout.write("  hash unchanged, abort\n")
                    return
            # configuration hash has changed
            # lock and reload the configuration
            persist.close()
            persist = self._server.begin_modify_conf()
            if persist is not None:
                if self.perform_changes():
                    # sys.stdout.write("%sDEBUG: DrbdManager: state changed%s\n"
                    #   % (COLOR_GREEN, COLOR_NONE))
                    self._server.save_conf_data(persist)
                else:
                    # sys.stdout.write("%sDEBUG: DrbdManager: state unchanged%s\n"
                    #   %(COLOR_DARKGREEN, COLOR_NONE))
                    pass
            else:
                # Could not instantiate PersistenceImpl
                # TODO: Error logging
                sys.stderr.write("%sDEBUG: DrbdManager: cannot open "
                  "persistent storage%s\n" % (COLOR_RED, COLOR_NONE))
            sys.stdout.write("%sDEBUG: DrbdManager: finished%s\n"
              % (COLOR_DARKGREEN, COLOR_NONE))
        except Exception as exc:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            sys.stderr.write("%sDrbdManager: Oops: %s%s\n"
              % (COLOR_RED, str(exc), COLOR_NONE))
            print exc_type
            print exc_obj
            print exc_tb
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
        # sys.stdout.write("%s--> DrbdManager: perform changes%s\n"
        #   % (COLOR_GREEN, COLOR_NONE))
        
        """
        Check whether the system the drbdmanaged server is running on is
        a registered node in the configuration
        """
        node = self._server.get_instance_node()
        if node is None:
            sys.stdout.write("%sDEBUG DrbdManager: this node is "
              "not registered%s\n"
              % (COLOR_RED, COLOR_NONE))
            return False
        
        sys.stdout.write("%sDEBUG: DrbdManager: Perform changes on '%s'%s\n"
          % (COLOR_DARKGREEN, node.get_name(), COLOR_NONE))
        
        """
        Check all assignments for changes
        """
        assignments = node.iterate_assignments()
        for assg in assignments:
            # sys.stdout.write("%s--> DrbdManager: assignment '%s:%s'%s\n"
            #   % (COLOR_GREEN, assg.get_node().get_name(),
            #   assg.get_resource().get_name(), COLOR_NONE))
            
            if assg.requires_action():
                # sys.stdout.write("%sDEBUG: %s cstate(%x)->tstate(%x)%s\n"
                #   % (COLOR_GREEN, assg.get_resource().get_name(),
                #   assg.get_cstate(), assg.get_tstate(), COLOR_NONE))
                               
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
                    rc = self._undeploy_assignment(assg)
                    assg.set_rc(rc)
                    if rc != 0:
                        failed_actions = True
                    # ignore other actions for the same assignment
                    # after undeploy
                    continue
                
                """
                Disconnect an assignment/resource
                """
                if (not failed_actions) and assg.requires_disconnect():
                    rc = self._disconnect(assg)
                    assg.set_rc(rc)
                    if rc != 0:
                        failed_actions = True
                
                """
                Update connections
                """
                assg_actions = assg.get_tstate()
                if (not failed_actions):
                    if (assg_actions & Assignment.FLAG_UPD_CON) != 0:
                        rc = self._update_connections(assg)
                        assg.set_rc(rc)
                        if rc != 0:
                            failed_actions = True
                    if (assg_actions & Assignment.FLAG_RECONNECT) != 0:
                        rc = self._reconnect(assg)
                        assg.set_rc(rc)
                        if rc != 0:
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
                            rc = self._deploy_volume(assg, vol_state)
                            assg.set_rc(rc)
                            if rc != 0:
                                failed_actions = True
                        elif vol_state.requires_undeploy():
                            pool_changed = True
                            rc = self._undeploy_volume(assg, vol_state)
                            assg.set_rc(rc)
                            if rc != 0:
                                failed_actions = True
                    
                    """
                    Attach a volume to or detach a volume from local storage
                    """
                    if (not failed_actions):
                        if vol_state.requires_attach():
                            rc = self._attach(assg, vol_state)
                            assg.set_rc(rc)
                            if rc != 0:
                                failed_actions = True
                        elif vol_state.requires_detach():
                            rc = self._detach(assg, vol_state)
                            assg.set_rc(rc)
                            if rc != 0:
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
                    rc = self._deploy_assignment(assg)
                    assg.set_rc(rc)
                    if rc != 0:
                        failed_actions = True
                
                if (not failed_actions) and assg.requires_connect():
                    rc = self._connect(assg)
                    assg.set_rc(rc)
                    if rc != 0:
                        failed_actions = True
                
                if (assg.get_tstate() & Assignment.FLAG_DISKLESS) != 0:
                    assg.set_cstate_flags(Assignment.FLAG_DISKLESS)
                
                if ((assg.get_tstate() & Assignment.ACT_IGN_MASK)
                  != assg.get_cstate()):
                    sys.stderr.write(COLOR_RED
                      + "Warning: End of perform_changes(), but assignment "
                      "seems to have pending actions" + COLOR_NONE + "\n")
        
        """
        Cleanup the server's data structures
        (remove entries that are no longer required)
        """
        self._server.cleanup()
        
        if pool_changed:
            self._server.update_pool_data()
        
        return state_changed
    
    
    def drbdctrl_res_up(self):
        # call drbdadm to bring up the resource
        drbd_proc = self._drbdadm.ext_conf_adjust(
          self._server.DRBDCTRL_RES_NAME)
        rc = drbd_proc.wait()
    
    
    def initial_up(self):
        """
        Attempts to bring up all deployed resources.
        Used when the drbdmanage server starts up.
        """
        node = self._server.get_instance_node()
        if node is not None:
            for assg in node.iterate_assignments():
                # sys.stderr.write("DEBUG: initial_up %s\n"
                #   % (assg.get_resource().get_name()))
                cstate = assg.get_cstate()
                tstate = assg.get_tstate()
                if assg.is_deployed():
                    try:
                        self._up_resource(assg)
                    except Exception as exc:
                        sys.stderr.write("DEBUG: initial_up: %s\n"
                          % (str(exc)))
    
    
    def _up_resource(self, assignment):
        """
        Brings up DRBD resources
        """
        bd_mgr   = self._server.get_bd_mgr()
        resource = assignment.get_resource()
        
        # call drbdadm to bring up the resource
        drbd_proc = self._drbdadm.adjust(resource.get_name())
        if drbd_proc is not None:
            self._resconf.write(drbd_proc.stdin, assignment, False)
            drbd_proc.stdin.close()
            rc = drbd_proc.wait()
        else:
            rc = DrbdManager.DRBDADM_EXEC_FAILED
        
        return rc
    
    
    def _down_resource(self, assignment):
        """
        Brings down DRBD resources
        """
        bd_mgr   = self._server.get_bd_mgr()
        resource = assignment.get_resource()
        
        # call drbdadm to bring up the resource
        drbd_proc = self._drbdadm.down(resource.get_name())
        if drbd_proc is not None:
            self._resconf.write(drbd_proc.stdin, assignment, False)
            drbd_proc.stdin.close()
            rc = drbd_proc.wait()
        else:
            rc = DrbdManager.DRBDADM_EXEC_FAILED
        
        return rc
    
    
    def _deploy_volume(self, assignment, vol_state):
        """
        Deploys a volume and update its state values
        """
        # do not create block devices for clients
        if not assignment.get_tstate() & Assignment.FLAG_DISKLESS != 0:
            bd_mgr   = self._server.get_bd_mgr()
            resource = assignment.get_resource()
            volume   = vol_state.get_volume()
            minor    = volume.get_minor()

            # DEBUG
            if resource is None:
                sys.stderr.write("DEBUG: _deploy_volume(): resource == NULL\n")
            
            bd = bd_mgr.create_blockdevice(resource.get_name(),
              volume.get_id(), volume.get_size_MiB())
            
            if bd is not None:
                vol_state.set_blockdevice(bd.get_name(), bd.get_path())
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
                for assg in resource.iterate_assignments():
                    if (assg.get_tstate() & Assignment.FLAG_DEPLOY) != 0:
                        nodes.append(assg.get_node())
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
                    drbdadm_conf = drbdmanage.conf.conffile.DrbdAdmConf()
                    self._resconf.write_excerpt(drbd_proc.stdin,
                      assignment, nodes, vol_states)
                    drbd_proc.stdin.close()
                    rc = drbd_proc.wait()
                else:
                    rc = DrbdManager.DRBDADM_EXEC_FAILED
                
                # Adjust the DRBD resource to configure the volume
                drbd_proc = self._drbdadm.adjust(resource.get_name())
                self._resconf.write_excerpt(drbd_proc.stdin,
                  assignment, nodes, vol_states)
                drbd_proc.stdin.close()
                rc = drbd_proc.wait()
                if rc == 0:
                    vol_state.set_cstate_flags(DrbdVolumeState.FLAG_ATTACH |
                      DrbdVolumeState.FLAG_DEPLOY)
                    # drbdadm adjust implicitly connects the resource
                    assg.set_cstate_flags(Assignment.FLAG_CONNECT)
            else:
                # block device allocation failed
                rc = -1
        else:
            vol_state.set_cstate_flags(DrbdVolumeState.FLAG_DEPLOY)
            rc = 0
        
        return rc
    
    
    def _undeploy_volume(self, assignment, vol_state):
        """
        Undeploys a volume, then reset the state values of the volume state
        entry, so it can be removed from the assignment by the cleanup
        function.
        """
        bd_mgr   = self._server.get_bd_mgr()
        resource = assignment.get_resource()
        volume   = vol_state.get_volume()
       
        nodes      = []
        vol_states = []
        for assg in resource.iterate_assignments():
            if (assg.get_tstate() & Assignment.FLAG_DEPLOY) != 0:
                nodes.append(assg.get_node())
        for vstate in assignment.iterate_volume_states():
            if ((vstate.get_tstate() & DrbdVolumeState.FLAG_DEPLOY) != 0 and
              (vstate.get_cstate() & DrbdVolumeState.FLAG_DEPLOY) != 0):
                vol_states.append(vstate)
        drbd_proc = self._drbdadm.adjust(resource.get_name())
        if drbd_proc is not None:
            self._resconf.write_excerpt(drbd_proc.stdin, assignment,
              nodes, vol_states)
            drbd_proc.stdin.close()
            rc = drbd_proc.wait()
            if rc == 0:
                vol_state.clear_cstate_flags(DrbdVolumeState.FLAG_ATTACH)
        else:
            rc = DrbdManager.DRBDADM_EXEC_FAILED
        
        if rc == 0:
            rc = -1
            tstate = assignment.get_tstate()
            if (tstate & Assignment.FLAG_DISKLESS) == 0:
                rc = bd_mgr.remove_blockdevice(resource.get_name(),
                  vol_state.get_id())
            if rc == DM_SUCCESS or (tstate & Assignment.FLAG_DISKLESS) != 0:
                rc = 0
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
        
        return rc
    
    
    def _deploy_assignment(self, assignment):
        """
        Finishes deployment of an assignment. The actual deployment of the
        assignment's/resource's volumes takes place in per-volume actions
        of the DrbdManager.perform_changes() function.
        """
        rc = 0
        deploy_fail = False
        resource = assignment.get_resource()
        tstate   = assignment.get_tstate()
        for vol_state in assignment.iterate_volume_states():
            if (vol_state.get_tstate() & DrbdVolumeState.FLAG_DEPLOY != 0
              and vol_state.get_cstate() & DrbdVolumeState.FLAG_DEPLOY == 0):
                deploy_fail = True
        if tstate & Assignment.FLAG_OVERWRITE != 0:
            drbd_proc = self._drbdadm.primary(resource.get_name(), True)
            if drbd_proc is not None:
                self._resconf.write(drbd_proc.stdin, assignment, False)
                drbd_proc.stdin.close()
                rc = drbd_proc.wait()
            else:
                rc = DrbdManager.DRBDADM_EXEC_FAILED
            assignment.clear_tstate_flags(Assignment.FLAG_OVERWRITE)
        elif tstate & Assignment.FLAG_DISCARD != 0:
            rc = self._reconnect(assignment)
        if deploy_fail:
            rc = -1
        else:
            assignment.set_cstate_flags(Assignment.FLAG_DEPLOY)
        return rc
    
    
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
            rc = drbd_proc.wait()
        else:
            rc = DrbdManager.DRBDADM_EXEC_FAILED
        
        if rc == 0:
            # call drbdadm to stop the DRBD on top of the blockdevice
            drbd_proc = self._drbdadm.down(resource.get_name())
            if drbd_proc is not None:
                self._resconf.write(drbd_proc.stdin, assignment, True)
                drbd_proc.stdin.close()
                rc = drbd_proc.wait()
                if rc == 0:
                    # remove the configuration file
                    self._server.remove_assignment_conf(resource.get_name())
                    assignment.set_cstate(0)
                    assignment.set_tstate(0)
            else:
                DrbdManager.DRBDADM_EXEC_FAILED
        
            # undeploy all volumes
            tstate = assignment.get_tstate()
            if (tstate & Assignment.FLAG_DISKLESS) == 0:
                for vol_state in assignment.iterate_volume_states():
                    stor_rc = bd_mgr.remove_blockdevice(resource.get_name(),
                      vol_state.get_id())
                    if stor_rc == DM_SUCCESS:
                        vol_state.set_cstate(0)
                        vol_state.set_tstate(0)
                    # update configuration file
                    self._server.export_assignment_conf(assignment)
            else:
                for vol_state in assignment.iterate_volume_states():
                    vol_state.set_cstate(0)
                    vol_state.set_tstate(0)
        
        return rc
    
    
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
            rc = drbd_proc.wait()
            assignment.set_cstate_flags(Assignment.FLAG_CONNECT)
            assignment.clear_tstate_flags(Assignment.FLAG_DISCARD)
        else:
            rc = DrbdManager.DRBDADM_EXEC_FAILED
        
        return rc
    
    
    def _disconnect(self, assignment):
        """
        Disconnects a resource on the current node from all peer nodes
        """
        resource = assignment.get_resource()
        drbd_proc = self._drbdadm.disconnect(resource.get_name())
        if drbd_proc is not None:
            self._resconf.write(drbd_proc.stdin, assignment, True)
            drbd_proc.stdin.close()
            rc = drbd_proc.wait()
            assignment.clear_cstate_flags(Assignment.FLAG_CONNECT)
        else:
            rc = DrbdManager.DRBDADM_EXEC_FAILED
        
        return rc
    
    
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
            rc = drbd_proc.wait()
        else:
            rc = DrbdManager.DRBDADM_EXEC_FAILED
        if rc == 0:
            assignment.set_cstate_flags(Assignment.FLAG_CONNECT)
            assignment.clear_tstate_flags(Assignment.FLAG_UPD_CON)
            for vol_state in assignment.iterate_volume_states():
                vol_state.set_cstate_flags(DrbdVolumeState.FLAG_ATTACH)
        
        return rc
    
    
    def _reconnect(self, assignment):
        """
        Disconnects, then connects again
        """
        # disconnect
        self._disconnect(assignment)
        # connect
        rc = self._connect(assignment)
        assignment.clear_tstate_flags(Assignment.FLAG_RECONNECT)
        
        return rc
    
    
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
            rc = drbd_proc.wait()
            if not assignment.get_tstate() & Assignment.FLAG_DISKLESS != 0:
                # TODO: order drbdadm to attach the volume
                vol_state.set_cstate_flags(DrbdVolumeState.FLAG_ATTACH)
        else:
            rc = DrbdManager.DRBDADM_EXEC_FAILED
        
        return rc
    
    
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
            rc = drbd_proc.wait()
            vol_state.clear_cstate_flags(DrbdVolumeState.FLAG_ATTACH)
        else:
            rc = DrbdManager.DRBDADM_EXEC_FAILED
        
        return rc
    
    
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
          * contains a-z, A-Z, 0-9 and _ characters only
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
        for idx in xrange(0, name_len):
            b = name_b[idx]
            if b >= ord('a') and b <= ord('z'):
                alpha = True
                continue
            if b >= ord('A') and b <= ord('Z'):
                alpha = True
                continue
            if b >= ord('0') and b <= ord('9') and idx >= 1:
                continue
            if b == ord("_"):
                continue
            raise InvalidNameException
        if not alpha:
            raise InvalidNameException
        return str(name_b)


class DrbdResource(object):
    NAME_MAXLEN   = 16
    PORT_NR_AUTO  = -1
    PORT_NR_ERROR = -2
    
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
        self._name        = self.name_check(name)
        self._secret      = "default"
        self._port        = port
        self._state       = 0
        self._volumes     = dict()
        self._assignments = dict()
    
    
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
    
    
    def get_volume(self, id):
        return self._volumes.get(id)
    
    
    def remove_volume(self, id):
        volume = self._volumes.get(id)
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
    
    
class DrbdResourceView(object):
    
    """
    Serializes and deserializes resource list entries
    
    This class is used by the drbdmanage server to serialize resource list
    entries for the resources and volumes list. The drbdmanage client uses this
    class to deserialize and display the information received from the
    drbdmanage server.
    """
    
    # array indexes
    RV_NAME     = 0
    RV_SECRET   = 1
    RV_PORT     = 2
    RV_STATE    = 3
    RV_VOLUMES  = 4
    RV_PROP_LEN = 5
    
    _name    = None
    _state   = None
    _secret  = None
    _port    = None
    _volumes = None
    
    _machine_readable = False
    
    
    def __init__(self, properties, machine_readable):
        if len(properties) < self.RV_PROP_LEN:
            raise IncompatibleDataException
        self._name  = properties[self.RV_NAME]
        self._secret = properties[self.RV_SECRET]
        try:
            self._state = long(properties[self.RV_STATE])
            self._port   = int(properties[self.RV_PORT])
        except ValueError:
            raise IncompatibleDataException
        self._volumes = []
        for vol_list in properties[self.RV_VOLUMES]:
            self._volumes.append(DrbdVolumeView(vol_list, machine_readable))
        self._machine_readable = machine_readable
    
    
    @classmethod
    def get_name_maxlen(self):
        return DrbdResource.NAME_MAXLEN
    
    
    @classmethod
    def get_properties(cls, resource):
        properties = []
        properties.append(resource.get_name())
        properties.append(resource.get_secret())
        if resource.get_port() == DrbdResource.PORT_NR_AUTO:
            properties.append("auto")
        else:
            properties.append(str(resource.get_port()))
        properties.append(resource.get_state())
        vol_list = []
        for volume in resource.iterate_volumes():
            vol_list.append(DrbdVolumeView.get_properties(volume))
        properties.append(vol_list)
        return properties
    
    
    def get_name(self):
        return self._name
    
    
    def get_secret(self):
        return self._secret
    
    
    def get_port(self):
        return self._port
    
    
    def get_state(self):
        text = "-"
        if self._state & DrbdResource.FLAG_REMOVE != 0:
            if self._machine_readable:
                text = "REMOVE"
            else:
                text = "remove"
        return text
    
    
    def get_volumes(self):
        return self._volumes
    
    
class DrbdVolume(GenericStorage):
    
    """
    Representation of a DRBD volume specification in drbdmanage's object model
    
    This class represents the specification of a DRBD volume in
    drbdmanage's configuration. DrbdVolume objects are referenced in
    DrbdResource objects.
    """
    
    _id          = None
    _size_MiB    = None
    _minor       = None
    _state       = None
    
    FLAG_REMOVE  = 0x1
    
    # STATE_MASK must include all valid flags;
    # used to mask the value supplied to set_state() to prevent setting
    # non-existent flags
    STATE_MASK   = FLAG_REMOVE
    
    def __init__(self, id, size_MiB, minor):
        if not size_MiB > 0:
            raise VolSizeRangeException
        super(DrbdVolume, self).__init__(size_MiB)
        self._id       = int(id)
        if self._id < 0 or self._id >= DrbdResource.MAX_RES_VOLS:
            raise ValueError
        self._size_MiB = size_MiB
        self._minor    = minor
        self._state    = 0
    
    
    def get_id(self):
        return self._id
    
    
    # returns a storagecore.MinorNr object
    def get_minor(self):
        return self._minor
    
    
    def get_state(self):
        return self._state
    
    
    def set_state(self, state):
        self._state = state & self.STATE_MASK
    
    
    def remove(self):
        self._state |= self.FLAG_REMOVE


class DrbdVolumeView(object):
    
    """
    Serializes and deserializes volume list entries
    
    This class is used by the drbdmanage server to serialize volume list
    entries for the resources and volumes list. The drbdmanage client uses this
    class to deserialize and display the information received from the
    drbdmanage server.
    """
    
    # array indexes
    VV_ID       = 0
    VV_SIZE_MiB = 1
    VV_MINOR    = 2
    VV_STATE    = 3
    VV_PROP_LEN = 4
    
    _id       = None
    _size_MiB = None
    _minor    = None
    _state    = None
    
    _machine_readable = False
    
    
    def __init__(self, properties, machine_readable):
        if len(properties) < self.VV_PROP_LEN:
            raise IncompatibleDataException
        self._id = properties[self.VV_ID]
        try:
            self._size_MiB = long(properties[self.VV_SIZE_MiB])
            self._minor    = int(properties[self.VV_MINOR])
            self._state    = long(properties[self.VV_STATE])
        except ValueError:
            raise IncompatibleDataException
        self._machine_readable = machine_readable
    
    
    @classmethod
    def get_properties(cls, volume):
        properties = []
        minor   = volume.get_minor()
        properties.append(volume.get_id())
        properties.append(volume.get_size_MiB())
        properties.append(minor.get_value())
        properties.append(volume.get_state())
        return properties
    
    
    def get_id(self):
        return self._id
    
    
    def get_size_MiB(self):
        return self._size_MiB
    
    
    def get_minor(self):
        if self._minor == -1:
            return "auto"
        elif self._minor == -2:
            # FIXME: Reserved for automatic selection of a minor number by
            #        the DRBD kernel module. This is probably nonsense,
            #        and if it is, it should be removed.
            return "auto-drbd"
        else:
            return str(self._minor)
    
    
    def get_state(self):
        text = ""
        if self._state & DrbdVolume.FLAG_REMOVE != 0:
            if self._machine_readable:
                text = "REMOVE"
            else:
                text = "remove"
        if len(text) == 0:
            text = "-"
        return text


class DrbdNode(object):
    
    """
    Represents a drbdmanage host node in drbdmanage's object model.
    """
    
    NAME_MAXLEN = 16
    
    AF_IPV4 = 4
    AF_IPV6 = 6
    
    AF_IPV4_LABEL = "ipv4"
    AF_IPV6_LABEL = "ipv6"
    
    _name     = None
    _ip       = None
    _af       = None
    _state    = None
    _poolsize = None
    _poolfree = None
    
    _assignments = None
    
    FLAG_REMOVE   =     0x1
    FLAG_UPD_POOL = 0x10000
    
    # STATE_MASK must include all valid flags;
    # used to mask the value supplied to set_state() to prevent setting
    # non-existent flags
    STATE_MASK = FLAG_REMOVE | FLAG_UPD_POOL
    
    
    def __init__(self, name, ip, af):
        self._name    = self.name_check(name)
        # TODO: there should be sanity checks on ip
        af_n = int(af)
        if af_n == self.AF_IPV4 or af_n == self.AF_IPV6:
            self._af = af_n
        else:
            raise InvalidAddrFamException
        self._ip          = ip
        self._assignments = dict()
        self._state       = 0
        self._poolfree    = -1
        self._poolsize    = -1
    
    
    def get_name(self):
        return self._name
    
    
    def get_ip(self):
        return self._ip
    
    
    def get_af(self):
        return self._af
    
    
    def get_af_label(self):
        label = "unknown"
        if self._af == self.AF_IPV4:
            label = self.AF_IPV4_LABEL
        elif self._af == self.AF_IPV6:
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


class DrbdNodeView(object):
    
    """
    Serializes and deserializes node list entries
    
    This class is used by the drbdmanage server to serialize node list entries.
    The drbdmanage client uses this class to deserialize and display the
    information received from the drbdmanage server.
    """
    
    # array indexes
    NV_NAME     = 0
    NV_AF_LABEL = 1
    NV_IP       = 2
    NV_POOLSIZE = 3
    NV_POOLFREE = 4
    NV_STATE    = 5
    NV_PROP_LEN = 6
    
    _name     = None
    _af       = None
    _ip       = None
    _poolsize = None
    _poolfree = None
    _state    = None
    
    _machine_readable = False
    
    
    def __init__(self, properties, machine_readable):
        if len(properties) < self.NV_PROP_LEN:
            raise IncompatibleDataException
        self._name     = properties[self.NV_NAME]
        self._af       = properties[self.NV_AF_LABEL]
        self._ip       = properties[self.NV_IP]
        try:
            self._poolsize = long(properties[self.NV_POOLSIZE])
            self._poolfree = long(properties[self.NV_POOLFREE])
            self._state    = long(properties[self.NV_STATE])
        except TypeError:
            raise IncompatibleDataException
        self._machine_readable = machine_readable
    
    
    @classmethod
    def get_name_maxlen(self):
        return DrbdNode.NAME_MAXLEN
    
    
    @classmethod
    def get_properties(cls, node):
        properties = []
        properties.append(node.get_name())
        properties.append(node.get_af_label())
        properties.append(node.get_ip())
        properties.append(node.get_poolsize())
        properties.append(node.get_poolfree())
        properties.append(node.get_state())
        return properties
    
    
    def get_name(self):
        return self._name
    
    
    def get_af(self):
        return self._af
    
    
    def get_ip(self):
        return self._ip
    
    
    def get_poolsize(self):
        return self._poolsize
    
    
    def get_poolfree(self):
        return self._poolfree
    
    
    def get_state(self):
        text = "-"
        if self._state & DrbdNode.FLAG_REMOVE != 0:
            if self._machine_readable:
                text = "REMOVE"
            else:
                text = "remove"
        return text
    
    
class DrbdVolumeState(object):
    
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
        self._volume = volume
        self._cstate = 0
        self._tstate = 0
    
    
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


class DrbdVolumeStateView(object):
    
    """
    Serializes and deserializes volume state list entries
    
    This class is used by the drbdmanage server to serialize volume state list
    entries for the assignments view. The drbdmanage client uses this class
    to deserialize and display the information received from the drbdmanage
    server.
    """
    
    # array indexes
    SV_ID       = 0
    SV_BD_PATH  = 1
    SV_CSTATE   = 2
    SV_TSTATE   = 3
    SV_PROP_LEN = 4
    
    _id      = None
    _bd_path = None
    _cstate  = None
    _tstate  = None
    
    _machine_readable = False
    
    
    def __init__(self, properties, machine_readable):
        if len(properties) < self.SV_PROP_LEN:
            raise IncompatibleDataException
        self._id = properties[self.SV_ID]
        self._bd_path = properties[self.SV_BD_PATH]
        self._cstate  = properties[self.SV_CSTATE]
        self._tstate  = properties[self.SV_TSTATE]
        self._machine_readable = machine_readable
    
    
    @classmethod
    def get_properties(cls, vol_state):
        properties = []
        volume  = vol_state.get_volume()
        bd_path = vol_state.get_bd_path()
        if bd_path is None:
            bd_path = "-"
        properties.append(vol_state.get_id())
        properties.append(bd_path)
        properties.append(vol_state.get_cstate())
        properties.append(vol_state.get_tstate())
        return properties
    
    
    def get_id(self):
        return self._id
    
    
    def get_bd_path(self):
        return self._bd_path
    
    
    def get_cstate(self):
        mr = self._machine_readable
        text = ""
        if self._cstate & DrbdVolumeState.FLAG_DEPLOY != 0:
            text = state_text_append(mr, text, "DEPLOY", "deployed")
        if self._cstate & DrbdVolumeState.FLAG_ATTACH != 0:
            text = state_text_append(mr, text, "ATTACH", "attached")
        # FIXME: experimental human-readable mask output
        if not self._machine_readable:
            if self._cstate & DrbdVolumeState.FLAG_ATTACH != 0:
                text = "a"
            else:
                text = "-"
            if self._cstate & DrbdVolumeState.FLAG_DEPLOY != 0:
                text += "d"
            else:
                text += "-"
        return text
    
    
    def get_tstate(self):
        mr = self._machine_readable
        text = ""
        if self._tstate & DrbdVolumeState.FLAG_DEPLOY != 0:
            text = state_text_append(mr, text, "DEPLOY", "deploy")
        if self._tstate & DrbdVolumeState.FLAG_ATTACH != 0:
            text = state_text_append(mr, text, "ATTACH", "attach")
        # FIXME: experimental human-readable mask output
        if not self._machine_readable:
            if self._tstate & DrbdVolumeState.FLAG_ATTACH != 0:
                text = "a"
            else:
                text = "-"
            if self._tstate & DrbdVolumeState.FLAG_DEPLOY != 0:
                text += "d"
            else:
                text += "-"
        return text
    
    
    # TODO: implement state views
    def get_state(self):
        return "<not implemented>"


class Assignment(object):
    
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
    
    NODE_ID_ERROR  = -1


    def __init__(self, node, resource, node_id, cstate, tstate):
        self._node        = node
        self._resource    = resource
        self._vol_states  = dict()
        for volume in resource.iterate_volumes():
            self._vol_states[volume.get_id()] = DrbdVolumeState(volume)
        self._node_id     = int(node_id)
        # current state
        self._cstate      = cstate
        # target state
        self._tstate      = tstate
        self._rc          = 0
    
    
    def get_node(self):
        return self._node
    
    
    def get_resource(self):
        return self._resource
    
    
    # used by AssignmentPersistence
    def add_volume_state(self, vol_state):
        self._vol_states[vol_state.get_id()] = vol_state
    
    
    def iterate_volume_states(self):
        return self._vol_states.itervalues()
    
    
    def get_volume_state(self, id):
        return self._vol_states.get(id)
    
    
    def remove_volume_state(self, id):
        vol_st = self._vol_states.get(id)
        if vol_st is not None:
            del self._vol_states[id]
    
    
    def update_volume_states(self):
        # create volume states for new volumes in the resource
        for volume in self._resource.iterate_volumes():
            # skip volumes that are pending removal
            if volume.get_state() & DrbdVolume.FLAG_REMOVE != 0:
                continue
            vol_st = self._vol_states.get(volume.get_id())
            if vol_st is None:
                vol_st = DrbdVolumeState(volume)
                self._vol_states[volume.get_id()] = vol_st
        # remove volume states for volumes that no longer exist in the resource
        for vol_st in self._vol_states.itervalues():
            volume = self._resource.get_volume(vol_st.get_id())
            if volume is None:
                del self._vol_states[vol_st.get_id()]
    
    
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
    
    
    def set_rc(self, rc):
        """
        Sets the return code of an action performed on the assignment
        
        Used to indicate the return code of failed actions, so the reason
        for a failed action can be queried on a remote node.
        """
        self._rc = rc
    
    
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


class AssignmentView(object):
    
    """
    Serializes and deserializes assignments list entries
    
    This class is used by the drbdmanage server to serialize assignment list
    entries. The drbdmanage client uses this class to deserialize and display
    the information received from the drbdmanage server.
    """
    
    # array indexes
    AV_NODE_NAME  = 0
    AV_RES_NAME   = 1
    AV_NODE_ID    = 2
    AV_CSTATE     = 3
    AV_TSTATE     = 4
    AV_VOL_STATES = 5
    AV_PROP_LEN   = 6
    
    _node         = None
    _resource     = None
    _node_id      = None
    _cstate       = None
    _tstate       = None
    
    _machine_readable = False
    
    
    def __init__(self, properties, machine_readable):
        if len(properties) < self.AV_PROP_LEN:
            raise IncompatibleDataException
        self._machine_readable = machine_readable
        self._node        = properties[self.AV_NODE_NAME]
        self._resource    = properties[self.AV_RES_NAME]
        self._node_id     = properties[self.AV_NODE_ID]
        try:
            self._cstate      = long(properties[self.AV_CSTATE])
            self._tstate      = long(properties[self.AV_TSTATE])
        except ValueError:
            raise IncompatibleDataException
        self._vol_states = []
        for vol_state in properties[self.AV_VOL_STATES]:
            self._vol_states.append(
              DrbdVolumeStateView(vol_state, machine_readable))
        
    
    @classmethod
    def get_properties(cls, assg):
        properties = []
        node     = assg.get_node()
        resource = assg.get_resource()
        properties.append(node.get_name())
        properties.append(resource.get_name())
        properties.append(assg.get_node_id())
        properties.append(assg.get_cstate())
        properties.append(assg.get_tstate())
        vol_state_list = []
        properties.append(vol_state_list)
        for vol_state in assg.iterate_volume_states():
            vol_state_list.append(
              DrbdVolumeStateView.get_properties(vol_state))
        return properties
    
    
    def get_node(self):
        return self._node
    
    
    def get_resource(self):
        return self._resource
    
    
    def get_volume_states(self):
        return self._vol_states
    
    
    def get_blockdevice(self):
        return self._blockdevice
    
    
    def get_node_id(self):
        return self._node_id
    
    
    def get_cstate(self):
        mr = self._machine_readable
        text = ""
        if self._cstate & Assignment.FLAG_DEPLOY != 0:
            text = state_text_append(mr, text, "DEPLOY", "deployed")
        if self._cstate & Assignment.FLAG_CONNECT != 0:
            text = state_text_append(mr, text, "CONNECT", "connected")
        if self._cstate & Assignment.FLAG_DISKLESS != 0:
            text = state_text_append(mr, text, "DISKLESS", "client")
        if len(text) == 0:
            text = "-"
        # FIXME: experimental human-readable mask output
        if not self._machine_readable:
            if self._cstate & Assignment.FLAG_CONNECT != 0:
                text = "c"
            else:
                text = "-"
            if self._cstate & Assignment.FLAG_DEPLOY != 0:
                text += "d"
            else:
                text += "-"
            if self._cstate & Assignment.FLAG_DISKLESS != 0:
                text += "D"
            else:
                text += "-"
        return text
    
    
    def get_tstate(self):
        mr = self._machine_readable
        text = ""
        if self._tstate & Assignment.FLAG_DEPLOY != 0:
            text = state_text_append(mr, text, "DEPLOY", "deploy")
        if self._tstate & Assignment.FLAG_CONNECT != 0:
            text = state_text_append(mr, text, "CONNECT", "connect")
        if self._tstate & Assignment.FLAG_UPD_CON != 0:
            text = state_text_append(mr, text, "UPD_CON", "update")
        if self._tstate & Assignment.FLAG_RECONNECT != 0:
            text = state_text_append(mr, text, "RECONNECT", "reconnect")
        if self._tstate & Assignment.FLAG_OVERWRITE != 0:
            text = state_text_append(mr, text, "OVERWRITE", "init-master")
        if self._tstate & Assignment.FLAG_DISCARD != 0:
            text = state_text_append(mr, text, "DISCARD", "discard")
        if len(text) == 0:
            text = "-"
        # FIXME: experimental human-readable mask output
        if not self._machine_readable:
            if self._tstate & Assignment.FLAG_CONNECT != 0:
                text = "c"
            else:
                text = "-"
            if self._tstate & Assignment.FLAG_DEPLOY != 0:
                text += "d"
            else:
                text += "-"
            if self._tstate & Assignment.FLAG_DISKLESS != 0:
                text += "D"
            else:
                text += "-"
            text += " ("
            if self._tstate & Assignment.FLAG_DISCARD != 0:
                text += "d"
            else:
                text += "-"
            if self._tstate & Assignment.FLAG_OVERWRITE != 0:
                text += "o"
            else:
                text += "-"
            if self._tstate & Assignment.FLAG_RECONNECT != 0:
                text += "r"
            else:
                text += "-"
            if self._tstate & Assignment.FLAG_UPD_CON != 0:
                text += "u"
            else:
                text += "-"
            text += ")"
        return text
    
    
    def get_state(self):
        mr = self._machine_readable
        text = ""
        if self._tstate & Assignment.FLAG_DEPLOY != 0:
            text = state_text_append(mr, text, "DEPLOY", "deployed")
        if self._tstate & Assignment.FLAG_ATTACH != 0:
            text = state_text_append(mr, text, "ATTACH", "attached")
        if self._tstate & Assignment.FLAG_CONNECT != 0:
            text = state_text_append(mr, text, "CONNECT", "connected")
        if self._tstate & Assignment.FLAG_DISKLESS != 0:
            text = state_text_append(mr, text, "DISKLESS", "client")
        if len(text) == 0:
            text = "-"
        return text


def state_text_append(machine_readable, text, mr_text, hr_text):
    if machine_readable:
        if len(text) > 0:
            text += "|"
        text += mr_text
    else:
        if len(text) > 0:
            text+= ","
        text += hr_text
    return text
