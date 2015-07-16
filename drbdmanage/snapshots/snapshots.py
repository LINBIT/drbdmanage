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

import logging
import drbdmanage.consts as consts
import drbdmanage.exceptions as exc
import drbdmanage.drbd.drbdcommon as drbdcommon
import drbdmanage.storage.storagecommon as storagecommon
import drbdmanage.utils as dmutils

class DrbdSnapshot(drbdcommon.GenericDrbdObject):

    NAME_MAXLEN  = consts.SNAPS_NAME_MAXLEN
    # Valid characters in addition to [a-zA-Z0-9]
    NAME_VALID_CHARS      = "_"
    # Additional valid characters, but not allowed as the first character
    NAME_VALID_INNER_CHARS = "-"

    _name        = None
    _resource    = None
    _assignments = None


    def __init__(self, name, resource, get_serial_fn, init_serial, init_props):
        super(DrbdSnapshot, self).__init__(
            get_serial_fn, init_serial, init_props
        )
        self._name        = self.name_check(name)
        self._resource    = resource
        self._assignments = {}


    def name_check(self, name):
        return drbdcommon.GenericDrbdObject.name_check(
            name, DrbdSnapshot.NAME_MAXLEN,
            DrbdSnapshot.NAME_VALID_CHARS, DrbdSnapshot.NAME_VALID_INNER_CHARS
        )


    def get_name(self):
        return self._name


    def get_resource(self):
        return self._resource


    def add_snaps_assg(self, snaps_assg):
        assignment = snaps_assg.get_assignment()
        node       = assignment.get_node()
        self._assignments[node.get_name()] = snaps_assg
        self.get_props().new_serial()


    def init_add_snaps_assg(self, snaps_assg):
        assignment = snaps_assg.get_assignment()
        node       = assignment.get_node()
        self._assignments[node.get_name()] = snaps_assg


    def get_snaps_assg(self, nodename):
        return self._assignments.get(nodename)


    def iterate_snaps_assgs(self):
        return self._assignments.itervalues()


    def has_snaps_assgs(self):
        return (True if len(self._assignments) > 0 else False)


    def remove_snaps_assg(self, snaps_assg):
        assignment = snaps_assg.get_assignment()
        node = assignment.get_node()
        try:
            del self._assignments[node.get_name()]
            self.get_props().new_serial()
        except KeyError:
            pass


    # TODO: The special properties are not implemented here;
    #       there may not be a need to match those, too, cause
    #       the name of the referenced resource and the snapshot
    #       name would probably be primary arguments for any
    #       function searching for or listing snapshots
    def filter_match(self, filter_props):
        match = False
        if filter_props is None or len(filter_props) == 0:
            match = True
        else:
            match = self.properties_match(filter_props)
        return match


    def is_deployed(self):
        deployed = False
        for snaps_assg in self._assignments.itervalues():
            if snaps_assg.is_deployed():
                deployed = True
        return deployed


    def remove(self):
        removable = []
        for snaps_assg in self._assignments.itervalues():
            removable.append(snaps_assg)
        for snaps_assg in removable:
            snaps_assg.remove()
        self._resource.remove_snapshot(self)


    def get_properties(self, req_props):
        properties = {}

        selector = dmutils.Selector(req_props)
        if req_props is not None and len(req_props) > 0:
            selected = selector.list_selector
        else:
            selected = selector.all_selector

        if selected(consts.SNAPS_NAME):
            properties[consts.SNAPS_NAME] = self._name
        if selected(consts.RES_NAME):
            properties[consts.RES_NAME] = self._resource.get_name()

        # Add PropsContainer properties
        for (key, val) in self.get_props().iteritems():
            if selected(key):
                if val is not None:
                    properties[key] = str(val)

        return properties


class DrbdSnapshotAssignment(drbdcommon.GenericDrbdObject):

    _snapshot         = None
    _assignment       = None
    _snaps_vol_states = None
    _cstate           = 0
    _tstate           = 0

    # Signal for status change notifications
    _signal           = None

    FLAG_DEPLOY = 1

    TSTATE_MASK = FLAG_DEPLOY
    CSTATE_MASK = FLAG_DEPLOY


    def __init__(self, snapshot, assignment, cstate, tstate,
                 get_serial_fn, init_serial, init_props):
        super(DrbdSnapshotAssignment, self).__init__(
            get_serial_fn, init_serial, init_props
        )
        self._snapshot         = snapshot
        self._assignment       = assignment
        self._cstate           = cstate
        self._tstate           = tstate
        self._snaps_vol_states = {}


    def add_snaps_vol_state(self, snaps_vol_state):
        self._snaps_vol_states[snaps_vol_state.get_id()] = snaps_vol_state
        self.get_props().new_serial()


    def init_add_snaps_vol_state(self, snaps_vol_state):
        self._snaps_vol_states[snaps_vol_state.get_id()] = snaps_vol_state


    def get_snaps_vol_state(self, vol_id):
        return self._snaps_vol_states.get(vol_id)


    def iterate_snaps_vol_states(self):
        return self._snaps_vol_states.itervalues()


    def remove_snaps_vol_state(self, vol_id):
        try:
            del self._snaps_vol_states[vol_id]
            self.get_props().new_serial()
        except KeyError:
            pass


    def get_snapshot(self):
        return self._snapshot


    def get_assignment(self):
        return self._assignment


    def remove(self):
        self._assignment.remove_snaps_assg(self)
        self._snapshot.remove_snaps_assg(self)


    def is_deployed(self):
        return (self._cstate, self.FLAG_DEPLOY)


    def undeploy(self):
        for snaps_vol_state in self._snaps_vol_states.itervalues():
            snaps_vol_state.undeploy()
        if self._tstate != 0:
            self._tstate = 0
            self.get_props().new_serial()


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
            except exc.DrbdManageException as dm_exc:
                logging.warning("Cannot send change notification signal: %s"
                                % (str(dm_exc)))
            except Exception:
                logging.warning("Cannot send change notification signal, "
                                "unhandled exception encountered")


    def notify_removed(self):
        """
        Removes the assignment's signal

        This method should be called when the snapshot assignment is removed
        """
        if self._signal is not None:
            try:
                self._signal.destroy()
                self._signal = None
            except exc.DrbdManageException as dm_exc:
                logging.warning("Cannot send removal notification signal: %s"
                                % (str(dm_exc)))
            except Exception:
                logging.warning("Cannot send removal notification signal, "
                                "unhandled exception encountered")


    def set_cstate(self, cstate):
        if cstate != self._cstate:
            self._cstate = cstate & self.CSTATE_MASK
            self.get_props().new_serial()


    def set_tstate(self, tstate):
        if tstate != self._tstate:
            self._tstate = tstate & self.TSTATE_MASK
            self.get_props().new_serial()


    def requires_deploy(self):
        """
        Returns True if the snapshot needs to be deployed, False otherwise
        """
        return (dmutils.is_set(self._tstate, self.FLAG_DEPLOY) and
                dmutils.is_unset(self._cstate, self.FLAG_DEPLOY))


    def requires_undeploy(self):
        """
        Returns True if the assignment needs to be undeployed, False otherwise
        """
        return (dmutils.is_unset(self._tstate, self.FLAG_DEPLOY) and
                dmutils.is_set(self._cstate, self.FLAG_DEPLOY))


    def set_error_code(self, error_code):
        """
        Sets an error code to indicate that a snapshot action has failed.
        """
        self._props.set_prop(consts.ERROR_CODE, str(error_code))


    def get_error_code(self):
        """
        Retrieves the error code

        See set_error_code.
        """
        error_code = 0
        error_code_entry = self._props.get_prop(consts.ERROR_CODE)
        if error_code_entry is not None:
            try:
                error_code = int(error_code_entry)
            except ValueError:
                error_code = -1
        return error_code


    def get_cstate(self):
        return self._cstate


    def get_tstate(self):
        return self._tstate


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


    def get_properties(self, req_props):
        properties = {}

        selector = dmutils.Selector(req_props)
        if req_props is not None and len(req_props) > 0:
            selected = selector.list_selector
        else:
            selected = selector.all_selector

        if selected(consts.NODE_NAME):
            properties[consts.NODE_NAME] = (
                self._assignment.get_node().get_name()
            )
        if selected(consts.RES_NAME):
            properties[consts.RES_NAME]  = (
                self._assignment.get_resource().get_name()
            )
        if selected(consts.SNAPS_NAME):
            properties[consts.SNAPS_NAME]  = self._snapshot.get_name()

        # target state flags
        if selected(consts.TSTATE_PREFIX + consts.FLAG_DEPLOY):
            properties[consts.TSTATE_PREFIX + consts.FLAG_DEPLOY] = (
                dmutils.bool_to_string(
                    dmutils.is_set(self._tstate, self.FLAG_DEPLOY)
                )
            )

        # current state flags
        if selected(consts.CSTATE_PREFIX + consts.FLAG_DEPLOY):
            properties[consts.CSTATE_PREFIX + consts.FLAG_DEPLOY] = (
                dmutils.bool_to_string(
                    dmutils.is_set(self._cstate, self.FLAG_DEPLOY)
                )
            )

        # Add PropsContainer properties
        for (key, val) in self.get_props().iteritems():
            if selected(key):
                if val is not None:
                    properties[key] = str(val)

        return properties


class DrbdSnapshotVolumeState(drbdcommon.GenericDrbdObject,
                              storagecommon.GenericStorage):

    _vol_id  = None
    _bd_path = None
    _bd_name = None
    _cstate  = 0
    _tstate  = 0

    FLAG_DEPLOY = 1

    TSTATE_MASK = FLAG_DEPLOY
    CSTATE_MASK = FLAG_DEPLOY


    def __init__(self, vol_id, size_kiB, cstate, tstate,
                 bd_name, bd_path,
                 get_serial_fn, init_serial, init_props):
        super(DrbdSnapshotVolumeState , self).__init__(
            get_serial_fn, init_serial, init_props
        )
        if not size_kiB > 0:
            raise exc.VolSizeRangeException
        storagecommon.GenericStorage.__init__(
            self, size_kiB
        )
        self._vol_id = vol_id
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
            self._tstate = self.FLAG_DEPLOY


    def get_id(self):
        return self._vol_id


    def is_deployed(self):
        return dmutils.is_set(self._cstate, self.FLAG_DEPLOY)


    def undeploy(self):
        if self._tstate != 0:
            self._tstate = 0
            self.get_props().new_serial()


    def get_bd_name(self):
        return self._bd_name


    def get_bd_path(self):
        return self._bd_path


    def set_bd(self, bd_name, bd_path):
        if bd_name != self._bd_name or bd_path != self._bd_path:
            self._bd_name = bd_name
            self._bd_path = bd_path
            self.get_props().new_serial()


    def set_cstate(self, cstate):
        if cstate != self._cstate:
            self._cstate = cstate & self.CSTATE_MASK
            self.get_props().new_serial()


    def set_tstate(self, tstate):
        if tstate != self._tstate:
            self._tstate = tstate & self.TSTATE_MASK
            self.get_props().new_serial()


    def requires_deploy(self):
        return (dmutils.is_set(self._tstate, self.FLAG_DEPLOY) and
                dmutils.is_unset(self._cstate, self.FLAG_DEPLOY))


    def requires_undeploy(self):
        return (dmutils.is_unset(self._tstate, self.FLAG_DEPLOY) and
                dmutils.is_set(self._cstate, self.FLAG_DEPLOY))


    def get_cstate(self):
        return self._cstate


    def get_tstate(self):
        return self._tstate


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


    def get_properties(self, req_props):
        properties = {}

        selector = dmutils.Selector(req_props)
        if req_props is not None and len(req_props) > 0:
            selected = selector.list_selector
        else:
            selected = selector.all_selector

        if selected(consts.VOL_ID):
            properties[consts.VOL_ID] = str(self._vol_id)
        if selected(consts.VOL_BDEV):
            properties[consts.VOL_BDEV] = (
                "" if self._bd_path is None else str(self._bd_path)
            )

        # target state flags
        if selected(consts.TSTATE_PREFIX + consts.FLAG_DEPLOY):
            properties[consts.TSTATE_PREFIX + consts.FLAG_DEPLOY] = (
                dmutils.bool_to_string(
                    dmutils.is_set(self._tstate, self.FLAG_DEPLOY)
                )
            )

        # current state flags
        if selected(consts.CSTATE_PREFIX + consts.FLAG_DEPLOY):
            properties[consts.CSTATE_PREFIX + consts.FLAG_DEPLOY] = (
                dmutils.bool_to_string(
                    dmutils.is_set(self._cstate, self.FLAG_DEPLOY)
                )
            )

        # Add PropsContainer properties
        for (key, val) in self.get_props().iteritems():
            if selected(key):
                if val is not None:
                    properties[key] = str(val)

        return properties
