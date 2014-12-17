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

import logging
import drbdmanage.consts as consts
import drbdmanage.exceptions as exc
import drbdmanage.drbd.drbdcommon as drbdcommon
import drbdmanage.storage.storagecommon as storagecommon


class DrbdSnapshot(drbdcommon.GenericDrbdObject):

    NAME_MAXLEN  = consts.SNAPS_NAME_MAXLEN
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
            name, DrbdSnapshot.NAME_MAXLEN
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

    # TODO: move to parent class?
    def filter_match(self, filter_props):
        match = False
        if filter_props is None or len(filter_props) == 0:
            match = True
        else:
            match = self.properties_match(filter_props)
        return match

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


class DrbdSnapshotAssignment(drbdcommon.GenericDrbdObject):

    _snapshot         = None
    _assignment       = None
    _snaps_vol_states = None
    _cstate           = 0
    _tstate           = 0

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
        return (self._cstate & self.FLAG_DEPLOY) != 0


    def undeploy(self):
        for snaps_vol_state in self._snaps_vol_states.itervalues():
            snaps_vol_state.undeploy()
        if self._tstate != 0:
            self._tstate = 0
            self.get_props().new_serial()


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


class DrbdSnapshotVolumeState(drbdcommon.GenericDrbdObject,
                              storagecommon.GenericStorage):

    _vol_id      = None
    _bd_path     = None
    _blockdevice = None
    _cstate      = 0
    _tstate      = 0

    FLAG_DEPLOY = 1

    TSTATE_MASK = FLAG_DEPLOY
    CSTATE_MASK = FLAG_DEPLOY


    def __init__(self, vol_id, size_kiB, cstate, tstate,
                 blockdevice, bd_path,
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
        if blockdevice is not None and bd_path is not None:
            self._blockdevice = blockdevice
            self._bd_path     = bd_path

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
        return (self._cstate & self.FLAG_DEPLOY) != 0


    def undeploy(self):
        if self._tstate != 0:
            self._tstate = 0
            self.get_props().new_serial()


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
