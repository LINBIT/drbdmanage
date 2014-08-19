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
import drbdmanage.drbd.drbdcommon as drbdcommon

class DrbdSnapshot(drbdcommon.GenericDrbdObject):

    NAME_MAXLEN  = consts.SNAPS_NAME_MAXLEN
    _name        = None
    _assignments = None


    def __init__(self, name):
        super(DrbdSnapshot, self).__init__()
        self._name        = self.name_check(name)
        self._assignments = {}


    def name_check(self, name):
        return drbdcommon.GenericDrbdObject.name_check(
            name, DrbdSnapshot.NAME_MAXLEN)


    def get_name(self):
        return self._name


class DrbdSnapshotAssignment(drbdcommon.GenericDrbdObject):

    _snapshot         = None
    _snaps_vol_states = None
    _cstate           = 0
    _tstate           = 0

    TSTATE_MASK = 0
    CSTATE_MASK = 0


    def __init__(self, snapshot):
        super(DrbdSnapshotAssignment, self).__init__()
        self._snapshot         = snapshot
        self._snaps_vol_states = {}


    def add_snaps_vol_state(self, snaps_vol_state):
        self._snaps_vol_states[snaps_vol_state.get_id()] = snaps_vol_state
        self.get_props().new_serial()


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


class DrbdSnapshotVolumeState(drbdcommon.GenericDrbdObject):

    _vol_id      = None
    _bd_path     = None
    _blockdevice = None
    _cstate      = 0
    _tstate      = 0

    TSTATE_MASK  = 0
    CSTATE_MASK  = 0


    def __init__(self, vol_id):
        super(DrbdSnapshotVolumeState , self).__init__()
        self._vol_id = vol_id


    def get_id(self):
        return self._vol_id


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