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

import drbdmanage.drbd.views as drbdviews
import drbdmanage.consts as consts

from drbdmanage.exceptions import IncompatibleDataException


class DrbdSnapshotView(drbdviews.GenericView):
    _name     = None
    _resource = None

    _machine_readable = False


    def __init__(self, properties, machine_readable):
        try:
            super(DrbdSnapshotView, self).__init__(properties)
            self._name     = properties[consts.SNAPS_NAME]
            self._resource = properties[consts.RES_NAME]
        except KeyError:
            raise IncompatibleDataException
        self._machine_readable = machine_readable


    @classmethod
    def get_name_maxlen(cls):
        return consts.SNAPS_NAME_MAXLEN


class DrbdSnapshotAssignmentView(drbdviews.GenericView):
    _node         = None
    _resource     = None
    _snapshot     = None

    _machine_readable = False

    # Machine readable texts for current state flags
    MR_CSTATE_TEXTS = [
        [consts.CSTATE_PREFIX + consts.FLAG_DEPLOY,
         consts.FLAG_DEPLOY,     None,       None]
    ]

    # Machine readable texts for target state flags
    MR_TSTATE_TEXTS = [
        [consts.TSTATE_PREFIX + consts.FLAG_DEPLOY,
         consts.FLAG_DEPLOY,     None,       None]
    ]

    # Human readable texts for current state flags
    HR_CSTATE_TEXTS = [
        [consts.CSTATE_PREFIX + consts.FLAG_DEPLOY,
         "d",     "-",    "?"]
    ]

    # Human readable texts for target state flags (without action flags)
    HR_TSTATE_TEXTS = [
        [consts.TSTATE_PREFIX + consts.FLAG_DEPLOY,
         "d",     "-",    "?"]
    ]


    def __init__(self, properties, machine_readable):
        try:
            super(DrbdSnapshotAssignmentView, self).__init__(properties)
            self._node     = properties[consts.NODE_NAME]
            self._resource = properties[consts.RES_NAME]
            self._snapshot = properties[consts.SNAPS_NAME]
        except KeyError:
            raise IncompatibleDataException
        self._machine_readable = machine_readable


    def get_cstate(self):
        if self._machine_readable:
            text = self.state_text(self.MR_CSTATE_TEXTS, "|")
        else:
            text = self.state_text(self.HR_CSTATE_TEXTS, "")
        return text


    def get_tstate(self):
        if self._machine_readable:
            text = self.state_text(self.MR_TSTATE_TEXTS, "|")
        else:
            text = self.state_text(self.HR_TSTATE_TEXTS, "")
        return text


class DrbdSnapshotVolumeStateView(drbdviews.GenericView):
    _id      = None

    _machine_readable = False

    # Machine readable texts for current state flags
    MR_CSTATE_TEXTS = [
        [consts.CSTATE_PREFIX + consts.FLAG_DEPLOY,
         consts.FLAG_DEPLOY,     None,   None]
    ]

    # Human readable texts for current state flags
    HR_CSTATE_TEXTS = [
        [consts.CSTATE_PREFIX + consts.FLAG_DEPLOY,
         "d",     "-",    "?"]
    ]

    # Machine readable texts for target state flags
    MR_TSTATE_TEXTS = [
        [consts.TSTATE_PREFIX + consts.FLAG_DEPLOY,
         consts.FLAG_DEPLOY,     None,   None]
    ]

    # Human readable texts for target state flags
    HR_TSTATE_TEXTS = [
        [consts.TSTATE_PREFIX + consts.FLAG_DEPLOY,
         "d",     "-",    "?"]
    ]


    def __init__(self, properties, machine_readable):
        try:
            super(DrbdSnapshotVolumeStateView, self).__init__(properties)
            self._id = properties[consts.VOL_ID]
        except KeyError:
            raise IncompatibleDataException
        self._machine_readable = machine_readable


    def get_id(self):
        return self._id


    def get_cstate(self):
        if self._machine_readable:
            text = self.state_text(self.MR_CSTATE_TEXTS, "|")
        else:
            text = self.state_text(self.HR_CSTATE_TEXTS, "")
        return text


    def get_tstate(self):
        if self._machine_readable:
            text = self.state_text(self.MR_TSTATE_TEXTS, "|")
        else:
            text = self.state_text(self.HR_TSTATE_TEXTS, "")
        return text