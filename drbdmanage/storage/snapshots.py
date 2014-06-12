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
