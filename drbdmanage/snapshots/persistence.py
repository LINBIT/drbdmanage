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

import drbdmanage.snapshots.snapshots as snaps

from drbdmanage.persistence import GenericPersistence


class DrbdSnapshotPersistence(GenericPersistence):

    """
    Serializes/deserializes DrbdSnapshot objects
    """

    SERIALIZABLE = [ "_name" ]


    def __init__(self, snapshot):
        super(DrbdSnapshotPersistence, self).__init__(snapshot)


    def save(self, container):
        snapshot = self.get_object()
        properties = self.load_dict(self.SERIALIZABLE)
        properties["props"] = snapshot.get_props().get_all_props()
        container[snapshot.get_name()] = properties


    @classmethod
    def load(cls, properties, get_serial_fn):
        snapshot = None
        try:
            init_props = properties.get("props")
            snapshot = snaps.DrbdSnapshot(
                properties["_name"], get_serial_fn, None, init_props)
        except Exception as exc:
            raise exc
        return snapshot