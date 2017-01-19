#!/usr/bin/env python2
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2013 - 2017  LINBIT HA-Solutions GmbH
                               Author: R. Altnoeder, Roland Kammerer

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
from drbdmanage.exceptions import PersistenceException


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
    def load(cls, properties, resource, get_serial_fn):
        snapshot = None
        try:
            init_props = properties.get("props")
            snapshot = snaps.DrbdSnapshot(
                properties["_name"], resource,
                get_serial_fn, None, init_props
            )
        except Exception:
            raise PersistenceException
        return snapshot


class DrbdSnapshotAssignmentPersistence(GenericPersistence):

    """
    Serializes/deserializes DrbdSnapshotAssignment objects
    """

    SERIALIZABLE = [ "_cstate", "_tstate" ]


    def __init__(self, snaps_assignment):
        super(DrbdSnapshotAssignmentPersistence, self).__init__(
            snaps_assignment
        )


    def save(self, container):
        snaps_assignment = self.get_object()
        properties = self.load_dict(self.SERIALIZABLE)
        properties["props"] = snaps_assignment.get_props().get_all_props()

        # Save the DrbdSnapshotVolumeState objects
        vol_states = {}
        for snaps_vol_state in snaps_assignment.iterate_snaps_vol_states():
            p_state = DrbdSnapshotVolumeStatePersistence(snaps_vol_state)
            p_state.save(vol_states)
        properties["vol_states"] = vol_states

        snapshot   = snaps_assignment.get_snapshot()
        snaps_name = snapshot.get_name()
        properties["snapshot"] = snaps_name

        container[snaps_name] = properties


    @classmethod
    def load(cls, properties, assignment, get_serial_fn):
        snaps_assignment = None
        try:
            init_props = properties.get("props")
            snaps_name = properties["snapshot"]

            resource = assignment.get_resource()
            snapshot = resource.get_snapshot(snaps_name)

            snaps_assignment = snaps.DrbdSnapshotAssignment(
                snapshot, assignment,
                properties["_cstate"], properties["_tstate"],
                get_serial_fn, None, init_props
            )

            # Load the DrbdSnapshotVolumeState objects
            vol_states = properties["vol_states"]
            for vol_state in vol_states.itervalues():
                snaps_vol_state = DrbdSnapshotVolumeStatePersistence.load(
                    vol_state, get_serial_fn
                )
                snaps_assignment.init_add_snaps_vol_state(snaps_vol_state)
        except Exception:
            raise PersistenceException
        return snaps_assignment


class DrbdSnapshotVolumeStatePersistence(GenericPersistence):

    """
    Serializes/deserializes DrbdSnapshotVolumeState objects
    """

    SERIALIZABLE = ["_vol_id", "_size_kiB", "_bd_path", "_bd_name",
                    "_cstate", "_tstate"]


    def __init__(self, snaps_vol_state):
        super(DrbdSnapshotVolumeStatePersistence, self).__init__(
            snaps_vol_state
        )


    def save(self, container):
        snaps_vol_state = self.get_object()
        properties = self.load_dict(self.SERIALIZABLE)
        properties["props"] = snaps_vol_state.get_props().get_all_props()
        container[snaps_vol_state.get_id()] = properties


    @classmethod
    def load(cls, properties, get_serial_fn):
        init_props = properties.get("props")
        snaps_vol_state = snaps.DrbdSnapshotVolumeState(
            properties["_vol_id"], long(properties["_size_kiB"]),
            properties["_cstate"], properties["_tstate"],
            properties.get("_bd_name"), properties.get("_bd_path"),
            get_serial_fn, None, init_props
        )
        return snaps_vol_state
