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

from drbdmanage.persistence import GenericPersistence
import drbdmanage.storage.storagecore


class BlockDevicePersistence(GenericPersistence):

    """
    Serializes/deserializes BlockDevice objects

    This class is for use by storage plugins.
    """

    SERIALIZABLE = [ "_name", "_path" ]
    def __init__(self, blockdev):
        super(BlockDevicePersistence, self).__init__(blockdev)

    def save(self, container):
        blockdev = self.get_object()
        properties = self.load_dict(self.SERIALIZABLE)
        properties["size_kiB"] = blockdev.get_size_kiB()
        container[blockdev.get_name()] = properties

    @classmethod
    def load(cls, properties):
        blockdev = None
        try:
            blockdev = drbdmanage.storage.storagecore.BlockDevice(
              properties["_name"],
              properties["size_kiB"],
              properties["_path"]
              )
        except Exception:
            pass
        return blockdev
