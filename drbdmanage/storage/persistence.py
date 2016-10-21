#!/usr/bin/env python2
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2013 - 2016  LINBIT HA-Solutions GmbH
                               Author: R. Altnoeder

    For further information see the COPYING file.
"""

import drbdmanage.storage.storagecore

from drbdmanage.persistence import GenericPersistence


class BlockDevicePersistence(GenericPersistence):

    """
    Serializes/deserializes BlockDevice objects

    This class is for use by storage plugins.
    """

    SERIALIZABLE = ["_name", "_path"]


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
