#!/usr/bin/python

from drbdmanage.persistence import GenericPersistence
import drbdmanage.storage.storagecore

__author__ = "raltnoeder"
__date__   = "$Sep 30, 2013 12:19:56 PM$"


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
