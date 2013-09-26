#!/usr/bin/python

from drbdmanage.storage.storagecore import *
from drbdmanage.exceptions import *

__author__="raltnoeder"
__date__ ="$Sep 12, 2013 10:49:42 AM$"


class LVM(object):
    _lvs = None
    
    def __init__(self):
        self._lvs = dict()
    
    def create_blockdevice(self, name, size):
        bd = BlockDevice(name, size, "/dev/mapper/drbdpool-" + name)
        self._lvs[name] = bd
        return bd
    
    def remove_blockdevice(self, blockdevice):
        try:
            del self._lvs[blockdevice.get_name()]
        except KeyError:
            return DM_ENOENT
        return DM_SUCCESS
    
    def get_blockdevice(self, name):
        bd = None
        try:
            bd = self._lvs[name]
        except KeyError:
            pass
        return bd
    
    def up_blockdevice(self, blockdevice):
        return DM_SUCCESS
    
    def down_blockdevice(self, blockdevice):
        return DM_SUCCESS
    
    def reconfigure(self):
        pass
