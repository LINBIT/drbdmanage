#!/usr/bin/python

__author__="raltnoeder"
__date__ ="$Sep 12, 2013 10:43:13 AM$"

import drbdmanage.storage.lvm

class GenericStorage(object):
    _size_MiB = None
    
    _base_2  = 0x0200
    _base_10 = 0x0A00
    
    UNIT_B   =  0 | _base_2
    UNIT_KiB = 10 | _base_2
    UNIT_MiB = 20 | _base_2
    UNIT_GiB = 30 | _base_2
    UNIT_TiB = 40 | _base_2
    UNIT_PiB = 50 | _base_2
    UNIT_EiB = 60 | _base_2
    UNIT_ZiB = 70 | _base_2
    UNIT_YiB = 80 | _base_2
    
    UNIT_KB =   3 | _base_10
    UNIT_MB =   6 | _base_10
    UNIT_GB =   9 | _base_10
    UNIT_TB =  12 | _base_10
    UNIT_PB =  15 | _base_10
    UNIT_EB =  18 | _base_10
    UNIT_ZB =  21 | _base_10
    UNIT_YB =  24 | _base_10
    
    def __init__(self, size_MiB):
        self._size_MiB = int(size_MiB)
    
    def get_size_MiB(self):
        return self._size_MiB

    def get_size(self, unit):
        pow  = unit & 0xff
        base = (unit & 0xffffff00) >> 8
        fac  = (base ** pow)
        
        size_b    = self._size_MiB * 0x100000
        size_unit = size_b / fac
        
        return size_unit


class BlockDevice(GenericStorage):
    """
    Represents a block device
    """
    NAME_MAXLEN = 16
    
    _path     = None
    _name     = None
    
    def __init__(self, name, size_MiB, path):
        super(BlockDevice, self).__init__(size_MiB)
        self._path     = path
        self._name     = self.name_check(name, self.NAME_MAXLEN)
    
    def __init__(self, path, size_MiB):
        self._path     = path
        self._name     = None
        self._size_MiB = size_MiB
    
    def get_name(self):
        return self._name
    
    def get_path(self):
        return self._path


class BlockDeviceManager(object):
    _plugin = None
    
    def __init__(self):
        self._plugin = drbdmanage.storage.lvm.LVM()
    
    def create_blockdevice(self, name, size):
        return self._plugin.create_blockdevice(name, size)
    
    def remove_blockdevice(self, name):
        # TODO: check whether bd exists
        bd = self._plugin.get_blockdevice(name)
        return self._plugin.remove_blockdevice(bd)
    
    def up_blockdevice(self, name):
        # TODO: check whether bd exists
        bd = self._plugin.get_blockdevice(name)
        return self._plugin.up_blockdevice(bd)
    
    def down_blockdevice(self, name):
        # TODO: check whether bd exists
        bd = self._plugin.get_blockdevice(name)
        return self._plugin.down_blockdevice(bd)
    
    def reconfigure(self):
        self._plugin.reconfigure()


class MinorNr(object):
    """
    Contains the minor number of a unix device file
    """
    _minor = None
    MINOR_MAX = 0xfffff
    
    def __init__(self, nr):
        self._minor = MinorNr.minor_check(nr)
    
    def get_value(self):
        return self._minor
    
    @classmethod
    def minor_check(cls, nr):
        if nr < 0 or nr > cls.MINOR_MAX:
            raise InvalidMinorNrException
        return nr


class MajorNr(object):
    """
    Contains the major number of a unix device file
    """
    _major = None
    MAJOR_MAX = 0xfff
    
    def __init__(self, nr):
        self._major = MajorNr.major_check(nr)
    
    def get_value(self):
        return self._major
    
    @classmethod
    def major_check(cls, nr):
        if nr < 0 or nr > cls.MAJOR_MAX:
            raise InvalidMajorNrException
        return nr


class DevNr(object):
    """
    Contains the major/minor numbers of unix device files
    """
    _minor = None
    _major = None
    
    def __init__self(self, major, minor):
        self._minor = minor
        self._major = major

    def get_minor(self):
        self._minor.get_value()
    
    def get_major(self):
        self._major.get_value()


class StoragePlugin(object):
    def __init__(self):
        pass
    
    def get_blockdevice(self, name):
        raise NotImplementedError
    
    def create_blockdevice(self, name, size):
        raise NotImplementedError
    
    def remove_blockdevice(self, blockdevice):
        raise NotImplementedError
    
    def up_blockdevice(self, blockdevice):
        raise NotImplementedError
    
    def down_blockdevice(self, blockdevice):
        raise NotImplementedError
    
    def reconfigure(self):
        raise NotImplementedError
