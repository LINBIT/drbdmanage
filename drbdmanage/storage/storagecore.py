#!/usr/bin/python

__author__="raltnoeder"
__date__ ="$Sep 12, 2013 10:43:13 AM$"

import drbdmanage.storage.lvm
import drbdmanage.utils
from drbdmanage.exceptions import *


class GenericStorage(object):
    _size_MiB = None
    
    
    def __init__(self, size_MiB):
        self._size_MiB = long(size_MiB)
    
    
    def get_size_MiB(self):
        return self._size_MiB


    def get_size(self, unit):
        return drbdmanage.utils.SizeCalc.convert(self._size_MiB,
          drbdmanage.utils.SizeCalc.UNIT_MiB, unit)


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
        self._name     = self.name_check(name)
    
    
    def name_check(self, name):
        DrbdManager = drbdmanage.drbd.drbdcore.DrbdManager
        return DrbdManager.name_check(name, self.NAME_MAXLEN)
    
    
    def get_name(self):
        return self._name
    
    
    def get_path(self):
        return self._path


class BlockDeviceManager(object):
    _plugin = None
    
    
    def __init__(self):
        self._plugin = self._plugin_import("drbdmanage.storage.lvm.LVM")
    
    
    def create_blockdevice(self, name, size):
        return self._plugin.create_blockdevice(name, size)
    
    
    def remove_blockdevice(self, name):
        bd = self._plugin.get_blockdevice(name)
        if bd is not None:
            return self._plugin.remove_blockdevice(bd)
        return DM_ENOENT
    
    
    def up_blockdevice(self, name):
        bd = self._plugin.get_blockdevice(name)
        if bd is not None:
            return self._plugin.up_blockdevice(bd)
        return DM_ENOENT
    
    
    def down_blockdevice(self, name):
        bd = self._plugin.get_blockdevice(name)
        if bd is not None:
            return self._plugin.down_blockdevice(bd)
        return DM_ENOENT
    
    
    def reconfigure(self):
        return self._plugin.reconfigure()
    
    
    def _plugin_import(self, path):
        p_mod   = None
        p_class = None
        p_inst  = None
        try:
            idx = path.rfind(".")
            if idx != -1:
                p_name = path[idx + 1:]
                p_path = path[:idx]
            else:
                p_name = path
                p_path = ""
            p_mod   = __import__(p_path, globals(), locals(), [p_name], -1)
            p_class = getattr(p_mod, p_name)
            p_inst  = p_class()
        except Exception as exc:
            print exc
        return p_inst


class MinorNr(object):
    """
    Contains the minor number of a unix device file
    """
    _minor = None
    MINOR_MAX = 0xfffff
    
    MINOR_AUTO     = -1
    MINOR_AUTODRBD = -2
    
    
    def __init__(self, nr):
        self._minor = MinorNr.minor_check(nr)
    
    
    def get_value(self):
        return self._minor
    
    
    @classmethod
    def minor_check(cls, nr):
        if nr != cls.MINOR_AUTO and nr != cls.MINOR_AUTODRBD:
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
