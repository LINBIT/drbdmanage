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
        """
        Returns the size of the volume in binary megabytes
        
        This is the size of the volume in units of (2 to the power of 20) bytes
        (bytes = size * 1048576).
        
        @return: volume size in MeBiByte (2 ** 20 bytes, binary megabytes)
        @rtype:  long
        """
        return self._size_MiB


    def get_size(self, unit):
        """
        Returns the size of the volume converted to the selected scale unit
        
        See the functions of the SizeCalc class in drbdmanage.utils for
        the unit selector constants.
        
        @return: volume size in the selected scale unit
        @rtype:  long
        """
        return drbdmanage.utils.SizeCalc.convert(self._size_MiB,
          drbdmanage.utils.SizeCalc.UNIT_MiB, unit)


class BlockDevice(GenericStorage):
    """
    Represents a block device
    """
    NAME_MAXLEN = 20
    
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
    
    
    def __init__(self, plugin_name):
        self._plugin = self._plugin_import(plugin_name)
        if self._plugin is None:
            sys.stderr.write("DEBUG: BlockDeviceManager(): Cannot import the "
              "storage management plugin (%s)\n" % plugin_name)
    
    
    def create_blockdevice(self, name, id, size):
        return self._plugin.create_blockdevice(name, id, size)
    
    
    def remove_blockdevice(self, name, id):
        bd = self._plugin.get_blockdevice(name, id)
        if bd is not None:
            return self._plugin.remove_blockdevice(bd)
        return DM_ENOENT
    
    
    def up_blockdevice(self, name, id):
        bd = self._plugin.get_blockdevice(name, id)
        if bd is not None:
            return self._plugin.up_blockdevice(bd)
        return DM_ENOENT
    
    
    def down_blockdevice(self, name, id):
        bd = self._plugin.get_blockdevice(name, id)
        if bd is not None:
            return self._plugin.down_blockdevice(bd)
        return DM_ENOENT
    
    
    def update_pool(self, drbdnode):
        return self._plugin.update_pool(drbdnode)
    
    
    def reconfigure(self):
        return self._plugin.reconfigure()
    
    
    def _plugin_import(self, path):
        p_mod   = None
        p_class = None
        p_inst  = None
        try:
            if path is not None:
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
    MINOR_NR_MAX = 0xfffff
    
    MINOR_NR_AUTO     = -1
    # FIXME: MINOR_NR_AUTODRBD will probably never be useful for anything.
    #        Reserved for automatic minor number allocation by the kernel
    #        module; should possibly be removed.
    MINOR_NR_AUTODRBD = -2
    MINOR_NR_ERROR    = -3
    
    def __init__(self, nr):
        self._minor = MinorNr.minor_check(nr)
    
    
    def get_value(self):
        return self._minor
    
    
    @classmethod
    def minor_check(cls, nr):
        if nr != cls.MINOR_NR_AUTO and nr != cls.MINOR_NR_AUTODRBD:
            if nr < 0 or nr > cls.MINOR_NR_MAX:
                raise InvalidMinorNrException
        return nr


class MajorNr(object):
    """
    Contains the major number of a unix device file
    """
    _major = None
    MAJOR_NR_MAX = 0xfff
    
    
    def __init__(self, nr):
        self._major = MajorNr.major_check(nr)
    
    
    def get_value(self):
        return self._major
    
    
    @classmethod
    def major_check(cls, nr):
        if nr < 0 or nr > cls.MAJOR_NR_MAX:
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
    
    """
    Interface for storage plugins
    
    Storage plugins are loaded dynamically at runtime. The block device manager
    expects storage plugins to implement the functions in this interface.
    Storage plugins should be subclasses of this interface, although
    technically, this is not strictly required, because Python does not care
    about the class hierarchy of objects as long as it finds all the
    functions it looks for.
    """
    
    def __init__(self):
        """
        Initializes the storage plugin
        """
        pass
    
    
    def get_blockdevice(self, name, id):
        """
        Retrieves a registered BlockDevice object
        
        The BlockDevice object allocated and registered under the supplied
        resource name and volume id is returned.
        
        @return: the specified block device; None on error
        @rtype:  BlockDevice object
        """
        raise NotImplementedError
    
    
    def create_blockdevice(self, name, id, size):
        """
        Allocates a block device as backing storage for a DRBD volume
        
        @param   name: resource name; subject to name constraints
        @type    name: str
        @param   id: volume id
        @type    id: int
        @param   size: size of the block device in MiB (binary megabytes)
        @type    size: long
        @return: block device of the specified size
        @rtype:  BlockDevice object; None if the allocation fails
        """
        raise NotImplementedError
    
    
    def remove_blockdevice(self, blockdevice):
        """
        Deallocates a block device
        
        @param   blockdevice: the block device to deallocate
        @type    blockdevice: BlockDevice object
        @return: standard return code (see drbdmanage.exceptions)
        """
        raise NotImplementedError
    
    
    def up_blockdevice(self, blockdevice):
        """
        Activates a block device (e.g., connects an iSCSI resource)
        
        @param blockdevice: the block device to deactivate
        @type  blockdevice: BlockDevice object
        """
        raise NotImplementedError
    
    
    def down_blockdevice(self, blockdevice):
        """
        Deactivates a block device (e.g., disconnects an iSCSI resource)
        
        @param blockdevice: the block device to deactivate
        @type  blockdevice: BlockDevice object
        """
        raise NotImplementedError
    
    
    def update_pool(self, drbdnode):
        """
        Updates the DrbdNode object with the current storage status
        
        Determines the current total and free space that is available for
        allocation on the host this instance of the drbdmanage server is
        running on and updates the DrbdNode object with that information.
        
        @param   node: The node to update
        @type    node: DrbdNode object
        @return: standard return code (see drbdmanage.exceptions)
        """
        raise NotImplementedError
    
    
    def reconfigure(self):
        """
        Reconfigures the storage plugin
        """
        raise NotImplementedError
