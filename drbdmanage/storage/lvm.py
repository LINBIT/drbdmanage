#!/usr/bin/python

from ..exceptions import *
from ..utils import DataHash
from ..conf.conffile import *
from persistence import BlockDevicePersistence
import storagecore
import json
import sys

__author__="raltnoeder"
__date__ ="$Sep 12, 2013 10:49:42 AM$"


class LVM(object):
    LVM_CONFFILE = "/opt/tmp/drbdmanaged-lvm.conf"
    LVM_SAVEFILE = "/opt/tmp/drbdmanaged-lvm.local.bin"
    _lvs  = None
    _conf = None
    
    _lv_prefix = "/dev/mapper/drbdpool-"
    _vg_name   = "drbdpool"
    
    
    def __init__(self):
        try:
            self._lvs = dict()
            try:
                self.load_state()
            except PersistenceException as p_exc:
                sys.stderr.write("Warning: Cannot load the LVM state file: %s\n"
                  % self.LVM_SAVEFILE)
            try:
                self._conf = self.load_conf()
            except IOError as io_err:
                sys.stderr.write("Warning: Cannot load the LVM configuration "
                  "file: %s\n" % self.LVM_CONFFILE)
            if self._conf is not None:
                val = self._conf.get(KEY_LV_PREFIX)
                if val is not None:
                    self._lv_prefix = val
                val = self._conf.get(KEY_VG_NAME)
                if val is not None:
                    self._vg_name = val
        except Exception as exc:
            print exc
    
    
    def create_blockdevice(self, name, size):
        bd = storagecore.BlockDevice(name, size,
          self._lv_prefix + name)
        self._lvs[name] = bd
        self.save_conf()
        return bd
    
    
    def remove_blockdevice(self, blockdevice):
        try:
            del self._lvs[blockdevice.get_name()]
        except KeyError:
            return DM_ENOENT
        self.save_conf()
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
    
    
    def load_conf(self):
        file = None
        conf = None
        try:
            file = open(self.LVM_CONFFILE, "r")
            conffile = ConfFile(file)
            conf = conffile.get_conf()
        except IOError as io_err:
            print io_err
        finally:
            if file is not None:
                file.close()
        return conf
    
    
    def load_state(self):
        file = None
        try:
            stored_hash = None
            file = open(self.LVM_SAVEFILE, "r")
            offset = 0
            line = file.readline()
            while len(line) > 0:
                if line.startswith("sig:"):
                    stored_hash = line[4:]
                    if stored_hash.endswith("\n"):
                        stored_hash = stored_hash[:len(stored_hash) - 1]
                    break
                else:
                    offset = file.tell()
                line = file.readline()
            file.seek(0)
            if offset != 0:
                load_data = file.read(offset)
            else:
                load_data = file.read()
            if stored_hash is not None:
                hash = DataHash()
                hash.update(load_data)
                computed_hash = hash.get_hex_hash()
                if computed_hash != stored_hash:
                    sys.stderr.write("Warning: configuration data does not "
                      "match its signature\n")
            lvm_con = json.loads(load_data)
            for properties in lvm_con.itervalues():
                bd = BlockDevicePersistence.load(properties)
                if bd is not None:
                    self._lvs[bd.get_name()] = bd
        except Exception as exc:
            raise PersistenceException
        finally:
            if file is not None:
                file.close()
    
    
    def save_state(self):
        lvm_con = dict()
        for bd in self._lvs.itervalues():
            bd_persist = BlockDevicePersistence(bd)
            bd_persist.save(lvm_con)
        file = None
        try:
            file = open(self.LVM_SAVEFILE, "w")
            hash = DataHash()
            save_data = json.dumps(lvm_con, indent=4, sort_keys=True) + "\n"
            hash.update(save_data)
            file.write(save_data)
            file.write("sig:" + hash.get_hex_hash() + "\n")
        except Exception as exc:
            raise PersistenceException
        finally:
            if file is not None:
                file.close()
    
    
    def reconfigure(self):
        pass
