#!/usr/bin/env python2
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

import drbdmanage.drbd.persistence as persistence
import drbdmanage.storage.storagecommon as storagecommon
import drbdmanage.storage.storagecore
import drbdmanage.drbd.drbdcommon as drbdcommon
import drbdmanage.utils as utils
import drbdmanage.storage.persistence as storpers
import drbdmanage.exceptions as dmexc
import drbdmanage.consts as consts
import drbdmanage.conf.conffile as dmconffile
import os
import errno
import logging
import json
import subprocess
import time

from drbdmanage.storage.storagecore import StoragePlugin


class LVMThinPool(drbdmanage.storage.storagecore.StoragePlugin):

    """
    LVM thinpool-LV backing store plugin for the drbdmanage server

    Provides backing store block devices for DRBD volumes by managing the
    allocation of logical volumes backed by a Thin Pool inside a volume group
    managed by the Logical Volume Manager (LVM).
    """

    LVM_THIN_SAVEFILE = (
        "/var/lib/drbdmanage/drbdmanaged-lvm-thinpool.local.json"
    )
    PLUGIN_CONFFILE = "/etc/drbdmanaged-lvm-thinpool.conf"


    # Configuration keys
    KEY_DEV_PATH   = "dev-path"
    KEY_VG_NAME    = "volume-group"
    KEY_LVM_PATH   = "lvm-path"
    KEY_POOL_RATIO = "pool-ratio"

    DEFAULT_POOL_RATIO = 135

    # Plugin configuration defaults
    CONF_DEFAULTS = {
        KEY_DEV_PATH  : "/dev",
        KEY_VG_NAME   : consts.DEFAULT_VG,
        KEY_LVM_PATH  : "/sbin",
        KEY_POOL_RATIO: str(DEFAULT_POOL_RATIO)
    }

    # LVM executable names
    LVCREATE = "lvcreate"
    LVREMOVE = "lvremove"
    LVCHANGE = "lvchange"
    VGCHANGE = "vgchange"
    VGS      = "vgs"

    # Plugin configuration
    _conf    = CONF_DEFAULTS

    # Map of volumes allocated by the plugin
    _volumes = None

    # Map of pools allocated by the plugin
    _pools   = None

    # Lookup table for finding the pool that contains a volume
    _pool_lookup = None


    def __init__(self):
        super(LVMThinPool, self).__init__()
        self._volumes = {}
        self._pools   = {}

        self._pool_lookup = {}

        self.reconfigure()


    def get_blockdevice(self, bd_name):
        """
        Retrieves a registered BlockDevice object

        The BlockDevice object allocated and registered under the supplied
        resource name and volume id is returned.

        @return: the specified block device; None on error
        @rtype:  BlockDevice object
        """
        blockdev = self._volumes.get(bd_name)
        return blockdev


    def create_blockdevice(self, name, vol_id, size):
        """
        Allocates a block device as backing storage for a DRBD volume

        @param   name: resource name; subject to name constraints
        @type    name: str
        @param   vol_id: volume id
        @type    vol_id: int
        @param   size: size of the block device in kiB (binary kilobytes)
        @type    size: long
        @return: block device of the specified size
        @rtype:  BlockDevice object; None if the allocation fails
        """
        blockdev = None
        lv_name = self._volume_name(name, vol_id)
        try:
            pool_name = ThinPool.generate_pool_name(name, vol_id)
            created = self._create_lv(lv_name, pool_name, size)
            if created:
                blockdev = drbdmanage.storage.storagecore.BlockDevice(
                    lv_name,
                    size,
                    utils.build_path(
                        self._conf[self.KEY_DEV_PATH],
                        self._conf[self.KEY_VG_NAME]
                    ) + "/" + lv_name
                )
                pool = ThinPool(pool_name, size)
                pool.add_volume(lv_name)
                self._volumes[lv_name] = blockdev
                self._pools[pool_name] = pool
                self._pool_lookup[lv_name] = pool_name
                self.up_blockdevice(blockdev)
                self.save_state()
        except Exception as exc:
            logging.error(
                "LVM plugin: Block device creation failed, "
                "unhandled exception: %s"
                % str(exc)
            )
        return blockdev


    def create_snapshot(self, name, vol_id, source_blockdev):
        """
        Allocates a block device as a snapshot of an existing block device

        @param   name: snapshot name; subject to name constraints
        @type    name: str
        @param   vol_id: volume id
        @type    vol_id: int
        @param   source_blockdev: the existing block device to snapshot
        @type    source_blockdev: BlockDevice object
        @return: block device of the specified size
        @rtype:  BlockDevice object; None if the allocation fails
        """
        blockdev  = None
        lv_name   = source_blockdev.get_name()
        pool_name = None
        pool      = None
        try:
            pool_name = self._pool_lookup[lv_name]
            pool      = self._pools[pool_name]
        except KeyError:
            logging.error(
                "LVM plugin: Snapshot block device creation failed, "
                "cannot find the associated thinpool"
            )
        try:
            if pool is not None:
                snaps_base_name = self._volume_name(name, vol_id)
                snaps_suffix    = pool.extract_pool_name_suffix()
                snaps_name      = snaps_base_name + snaps_suffix

                # Attempt to create the snapshot
                created = self._create_snapshot_lv(snaps_name, lv_name)
                if created:
                    size = source_blockdev.get_size_kiB()
                    blockdev = drbdmanage.storage.storagecore.BlockDevice(
                        snaps_name,
                        size,
                        utils.build_path(
                            self._conf[self.KEY_DEV_PATH],
                            self._conf[self.KEY_VG_NAME]
                        ) + "/" + snaps_name
                    )
                    pool.add_volume(snaps_name)
                    self._volumes[snaps_name] = blockdev
                    self._pool_lookup[snaps_name] = pool_name
                    self.up_blockdevice(blockdev)
                    self.save_state()
        except Exception as exc:
            logging.error(
                "LVM plugin: Block device creation failed, "
                "unhandled exception: %s"
                % str(exc)
            )
        return blockdev


    def restore_snapshot(self, name, vol_id, source_blockdev):
        """
        Restore a snapshot; currently an alias for create_snapshot()
        """
        return self.create_snapshot(name, vol_id, source_blockdev)


    def remove_snapshot(self, blockdevice):
        """
        Deallocates a snapshot block device

        @param   blockdevice: the block device to deallocate
        @type    blockdevice: BlockDevice object
        @return: standard return code (see drbdmanage.exceptions)
        """
        return self.remove_blockdevice(blockdevice)


    def remove_blockdevice(self, blockdevice):
        """
        Deallocates a block device

        @param   blockdevice: the block device to deallocate
        @type    blockdevice: BlockDevice object
        @return: standard return code (see drbdmanage.exceptions)
        """
        try:
            fn_rc     = dmexc.DM_ESTORAGE
            lv_name   = blockdevice.get_name()
            pool_name = None
            pool      = None
            try:
                pool_name = self._pool_lookup[lv_name]
                pool      = self._pools[pool_name]
            except KeyError:
                pass
            lv_removed = self._remove_lv(lv_name)
            if lv_removed:
                del self._volumes[lv_name]
                del self._pool_lookup[lv_name]
                pool.remove_volume(lv_name)
                if pool is not None and pool.is_empty():
                    pool_removed = self._remove_pool(pool.get_name())
                    if pool_removed:
                        del self._pools[pool.get_name()]
            self.save_state()
            if lv_removed:
                fn_rc = dmexc.DM_SUCCESS
        except Exception as exc:
            logging.error(
                "LVM plugin: Block device removal failed, "
                "unhandled exception: %s"
                % str(exc)
            )
        return fn_rc


    def up_blockdevice(self, blockdevice):
        """
        Activates a block device (e.g., connects an iSCSI resource)

        @param blockdevice: the block device to deactivate
        @type  blockdevice: BlockDevice object
        """
        fn_rc = dmexc.DM_ESTORAGE
        # lvchange -ay -kn -K drbdpool/<name>

        # Prepare the path for LVM's 'lvchange' utility
        lvchange = utils.build_path(
            self._conf[self.KEY_LVM_PATH],
            self.LVCHANGE
        )
        vgchange = utils.build_path(
            self._conf[self.KEY_LVM_PATH],
            self.VGCHANGE
        )

        lv_name = blockdevice.get_name()

        pool_name = None
        try:
            pool_name = self._pool_lookup[lv_name]
        except KeyError:
            pass

        lvm_proc = None
        chg_vg_rc   = -1
        chg_pool_rc = -1
        chg_lv_rc   = -1
        try:
            logging.debug(
                "LVMThinPool: attempting to auto-activating all "
                "volumes in VG %s"
                % (self._conf[self.KEY_VG_NAME])
            )
            lvm_proc = subprocess.Popen(
                [
                    vgchange,
                    "-ay", self._conf[self.KEY_VG_NAME]
                ],
                0, vgchange,
                env=self._subproc_env(),
                close_fds=True
            )
            chg_vg_rc = lvm_proc.wait()

            if pool_name is not None:
                # Modify pool state
                logging.debug(
                    "LVMThinPool: exec: %s %s %s/%s"
                    % (lvchange, "-ay -kn -K", self._conf[self.KEY_VG_NAME],
                       pool_name)
                )
                lvm_proc = subprocess.Popen(
                    [
                        lvchange,
                        "-ay", "-kn", "-K", self._conf[self.KEY_VG_NAME] +
                        "/" + pool_name
                    ],
                    0, lvchange,
                    env=self._subproc_env(),
                    close_fds=True
                )
                chg_pool_rc = lvm_proc.wait()
            else:
                logging.debug(
                    "LVMThinPool: cannot find thin pool for volume '%s'"
                    % (lv_name)
                )

            # Modify volume state
            logging.debug(
                "LVMThinPool: exec: %s %s %s/%s"
                % (lvchange, "-ay -kn -K", self._conf[self.KEY_VG_NAME],
                   lv_name)
            )
            lvm_proc = subprocess.Popen(
                [
                    lvchange,
                    "-ay", "-kn", "-K", self._conf[self.KEY_VG_NAME] + "/" +
                    lv_name
                ],
                0, lvchange,
                env=self._subproc_env(),
                close_fds=True
            )
            chg_lv_rc = lvm_proc.wait()

            if chg_vg_rc == 0 and chg_pool_rc == 0 and chg_lv_rc == 0:
                fn_rc = dmexc.DM_SUCCESS
        finally:
            if lvm_proc is not None:
                try:
                    lvm_proc.stdout.close()
                except Exception:
                    pass
                lvm_proc.wait()
        return fn_rc


    def down_blockdevice(self, blockdevice):
        """
        Deactivates a block device (e.g., disconnects an iSCSI resource)

        @param blockdevice: the block device to deactivate
        @type  blockdevice: BlockDevice object
        """
        return dmexc.DM_SUCCESS


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
        fn_rc    = dmexc.DM_ESTORAGE
        poolsize = -1
        poolfree = -1

        # Prepare the path to LVM's 'lvremove' utility
        vgs = utils.build_path(
            self._conf[self.KEY_LVM_PATH],
            self.VGS
        )

        lvm_proc = None
        try:
            lvm_proc = subprocess.Popen(
                [
                    vgs, "--noheadings", "--nosuffix", "--units", "k",
                    "--separator", ",", "--options", "vg_size,vg_free",
                    self._conf[self.KEY_VG_NAME],
                ],
                env=self._subproc_env(),
                stdout=subprocess.PIPE, close_fds=True
            )
            pool_str = lvm_proc.stdout.readline()
            if pool_str is not None:
                pool_str = pool_str.strip()
                size_str, free_str = pool_str.split(",")
                idx = size_str.find(".")
                if idx != -1:
                    size_str = size_str[:idx]
                idx = free_str.find(".")
                if idx != -1:
                    free_str = free_str[:idx]
                try:
                    poolsize = long(size_str)
                    poolfree = long(free_str)
                    fn_rc = dmexc.DM_SUCCESS
                except ValueError:
                    poolsize = -1
                    poolfree = -1
        finally:
            if lvm_proc is not None:
                try:
                    lvm_proc.stdout.close()
                except Exception:
                    pass
                lvm_proc.wait()
        return (fn_rc, poolsize, poolfree)


    def reconfigure(self):
        """
        Reconfigures the storage plugin
        """
        try:
            # Load volumes / pools state
            try:
                self.load_state()
            except dmexc.PersistenceException:
                logging.warning(
                    "LVM plugin: Cannot load state file '%s'"
                    % (self.LVM_THIN_SAVEFILE)
                )

            # Load the plugin configuration
            conf_loaded = None
            try:
                conf_loaded = self.load_conf()
            except IOError:
                logging.warning(
                    "LVM plugin: Cannot load configuration file '%s'"
                    % (self.PLUGIN_CONFFILE)
                )
            if conf_loaded is not None:
                self._conf = dmconffile.ConfFile.conf_defaults_merge(
                    self.CONF_DEFAULTS,
                    conf_loaded
                )
        except Exception as exc:
            logging.error(
                "LVM plugin: initialization failed, unhandled exception: %s"
                % (str(exc))
            )


    def load_state(self):
        """
        Saves the blockdevices & pools map
        """
        state_file = None
        try:
            stored_hash = None
            state_file  = open(self.LVM_THIN_SAVEFILE, "r")

            load_data = state_file.read()

            line_begin = 0
            line_end   = 0
            while line_end >= 0 and stored_hash is None:
                line_end = load_data.find("\n", line_begin)
                if line_end != -1:
                    line = load_data[line_begin:line_end]
                else:
                    line = load_data[line_begin:]
                if line.startswith("sig:"):
                    stored_hash = line[4:]
                else:
                    line_begin = line_end + 1
            if stored_hash is not None:
                # truncate load_data so it does not contain the signature line
                load_data = load_data[:line_begin]
                data_hash = utils.DataHash()
                data_hash.update(load_data)
                computed_hash = data_hash.get_hex_hash()
                if computed_hash != stored_hash:
                    logging.warning("lvm_thinpool plugin: state data does not "
                                    "match its signature")
            else:
                logging.warning("lvm_thinpool plugin: state data does not "
                                "contain a signature")

            # Load the state from the JSON data
            state_con = json.loads(load_data)

            # Deserialize the block devices from the volumes map
            loaded_volumes = {}
            volumes_con = state_con["volumes"]
            for properties in volumes_con.itervalues():
                blockdev = storpers.BlockDevicePersistence.load(properties)
                if blockdev is not None:
                    loaded_volumes[blockdev.get_name()] = blockdev

            # Deserialize the thin pools from the pools map
            loaded_pools = {}
            pools_con = state_con["pools"]
            for properties in pools_con.itervalues():
                thin_pool = ThinPoolPersistence.load(properties)
                if thin_pool is not None:
                    loaded_pools[thin_pool.get_name()] = thin_pool

            # Build the pool lookup table
            loaded_lookup = {}
            for pool in loaded_pools.itervalues():
                pool_name = pool.get_name()
                for volume_name in pool.iterate_volumes():
                    loaded_lookup[volume_name] = pool_name

            # Activate the newly loaded configuration
            # (and implicitly discard the old one)
            self._pools       = loaded_pools
            self._volumes     = loaded_volumes
            self._pool_lookup = loaded_lookup
        except IOError:
            raise dmexc.PersistenceException
        except Exception:
            # FIXME: catch JSON exceptions here, but probably not much else
            pass
        finally:
            if state_file is not None:
                state_file.close()


    def save_state(self):
        """
        Saves the blockdevices & pools map
        """
        state_con   = {}

        # Serialize the block devices into the volumes map
        volumes_con = {}
        for blockdev in self._volumes.itervalues():
            bd_persist = storpers.BlockDevicePersistence(blockdev)
            bd_persist.save(volumes_con)

        # Save the volumes map to the state map
        state_con["volumes"] = volumes_con

        # Save the thin pools to the state map
        pools_con = {}
        for thin_pool in self._pools.itervalues():
            p_thin_pool = ThinPoolPersistence(thin_pool)
            p_thin_pool.save(pools_con)
        state_con["pools"] = pools_con

        # Save the state to the file
        out_file = None
        try:
            out_file = open(self.LVM_THIN_SAVEFILE, "w")

            data_hash = utils.DataHash()
            save_data = json.dumps(state_con, indent=4, sort_keys=True) + "\n"
            data_hash.update(save_data)

            out_file.write(save_data)
            out_file.write("sig:" + data_hash.get_hex_hash() + "\n")
        except IOError:
            logging.error("LVM plugin: saving state data failed " +
                          "due to an I/O error")
            raise dmexc.PersistenceException
        except Exception as exc:
            logging.error("LVM plugin: saving state data failed, " +
                          "unhandled exception: %s" % (str(exc)))
            raise dmexc.PersistenceException
        finally:
            if out_file is not None:
                out_file.close()


    def load_conf(self):
        """
        Loads the plugin configuration file
        """
        in_file = None
        conf    = None
        try:
            in_file  = open(self.PLUGIN_CONFFILE, "r")
            conffile = dmconffile.ConfFile(in_file)
            conf     = conffile.get_conf()
        except IOError as ioerr:
            if ioerr.errno == errno.EACCES:
                logging.error(
                    "LVM plugin: cannot open configuration file " +
                    "'%s': Permission denied"
                    % (self.PLUGIN_CONFFILE)
                )
            elif ioerr.errno != errno.ENOENT:
                logging.error(
                    "LVM plugin: cannot open configuration file " +
                    "'%s', error returned by the OS is: %s"
                    % (self.PLUGIN_CONFFILE, ioerr.strerror)
                )
        finally:
            if in_file is not None:
                in_file.close()
        return conf


    def _create_lv(self, name, pool_name, size):
        """
        Creates an LVM logical volume inside a newly created LVM thin pool
        """
        created = False

        # Prepare the path for LVM's 'lvcreate' utility
        lvcreate = utils.build_path(
            self._conf[self.KEY_LVM_PATH],
            self.LVCREATE
        )

        pool_ratio = self.DEFAULT_POOL_RATIO
        try:
            pool_ratio = float(self._conf[self.KEY_POOL_RATIO])
            # Fall back to the default if the size_ratio really does not
            # make any sense
            if pool_ratio <= 0:
                pool_ratio = self.DEFAULT_POOL_RATIO
        except ValueError:
            pass
        pool_size = long(size * (pool_ratio / 100))

        # Create the thin pool for the volume
        logging.debug(
            "LVMThinPool: exec: %s -L %sk -T %s/%s"
            % (lvcreate, str(pool_size), self._conf[self.KEY_VG_NAME], pool_name)
        )
        lvm_proc = subprocess.Popen(
            [
                lvcreate,
                "-L", str(pool_size) + "k",
                "-T", self._conf[self.KEY_VG_NAME] + "/" + pool_name
            ],
            0, lvcreate,
            env=self._subproc_env(),
            close_fds=True
        )
        create_pool_rc = lvm_proc.wait()
        # If the creation of the thinpool succeeded, create the volume
        if create_pool_rc == 0:
            logging.debug(
                "LVMThinPool: exec: %s -n %s -V %sk --thinpool %s %s"
                % (lvcreate, name, str(size),
                   pool_name, self._conf[self.KEY_VG_NAME])
            )
            lvm_proc = subprocess.Popen(
                [
                    lvcreate,
                    "-n", name,
                    "-V", str(size) + "k",
                    "--thinpool", pool_name,
                    self._conf[self.KEY_VG_NAME]
                ],
                0, lvcreate,
                env=self._subproc_env(),
                close_fds=True
            )
            fn_rc = lvm_proc.wait()
            # If the creation of the volume succeeded, indicate successful
            # creation of the requested logical volume
            if fn_rc == 0:
                created = True

        return created


    def _create_snapshot_lv(self, snaps_name, lv_name):
        """
        Creates an LVM snapshot LV of an existing LV
        """
        # lvcreate -s drbdpool/<volname> -n <snapshotname>
        created = False

        # Prepare the path for LVM's 'lvcreate' utility
        lvcreate = utils.build_path(
            self._conf[self.KEY_LVM_PATH],
            self.LVCREATE
        )

        # Create the thin pool for the volume
        logging.debug(
            "LVMThinPool: exec: %s -s %s/%s -n %s"
            % (lvcreate, self._conf[self.KEY_VG_NAME], lv_name, snaps_name)
        )
        lvm_proc = subprocess.Popen(
            [
                lvcreate,
                "-s", self._conf[self.KEY_VG_NAME] + "/" + lv_name,
                "-n", snaps_name
            ],
            0, lvcreate,
            env=self._subproc_env(),
            close_fds=True
        )
        create_rc = lvm_proc.wait()
        if create_rc == 0:
            created = True

        return created


    def _remove_lv(self, name):
        """
        Removes an LVM logical volume
        """
        removed = False

        # Prepare the path to LVM's 'lvremove' utility
        lvremove = utils.build_path(
            self._conf[self.KEY_LVM_PATH],
            self.LVREMOVE
        )

        # Remove the logical volume
        logging.debug(
            "LVMThinPool: exec: %s --force %s/%s"
            % (lvremove, self._conf[self.KEY_VG_NAME], name)
        )
        lvm_proc = subprocess.Popen(
            [
                lvremove, "--force",
                self._conf[self.KEY_VG_NAME] + "/" + name
            ],
            0, lvremove,
            env=self._subproc_env(),
            close_fds=True
        )
        fn_rc = lvm_proc.wait()
        if fn_rc == 0:
            removed = True

        return removed


    def _remove_pool(self, pool_name):
        """
        Removes an LVM thin pool
        """
        removed = False

        # Prepare the path to LVM's 'lvremove' utility
        lvremove = utils.build_path(
            self._conf[self.KEY_LVM_PATH],
            self.LVREMOVE
        )

        # Remove the thin pool
        lvm_proc = subprocess.Popen(
            [
                lvremove, "--force",
                self._conf[self.KEY_VG_NAME] + "/" + pool_name
            ],
            0, lvremove,
            env=self._subproc_env(),
            close_fds=True
        )
        fn_rc = lvm_proc.wait()
        if fn_rc == 0:
            removed = True

        return removed


    def _volume_name(self, name, vol_id):
        """
        Formats a volume name from a resource name and a volume id
        """
        volume_name = "%s_%.2d" % (name, vol_id)
        return volume_name


    def _subproc_env(self):
        """
        Returns a suitable environment for external LVM commands
        """
        return dict(
            os.environ.items() +
            [
                ('LC_ALL', 'C'),
                ('LANG', 'C'),
            ]
        )


class ThinPool(storagecommon.GenericStorage):

    """
    Represents the configuration of an LVM ThinPool
    """

    # Name generation for thin pools:
    # Resource name, underscore, 2-digit volume id, underscore,
    # up to 15 digit time value (YYYYYMMDDhhmmss)
    # Therefore, 3 characters are required for the volume id string, and
    # up to 16 characters are required for the time value, so the maximum
    # length of a thin pool name must be allowed to be 19 characters longer
    # than a resource name.
    THINPOOL_NAME_MAXLEN = consts.RES_NAME_MAXLEN + 19
    # Valid characters in addition to [a-zA-Z0-9]
    NAME_VALID_CHARS      = "_"
    # Additional valid characters, but not allowed as the first character
    NAME_VALID_INNER_CHARS = "-"

    _name    = None
    _volumes = None


    def __init__(self, name, size_kiB):
        super(ThinPool, self).__init__(size_kiB)
        self._name = drbdcommon.GenericDrbdObject.name_check(
            name, ThinPool.THINPOOL_NAME_MAXLEN,
            ThinPool.NAME_VALID_CHARS, ThinPool.NAME_VALID_INNER_CHARS
        )
        self._volumes = {}


    def get_name(self):
        return self._name


    def add_volume(self, name):
        self._volumes[name] = None


    def remove_volume(self, name):
        try:
            del self._volumes[name]
        except KeyError:
            pass


    def iterate_volumes(self):
        return self._volumes.iterkeys()


    def is_empty(self):
        """
        Indicates whether the thinpool contains any volumes
        """
        return False if len(self._volumes) > 0 else True


    @classmethod
    def generate_pool_name(cls, name, vol_id):
        pool_name = (
            "%s_%.2d_%lu"
            % (name, vol_id, long(time.strftime("%Y%m%d%H%M%S")))
        )
        return pool_name


    def extract_pool_name_suffix(self):
        index  = self._name.rfind("_")
        suffix = ""
        if index != -1:
            suffix = self._name[index:]
        return suffix


class ThinPoolPersistence(persistence.GenericPersistence):

    """
    Serializes ThinPool objects
    """

    SERIALIZABLE = ["_name", "_size_kiB"]


    def __init__(self, thin_pool):
        super(ThinPoolPersistence, self).__init__(thin_pool)


    def save(self, container):
        thin_pool  = self.get_object()
        properties = self.load_dict(self.SERIALIZABLE)
        volume_con = []
        for volume_name in thin_pool.iterate_volumes():
            volume_con.append(volume_name)
        properties["volumes"] = volume_con
        container[thin_pool.get_name()] = properties


    @classmethod
    def load(cls, properties):
        thin_pool = None
        try:
            thin_pool = ThinPool(
                properties["_name"],
                properties["_size_kiB"]
            )
            volume_con = properties["volumes"]
            for volume_name in volume_con:
                thin_pool.add_volume(volume_name)
        except (KeyError, TypeError):
            pass
        return thin_pool
