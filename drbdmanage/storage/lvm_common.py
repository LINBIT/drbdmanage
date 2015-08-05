#!/usr/bin/env python2

import subprocess
import logging
import errno
import json
import drbdmanage.conf.conffile as cf
import drbdmanage.utils as utils
import drbdmanage.exceptions as exc
import drbdmanage.storage.lvm_exceptions as lvmexc
import drbdmanage.storage.storagecore as storcore
import drbdmanage.storage.persistence as storpers

class LvmCommon(storcore.StoragePlugin):

    """
    Generic superclass for LVM storage management plugins
    """

    LVM_LVS_ENOENT = 5

    def __init__(self):
        super(LvmCommon, self).__init__()


    def check_lv_exists(self, lv_name, vg_name,
                        cmd_lvs, subproc_env, plugin_name):
        """
        Check whether an LVM logical volume exists

        @returns: True if the LV exists, False if the LV does not exist
        Throws an LvmCheckFailedException if the check itself fails
        """
        exists = False

        try:
            exec_args = [
                cmd_lvs, "--noheadings", "--options", "lv_name",
                vg_name + "/" + lv_name
            ]
            utils.debug_log_exec_args(self.__class__.__name__, exec_args)
            lvm_proc = subprocess.Popen(
                exec_args,
                0, cmd_lvs,
                env=subproc_env, stdout=subprocess.PIPE,
                close_fds=True
            )
            lv_entry = lvm_proc.stdout.readline()
            if len(lv_entry) > 0:
                lv_entry = lv_entry[:-1].strip()
                if lv_entry == lv_name:
                    exists = True
            lvm_rc = lvm_proc.wait()
            # LVM's "lvs" utility exits with exit code 5 if the
            # LV was not found
            if lvm_rc != 0 and lvm_rc != LvmCommon.LVM_LVS_ENOENT:
                raise lvmexc.LvmCheckFailedException
        except OSError:
            logging.error(
                plugin_name + ": Unable to retrieve the list of existing LVs"
            )
            raise lvmexc.LvmCheckFailedException

        return exists


    def load_conf(self, filename, plugin_name):
        """
        Loads settings from the module configuration file
        """
        conf_file   = None
        loaded_conf = None

        try:
            conf_file = open(filename, "r")
            conf_obj  = cf.ConfFile(conf_file)
            loaded_conf = conf_obj.get_conf()
        except IOError as io_err:
            if io_err.errno == errno.EACCES:
                logging.error(
                    plugin_name + ": Cannot open configuration file '%s': "
                    "Permission denied"
                    % (filename)
                )
            elif io_err.errno == errno.ENOENT:
                # No configuration file, use defaults. Not an error, ignore.
                pass
            else:
                logging.error(
                    plugin_name + ": Cannot open configuration file '%s', "
                    "error message from the OS: %s"
                    % (filename, io_err.strerror)
                )
        finally:
            if conf_file is not None:
                conf_file.close()

        return loaded_conf


    def load_state(self, state_filename, plugin_name):
        """
        Load the saved state of this module's managed logical volumes
        """
        loaded_objects = {}
        state_file     = None
        try:
            state_file  = open(state_filename, "r")

            loaded_data = state_file.read()

            state_file.close()
            state_file = None

            stored_hash = None
            line_begin  = 0
            line_end    = 0
            while line_end >= 0 and stored_hash is None:
                line_end = loaded_data.find("\n", line_begin)
                if line_end != -1:
                    line = loaded_data[line_begin:line_end]
                else:
                    line = loaded_data[line_begin:]
                if line.startswith("sig:"):
                    stored_hash = line[4:]
                else:
                    line_begin = line_end + 1
            if stored_hash is not None:
                # truncate load_data so it does not contain the signature line
                loaded_data = loaded_data[:line_begin]
                data_hash = utils.DataHash()
                data_hash.update(loaded_data)
                computed_hash = data_hash.get_hex_hash()
                if computed_hash != stored_hash:
                    logging.warning(
                        plugin_name + ": Data in state file '%s' has "
                        "an invalid signature, this file may be corrupt"
                        % (state_filename)
                    )
            else:
                logging.warning(
                    plugin_name + ": Data in state file '%s' is unsigned"
                    % (state_filename)
                )

            # Deserialize the saved objects
            loaded_property_map = json.loads(loaded_data)
            for blockdev_properties in loaded_property_map.itervalues():
                blockdev = storpers.BlockDevicePersistence.load(
                    blockdev_properties
                )
                if blockdev is not None:
                    loaded_objects[blockdev.get_name()] = blockdev
                else:
                    raise exc.PersistenceException

        except exc.PersistenceException as pers_exc:
            # re-raise
            raise pers_exc
        except IOError as io_err:
            if io_err.errno == errno.ENOENT:
                # State file does not exist, probably because the module
                # is being used for the first time.
                #
                # Ignore and continue with an empty configuration
                pass
            else:
                logging.error(
                    plugin_name + ": Loading the state file '%s' failed due to an "
                    "I/O error, error message from the OS: %s"
                    % (state_filename, io_err.strerror)
                )
                raise exc.PersistenceException
        except OSError as os_err:
            logging.error(
                plugin_name + ": Loading the state file '%s' failed, "
                "error message from the OS: %s"
                % (state_filename, str(os_err))
            )
            raise exc.PersistenceException
        except Exception as unhandled_exc:
            logging.error(
                plugin_name + ": Loading the state file '%s' failed, "
                "unhandled exception: %s"
                % (state_filename, str(unhandled_exc))
            )
            raise exc.PersistenceException
        finally:
            if state_file is not None:
                state_file.close()

        return loaded_objects


    def remove_lv(self, lv_name, vg_name,
                  cmd_remove, subproc_env, plugin_name):
        try:
            exec_args = [
                cmd_remove, "--force",
                vg_name + "/" + lv_name
            ]
            utils.debug_log_exec_args(self.__class__.__name__, exec_args)
            subprocess.call(
                exec_args,
                0, cmd_remove,
                env=subproc_env, close_fds=True
            )
        except OSError as os_err:
            logging.error(
                plugin_name + ": LV removal failed, unable to run "
                "external program '%s', error message from the OS: %s"
                % (cmd_remove, str(os_err))
            )
            raise lvmexc.LvmException


    def discard_fraction(self, text):
        """
        Discards the fraction part from a string representing a number
        """
        idx = text.find(".")
        if idx != -1:
            text = text[:idx]
        return text


    def lv_name(self, name, vol_id):
        """
        Build an LV name from the resource name and volume id
        """
        return ("%s_%.2d" % (name, vol_id))
