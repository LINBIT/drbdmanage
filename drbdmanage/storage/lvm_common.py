#!/usr/bin/env python2

import subprocess
import logging
import drbdmanage.utils as utils
import drbdmanage.storage.storagecore as storcore
from drbdmanage.storage.storageplugin_common import (
    StoragePluginCommon, StoragePluginException, StoragePluginCheckFailedException)


class LvmCommon(StoragePluginCommon, storcore.StoragePlugin):

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
        Throws an StoragePluginCheckFailedException if the check itself fails
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
                raise StoragePluginCheckFailedException
        except OSError:
            logging.error(
                plugin_name + ": Unable to retrieve the list of existing LVs"
            )
            raise StoragePluginCheckFailedException

        return exists

    def extend_lv(self, lv_name, vg_name, size, cmd_extend, subproc_env, plugin_name):
        """
        Extends an LVM logical volume
        """
        status = False
        try:
            exec_args = [
                cmd_extend, "-L", str(size) + "k",
                vg_name + "/" + lv_name
            ]
            utils.debug_log_exec_args(self.__class__.__name__, exec_args)
            proc_rc = subprocess.call(
                exec_args,
                0, cmd_extend,
                env=subproc_env, close_fds=True
            )
            if proc_rc == 0:
                status = True
        except OSError as os_err:
            logging.error(
                plugin_name + ": LV extension failed, unable to run "
                "external program '%s', error message from the OS: %s"
                % (cmd_extend, str(os_err))
            )
        return status

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
            raise StoragePluginException

    def discard_fraction(self, text):
        """
        Discards the fraction part from a string representing a number
        """
        idx = text.find(".")
        if idx != -1:
            text = text[:idx]
        return text
