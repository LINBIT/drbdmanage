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

import subprocess
import errno
import sys
import logging
import os
import drbdmanage.utils as utils

class DrbdAdm(object):

    """
    Calls the external drbdadm command to control DRBD
    """

    DRBDADM_UTIL   = "drbdadm"
    DRBDMETA_UTIL  = "drbdmeta"
    DRBDSETUP_UTIL = "drbdsetup"

    # Used as a return code to indicate that drbdadm could not be executed
    DRBDADM_EXEC_FAILED = 127

    def __init__(self):
        pass

    def ext_conf_adjust(self, res_name):
        """
        Adjusts a resource that has an external configuration file
        (Does not pipe the configuration into drbdadm;
         Normally used to start up the control volume for drbdmanage)

        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "adjust", res_name]
        return self._run_drbdadm(exec_args)


    def ext_conf_down(self, res_name):
        """
        Stops a resource that has an external configuration file
        (Does not pipe the configuration into drbdadm;
         Normally used to stop the control volume for drbdmanage)

        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "down", res_name]
        return self._run_drbdadm(exec_args)


    def adjust(self, res_name):
        """
        Adjusts a resource

        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "adjust", res_name]
        return self._run_drbdadm(exec_args)


    def resize(self, res_name, vol_id):
        """
        Resizes a resource

        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "resize", res_name + "/" + str(vol_id)]
        return self._run_drbdadm(exec_args)


    def up(self, res_name):
        """
        OBSOLETE. Brings up a DRBD resource. Use adjust instead

        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "up", res_name]
        return self._run_drbdadm(exec_args)


    def down(self, res_name):
        """
        Shuts down (unconfigures) a DRBD resource

        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "down", res_name]
        return self._run_drbdadm(exec_args)


    def fallback_down(self, res_name):
        """
        Shuts down (unconfigures) a DRBD resource

        @return: True if the fallback executable exited with exit code 0, False otherwise
        """
        fallback_ok = False
        exec_args = [self.DRBDSETUP_UTIL, "down", res_name]
        utils.debug_log_exec_args(self.__class__.__name__, exec_args)
        try:
            subprocess.check_call(exec_args)
            fallback_ok = True
        except subprocess.CalledProcessError:
            pass
        return fallback_ok


    def primary(self, res_name, force):
        """
        Switches a DRBD resource to primary mode

        @param   res_name: DRBD configuration name of the resource
        @param   force: if set, adds the --force flag for drbdsetup
        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL]
        if force:
            exec_args.append("--")
            exec_args.append("--force")
        exec_args.append("primary")
        exec_args.append(res_name)
        return self._run_drbdadm(exec_args)


    def secondary(self, res_name):
        """
        Switches a resource to secondary mode
        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "secondary", res_name]
        return self._run_drbdadm(exec_args)


    def connect(self, res_name, discard):
        """
        Connects a resource to its peer resources on other hosts
        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL]
        if discard:
            exec_args.append("--")
            exec_args.append("--discard-my-data")
        exec_args.append("connect")
        exec_args.append(res_name)
        return self._run_drbdadm(exec_args)


    def disconnect(self, res_name):
        """
        Disconnects a resource from its peer resources on other hosts
        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "disconnect", res_name]
        return self._run_drbdadm(exec_args)


    def attach(self, res_name, vol_id):
        """
        Attaches a volume to its disk
        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "attach",
                res_name + "/" + str(vol_id)]
        return self._run_drbdadm(exec_args)


    def detach(self, res_name, vol_id):
        """
        Detaches a volume to its disk
        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "detach",
                res_name + "/" + str(vol_id)]
        return self._run_drbdadm(exec_args)


    def create_md(self, res_name, vol_id, peers):
        """
        Calls drbdadm to create the metadata information for a volume
        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "--max-peers", str(peers),
                "--", "--force", "create-md", res_name + "/" + str(vol_id)]
        return self._run_drbdadm(exec_args)


    def set_gi(self, node_id, minor_nr, bd_path, current_gi, history_1_gi=None, set_flags=False):
        """
        Calls drbdadm to create the metadata information for a volume
        @return: process handle of the drbdadm process
        """
        set_gi_check = False
        gi_data = current_gi + ":"
        if set_flags or history_1_gi is not None:
            if history_1_gi is None:
                history_1_gi = "0"
            gi_data += "0:" + history_1_gi + ":0:"
            if set_flags:
                gi_data += "1:1:"
        exec_args = [
            self.DRBDMETA_UTIL, "--force", "--node-id", node_id,
            minor_nr, "v09", bd_path, "internal", "set-gi", gi_data
        ]
        utils.debug_log_exec_args(self.__class__.__name__, exec_args)
        try:
            subprocess.check_call(exec_args)
            set_gi_check = True
        except subprocess.CalledProcessError:
            pass
        return set_gi_check


    def new_current_uuid(self, res_name, vol_id):
        """
        Calls drbdadm to set a new current GI
        @return: True if the command succeeded (exit code 0), False otherwise
        """
        cmd_check = False
        exec_args = [
            self.DRBDADM_UTIL, "--clear-bitmap", "new-current-uuid", res_name + "/" + str(vol_id)
        ]
        utils.debug_log_exec_args(self.__class__.__name__, exec_args)
        try:
            subprocess.check_call(exec_args)
            cmd_check = True
        except subprocess.CalledProcessError:
            pass
        return cmd_check


    def _run_drbdadm(self, exec_args):
        """
        Runs the drbdadm command as a child process with its standard input
        redirected to a pipe from the drbdmanage server
        """
        drbd_proc = None
        drbdadm_rc = DrbdAdm.DRBDADM_EXEC_FAILED
        try:
            utils.debug_log_exec_args(self.__class__.__name__, exec_args)
            drbd_proc = subprocess.Popen(
                exec_args, 0, self.DRBDADM_UTIL,
                stderr=subprocess.PIPE, close_fds=True
            )
            subprocess.Popen(
                [
                    'logger', '-t', 'DRBDmanage:%d' % drbd_proc.pid
                ],
                0, 'logger',
                close_fds=True,
                stdin=drbd_proc.stderr
            )
            drbd_proc.stderr.close()
        except OSError as oserr:
            if oserr.errno == errno.ENOENT:
                logging.error("Cannot find the drbdadm utility, in PATH '%s'" % (os.environ['PATH']))
            elif oserr.errno == errno.EACCES:
                logging.error("Cannot execute the drbdadm utility, permission denied")
            else:
                logging.error(
                    "Cannot execute the drbdadm utility, error returned by "
                    "the OS is: %s\n"
                    % (oserr.strerror)
                )
        if drbd_proc is not None:
            drbdadm_rc = drbd_proc.wait()
        return drbdadm_rc
