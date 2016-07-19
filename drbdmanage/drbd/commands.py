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

import errno
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
    DRBDUTIL_EXEC_FAILED = 127

    def __init__(self):
        pass

    def adjust(self, res_name, skip_net=False, skip_disk=False, discard=False, vol_id=None):
        """
        Adjusts a resource

        @return: process handle of the drbdadm process
        """

        exec_args = [self.DRBDADM_UTIL, "-vvv", "adjust"]
        if discard:
            exec_args.append('--discard-my-data')

        if vol_id is not None:
            res_name += '/%s' % str(vol_id)

        if skip_net:
            exec_args.append("--skip-net")
        if skip_disk:
            exec_args.append("--skip-disk")
        exec_args.append(res_name)
        return self._run_drbdutils(exec_args)

    def resize(self, res_name, vol_id, assume_clean):
        """
        Resizes a resource

        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "-vvv"]
        if assume_clean:
            exec_args.append("--")
            exec_args.append("--assume-clean")
        exec_args.append("resize")
        exec_args.append(res_name + "/" + str(vol_id))
        return self._run_drbdutils(exec_args)

    def down(self, res_name):
        """
        Shuts down (unconfigures) a DRBD resource

        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "-vvv", "down", res_name]
        return self._run_drbdutils(exec_args)

    def fallback_down(self, res_name):
        """
        Shuts down (unconfigures) a DRBD resource

        @return: True if the fallback executable exited with exit code 0, False otherwise
        """
        exec_args = [self.DRBDSETUP_UTIL, "-vvv", "down", res_name]
        exit_code = self._run_drbdutils(exec_args)
        return (exit_code == 0)

    def primary(self, res_name, force):
        """
        Switches a DRBD resource to primary mode

        @param   res_name: DRBD configuration name of the resource
        @param   force: if set, adds the --force flag for drbdsetup
        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "-vvv"]
        if force:
            exec_args.append("--")
            exec_args.append("--force")
        exec_args.append("primary")
        exec_args.append(res_name)
        return self._run_drbdutils(exec_args)

    def secondary(self, res_name):
        """
        Switches a resource to secondary mode
        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "-vvv", "secondary", res_name]
        return self._run_drbdutils(exec_args)

    def connect(self, res_name, discard):
        """
        Connects a resource to its peer resources on other hosts
        @return: process handle of the drbdadm process
        """
        return self.adjust(res_name, skip_disk=True, discard=discard)

    def disconnect(self, res_name):
        """
        Disconnects a resource from its peer resources on other hosts
        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "-vvv", "disconnect", res_name]
        return self._run_drbdutils(exec_args)

    def attach(self, res_name, vol_id):
        """
        Attaches a volume to its disk
        @return: process handle of the drbdadm process
        """
        return self.adjust(res_name, skip_net=True, vol_id=vol_id)

    def detach(self, res_name, vol_id):
        """
        Detaches a volume to its disk
        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "-vvv", "detach",
                     res_name + "/" + str(vol_id)]
        return self._run_drbdutils(exec_args)

    def create_md(self, res_name, vol_id, peers):
        """
        Calls drbdadm to create the metadata information for a volume
        @return: process handle of the drbdadm process
        """
        exec_args = [self.DRBDADM_UTIL, "-vvv", "--max-peers", str(peers),
                     "--", "--force", "create-md", res_name + "/" + str(vol_id)]
        return self._run_drbdutils(exec_args)

    def set_gi(self, node_id, minor_nr, bd_path, current_gi, history_1_gi=None, set_flags=False):
        """
        Calls drbdadm to create the metadata information for a volume
        @return: process handle of the drbdadm process
        """
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
        exit_code = self._run_drbdutils(exec_args)
        return (exit_code == 0)

    def new_current_uuid(self, res_name, vol_id):
        """
        Calls drbdadm to set a new current GI
        @return: True if the command succeeded (exit code 0), False otherwise
        """
        exec_args = [
            self.DRBDADM_UTIL, "-vvv", "--clear-bitmap", "new-current-uuid", res_name + "/" + str(vol_id)
        ]
        exit_code = self._run_drbdutils(exec_args)
        return (exit_code == 0)

    def _run_drbdutils(self, exec_args):
        """
        Runs the drbdadm command as a child process with its standard input
        redirected to a pipe from the drbdmanage server
        """
        drbdutil_rc = DrbdAdm.DRBDUTIL_EXEC_FAILED
        try:
            # Always log what's being executed and what the exit code was
            drbdutil_exec = utils.ExternalCommandBuffer(
                self.__class__.__name__, exec_args,
                trace_exec_args=utils.info_trace_exec_args,
                trace_exit_code=utils.smart_trace_exit_code,
            )
            drbdutil_rc = drbdutil_exec.run()
            # Log stdout/stderr at the error loglevel if the
            # command failed, otherwise log at the debug loglevel
            if drbdutil_rc != 0:
                drbdutil_exec.log_stdout()
                drbdutil_exec.log_stderr()
            else:
                drbdutil_exec.log_stdout(log_handler=logging.debug)
                drbdutil_exec.log_stderr(log_handler=logging.debug)
        except OSError as oserr:
            if oserr.errno == errno.ENOENT:
                logging.error("Cannot find drbdutils utility '%s', in PATH '%s'"
                              % (exec_args[0], os.environ['PATH']))
            elif oserr.errno == errno.EACCES:
                logging.error("Cannot execute drbdutils utility '%s', permission denied"
                              % (exec_args[0]))
            else:
                logging.error(
                    "Cannot execute drbdadm utility '%s', error returned by "
                    "the OS is: %s\n"
                    % (exec_args[0], oserr.strerror)
                )
        return drbdutil_rc
