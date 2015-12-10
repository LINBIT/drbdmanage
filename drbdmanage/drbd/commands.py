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
import drbdmanage.utils as utils

class DrbdAdm(object):

    """
    Calls the external drbdadm command to control DRBD
    """

    EXECUTABLE      = "drbdadm"
    RES_ALL_KEYWORD = "all"

    execpath = None

    def __init__(self, path):
        self.execpath = utils.build_path(path, self.EXECUTABLE)


    def ext_conf_adjust(self, res_name):
        """
        Adjusts a resource that has an external configuration file
        (Does not pipe the configuration into drbdadm;
         Normally used to start up the control volume for drbdmanage)

        @return: process handle of the drbdadm process
        """
        exec_args = [self.EXECUTABLE, "adjust", res_name]
        return self._run_drbdadm(exec_args)


    def ext_conf_down(self, res_name):
        """
        Stops a resource that has an external configuration file
        (Does not pipe the configuration into drbdadm;
         Normally used to stop the control volume for drbdmanage)

        @return: process handle of the drbdadm process
        """
        exec_args = [self.EXECUTABLE, "down", res_name]
        return self._run_drbdadm(exec_args)


    def adjust(self, res_name):
        """
        Adjusts a resource

        @return: process handle of the drbdadm process
        """
        logging.debug("DrbdAdm: adjust %s" % (res_name))
        exec_args = [self.EXECUTABLE, "-c", "-", "adjust", res_name]
        return self._run_drbdadm(exec_args)


    def resize(self, res_name, vol_id):
        """
        Resizes a resource

        @return: process handle of the drbdadm process
        """
        logging.debug("DrbdAdm: resize %s" % (res_name))
        exec_args = [self.EXECUTABLE, "-c", "-", "resize", res_name + "/" + str(vol_id)]
        return self._run_drbdadm(exec_args)


    def up(self, res_name):
        """
        OBSOLETE. Brings up a DRBD resource. Use adjust instead

        @return: process handle of the drbdadm process
        """
        logging.warning("DEPRECATED: DrbdAdm: up %s" % (res_name))
        exec_args = [self.EXECUTABLE, "-c", "-", "up", res_name]
        return self._run_drbdadm(exec_args)


    def down(self, res_name):
        """
        Shuts down (unconfigures) a DRBD resource

        @return: process handle of the drbdadm process
        """
        logging.debug("DrbdAdm: down %s" % (res_name))
        exec_args = [self.EXECUTABLE, "-c", "-", "down", res_name]
        return self._run_drbdadm(exec_args)


    def primary(self, res_name, force):
        """
        Switches a DRBD resource to primary mode

        @param   res_name: DRBD configuration name of the resource
        @param   force: if set, adds the --force flag for drbdsetup
        @return: process handle of the drbdadm process
        """
        if force:
            logging.debug("DrbdAdm: primary %s --force" % (res_name))
        else:
            logging.debug("DrbdAdm: primary %s" % (res_name))
        exec_args = [self.EXECUTABLE, "-c", "-"]
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
        logging.debug("DrbdAdm: secondary %s" % (res_name))
        exec_args = [self.EXECUTABLE, "-c", "-", "secondary", res_name]
        return self._run_drbdadm(exec_args)


    def connect(self, res_name, discard):
        """
        Connects a resource to its peer resources on other hosts
        @return: process handle of the drbdadm process
        """
        if discard:
            logging.debug("DrbdAdm: connect %s --discard-my-data" % (res_name))
        else:
            logging.debug("DrbdAdm: connect %s" % (res_name))
        exec_args = [self.EXECUTABLE, "-c", "-"]
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
        logging.debug("DrbdAdm: disconnect %s" % (res_name))
        exec_args = [self.EXECUTABLE, "-c", "-", "disconnect", res_name]
        return self._run_drbdadm(exec_args)


    def attach(self, res_name, vol_id):
        """
        Attaches a volume to its disk
        @return: process handle of the drbdadm process
        """
        logging.debug("DrbdAdm: attach %s %d" % (res_name, vol_id))
        exec_args = [self.EXECUTABLE, "-c", "-", "attach",
                res_name + "/" + str(vol_id)]
        return self._run_drbdadm(exec_args)


    def detach(self, res_name, vol_id):
        """
        Detaches a volume to its disk
        @return: process handle of the drbdadm process
        """
        logging.debug("DrbdAdm: detach %s %d" % (res_name, vol_id))
        exec_args = [self.EXECUTABLE, "-c", "-", "detach",
                res_name + "/" + str(vol_id)]
        return self._run_drbdadm(exec_args)


    def create_md(self, res_name, vol_id, peers):
        """
        Calls drbdadm to create the metadata information for a volume
        @return: process handle of the drbdadm process
        """
        logging.debug("DrbdAdm: create-md %s %d" % (res_name, vol_id))
        exec_args = [self.EXECUTABLE, "-c", "-", "--max-peers", str(peers),
                "--", "--force", "create-md", res_name + "/" + str(vol_id)]
        return self._run_drbdadm(exec_args)

    def _run_drbdadm_preexec(self, args):
        sys.stderr.write("spawning %s" % args)

    def _run_drbdadm(self, exec_args):
        """
        Runs the drbdadm command as a child process with its standard input
        redirected to a pipe from the drbdmanage server
        """
        drbd_proc = None

        try:
            utils.debug_log_exec_args(self.__class__.__name__, exec_args)
            drbd_proc = subprocess.Popen(
                exec_args, 0, self.execpath,
                preexec_fn=lambda *rest: self._run_drbdadm_preexec(exec_args),
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE, close_fds=True
            )
            subprocess.Popen(['logger', '--tag',
                              'DRBDmanage:%d' % drbd_proc.pid],
                             0, 'logger', stdin=drbd_proc.stderr)
            drbd_proc.stderr.close()
        except OSError as oserr:
            if oserr.errno == errno.ENOENT:
                logging.error("Cannot find the drbdadm utility")
            elif oserr.errno == errno.EACCES:
                logging.error(
                    "Cannot execute the drbdadm utility, permission denied"
                )
            else:
                logging.error(
                    "Cannot execute the drbdadm utility, error returned by "
                    "the OS is: %s\n"
                    % (oserr.strerror))
        return drbd_proc
