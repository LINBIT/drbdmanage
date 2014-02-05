#!/usr/bin/python

__author__="raltnoeder"
__date__ ="$Oct 24, 2013 4:04:23 PM$"

import os
import sys
import subprocess
import errno
import logging

from drbdmanage.utils import *

class DrbdAdm(object):
    
    """
    Calls the external drbdadm command to control DRBD
    """
    
    EXECUTABLE = "drbdadm"
    
    execpath = None
    
    def __init__(self, path):
        self.execpath = build_path(path, self.EXECUTABLE)
    
    
    def ext_conf_adjust(self, res_name):
        """
        Adjusts a resource that has an external configuration file
        (Normally used to start up the control volume for drbdmanage)
        
        @return: process handle of the drbdadm process
        """
        args = [self.EXECUTABLE, "adjust", res_name]
        return self._run_drbdadm(args)


    def adjust(self, res_name):
        """
        Adjusts a resource
        
        @return: process handle of the drbdadm process
        """
        logging.debug("DrbdAdm: adjust %s" % res_name)
        args = [self.EXECUTABLE, "-c", "-", "adjust", res_name]
        return self._run_drbdadm(args)
    
    
    def up(self, res_name):
        """
        OBSOLETE. Brings up a DRBD resource. Use adjust instead
        
        @return: process handle of the drbdadm process
        """
        logging.warning("DEPRECATED: DrbdAdm: up %s" % res_name)
        args = [self.EXECUTABLE, "-c", "-", "up", res_name]
        return self._run_drbdadm(args)
    
    
    def down(self, res_name):
        """
        Shuts down (unconfigures) a DRBD resource
        
        @return: process handle of the drbdadm process
        """
        logging.debug("DrbdAdm: down %s" % res_name)
        args = [self.EXECUTABLE, "-c", "-", "down", res_name]
        return self._run_drbdadm(args)
    
    
    def primary(self, res_name, force):
        """
        Switches a DRBD resource to primary mode
        
        @param   res_name: DRBD configuration name of the resource
        @param   force: if set, adds the --force flag for drbdsetup
        @return: process handle of the drbdadm process
        """
        if force:
            logging.debug("DrbdAdm: primary %s --force" % res_name)
        else:
            logging.debug("DrbdAdm: primary %s" % res_name)
        args = [self.EXECUTABLE, "-c", "-"]
        if force:
            args.append("--")
            args.append("--force")
        args.append("primary")
        args.append(res_name)
        return self._run_drbdadm(args)
    
    
    def secondary(self, res_name):
        """
        Switches a resource to secondary mode
        @return: process handle of the drbdadm process
        """
        logging.debug("DrbdAdm: secondary %s" % res_name)
        args = [self.EXECUTABLE, "-c", "-", "secondary", res_name]
        return self._run_drbdadm(args)
    
    
    def connect(self, res_name, discard):
        """
        Connects a resource to its peer resources on other hosts
        @return: process handle of the drbdadm process
        """
        if discard:
            logging.debug("DrbdAdm: connect %s --discard-my-data" % res_name)
        else:
            logging.debug("DrbdAdm: connect %s" % res_name)
        args = [self.EXECUTABLE, "-c", "-"]
        if discard:
            args.append("--")
            args.append("--discard-my-data")
        args.append("connect")
        args.append(res_name)
        return self._run_drbdadm(args)
    
    
    def disconnect(self, res_name):
        """
        Disconnects a resource from its peer resources on other hosts
        @return: process handle of the drbdadm process
        """
        logging.debug("DrbdAdm: disconnect %s" % res_name)
        args = [self.EXECUTABLE, "-c", "-", "disconnect", res_name]
        return self._run_drbdadm(args)
    
    
    def attach(self, res_name, vol_id):
        """
        Attaches a volume to its disk
        @return: process handle of the drbdadm process
        """
        logging.debug("DrbdAdm: attach %s %d" % (res_name, vol_id))
        args = [self.EXECUTABLE, "-c", "-", "attach",
          res_name + "/" + str(vol_id)]
        return self._run_drbdadm(args)
    
    
    def detach(self, res_name, vol_id):
        """
        Detaches a volume to its disk
        @return: process handle of the drbdadm process
        """
        logging.debug("DrbdAdm: detach %s %d" % (res_name, vol_id))
        args = [self.EXECUTABLE, "-c", "-", "detach",
          res_name + "/" + str(vol_id)]
        return self._run_drbdadm(args)
    
    
    def create_md(self, res_name, vol_id, peers):
        """
        Calls drbdadm to create the metadata information for a volume
        @return: process handle of the drbdadm process
        """
        logging.debug("DrbdAdm: create-md %s %d" % (res_name, vol_id))
        args = [self.EXECUTABLE, "-c", "-", "--max-peers", str(peers),
          "--", "--force", "create-md", res_name + "/" + str(vol_id)]
        return self._run_drbdadm(args)
    
    
    def _run_drbdadm(self, args):
        """
        Runs the drbdadm command as a child process with its standard input
        redirected to a pipe from the drbdmanage server
        """
        drbd_proc = None
        try:
            drbd_proc = subprocess.Popen(args, 0, self.execpath,
              stdin=subprocess.PIPE, close_fds=True)
        except OSError as oserr:
            if oserr.errno == errno.ENOENT:
                logging.error("Cannot find the drbdadm utility")
            elif oserr.errno == errno.EACCES:
                logging.error("Cannot execute the drbdadm utility, "
                  "permission denied")
            else:
                logging.error("Cannot execute the drbdadm utility, "
                  "error returned by the OS is: "
                  "%s\n" % oserr.strerror)
        return drbd_proc
    