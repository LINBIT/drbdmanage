#!/usr/bin/env python2
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2013 - 2016  LINBIT HA-Solutions GmbH
                               Author: R. Altnoeder, Roland Kammerer

    For further information see the COPYING file.
"""

"""
Global exceptions and error codes for drbdmanage

This module defines exceptions, numeric error codes and the corresponding
default error messages for drbdmanage and utility functions to work with
those objects.
"""

import sys
import traceback


# return code for successful operations
DM_SUCCESS  = 0

# return code for supplemental information
DM_INFO     = 1

# ========================================
# return codes for failed operations
# ========================================

# function not implemented
DM_ENOTIMPL = 0x7fffffff

# Messages for return codes 1 to 99 are commonly
# not displayed by the drbdmanage client, and are
# not considered an error.

# invalid name for an object
DM_ENAME    = 100

# no entry = object not found
DM_ENOENT   = 101

# entry already exists
DM_EEXIST   = 102

# invalid IP type (not 4=IPv4 or 6=IPv6)
DM_EIPTYPE  = 103

# invalid minor number
DM_EMINOR   = 104

# Volume size out of range
DM_EVOLSZ   = 105

# Invalid option value
DM_EINVAL   = 106

# Cannot write configuration to or load configuration from persistent storage
DM_EPERSIST = 107

# Invalid node id or no free node id for auto-assignment
DM_ENODEID  = 108

# Invalid volume id or no free volume id for auto-assignment
DM_EVOLID   = 109

# Invalid port number or no free port numbers for auto-assignment
DM_EPORT    = 110

# An operation of the storage subsystem layer failed
DM_ESTORAGE = 111

# Not enough free memory
DM_ENOSPC   = 112

# Not enough nodes for deployment
DM_ENODECNT = 113

# Plugin load failed
DM_EPLUGIN  = 114

# Generation of the shared secret failed
DM_ESECRETG = 115

# Control volume error
DM_ECTRLVOL = 116

# Absence of quorum
DM_EQUORUM  = 117

# Operation not allowed on satellite
DM_ESATELLITE = 118

# drbdutils command failed
DM_EDRBDCMD = 119

# Unable to write the assignment configuration file
DM_ERESFILE = 120

# DEBUG value
DM_DEBUG    = 1023

_DM_EXC_TEXTS = {}
_DM_EXC_TEXTS[DM_SUCCESS]  = "Operation completed successfully"
_DM_EXC_TEXTS[DM_INFO]     = "(Additional information included)"
_DM_EXC_TEXTS[DM_ENAME]    = "Invalid name"
_DM_EXC_TEXTS[DM_ENOENT]   = "Object not found"
_DM_EXC_TEXTS[DM_EEXIST]   = "Object already exists"
_DM_EXC_TEXTS[DM_EIPTYPE]  = "Invalid IP protocol type"
_DM_EXC_TEXTS[DM_EMINOR]   = "Minor number out of range or no " \
                             "free minor numbers"
_DM_EXC_TEXTS[DM_EVOLSZ]   = "Volume size out of range"
_DM_EXC_TEXTS[DM_EINVAL]   = "Invalid option"
_DM_EXC_TEXTS[DM_DEBUG]    = "Debug exception / internal error"
_DM_EXC_TEXTS[DM_ENOTIMPL] = "Function not implemented"
_DM_EXC_TEXTS[DM_EPERSIST] = "I/O error while accessing persistent " \
                             "configuration storage"
_DM_EXC_TEXTS[DM_ENODEID]  = "Invalid node id or no free node id number"
_DM_EXC_TEXTS[DM_EVOLID]   = "Invalid volume id or no free volume id number"
_DM_EXC_TEXTS[DM_EPORT]    = "Invalid port number or no free port numbers"
_DM_EXC_TEXTS[DM_ESTORAGE] = "The storage subsystem failed to perform the " \
                             "requested operation"
_DM_EXC_TEXTS[DM_ENOSPC]   = "Not enough free space"
_DM_EXC_TEXTS[DM_ENODECNT] = "Deployment node count exceeds the number of " \
                             "nodes in the cluster"
_DM_EXC_TEXTS[DM_EPLUGIN]  = "Plugin cannot be loaded"
_DM_EXC_TEXTS[DM_ESECRETG] = "Generation of the shared secret failed"
_DM_EXC_TEXTS[DM_ECTRLVOL] = "Reconfiguring the control volume failed"
_DM_EXC_TEXTS[DM_EQUORUM]  = "Partition does not have a quorum"
_DM_EXC_TEXTS[DM_ESATELLITE]  = "Operation not allowed on satellite node"
_DM_EXC_TEXTS[DM_EDRBDCMD] = "A drbdutils command failed"
_DM_EXC_TEXTS[DM_ERESFILE] = "Updating a DRBD resource configuration file failed"


def dm_exc_text(exc_id):

    """
    Retrieve the default error message for a standard return code
    """
    try:
        text = _DM_EXC_TEXTS[exc_id]
    except KeyError:
        text = "<<No error message for id %d>>" % (str(exc_id))
    return text


class DrbdManageException(Exception):

    """
    Base class for exceptions
    """

    error_code = DM_DEBUG

    def __init__(self, message=None):
        super(DrbdManageException, self).__init__(message)

    def __str__(self):
        exception = self.__class__.__name__
        stack_trace = []

        message = self.message

        if message is not None:
            exception += ": " + message

        (exc_type, exc_value, exc_traceback) = sys.exc_info()
        if exc_traceback is not None:
            stack_trace = traceback.format_tb(exc_traceback)
        else:
            exception += " (uncaught)"

        formatted_exc = exception + "\n"
        if len(stack_trace) > 0:
            formatted_exc += "Exception stack trace:\n"
            for line in stack_trace:
                formatted_exc += line

        return formatted_exc

    def add_rc_entry(self, fn_rc):
        fn_rc.append([self.error_code, dm_exc_text(self.error_code), []])

    def add_rc_entry_message(self, fn_rc, message, args):
        fn_rc.append([self.error_code, message, args])


class InvalidNameException(DrbdManageException):

    """
    Raised on an attempt to use a string that does not match the naming
    criteria as a name for an object
    """

    def __init__(self, message=None):
        super(InvalidNameException, self).__init__(message)
        self.error_code = DM_ENAME


class InvalidAddrFamException(DrbdManageException):

    """
    Raised if an unknown address family is specified
    """

    def __init__(self, message=None):
        super(InvalidAddrFamException, self).__init__(message)
        self.error_code = DM_EINVAL


class VolSizeRangeException(DrbdManageException):

    """
    Raised if the size specification for a volume is out of range
    """

    def __init__(self, message=None):
        super(VolSizeRangeException, self).__init__(message)
        self.error_code = DM_EVOLSZ


class InvalidMinorNrException(DrbdManageException):

    """
    Raised if a device minor number is out of range or unparseable
    """

    def __init__(self, message=None):
        super(InvalidMinorNrException, self).__init__(message)
        self.error_code = DM_EMINOR


class InvalidMajorNrException(DrbdManageException):

    """
    Raised if a device major number is out of range or unparseable
    """

    def __init__(self, message=None):
        super(InvalidMajorNrException, self).__init__(message)
        self.error_code = DM_EINVAL


class IncompatibleDataException(DrbdManageException):

    """
    Raised if received data is not in a format expected and/or recognizable
    by the receiver.

    This exception is used by the drbdmanage client to signal that a list view
    generated by the drbdmanage server cannot be deserialized by the client.
    That should only happen with the combination of incompatible versions of
    drbdmanage client and server, otherwise it is a bug.
    """

    def __init__(self, message=None):
        super(IncompatibleDataException, self).__init__(message)
        self.error_code = DM_EINVAL


class SyntaxException(DrbdManageException):

    """
    Raised on syntax errors in input data
    """

    def __init__(self, message=None):
        super(SyntaxException, self).__init__(message)


class PersistenceException(DrbdManageException):

    """
    Raised if access to persistent storage fails
    """

    def __init__(self, message=None):
        super(PersistenceException, self).__init__(message)
        self.error_code = DM_EPERSIST


class QuorumException(DrbdManageException):

    """
    Raised if a partition does not have a quorum
    """

    def __init__(self, message=None):
        super(QuorumException, self).__init__(message)
        self.error_code = DM_EQUORUM


class DrbdCommandException(DrbdManageException):

    """
    Raised if a drbdutils command fails
    """

    def __init__(self, message=None):
        super(DrbdCommandException, self).__init__(message)
        self.error_code = DM_EDRBDCMD


class ResourceFileException(DrbdManageException):

    """
    Raised if a drbd resource configuration file cannot be updated
    """

    file_path = None

    def __init__(self, file_path_arg=None, message=None):
        super(ResourceFileException, self).__init__(message)
        self.file_path = file_path_arg
        self.error_code = DM_ERESFILE

    def get_log_message(self):
        log_message = None
        if self.file_path is not None:
            log_message = "Updating the DRBD resource configuration file '%s' failed" % (self.file_path)
        else:
            log_message = dm_exc_text(self.error_code)
        return log_message

    def add_rc_entry(self, fn_rc):
        if self.file_path is not None:
            fn_rc.append(
                [
                    self.error_code,
                    "Updating the DRBD resource configuration file %(res_name) failed",
                    [ ["res_name", self.file_path ] ]
                ]
            )
        else:
            fn_rc.append(
                [
                    self.error_code,
                    dm_exc_text(self.error_code),
                    []
                ]
            )


class PluginException(DrbdManageException):

    """
    Raised if a plugin cannot be loaded
    """

    def __init__(self, message=None):
        super(PluginException, self).__init__(message)
        self.error_code = DM_EPLUGIN


class AbortException(DrbdManageException):

    """
    Raised to abort execution of a chain of operations
    """

    def __init__(self, message=None):
        super(AbortException, self).__init__(message)


class DeployerException(DrbdManageException):

    """
    Raised if selecting nodes for deploying a resource fails
    """

    def __init__(self, message=None):
        super(DeployerException, self).__init__(message)
        self.error_code = DM_EPLUGIN


class DebugException(DrbdManageException):

    """
    Raised to indicate an implementation error
    """

    def __init__(self, message=None):
        super(DebugException, self).__init__(message)
        # Set this again just in case that the default value
        # in the super-class was changed
        self.error_code = DM_DEBUG
