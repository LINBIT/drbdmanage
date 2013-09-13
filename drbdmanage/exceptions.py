#!/usr/bin/python

__author__="raltnoeder"
__date__ ="$Sep 12, 2013 11:15:38 AM$"

"""
return code for successful operations
"""
DM_SUCCESS  = 0

"""
return codes for failed operations
"""
# function not implemented
DM_ENOTIMPL = 0x7fffffff

# invalid name for an object
DM_ENAME    = 100

# no entry = object not found
DM_ENOENT   = 101

# entry already exists
DM_EEXIST   = 102

# invalid IP type (not 4=IPv4 or 6=IPv6)
DM_EIPTYPE  = 103

# DEBUG value
DM_DEBUG    = 1023

_DM_EXC_TEXTS = dict()
_DM_EXC_TEXTS[DM_ENAME]    = "Invalid name"
_DM_EXC_TEXTS[DM_ENOENT]   = "Object not found"
_DM_EXC_TEXTS[DM_EEXIST]   = "Object already exists"
_DM_EXC_TEXTS[DM_DEBUG]    = "Debug exception / internal error"

def dm_exc_text(id):
    try:
        text = _DM_EXC_TEXTS[id]
    except KeyError:
        text = "<<No error message for id " + str(id) + ">>"
    return text

class InvalidNameException(Exception):
    def __init__(self):
        pass

class InvalidIpTypeException(Exception):
    def __init__(self):
        pass
