#!/usr/bin/python

__author__="raltnoeder"
__date__ ="$Sep 12, 2013 10:43:21 AM$"

from drbdmanage.exceptions import *
from drbdmanage.storage.storagecore import GenericStorage
from drbdmanage.exceptions import *

class DrbdManager(object):
    def __init__(self):
        pass
    
    @staticmethod
    def name_check(name, length):
        """
        Check the validity of a string for use as a name for
        objects like nodes or volumes.
        A valid name must match these conditions:
          * must at least be 1 byte long
          * must not be longer than specified by the caller
          * contains a-z, A-Z, 0-9 and _ characters only
          * contains at least one alpha character (a-z, A-Z)
          * must not start with a numeric character
        """
        if name == None or length == None:
            raise TypeError
        name_b   = bytearray(str(name), "utf-8")
        name_len = len(name_b)
        if name_len < 1 or name_len > length:
            raise InvalidNameException
        alpha = False
        for idx in range(0, name_len):
            b = name_b[idx]
            if b >= ord('a') and b <= ord('z'):
                alpha = True
                continue
            if b >= ord('A') and b <= ord('Z'):
                alpha = True
                continue
            if b >= ord('0') and b <= ord('9') and idx >= 1:
                continue
            if b == ord("_"):
                continue
            raise InvalidNameException
        if not alpha:
            raise InvalidNameException
        return str(name_b)


class DrbdVolume(GenericStorage):
    NAME_MAXLEN = 16
    
    _name     = None
    _minor    = None
    _state    = None
    
    _assignments = None
    
    FLAG_REMOVE = 0x1
    FLAG_NEW    = 0x2
    
    def __init__(self, name, size_MiB, minor):
        if not size_MiB > 0:
            raise VolSizeRangeException
        super(DrbdVolume, self).__init__(size_MiB)
        self._name   = self.name_check(name)
        self._minor  = minor
        self._state = 0
        self._assignments = dict()
        
    def get_name(self):
        return self._name
    
    def get_minor(self):
        return self._minor
    
    def name_check(self, name):
        return DrbdManager.name_check(name, self.NAME_MAXLEN)
    
    def add_assignment(self, assignment):
        node = assignment.get_node()
        self._assignments[node.get_name()] = assignment
        
    def get_assignment(self, name):
        assignment = None
        try:
            assignment = self._assignments[name]
        except KeyError:
            pass
        return assignment
    
    def remove_assignment(self, assignment):
        node = assignment.get_node()
        del self._assignments[node.get_name()]
    
    def has_assignments(self):
        return len(self._assignments) > 0
    
    def iterate_assignments(self):
        return self._assignments.itervalues()
    
    def get_state(self):
        return self._state
    
    def set_state(self, state):
        self._state = state
    
    def mark_remove(self):
        self._state |= self.FLAG_REMOVE


class DrbdNode(object):
    NAME_MAXLEN = 16
    
    IPV4_TYPE = 4
    IPV6_TYPE = 6
    
    _name     = None
    _ip       = None
    _ip_type  = None
    _state    = None
    _poolsize = None
    _poolfree = None
    
    _assignments = None
    
    FLAG_REMOVE = 0x1
    
    def __init__(self, name, ip, ip_type):
        self._name    = self.name_check(name)
        # TODO: there should be sanity checks on ip
        type = int(ip_type)
        if type == self.IPV4_TYPE or type == self.IPV6_TYPE:
            self._ip_type = type
        else:
            raise InvalidIpTypeException
        self._ip          = ip
        self._assignments = dict()
        self._state       = 0
        self._poolfree    = -1
        self._poolsize    = -1
    
    def get_name(self):
        return self._name
    
    def get_ip(self):
        return self._ip
    
    def get_ip_type(self):
        return self._ip_type
    
    def get_state(self):
        return self._state
    
    def set_state(self, state):
        self._state = state
    
    def mark_remove(self):
        self._state |= self.FLAG_REMOVE
    
    def name_check(self, name):
        return DrbdManager.name_check(name, self.NAME_MAXLEN)
    
    def add_assignment(self, assignment):
        self._assignments[assignment.get_volume().get_name()] = assignment
    
    def get_assignment(self, name):
        assignment = None
        try:
            assignment = self._assignments[name]
        except KeyError:
            pass
        return assignment

    def remove_assignment(self, assignment):
        volume = assignment.get_volume()
        del self._assignments[volume.get_name()]
    
    def has_assignments(self):
        return len(self._assignments) > 0
    
    def iterate_assignments(self):
        return self._assignments.itervalues()

class Assignment(object):
    _node        = None
    _volume      = None
    _blockdevice = None
    _node_id     = None
    _cstate      = 0
    _tstate      = 0
    # return code of operations
    _rc          = 0

    FLAG_DEPLOY    = 0x1
    FLAG_ATTACH    = 0x2
    FLAG_CONNECT   = 0x4
    FLAG_DISKLESS  = 0x8
    
    FLAG_UPD_CON   = 0x10000
    FLAG_RECONNECT = 0x20000
    # --overwrite-data-of-peer / primary --force
    FLAG_OVERWRITE = 0x40000
    # --discard-my-data upon connect / resolve split-brain
    FLAG_DISCARD   = 0x80000

    def __init__(self, node, volume, blockdevice, node_id, cstate, tstate):
        self._node        = node
        self._volume      = volume
        self._blockdevice = blockdevice
        self._node_id     = int(node_id)
        # current state
        self._cstate      = cstate
        # target state
        self._tstate      = tstate
        self._rc          = 0
    
    def get_node(self):
        return self._node
    
    def get_volume(self):
        return self._volume
    
    def remove(self):
        self._node.remove_assignment(self)
        self._volume.remove_assignment(self)
    
    def get_blockdevice(self):
        return self._blockdevice
    
    def get_node_id(self):
        return self._node_id
    
    def get_cstate(self):
        return self._cstate
    
    def set_cstate(self, cstate):
        self._cstate = cstate
    
    def get_tstate(self):
        return self._tstate
    
    def set_tstate(self, tstate):
        self._tstate = tstate
    
    def deploy(self):
        self._tstate = self._tstate | self.FLAG_DEPLOY
    
    def undeploy(self):
        self._tstate = (self._tstate | self.FLAG_DEPLOY) ^ self.FLAG_DEPLOY
    
    def connect(self):
        self._tstate = self._tstate | self.FLAG_CONNECT
    
    def reconnect(self):
        self._tstate = self._tstate | self.FLAG_RECONNECT
    
    def disconnect(self):
        self._tstate = (self._tstate | self.FLAG_CONNECT) | self.FLAG_CONNECT
    
    def attach(self):
        self._tstate = self._tstate | self.FLAG_ATTACH
    
    def detach(self):
        self._tstate = (self._tstate | self.FLAG_ATTACH) ^ self.FLAG_ATTACH
    
    def deploy_client(self):
        self._tstate = self._tstate | self.FLAG_DEPLOY | self.FLAG_DISKLESS
    
    def update_connections(self):
        self._tstate = self._tstate | self.FLAG_UPD_CON
    
    def set_rc(self, rc):
        self._rc = rc
    
    def get_rc(self):
        return self._rc
    
    def is_deployed(self):
        return (self._cstate & self.FLAG_DEPLOY) != 0
    
    def is_connected(self):
        return (self._cstate & self.FLAG_CONNECT) != 0
    
    def is_attached(self):
        return (self._cstate & self.FLAG_ATTACH) != 0
