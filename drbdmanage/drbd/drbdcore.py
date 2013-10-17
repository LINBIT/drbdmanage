#!/usr/bin/python

__author__="raltnoeder"
__date__ ="$Sep 12, 2013 10:43:21 AM$"

"""
WARNING!
  do not import anything from drbdmanage.drbd.persistence
"""
from drbdmanage.storage.storagecore import GenericStorage
from drbdmanage.storage.storagecore import BlockDevice
from drbdmanage.exceptions import *
from drbdmanage.utils import *


class DrbdManager(object):
    _server = None
    
    
    def __init__(self, server):
        self._server = server
    
    
    # FIXME
    def run(self):
        persist = None
        sys.stdout.write("%sDrbdManager invoked%s\n"
          % (COLOR_YELLOW, COLOR_NONE))
        try:
            persist = self._server.open_conf()
            if persist is not None:
                sys.stderr.write("%sDEBUG: drbdcore check/hash: %s%s\n"
                  % (COLOR_DARKPINK, hex_from_bin(persist.get_stored_hash()),
                  COLOR_NONE))
                if self._server.hashes_match(persist.get_stored_hash()):
                    # configuration did not change, bail out
                    sys.stdout.write("  hash unchanged, abort\n")
                    return
            # lock and reload the configuration
            persist.close()
            persist = self._server.begin_modify_conf()
            if persist is not None:
                if self.perform_changes():
                    sys.stdout.write("  perform_changes(): state changed\n")
                    self._server.save_conf_data(persist)
                else:
                    sys.stdout.write("  perform_changes(): no state change\n")
            else:
                # Could not instantiate PersistenceImpl
                # TODO: Error logging
                sys.stderr.write("%sDrbdManager: cannot open "
                  "persistent storage%s\n" % (COLOR_RED, COLOR_NONE))
            sys.stdout.write("%s--> DrbdManager: finished%s\n"
              % (COLOR_GREEN, COLOR_NONE))
        except Exception as exc:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            sys.stderr.write("%sDrbdManager: Oops: %s%s\n"
              % (COLOR_RED, str(exc), COLOR_NONE))
            print exc_type
            print exc_obj
            print exc_tb
        finally:
            # this also works for read access
            self._server.end_modify_conf(persist)
    
    
    # FIXME
    def perform_changes(self):
        state_changed = False
        sys.stdout.write("%s--> DrbdManager: perform changes%s\n"
          % (COLOR_GREEN, COLOR_NONE))
        node = self._server.get_instance_node()
        if node is None:
            sys.stdout.write("%s--> DrbdManager: this node is "
              "not registered%s\n"
              % (COLOR_RED, COLOR_NONE))
            return False
        sys.stdout.write("%s--> DrbdManager: This node is '%s'%s\n"
          % (COLOR_GREEN, node.get_name(), COLOR_NONE))
        assignments = node.iterate_assignments()
        for assg in assignments:
            sys.stdout.write("%s--> DrbdManager: assignment '%s:%s'%s\n"
              % (COLOR_GREEN, assg.get_node().get_name(),
              assg.get_resource().get_name(), COLOR_NONE))
            
            if assg.requires_action():
                sys.stdout.write("%s-->   (requires action)%s\n"
                  % (COLOR_GREEN, COLOR_NONE))
                # TODO: perform actions required by the assignment
                state_changed = True
        
        # Clean up undeployed resources
        self._server.cleanup()
        
        return state_changed
    
    
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
        for idx in xrange(0, name_len):
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


class DrbdResource(object):
    NAME_MAXLEN = 16
    
    _name        = None
    _secret      = None
    _state       = None
    _volumes     = None
    _assignments = None
    
    FLAG_REMOVE  = 0x1
    FLAG_NEW     = 0x2
    
    STATE_MASK   = FLAG_REMOVE | FLAG_NEW
    
    # maximum volumes per resource
    MAX_RES_VOLS = 64
    
    def __init__(self, name):
        self._name        = self.name_check(name)
        self._secret      = None
        self._state       = 0
        self._volumes     = dict()
        self._assignments = dict()
    
    
    def get_name(self):
        return self._name
    
    
    def name_check(self, name):
        return DrbdManager.name_check(name, self.NAME_MAXLEN)
    
    
    def add_assignment(self, assignment):
        node = assignment.get_node()
        self._assignments[node.get_name()] = assignment
    
    
    def get_assignment(self, name):
        return self._assignments.get(name)
    
    
    def remove_assignment(self, name):
        node = self._assignments.get(name)
        if node is not None:
            del self._assignments[node.get_name()]
    
    
    def iterate_assignments(self):
        return self._assignments.itervalues()
    
    
    def has_assignments(self):
        return len(self._assignments) > 0
    
    
    def add_volume(self, volume):
        self._volumes[volume.get_id()] = volume
    
    
    def get_volume(self, id):
        return self._volumes.get(id)
    
    
    def remove_volume(self, id):
        volume = self._volumes.get(id)
        if volume is not None:
            del self._volumes[volume.get_id()]
    
    
    def iterate_volumes(self):
        return self._volumes.itervalues()
    
    
    def remove(self):
        self._state |= self.FLAG_REMOVE
    
    
    def set_secret(self, secret):
        self._secret = secret
    
    
    def get_secret(self):
        return self._secret
    
    
    def get_state(self):
        return self._state
    
    
    def set_state(self, state):
        self._state = state & self.STATE_MASK
    
    
class DrbdResourceView(object):
    # array indexes
    RV_NAME     = 0
    RV_STATE    = 1
    RV_VOLUMES  = 2
    RV_PROP_LEN = 3
    
    _name    = None
    _state   = None
    _volumes = None
    
    _machine_readable = False
    
    
    def __init__(self, properties, machine_readable):
        if len(properties) < self.RV_PROP_LEN:
            raise IncompatibleDataException
        self._name  = properties[self.RV_NAME]
        try:
            self._state = long(properties[self.RV_STATE])
        except ValueError:
            raise IncompatibleDataException
        self._volumes = []
        for vol_list in properties[self.RV_VOLUMES]:
            self._volumes.append(DrbdVolumeView(vol_list, machine_readable))
        self._machine_readable = machine_readable
    
    
    @classmethod
    def get_name_maxlen(self):
        return DrbdResource.NAME_MAXLEN
    
    
    @classmethod
    def get_properties(cls, resource):
        properties = []
        properties.append(resource.get_name())
        properties.append(resource.get_state())
        vol_list = []
        for volume in resource.iterate_volumes():
            vol_list.append(DrbdVolumeView.get_properties(volume))
        properties.append(vol_list)
        return properties
    
    
    def get_name(self):
        return self._name
    
    
    def get_state(self):
        text = "-"
        if self._state & DrbdResource.FLAG_REMOVE != 0:
            if self._machine_readable:
                text = "REMOVE"
            else:
                text = "remove"
        return text
    
    
    def get_volumes(self):
        return self._volumes
    
    
class DrbdVolume(GenericStorage):    
    _id          = None
    _size_MiB    = None    
    _minor       = None
    _state       = None
    
    FLAG_REMOVE  = 0x1
    
    STATE_MASK   = FLAG_REMOVE
    
    def __init__(self, id, size_MiB, minor):
        if not size_MiB > 0:
            raise VolSizeRangeException
        super(DrbdVolume, self).__init__(size_MiB)
        self._id       = int(id)
        if self._id < 0 or self._id >= DrbdResource.MAX_RES_VOLS:
            raise ValueError
        self._size_MiB = size_MiB
        self._minor    = minor
        self._state    = 0
    
    
    def get_id(self):
        return self._id
    
    
    # returns a storagecore.MinorNr object
    def get_minor(self):
        return self._minor
    
    
    def get_state(self):
        return self._state
    
    
    def set_state(self, state):
        self._state = state & self.STATE_MASK
    
    
    def remove(self):
        self._state |= self.FLAG_REMOVE


class DrbdVolumeView(object):
    # array indexes
    VV_ID       = 0
    VV_SIZE_MiB = 1
    VV_MINOR    = 2
    VV_STATE    = 3
    VV_PROP_LEN = 4
    
    _id       = None
    _size_MiB = None
    _minor    = None
    _state    = None
    
    _machine_readable = False
    
    
    def __init__(self, properties, machine_readable):
        if len(properties) < self.VV_PROP_LEN:
            raise IncompatibleDataException
        self._id = properties[self.VV_ID]
        try:
            self._size_MiB = long(properties[self.VV_SIZE_MiB])
            self._minor    = int(properties[self.VV_MINOR])
            self._state    = long(properties[self.VV_STATE])
        except ValueError:
            raise IncompatibleDataException
        self._machine_readable = machine_readable
    
    
    @classmethod
    def get_properties(cls, volume):
        properties = []
        minor   = volume.get_minor()
        properties.append(volume.get_id())
        properties.append(volume.get_size_MiB())
        properties.append(minor.get_value())
        properties.append(volume.get_state())
        return properties
    
    
    def get_id(self):
        return self._id
    
    
    def get_size_MiB(self):
        return self._size_MiB
    
    
    def get_minor(self):
        if self._minor == -1:
            return "auto"
        elif self._minor == -2:
            return "auto-drbd"
        else:
            return str(self._minor)
    
    
    def get_state(self):
        text = ""
        if self._state & DrbdVolume.FLAG_REMOVE != 0:
            if self._machine_readable:
                text = "REMOVE"
            else:
                text = "remove"
        if len(text) == 0:
            text = "-"
        return text


class DrbdNode(object):
    NAME_MAXLEN = 16
    
    AF_IPV4 = 4
    AF_IPV6 = 6
    
    AF_IPV4_LABEL = "ipv4"
    AF_IPV6_LABEL = "ipv6"
    
    _name     = None
    _ip       = None
    _af       = None
    _state    = None
    _poolsize = None
    _poolfree = None
    
    _assignments = None
    
    FLAG_REMOVE = 0x1
    
    STATE_MASK = FLAG_REMOVE
    
    
    def __init__(self, name, ip, af):
        self._name    = self.name_check(name)
        # TODO: there should be sanity checks on ip
        af_n = int(af)
        if af_n == self.AF_IPV4 or af_n == self.AF_IPV6:
            self._af = af_n
        else:
            raise InvalidAddrFamException
        self._ip          = ip
        self._assignments = dict()
        self._state       = 0
        self._poolfree    = -1
        self._poolsize    = -1
    
    
    def get_name(self):
        return self._name
    
    
    def get_ip(self):
        return self._ip
    
    
    def get_af(self):
        return self._af
    
    
    def get_af_label(self):
        label = "unknown"
        if self._af == self.AF_IPV4:
            label = self.AF_IPV4_LABEL
        elif self._af == self.AF_IPV6:
            label = self.AF_IPV6_LABEL
        return label
    
    
    def get_state(self):
        return self._state
    
    
    def set_state(self, state):
        self._state = state & self.STATE_MASK
    
    
    def get_poolsize(self):
        return self._poolsize
    
    
    def get_poolfree(self):
        return self._poolfree
    
    
    def set_poolsize(self, size):
        self._poolsize = size
    
    
    def set_poolfree(self, size):
        self._poolfree = size
    
    
    def remove(self):
        self._state |= self.FLAG_REMOVE
    
    
    def name_check(self, name):
        return DrbdManager.name_check(name, self.NAME_MAXLEN)
    
    
    def add_assignment(self, assignment):
        resource = assignment.get_resource()
        self._assignments[resource.get_name()] = assignment
    
    
    def get_assignment(self, name):
        assignment = None
        try:
            assignment = self._assignments[name]
        except KeyError:
            pass
        return assignment


    def remove_assignment(self, assignment):
        resource = assignment.get_resource()
        del self._assignments[resource.get_name()]
    
    
    def has_assignments(self):
        return len(self._assignments) > 0
    
    
    def iterate_assignments(self):
        return self._assignments.itervalues()


class DrbdNodeView(object):
    # array indexes
    NV_NAME     = 0
    NV_AF_LABEL = 1
    NV_IP       = 2
    NV_POOLSIZE = 3
    NV_POOLFREE = 4
    NV_STATE    = 5
    NV_PROP_LEN = 6
    
    _name     = None
    _af       = None
    _ip       = None
    _poolsize = None
    _poolfree = None
    _state    = None
    
    _machine_readable = False
    
    
    def __init__(self, properties, machine_readable):
        if len(properties) < self.NV_PROP_LEN:
            raise IncompatibleDataException
        self._name     = properties[self.NV_NAME]
        self._af       = properties[self.NV_AF_LABEL]
        self._ip       = properties[self.NV_IP]
        try:
            self._poolsize = long(properties[self.NV_POOLSIZE])
            self._poolfree = long(properties[self.NV_POOLFREE])
            self._state    = long(properties[self.NV_STATE])
        except TypeError:
            raise IncompatibleDataException
        self._machine_readable = machine_readable
    
    
    @classmethod
    def get_name_maxlen(self):
        return DrbdNode.NAME_MAXLEN
    
    
    @classmethod
    def get_properties(cls, node):
        properties = []
        properties.append(node.get_name())
        properties.append(node.get_af_label())
        properties.append(node.get_ip())
        properties.append(node.get_poolsize())
        properties.append(node.get_poolfree())
        properties.append(node.get_state())
        return properties
    
    
    def get_name(self):
        return self._name
    
    
    def get_af(self):
        return self._af
    
    
    def get_ip(self):
        return self._ip
    
    
    def get_poolsize(self):
        return self._poolsize
    
    
    def get_poolfree(self):
        return self._poolfree
    
    
    def get_state(self):
        text = "-"
        if self._state & DrbdNode.FLAG_REMOVE != 0:
            if self._machine_readable:
                text = "REMOVE"
            else:
                text = "remove"
        return text
    
    
class DrbdVolumeState(object):
    _volume      = None
    _bd_path     = None
    _blockdevice = None
    _cstate      = 0
    _tstate      = 0
    
    FLAG_DEPLOY    = 0x1
    FLAG_ATTACH    = 0x2
    
    CSTATE_MASK    = FLAG_DEPLOY | FLAG_ATTACH
    TSTATE_MASK    = FLAG_DEPLOY | FLAG_ATTACH
    
    def __init__(self, volume):
        self._volume = volume
        self._cstate = 0
        self._tstate = 0
    
    
    def get_volume(self):
        return self._volume
    
    
    def get_id(self):
        return self._volume.get_id()
    
    
    def get_bd_path(self):
        return self._bd_path
    
    
    def get_blockdevice(self):
        return self._blockdevice
    
    
    def set_blockdevice(self, blockdevice, bd_path):
        self._blockdevice = blockdevice
        self._bd_path     = bd_path
    
    
    def requires_action(self):
        return (self._cstate != self._tstate)
    
    
    def requires_deploy(self):
        return (self._tstate & self.FLAG_DEPLOY == self.FLAG_DEPLOY) \
          and (self._cstate & self.FLAG_DEPLOY == 0)
    
    
    def requires_attach(self):
        return (self._tstate & self.FLAG_ATTACH == self.FLAG_ATTACH) \
          and (self._cstate & self.FLAG_ATTACH == 0)
    
    
    def requires_undeploy(self):
        return (self._tstate & self.FLAG_DEPLOY == 0) \
          and (self._cstate & self.FLAG_DEPLOY != 0)
    
    
    def requires_detach(self):
        return (self._tstate & self.FLAG_ATTACH == 0) \
          and (self._cstate & self.FLAG_ATTACH != 0)
    
    
    def set_cstate(self, cstate):
        self._cstate = cstate
    
    
    def set_tstate(self, tstate):
        self._tstate = tstate
    
    
    def get_cstate(self):
        return self._cstate & self.CSTATE_MASK
    
    
    def get_tstate(self):
        return self._tstate & self.TSTATE_MASK
    
    
    def deploy(self):
        self._tstate = self._tstate | self.FLAG_DEPLOY
    
    
    def undeploy(self):
        self._tstate = (self._tstate | self.FLAG_DEPLOY) ^ self.FLAG_DEPLOY
    
    
    def attach(self):
        self._tstate = self._tstate | self.FLAG_ATTACH
    
    
    def detach(self):
        self._tstate = (self._tstate | self.FLAG_ATTACH) ^ self.FLAG_ATTACH
    
    
    def set_deployed(self):
        self._cstate = self._cstate | self.FLAG_DEPLOYED
    
    
    def set_undeployed(self):
        self._cstate = (self._cstate | self.FLAG_DEPLOY) ^ self.FLAG_DEPLOY


class DrbdVolumeStateView(object):
    # array indexes
    SV_ID       = 0
    SV_BD_PATH  = 1
    SV_CSTATE   = 2
    SV_TSTATE   = 3
    SV_PROP_LEN = 4
    
    _id      = None
    _bd_path = None
    _cstate  = None
    _tstate  = None
    
    _machine_readable = False
    
    
    def __init__(self, properties, machine_readable):
        if len(properties) < self.SV_PROP_LEN:
            raise IncompatibleDataException
        self._id = properties[self.SV_ID]
        self._bd_path = properties[self.SV_BD_PATH]
        self._cstate  = properties[self.SV_CSTATE]
        self._tstate  = properties[self.SV_TSTATE]
        self._machine_readable = machine_readable
    
    
    @classmethod
    def get_properties(cls, vol_state):
        properties = []
        volume  = vol_state.get_volume()
        bd_path = vol_state.get_bd_path()
        if bd_path is None:
            bd_path = "-"
        properties.append(vol_state.get_id())
        properties.append(bd_path)
        properties.append(vol_state.get_cstate())
        properties.append(vol_state.get_tstate())
        return properties
    
    
    def get_id(self):
        return self._id
    
    
    def get_bd_path(self):
        return self._bd_path
    
    
    def get_cstate(self):
        return self._cstate
    
    
    def get_tstate(self):
        return self._tstate
    
    
    # TODO: implement state views
    def get_state(self):
        return "<not implemented>"


class Assignment(object):
    _node        = None
    _resource    = None
    _vol_states  = None
    _node_id     = None
    _cstate      = 0
    _tstate      = 0
    # return code of operations
    _rc          = 0

    FLAG_DEPLOY    = 0x1
    FLAG_CONNECT   = 0x2
    FLAG_DISKLESS  = 0x4
    
    FLAG_UPD_CON   = 0x10000
    FLAG_RECONNECT = 0x20000
    # --overwrite-data-of-peer / primary --force
    FLAG_OVERWRITE = 0x40000
    # --discard-my-data upon connect / resolve split-brain
    FLAG_DISCARD   = 0x80000
    
    CSTATE_MASK    = FLAG_DEPLOY | FLAG_CONNECT | FLAG_DISKLESS
    TSTATE_MASK    = FLAG_DEPLOY | FLAG_CONNECT | FLAG_DISKLESS \
                       | FLAG_UPD_CON | FLAG_RECONNECT \
                       | FLAG_OVERWRITE | FLAG_DISCARD
    
    NODE_ID_ERROR  = -1


    def __init__(self, node, resource, node_id, cstate, tstate):
        self._node        = node
        self._resource    = resource
        self._vol_states  = dict()
        for volume in resource.iterate_volumes():
            self._vol_states[volume.get_id()] = DrbdVolumeState(volume)
        self._node_id     = int(node_id)
        # current state
        self._cstate      = cstate
        # target state
        self._tstate      = tstate
        self._rc          = 0
    
    
    def get_node(self):
        return self._node
    
    
    def get_resource(self):
        return self._resource
    
    
    # used by AssignmentPersistence
    def add_volume_state(self, vol_state):
        self._vol_states[vol_state.get_id()] = vol_state
    
    
    def iterate_volume_states(self):
        return self._vol_states.itervalues()
    
    
    def get_volume_state(self, id):
        return self._vol_states.get(id)
    
    
    def remove_volume_state(self, id):
        vol_st = self._vol_states.get(id)
        if vol_st is not None:
            del self._vol_states[id]
    
    
    def update_volume_states(self):
        # create volume states for new volumes in the resource
        for volume in self._resource.iterate_volumes():
            vol_st = self._vol_states.get(volume.get_id())
            if vol_st is None:
                vol_st = DrbdVolumeState(volume)
                self._vol_states[volume.get_id()] = vol_st
        # remove volume states for volumes that no longer exist in the resource
        for vol_st in self._vol_states.itervalues():
            volume = self._resource.get_volume(vol_st.get_id())
            if volume is None:
                del self._vol_states[vol_st.get_id()]
    
    
    def remove(self):
        self._node.remove_assignment(self)
        self._resource.remove_assignment(self)
    
    
    def get_node_id(self):
        return self._node_id
    
    
    def get_cstate(self):
        return self._cstate
    
    
    def set_cstate(self, cstate):
        self._cstate = cstate & self.CSTATE_MASK
    
    
    def get_tstate(self):
        return self._tstate
    
    
    def set_tstate(self, tstate):
        self._tstate = tstate & self.TSTATE_MASK
    
    
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
    
    
    def deploy_client(self):
        self._tstate = self._tstate | self.FLAG_DEPLOY | self.FLAG_DISKLESS
    
    
    def update_connections(self):
        self._tstate = self._tstate | self.FLAG_UPD_CON
    
    
    def set_deployed(self):
        self._cstate = self._cstate | self.FLAG_DEPLOY
    
    
    def set_undeployed(self):
        self._cstate = (self._cstate | self.FLAG_DEPLOY) ^ self.FLAG_DEPLOY
    
    
    def set_connected(self):
        self._cstate = self._cstate | self.FLAG_CONNECT
    
    
    def set_reconnected(self):
        self._tstate = (self._tstate | self.FLAG_RECONNECT) \
          ^ self.FLAG_RECONNECT
    
    
    def set_disconnected(self):
        self._cstate = (self._cstate | self.FLAG_CONNECT) | self.FLAG_CONNECT
    
    
    def set_deployed_client(self):
        self._cstate = self._cstate | self.FLAG_DEPLOY | self.FLAG_DISKLESS
    
    
    def set_updated_connections(self):
        self._tstate = (self._tstate | self.FLAG_UPD_CON) ^ self.FLAG_UPD_CON
    
    
    def set_rc(self, rc):
        self._rc = rc
    
    
    def get_rc(self):
        return self._rc
    
    
    def is_deployed(self):
        return (self._cstate & self.FLAG_DEPLOY) != 0
    
    
    def is_connected(self):
        return (self._cstate & self.FLAG_CONNECT) != 0
    
    
    def requires_action(self):
        """
        If the state of the assignment itself requires action, or the
        state of any of the volumes of the resource associated with this
        assignment requires action, return True
        """
        req_act = False
        for vol_state in self._vol_states:
            if vol_state.requires_action():
                req_act = True
        return (self._cstate != self._tstate) or req_act
    
    
    def requires_deploy(self):
        return (self._tstate & self.FLAG_DEPLOY == self.FLAG_DEPLOY) \
          and (self._cstate & self.FLAG_DEPLOY == 0)
    
    
    def requires_connect(self):
        return (self._tstate & self.FLAG_CONNECT == self.FLAG_CONNECT) \
          and (self._cstate & self.FLAG_CONNECT == 0)
    
    
    def requires_undeploy(self):
        return (self._cstate & self.FLAG_DEPLOY == self.FLAG_DEPLOY) \
          and (self._tstate & self.FLAG_DEPLOY == 0)
    
    
    def requires_disconnect(self):
        return (self._cstate & self.FLAG_CONNECT == self.FLAG_CONNECT) \
          and (self._tstate & self.FLAG_CONNECT == 0)


class AssignmentView(object):
    # array indexes
    AV_NODE_NAME  = 0
    AV_RES_NAME   = 1
    AV_NODE_ID    = 2
    AV_CSTATE     = 3
    AV_TSTATE     = 4
    AV_VOL_STATES = 5
    AV_PROP_LEN   = 6
    
    _node         = None
    _resource     = None
    _node_id      = None
    _cstate       = None
    _tstate       = None
    
    _machine_readable = False
    
    
    def __init__(self, properties, machine_readable):
        if len(properties) < self.AV_PROP_LEN:
            raise IncompatibleDataException
        self._machine_readable = machine_readable
        self._node        = properties[self.AV_NODE_NAME]
        self._resource    = properties[self.AV_RES_NAME]
        self._node_id     = properties[self.AV_NODE_ID]
        try:
            self._cstate      = long(properties[self.AV_CSTATE])
            self._tstate      = long(properties[self.AV_TSTATE])
        except ValueError:
            raise IncompatibleDataException
        self._vol_states = []
        for vol_state in properties[self.AV_VOL_STATES]:
            self._vol_states.append(
              DrbdVolumeStateView(vol_state, machine_readable))
        
    
    @classmethod
    def get_properties(cls, assg):
        properties = []
        node     = assg.get_node()
        resource = assg.get_resource()
        properties.append(node.get_name())
        properties.append(resource.get_name())
        properties.append(assg.get_node_id())
        properties.append(assg.get_cstate())
        properties.append(assg.get_tstate())
        vol_state_list = []
        properties.append(vol_state_list)
        for vol_state in assg.iterate_volume_states():
            vol_state_list.append(
              DrbdVolumeStateView.get_properties(vol_state))
        return properties
    
    
    def get_node(self):
        return self._node
    
    
    def get_resource(self):
        return self._resource
    
    
    def get_volume_states(self):
        return self._vol_states
    
    
    def get_blockdevice(self):
        return self._blockdevice
    
    
    def get_node_id(self):
        return self._node_id
    
    
    def get_cstate(self):
        mr = self._machine_readable
        text = ""
        if self._cstate & Assignment.FLAG_DEPLOY != 0:
            text = state_text_append(mr, text, "DEPLOY", "deployed")
        if self._cstate & Assignment.FLAG_CONNECT != 0:
            text = state_text_append(mr, text, "CONNECT", "connected")
        if self._cstate & Assignment.FLAG_DISKLESS != 0:
            text = state_text_append(mr, text, "DISKLESS", "client")
        if len(text) == 0:
            text = "-"
        return text
    
    
    def get_tstate(self):
        mr = self._machine_readable
        text = ""
        if self._tstate & Assignment.FLAG_DEPLOY != 0:
            text = state_text_append(mr, text, "DEPLOY", "deploy")
        if self._tstate & Assignment.FLAG_CONNECT != 0:
            text = state_text_append(mr, text, "CONNECT", "connect")
        if self._tstate & Assignment.FLAG_UPD_CON != 0:
            text = state_text_append(mr, text, "UPD_CON", "update")
        if self._tstate & Assignment.FLAG_RECONNECT != 0:
            text = state_text_append(mr, text, "RECONNECT", "reconnect")
        if self._tstate & Assignment.FLAG_OVERWRITE != 0:
            text = state_text_append(mr, text, "OVERWRITE", "init-master")
        if self._tstate & Assignment.FLAG_DISCARD != 0:
            text = state_text_append(mr, text, "DISCARD", "discard")
        if len(text) == 0:
            text = "-"
        return text
    
    
    def get_state(self):
        mr = self._machine_readable
        text = ""
        if self._cstate & Assignment.FLAG_DEPLOY != 0:
            text = state_text_append(mr, text, "DEPLOY", "deployed")
        if self._cstate & Assignment.FLAG_ATTACH != 0:
            text = state_text_append(mr, text, "ATTACH", "attached")
        if self._cstate & Assignment.FLAG_CONNECT != 0:
            text = state_text_append(mr, text, "CONNECT", "connected")
        if self._cstate & Assignment.FLAG_DISKLESS != 0:
            text = state_text_append(mr, text, "DISKLESS", "client")
        if len(text) == 0:
            text = "-"
        return text


def state_text_append(machine_readable, text, mr_text, hr_text):
    if machine_readable:
        if len(text) > 0:
            text += "|"
        text += mr_text
    else:
        if len(text) > 0:
            text+= ","
        text += hr_text
    return text
