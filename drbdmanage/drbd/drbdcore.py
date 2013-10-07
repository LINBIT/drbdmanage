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
                if self._server.hashes_match(persist.get_stored_hash()):
                    # configuration did not change, bail out
                    return
            # lock and reload the configuration
            persist.close()
            persist = self._server.begin_modify_conf()
            if persist is not None:
                if self.perform_changes():
                    self._server.save_conf_data(persist)
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
              assg.get_volume().get_name(), COLOR_NONE))
            
            if assg.requires_action():
                sys.stdout.write("%s-->   (requires action)%s\n"
                  % (COLOR_GREEN, COLOR_NONE))
                state_changed = True
                vol = assg.get_volume()
                vol_name = vol.get_name()
                
                if assg.requires_connect():
                    sys.stdout.write("  %sconnect:%s   %s\n"
                      % (COLOR_GREEN, COLOR_NONE, vol_name))
                    assg.set_connected()
                elif assg.requires_disconnect():
                    sys.stdout.write("  %sdisconnect:%s   %s\n"
                      % (COLOR_GREEN, COLOR_NONE, vol_name))
                    assg.set_disconnected()
                
                if assg.requires_attach():
                    sys.stdout.write("  %sattach:%s   %s\n"
                      % (COLOR_GREEN, COLOR_NONE, vol_name))
                    assg.set_attached()
                elif assg.requires_detach():
                    sys.stdout.write("  %sdetach:%s   %s\n"
                      % (COLOR_GREEN, COLOR_NONE, vol_name))
                    assg.set_detached()
                
                if assg.requires_deploy():
                    sys.stdout.write("  %sdeploy:%s   %s\n"
                      % (COLOR_GREEN, COLOR_NONE, vol_name))
                    assg.set_deployed()
                elif assg.requires_undeploy():
                    sys.stdout.write("  %sundeploy:%s   %s\n"
                      % (COLOR_GREEN, COLOR_NONE, vol_name))
                    assg.set_undeployed()
        
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


class DrbdVolumeView(object):
    
    _name  = None
    _size  = None
    _minor = None
    _state = None
    
    _machine_readable = False
    
    
    def __init__(self, properties, machine_readable):
        if len(properties) < 4:
            raise IncompatibleDataException
        self._name = properties[0]
        self._size = properties[1]
        try:
            self._minor = int(properties[2])
            self._state = long(properties[3])
        except Exception:
            raise IncompatibleDataException
        self._machine_readable = machine_readable
    
    
    @classmethod
    def get_name_maxlen(self):
        return DrbdVolume.NAME_MAXLEN
    
    
    @classmethod
    def get_properties(self, volume):
        properties = []
        minor = volume.get_minor()
        properties.append(volume.get_name())
        properties.append(str(volume.get_size_MiB()))
        properties.append(str(minor.get_value()))
        properties.append(str(volume.get_state()))
        return properties
    
    
    def get_name(self):
        return self._name
    
    
    def get_size(self):
        return self._size
    
    
    def get_minor(self):
        if self._minor == -1:
            return "auto"
        elif self._minor == -2:
            return "auto-drbd"
        else:
            return str(self._minor)
    
    
    def get_state(self):
        text = ""
        if self._state & DrbdVolume.FLAG_NEW != 0:
            if self._machine_readable:
                text = "NEW"
            else:
                text = "new"
        if self._state & DrbdVolume.FLAG_REMOVE != 0:
            if self._machine_readable:
                if (len(text) > 0):
                    text += "|"
                text += "REMOVE"
            else:
                if (len(text) > 0):
                    text += ","
                text += "remove"
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
        self._state = state
    
    
    def get_poolsize(self):
        return self._poolsize
    
    
    def get_poolfree(self):
        return self._poolfree
    
    
    def set_poolsize(self, size):
        self._poolsize = size
    
    
    def set_poolfree(self, size):
        self._poolfree = size
    
    
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


class DrbdNodeView(object):
    
    _name     = None
    _af       = None
    _ip       = None
    _poolsize = None
    _poolfree = None
    _state    = None
    
    _machine_readable = False
    
    
    def __init__(self, properties, machine_readable):
        if len(properties) < 6:
            raise IncompatibleDataException
        self._name     = properties[0]
        self._af       = properties[1]
        self._ip       = properties[2]
        self._poolsize = properties[3]
        self._poolfree = properties[4]
        try:
            self._state    = long(properties[5])
        except Exception:
            raise IncompatibleDataException
        self._machine_readable = machine_readable
    
    
    @classmethod
    def get_name_maxlen(self):
        return DrbdNode.NAME_MAXLEN
    
    
    @classmethod
    def get_properties(self, node):
        properties = []
        properties.append(node.get_name())
        properties.append(node.get_af_label())
        properties.append(node.get_ip())
        properties.append(str(node.get_poolsize()))
        properties.append(str(node.get_poolfree()))
        properties.append(str(node.get_state()))
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


class Assignment(object):
    _node        = None
    _volume      = None
    _blockdevice = None
    _bd_path     = None
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


    def __init__(self, node, volume, node_id, cstate, tstate):
        self._node        = node
        self._volume      = volume
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
    
    
    def get_bd_path(self):
        return self._bd_path
    
    
    def set_blockdevice(self, blockdevice, path):
        self._blockdevice = blockdevice
        self._bd_path     = path
    
    
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
    
    
    def set_attached(self):
        self._cstate = self._cstate | self.FLAG_ATTACH
    
    
    def set_detached(self):
        self._cstate = (self._cstate | self.FLAG_ATTACH) ^ self.FLAG_ATTACH
    
    
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
    
    
    def is_attached(self):
        return (self._cstate & self.FLAG_ATTACH) != 0
    
    
    def requires_action(self):
        return (self._cstate != self._tstate)
    
    
    def requires_deploy(self):
        return (self._tstate & self.FLAG_DEPLOY == self.FLAG_DEPLOY) \
          and (self._cstate & self.FLAG_DEPLOY == 0)
    
    
    def requires_attach(self):
        return (self._tstate & self.FLAG_ATTACH == self.FLAG_ATTACH) \
          and (self._cstate & self.FLAG_ATTACH == 0)
    
    
    def requires_connect(self):
        return (self._tstate & self.FLAG_CONNECT == self.FLAG_CONNECT) \
          and (self._cstate & self.FLAG_CONNECT == 0)
    
    
    def requires_undeploy(self):
        return (self._cstate & self.FLAG_DEPLOY == self.FLAG_DEPLOY) \
          and (self._tstate & self.FLAG_DEPLOY == 0)
    
    
    def requires_detach(self):
        return (self._cstate & self.FLAG_ATTACH == self.FLAG_ATTACH) \
          and (self._tstate & self.FLAG_ATTACH == 0)
    
    
    def requires_disconnect(self):
        return (self._cstate & self.FLAG_CONNECT == self.FLAG_CONNECT) \
          and (self._tstate & self.FLAG_CONNECT == 0)


class AssignmentView(object):
    _node        = None
    _volume      = None
    _blockdevice = None
    _node_id     = None
    _cstate      = None
    _tstate      = None
    
    _machine_readable = False
    
    
    def __init__(self, properties, machine_readable):
        if len(properties) < 6:
            raise IncompatibleDataException
        self._machine_readable = machine_readable
        self._node        = properties[0]
        self._volume      = properties[1]
        self._blockdevice = properties[2]
        self._node_id     = properties[3]
        self._cstate      = long(properties[4])
        self._tstate      = long(properties[5])
    
    
    @classmethod
    def get_properties(self, assg):
        bd_path = assg.get_bd_path()
        if bd_path is None:
            bd_str = "-"
        else:
            bd_str = bd_path
        properties = []
        node   = assg.get_node()
        volume = assg.get_volume()
        properties.append(node.get_name())
        properties.append(volume.get_name())
        properties.append(bd_str)
        properties.append(str(assg.get_node_id()))
        properties.append(str(assg.get_cstate()))
        properties.append(str(assg.get_tstate()))
        return properties
    
    
    def get_node(self):
        return self._node
    
    
    def get_volume(self):
        return self._volume
    
    
    def get_blockdevice(self):
        return self._blockdevice
    
    
    def get_node_id(self):
        return self._node_id
    
    
    def get_cstate(self):
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
    
    
    def get_tstate(self):
        mr = self._machine_readable
        text = ""
        if self._tstate & Assignment.FLAG_DEPLOY != 0:
            text = state_text_append(mr, text, "DEPLOY", "deploy")
        if self._tstate & Assignment.FLAG_ATTACH != 0:
            text = state_text_append(mr, text, "ATTACH", "attach")
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
