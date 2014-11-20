#!/usr/bin/python
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

import drbdmanage.consts as consts

"""
WARNING!
  do not import anything from drbdmanage.drbd.persistence
"""
from drbdmanage.exceptions import IncompatibleDataException
from drbdmanage.utils import string_to_bool


class GenericView(object):

    """
    Base class for more specialized View objects
    """

    props = None


    def __init__(self, props):
        self.props = props


    def set_property(self, key, val):
        self.props[str(key)] = str(val)


    def get_property(self, key):
        if self.props is not None:
            try:
                val = self.props.get(str(key))
                if val is not None:
                    val = str(val)
            except (ValueError, TypeError):
                val = None
            return val
        else:
            return None


    def state_text(self, flags_texts, sepa):
        text_list = []
        for item in flags_texts:
            flag_name, text_true, text_false, text_unkn = item
            try:
                if string_to_bool(self.get_property(flag_name)):
                    if text_true is not None:
                        text_list.append(text_true)
                else:
                    if text_false is not None:
                        text_list.append(text_false)
            except ValueError:
                if text_unkn is not None:
                    text_list.append(text_unkn)
        return str(sepa.join(text_list))


class AssignmentView(GenericView):

    """
    Formats Assignment objects for human- or machine-readable output

    This class is used by the drbdmanage server to serialize assignment list
    entries. The drbdmanage client uses this class to deserialize and display
    the information received from the drbdmanage server.
    """

    _node         = None
    _resource     = None

    _machine_readable = False

    # Machine readable texts for current state flags
    MR_CSTATE_TEXTS = [
        [consts.CSTATE_PREFIX + consts.FLAG_CONNECT,
         consts.FLAG_CONNECT,    None,       None],
        [consts.CSTATE_PREFIX + consts.FLAG_DEPLOY,
         consts.FLAG_DEPLOY,     None,       None],
        [consts.CSTATE_PREFIX + consts.FLAG_DISKLESS,
         consts.FLAG_DISKLESS,   None,       None]
    ]

    # Machine readable texts for target state flags
    MR_TSTATE_TEXTS = [
        [consts.TSTATE_PREFIX + consts.FLAG_CONNECT,
         consts.FLAG_CONNECT,    None,       None],
        [consts.TSTATE_PREFIX + consts.FLAG_DEPLOY,
         consts.FLAG_DEPLOY,     None,       None],
        [consts.TSTATE_PREFIX + consts.FLAG_DISKLESS,
         consts.FLAG_DISKLESS,   None,       None],
        [consts.TSTATE_PREFIX + consts.FLAG_DISCARD,
         consts.FLAG_DISCARD,    None,       None],
        [consts.TSTATE_PREFIX + consts.FLAG_OVERWRITE,
         consts.FLAG_OVERWRITE,  None,       None],
        [consts.TSTATE_PREFIX + consts.FLAG_RECONNECT,
         consts.FLAG_RECONNECT,  None,       None],
        [consts.TSTATE_PREFIX + consts.FLAG_UPD_CON,
         consts.FLAG_UPD_CON,    None,       None]
    ]

    # Human readable texts for current state flags
    HR_CSTATE_TEXTS = [
        [consts.CSTATE_PREFIX + consts.FLAG_CONNECT,
         "c",     "-",    "?"],
        [consts.CSTATE_PREFIX + consts.FLAG_DEPLOY,
         "d",     "-",    "?"],
        [consts.CSTATE_PREFIX + consts.FLAG_DISKLESS,
         "D",     "-",    "?"]
    ]

    # Human readable texts for target state flags (without action flags)
    HR_TSTATE_TEXTS = [
        [consts.TSTATE_PREFIX + consts.FLAG_CONNECT,
         "c",     "-",    "?"],
        [consts.TSTATE_PREFIX + consts.FLAG_DEPLOY,
         "d",     "-",    "?"],
        [consts.TSTATE_PREFIX + consts.FLAG_DISKLESS,
         "D",     "-",    "?"]
    ]

    # Human readable texts for target state action flags
    HR_ACTFLAGS_TEXTS = [
        [consts.TSTATE_PREFIX + consts.FLAG_DISCARD,
         "d",     "-",    "?"],
        [consts.TSTATE_PREFIX + consts.FLAG_OVERWRITE,
         "o",     "-",    "?"],
        [consts.TSTATE_PREFIX + consts.FLAG_RECONNECT,
         "r",     "-",    "?"],
        [consts.TSTATE_PREFIX + consts.FLAG_UPD_CON,
         "u",     "-",    "?"]
    ]


    def __init__(self, properties, machine_readable):
        try:
            super(AssignmentView, self).__init__(properties)
            self._node     = properties[consts.NODE_NAME]
            self._resource = properties[consts.RES_NAME]
        except KeyError:
            raise IncompatibleDataException
        self._machine_readable = machine_readable


    def get_cstate(self):
        if self._machine_readable:
            text = self.state_text(self.MR_CSTATE_TEXTS, "|")
        else:
            text = self.state_text(self.HR_CSTATE_TEXTS, "")
        return text


    def get_tstate(self):
        if self._machine_readable:
            text = self.state_text(self.MR_TSTATE_TEXTS, "|")
        else:
            text = self.state_text(self.HR_TSTATE_TEXTS, "")
            text += " ("
            text += self.state_text(self.HR_ACTFLAGS_TEXTS, "")
            text += ")"
        return text


class DrbdNodeView(GenericView):

    """
    Formats DrbdNode objects for human- or machine-readable output

    This class is used by the drbdmanage server to serialize node list entries.
    The drbdmanage client uses this class to deserialize and display the
    information received from the drbdmanage server.
    """

    _name     = None

    _machine_readable = False

    # Machine readable texts for target state flags
    MR_TSTATE_TEXTS = [
        [consts.TSTATE_PREFIX + consts.FLAG_REMOVE,
         consts.FLAG_REMOVE,     None,   None],
        [consts.TSTATE_PREFIX + consts.FLAG_UPD_POOL,
         consts.FLAG_UPD_POOL,   None,   None],
        [consts.TSTATE_PREFIX + consts.FLAG_UPDATE,
         consts.FLAG_UPDATE,     None,   None]

    ]

    # Human readable texts for target state flags
    HR_TSTATE_TEXTS = [
        [consts.TSTATE_PREFIX + consts.FLAG_REMOVE,
         "r",     "-",    "?"],
        [consts.TSTATE_PREFIX + consts.FLAG_UPD_POOL,
         "p",     "-",    "?"],
        [consts.TSTATE_PREFIX + consts.FLAG_UPDATE,
         "u",     "-",    "?"]
    ]


    def __init__(self, properties, machine_readable):
        try:
            super(DrbdNodeView, self).__init__(properties)
            self._name = properties[consts.NODE_NAME]
        except KeyError:
            raise IncompatibleDataException
        self._machine_readable = machine_readable


    @classmethod
    def get_name_maxlen(cls):
        return consts.NODE_NAME_MAXLEN


    def get_state(self):
        if self._machine_readable:
            text = self.state_text(self.MR_TSTATE_TEXTS, "|")
        else:
            text = self.state_text(self.HR_TSTATE_TEXTS, "")
        return text


class DrbdResourceView(GenericView):

    """
    Formats DrbdResource objects for human- or machine-readable output

    This class is used by the drbdmanage server to serialize resource list
    entries for the resources and volumes list. The drbdmanage client uses this
    class to deserialize and display the information received from the
    drbdmanage server.
    """

    _name    = None
    props    = None

    _machine_readable = False

    # Machine readable texts for target state flags
    MR_TSTATE_TEXTS = [
        [consts.TSTATE_PREFIX + consts.FLAG_REMOVE,
         consts.FLAG_REMOVE,     None,   None]
    ]

    # Human readable texts for target state flags
    HR_TSTATE_TEXTS = [
        [consts.TSTATE_PREFIX + consts.FLAG_REMOVE,
         "r",     "-",    "?"]
    ]


    def __init__(self, properties, machine_readable):
        try:
            super(DrbdResourceView, self).__init__(properties)
            self._name = properties[consts.RES_NAME]
        except KeyError:
            raise IncompatibleDataException
        self.props = properties


    @classmethod
    def get_name_maxlen(cls):
        return consts.RES_NAME_MAXLEN


    def get_state(self):
        if self._machine_readable:
            text = self.state_text(self.MR_TSTATE_TEXTS, "|")
        else:
            text = self.state_text(self.HR_TSTATE_TEXTS, "")
        return text


class DrbdVolumeView(GenericView):

    """
    Formats DrbdVolume objects for human- or machine-readable output

    This class is used by the drbdmanage server to serialize volume list
    entries for the resources and volumes list. The drbdmanage client uses this
    class to deserialize and display the information received from the
    drbdmanage server.
    """

    _id       = None
    _size_kiB = None

    _machine_readable = False

    # Machine readable texts for target state flags
    MR_TSTATE_TEXTS = [
        [consts.TSTATE_PREFIX + consts.FLAG_REMOVE,
         consts.FLAG_REMOVE,     None,   None]
    ]

    # Human readable texts for target state flags
    HR_TSTATE_TEXTS = [
        [consts.TSTATE_PREFIX + consts.FLAG_REMOVE,
         "r",     "-",    "?"]
    ]


    def __init__(self, properties, machine_readable):
        try:
            super(DrbdVolumeView, self).__init__(properties)
            self._id = properties[consts.VOL_ID]
            self._size_kiB = long(properties[consts.VOL_SIZE])
        except (KeyError, ValueError):
            raise IncompatibleDataException
        self._machine_readable = machine_readable


    def get_id(self):
        return self._id


    def get_size_kiB(self):
        return self._size_kiB


    def get_state(self):
        if self._machine_readable:
            text = self.state_text(self.MR_TSTATE_TEXTS, "|")
        else:
            text = self.state_text(self.HR_TSTATE_TEXTS, "")
        return text


class DrbdVolumeStateView(GenericView):

    """
    Formats DrbdVolumeState objects for human- or machine-readable output

    This class is used by the drbdmanage server to serialize volume state list
    entries for the assignments view. The drbdmanage client uses this class
    to deserialize and display the information received from the drbdmanage
    server.
    """

    _id      = None

    _machine_readable = False

    # Machine readable texts for current state flags
    MR_CSTATE_TEXTS = [
        [consts.CSTATE_PREFIX + consts.FLAG_ATTACH,
         consts.FLAG_ATTACH,     None,   None],
        [consts.CSTATE_PREFIX + consts.FLAG_DEPLOY,
         consts.FLAG_DEPLOY,     None,   None]
    ]

    # Human readable texts for current state flags
    HR_CSTATE_TEXTS = [
        [consts.CSTATE_PREFIX + consts.FLAG_ATTACH,
         "a",     "-",    "?"],
        [consts.CSTATE_PREFIX + consts.FLAG_DEPLOY,
         "d",     "-",    "?"]
    ]

    # Machine readable texts for target state flags
    MR_TSTATE_TEXTS = [
        [consts.TSTATE_PREFIX + consts.FLAG_ATTACH,
         consts.FLAG_ATTACH,     None,   None],
        [consts.TSTATE_PREFIX + consts.FLAG_DEPLOY,
         consts.FLAG_DEPLOY,     None,   None]
    ]

    # Human readable texts for target state flags
    HR_TSTATE_TEXTS = [
        [consts.TSTATE_PREFIX + consts.FLAG_ATTACH,
         "a",     "-",    "?"],
        [consts.TSTATE_PREFIX + consts.FLAG_DEPLOY,
         "d",     "-",    "?"]
    ]


    def __init__(self, properties, machine_readable):
        try:
            super(DrbdVolumeStateView, self).__init__(properties)
            self._id = properties[consts.VOL_ID]
        except KeyError:
            raise IncompatibleDataException
        self._machine_readable = machine_readable


    def get_id(self):
        return self._id


    def get_cstate(self):
        if self._machine_readable:
            text = self.state_text(self.MR_CSTATE_TEXTS, "|")
        else:
            text = self.state_text(self.HR_CSTATE_TEXTS, "")
        return text


    def get_tstate(self):
        if self._machine_readable:
            text = self.state_text(self.MR_TSTATE_TEXTS, "|")
        else:
            text = self.state_text(self.HR_TSTATE_TEXTS, "")
        return text
