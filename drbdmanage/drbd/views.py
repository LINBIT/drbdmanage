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

import drbdmanage.consts as consts

"""
WARNING!
  do not import anything from drbdmanage.drbd.persistence
"""
import drbdmanage.exceptions as exc
import drbdmanage.utils as utils


class GenericView(object):

    """
    Base class for more specialized View objects
    """

    STATE_NORM  = 0
    STATE_WARN  = 1
    STATE_ALERT = 2

    _props = None

    _level = STATE_NORM

    _state_text   = ""
    _pending_text = ""

    def __init__(self, props):
        self._props = props


    def set_property(self, key, val):
        self._props[str(key)] = str(val)


    def get_property(self, key):
        if self._props is not None:
            try:
                val = self._props.get(str(key))
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
                if utils.string_to_bool(self.get_property(flag_name)):
                    if text_true is not None:
                        text_list.append(text_true)
                else:
                    if text_false is not None:
                        text_list.append(text_false)
            except ValueError:
                if text_unkn is not None:
                    text_list.append(text_unkn)
        return str(sepa.join(text_list))


    def raise_level(self, requested):
        """
        Conditionally raises and returns the current warning level
        """
        if (self._level == GenericView.STATE_NORM or
            self._level == GenericView.STATE_WARN):
            if (requested == GenericView.STATE_NORM or
                requested == GenericView.STATE_WARN or
                requested == GenericView.STATE_ALERT):
                    if requested > self._level:
                        self._level = requested
            else:
                # If the requested level is invalid, raise the
                # warning levelto alert
                self._level = GenericView.STATE_ALERT

    def get_level(self):
        return self._level


    def add_pending_text(self, text):
        if len(self._pending_text) == 0:
            self._pending_text = "pending actions: " + text
        else:
            self._pending_text += ", " + text


    def add_state_text(self, text):
        if len(self._state_text) == 0:
            self._state_text += text
        else:
            self._state_text += ", " + text


    def format_state_info(self):
        text = self._state_text
        if len(text) > 0:
            if len(self._pending_text) > 0:
                text += ", " + self._pending_text
        else:
            if len(self._pending_text) > 0:
                text = self._pending_text
            else:
                text = "ok"
        return text


    def state_info(self):
        return GenericView.STATE_ALERT, "no state information available"


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
         consts.FLAG_UPD_CON,    None,       None],
        [consts.TSTATE_PREFIX + consts.FLAG_UPD_CONFIG,
         consts.FLAG_UPD_CONFIG, None,       None]
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
         "u",     "-",    "?"],
        [consts.TSTATE_PREFIX + consts.FLAG_UPD_CONFIG,
         "C",     "-",    "?"]
    ]


    def __init__(self, properties, machine_readable):
        try:
            super(AssignmentView, self).__init__(properties)
            self._node     = properties[consts.NODE_NAME]
            self._resource = properties[consts.RES_NAME]
        except KeyError:
            raise exc.IncompatibleDataException
        self._machine_readable = machine_readable


    def state_info(self):
        c_connect = utils.string_to_bool(
            self.get_property(consts.CSTATE_PREFIX + consts.FLAG_CONNECT)
        )
        c_deploy = utils.string_to_bool(
            self.get_property(consts.CSTATE_PREFIX + consts.FLAG_DEPLOY)
        )
        t_connect = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_CONNECT)
        )
        t_deploy = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_DEPLOY)
        )
        t_diskless = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_DISKLESS)
        )

        a_discard = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_DISCARD)
        )
        a_overwrite = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_OVERWRITE)
        )
        a_reconnect = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_RECONNECT)
        )
        a_upd_con = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_UPD_CON)
        )
        a_upd_config = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_UPD_CONFIG)
        )

        if (not c_deploy) and (not t_deploy):
            self.add_pending_text("cleanup")
        elif c_deploy and (not t_deploy):
            self.add_pending_text("decommission")
            self.raise_level(GenericView.STATE_ALERT)
        elif (not c_deploy) and t_deploy:
            self.add_pending_text("commission")
            self.raise_level(GenericView.STATE_WARN)

        if (not c_connect) and (not t_connect):
            self.add_state_text("disconnected")
            self.raise_level(GenericView.STATE_WARN)
        elif c_connect and (not t_connect) and t_deploy:
            self.add_pending_text("disconnect")
            self.raise_level(GenericView.STATE_WARN)
        elif (not c_connect) and t_connect and c_deploy and t_deploy:
            self.add_pending_text("connect")
            self.raise_level(GenericView.STATE_WARN)

        if t_diskless:
            self.add_state_text("client")

        if a_discard and t_deploy:
            self.add_state_text("discard data")
            self.raise_level(GenericView.STATE_ALERT)
        if a_overwrite and t_deploy:
            self.add_state_text("overwrite peers")
            self.raise_level(GenericView.STATE_ALERT)
        if a_reconnect and t_deploy:
            self.add_pending_text("cycle connections")
            self.raise_level(GenericView.STATE_WARN)
        if a_upd_con and t_deploy:
            self.add_pending_text("adjust connections")
            self.raise_level(GenericView.STATE_WARN)
        if a_upd_config and t_deploy:
            self.add_pending_text("adjust configuration")
            self.raise_level(GenericView.STATE_WARN)

        return self.get_level(), self.format_state_info()


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
         consts.FLAG_UPDATE,     None,   None],
        [consts.TSTATE_PREFIX + consts.FLAG_DRBDCTRL,
         consts.FLAG_DRBDCTRL,   None ,  None],
        [consts.TSTATE_PREFIX + consts.FLAG_STORAGE,
         consts.FLAG_STORAGE,    None,   None],
        [consts.TSTATE_PREFIX + consts.FLAG_QIGNORE,
         consts.FLAG_QIGNORE,    None,   None],
        [consts.TSTATE_PREFIX + consts.FLAG_STANDBY,
         consts.FLAG_STANDBY,    None,   None]
    ]

    # Human readable texts for target state flags
    HR_TSTATE_TEXTS = [
        [consts.TSTATE_PREFIX + consts.FLAG_REMOVE,
         "r",     "-",    "?"],
        [consts.TSTATE_PREFIX + consts.FLAG_UPD_POOL,
         "p",     "-",    "?"],
        [consts.TSTATE_PREFIX + consts.FLAG_UPDATE,
         "u",     "-",    "?"],
        [consts.TSTATE_PREFIX + consts.FLAG_DRBDCTRL,
         "C",     "-",    "?"],
        [consts.TSTATE_PREFIX + consts.FLAG_STORAGE,
         "S",     "-",    "?"],
        [consts.TSTATE_PREFIX + consts.FLAG_STANDBY,
         "X",     "-",    "?"],
        [consts.TSTATE_PREFIX + consts.FLAG_QIGNORE,
         "Q",     "-",    "?"]
    ]


    def __init__(self, properties, machine_readable):
        try:
            super(DrbdNodeView, self).__init__(properties)
            self._name = properties[consts.NODE_NAME]
        except KeyError:
            raise exc.IncompatibleDataException
        self._machine_readable = machine_readable


    @classmethod
    def get_name_maxlen(cls):
        return consts.NODE_NAME_MAXLEN


    def state_info(self):
        s_remove = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_REMOVE)
        )
        s_upd_pool = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_UPD_POOL)
        )
        s_update = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_UPDATE)
        )
        s_drbdctrl = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_DRBDCTRL)
        )
        s_storage = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_STORAGE)
        )
        s_standby = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_STANDBY)
        )
        s_qignore = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_QIGNORE)
        )
        i_offline = False
        i_offline_str = self.get_property(consts.IND_NODE_OFFLINE)
        if i_offline_str is not None:
            i_offline = utils.string_to_bool(i_offline_str)

        if s_remove:
            self.add_pending_text("remove")
            self.raise_level(GenericView.STATE_ALERT)
        else:
            if i_offline:
                if s_qignore:
                    self.raise_level(GenericView.STATE_WARN)
                    self.add_state_text("offline/quorum vote ignored")
                else:
                    self.raise_level(GenericView.STATE_ALERT)
                    self.add_state_text("OFFLINE")
            else:
                # Online, but quorum vote ignored; this should be resolved
                # automatically by the server
                if s_qignore:
                    self.raise_level(GenericView.STATE_WARN)
                    self.add_state_text("online/quorum vote ignored")
            if s_update:
                self.add_pending_text("adjust connections")
                self.raise_level(GenericView.STATE_ALERT)
            if s_upd_pool:
                self.raise_level(GenericView.STATE_WARN)
                self.add_pending_text("check space")
            if not s_drbdctrl:
                self.add_state_text("satellite node")
            if not s_storage:
                self.add_state_text("no storage")
            if s_standby:
                self.raise_level(GenericView.STATE_WARN)
                self.add_state_text("standby")

        return self.get_level(), self.format_state_info()

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
            raise exc.IncompatibleDataException
        self.props = properties
        self._machine_readable = machine_readable


    @classmethod
    def get_name_maxlen(cls):
        return consts.RES_NAME_MAXLEN


    def state_info(self):
        s_remove = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_REMOVE)
        )

        if s_remove:
            self.add_pending_text("remove")
            self.raise_level(GenericView.STATE_ALERT)

        return self.get_level(), self.format_state_info()


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
            raise exc.IncompatibleDataException
        self._machine_readable = machine_readable


    def get_id(self):
        return self._id


    def get_size_kiB(self):
        return self._size_kiB


    def state_info(self):
        s_remove = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_REMOVE)
        )

        if s_remove:
            self.add_pending_text("remove")
            self.raise_level(GenericView.STATE_ALERT)

        return self.get_level(), self.format_state_info()


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
            raise exc.IncompatibleDataException
        self._machine_readable = machine_readable


    def get_id(self):
        return self._id


    def state_info(self):
        c_deploy = utils.string_to_bool(
            self.get_property(consts.CSTATE_PREFIX + consts.FLAG_DEPLOY)
        )
        c_attach = utils.string_to_bool(
            self.get_property(consts.CSTATE_PREFIX + consts.FLAG_ATTACH)
        )
        t_deploy = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_DEPLOY)
        )
        t_attach = utils.string_to_bool(
            self.get_property(consts.TSTATE_PREFIX + consts.FLAG_ATTACH)
        )

        if (not c_deploy) and (not t_deploy):
            self.add_pending_text("cleanup")
        elif c_deploy and (not t_deploy):
            self.add_pending_text("decommission")
            self.raise_level(GenericView.STATE_ALERT)
        elif (not c_deploy) and t_deploy:
            self.add_pending_text("commission")
            self.raise_level(GenericView.STATE_WARN)

        if c_attach and (not t_attach) and t_deploy:
            self.add_pending_text("detach")
            self.raise_level(GenericView.STATE_WARN)
        elif (not c_attach) and t_attach and t_deploy:
            self.add_pending_text("attach")
            self.raise_level(GenericView.STATE_WARN)

        return self.get_level(), self.format_state_info()


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
