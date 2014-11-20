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

import logging

from drbdmanage.exceptions import InvalidMinorNrException


class ConfFile(object):

    """
    Configuration file parser for drbdmanage configuration files

    Basic rules of the configuration file format:
      key = value free format
      leading spaces and tabs removed from key and value
      backslash ( \ ) acts as an escape character
        backslash followed by 'n' creates a newline character
        backslash followed by 't' creates a tab
        backslash followed by anything else creates the respective character
          (useful to intentionally add leading or trailing spaces)
      backslash at the end of a line extends the line with the contents of the
      following line
      lines starting with a hash sign as the first non-space, non-tab character
        are comment lines
    """

    _input = None


    def __init__(self, stream):
        self._input = stream


    def get_conf(self):
        in_file      = self._input
        split_idx    = self._split_idx
        unescape     = self._unescape
        extend_line  = self._extend_line
        comment_line = self._comment_line

        key  = None
        val  = None
        conf = {}

        while True:
            line = in_file.readline()
            if not len(line) > 0:
                break

            if line.endswith("\n"):
                line = line[:len(line) - 1]
            if key is None:
                # new key/val line
                # check for comment lines
                if comment_line(line):
                    continue
                idx = split_idx(line, '=')
                if idx != -1:
                    raw_key = line[:idx]
                    raw_val = line[idx + 1:]
                    key = unescape(raw_key)
                    val = unescape(raw_val)
                else:
                    # TODO: bad line, no key/val pair
                    continue
            else:
                # val continuation line
                val += unescape(line)
            if not extend_line(raw_val):
                conf[key] = val
                key = None
                val = None
        if key is not None:
            conf[key] = val
        return conf


    @classmethod
    def conf_defaults_merge(cls, conf_defaults, conf_loaded):
        """
        Overrides values from the default configuration with values from a
        configuration loaded from a configuration file, without importing
        any keys that had not been defined in the default configuration.
        The new configuration contains only keys from the default configuration.
        """
        conf = {}
        for key in conf_defaults.iterkeys():
            val = conf_loaded.get(key)
            if val is not None:
                conf[key] = val
            else:
                conf[key] = conf_defaults.get(key)
        return conf


    @classmethod
    def conf_defaults_union(cls, conf_defaults, conf_loaded):
        """
        Overrides values from the default configuration with values from a
        configuration loaded from a configuration file, and also load new
        key/value pairs into the new configuration.
        The new configuration contains all keys from the default configuration
        plus any keys defined by the configuration loaded from the configuration
        file.
        """
        conf = {}
        conf = cls.conf_defaults_merge(conf_defaults, conf_loaded)
        for key in conf_loaded.iterkeys():
            val = conf.get(key)
            if val is None:
                conf[key] = conf_loaded.get(key)
        return conf


    def _split_idx(self, line, s_char):
        """
        Returns the index of an unescaped occurrence of s_char in a string
        """
        lidx = 0
        idx  = 0
        split_idx = -1
        midx = len(line) - 1
        while idx != -1:
            bidx = line.find('\\', lidx)
            sidx = line.find(s_char, lidx)
            idx = self._min_idx(bidx, sidx)
            if idx != -1:
                fchar = line[idx]
                if fchar == '\\':
                    lidx = idx + 2
                elif fchar == s_char:
                    split_idx = sidx
                    break
                if lidx > midx:
                    break
        return split_idx


    def _comment_line(self, line):
        """
        Indicates whether a line is a comment line (True) or not (False)
        """
        fn_rc = False
        idx  = 0
        midx = len(line)
        while idx < midx:
            item = line[idx]
            if not (item == ' ' or item == '\t'):
                if item == '#':
                    fn_rc = True
                break
            idx += 1
        return fn_rc


    def _min_idx(self, x_idx, y_idx):
        """
        Returns the lesser value unless it is -1, or -1 if both are -1
        """
        if x_idx < y_idx:
            idx = x_idx if x_idx != -1 else y_idx
        else:
            idx = y_idx if y_idx != -1 else x_idx
        return idx


    def _unescape(self, line):
        """
        Resolves escape sequences, removes leading/trailing whitespaces
        """
        u_line = ""
        lidx = 0
        idx  = 0
        midx = len(line)
        # remove leading tabs and spaces
        while idx < midx:
            item = line[idx]
            if not (item == ' ' or item == '\t'):
                line = line[idx:]
                break
            idx += 1
        # replace escape sequences
        midx = len(line) - 1
        while idx != -1:
            idx = line.find('\\', lidx)
            if idx != -1:
                u_line += line[lidx:idx]
                if idx < midx:
                    cchar = line[idx + 1]
                    if cchar == 'n':
                        u_line += '\n'
                    elif cchar == 't':
                        u_line += '\t'
                    else:
                        u_line += cchar
                    lidx = idx + 2
                else:
                    # line ends with backslash, remove the backslash
                    lidx = len(line)
        # remove trailing spaces
        idx = lidx
        spaces = True
        while idx <= midx:
            item = line[idx]
            if spaces:
                if not (item == ' ' or item == '\t'):
                    spaces = False
                    u_line += line[lidx:idx]
                    lidx = idx
            else:
                if (item == ' ' or item == '\t'):
                    spaces = True
                    u_line += line[lidx:idx]
                    lidx = idx
            idx += 1
        if not spaces:
            u_line += line[lidx:]
        return u_line


    def _extend_line(self, line):
        """
        Indicates whether a line needs to be extended with the next line

        If a line ends with an unescaped backslash, it shall be extended
        with the next line
        """
        fn_rc   = False
        lidx = 0
        idx  = 0
        midx = len(line) - 1
        while idx != -1:
            idx = line.find("\\", lidx)
            if idx != -1:
                if idx >= midx:
                    fn_rc = True
                    break
                else:
                    lidx = idx + 2
            if lidx > midx:
                break
        return fn_rc


class DrbdAdmConf(object):

    KEY_SECRET = "secret"
    KEY_ADDRESS = "port"
    KEY_BDEV   = "blockdevice"

    def __init__(self):
        pass


    def write(self, stream, assignment, undeployed_flag):
        try:
            resource = assignment.get_resource()
            secret   = resource.get_secret()
            if secret is None:
                secret = ""

            # begin resource
            stream.write(
                "resource %s {\n"
                "    net {\n"
                "        cram-hmac-alg sha1;\n"
                "        shared-secret \"%s\";\n"
                "    }\n"
                % (resource.get_name(), secret)
            )

            # begin resource/nodes
            local_node = assignment.get_node()
            for assg in resource.iterate_assignments():
                if ((assg.get_tstate() & assg.FLAG_DEPLOY != 0) or
                    undeployed_flag):
                        node = assg.get_node()
                        stream.write(
                            "    on %s {\n"
                            "        node-id %s;\n"
                            "        address %s:%d;\n"
                            % (node.get_name(), assg.get_node_id(),
                               node.get_addr(), resource.get_port())
                        )
                        for vol_state in assg.iterate_volume_states():
                            tstate = vol_state.get_tstate()
                            if (tstate & vol_state.FLAG_DEPLOY) != 0:
                                volume  = vol_state.get_volume()
                                minor   = volume.get_minor()
                                if minor is None:
                                    raise InvalidMinorNrException
                                bd_path = vol_state.get_bd_path()
                                if bd_path is None:
                                    if node is local_node:
                                        # If the local node has no
                                        # backend storage, configure it as
                                        # a DRBD client
                                        bd_path = "none"
                                    else:
                                        # If a remote node has no
                                        # backend storage (probably because it
                                        # is not deployed yet), pretend that
                                        # there is backend storage on that
                                        # node. This should prevent a
                                        # situation where drbdadm refuses to
                                        # adjust the configuration because
                                        # none of the nodes seems to have some
                                        # backend storage
                                        bd_path = "/dev/null"
                                stream.write(
                                    "        volume %d {\n"
                                    "            device minor %d;\n"
                                    "            disk %s;\n"
                                    "            meta-disk internal;\n"
                                    "        }\n"
                                    % (volume.get_id(), minor.get_value(),
                                       bd_path)
                                )
                        stream.write("    }\n")
            # end resource/nodes

            # begin resource/connection
            stream.write(
                "    connection-mesh {\n"
                "        hosts"
            )
            for assg in resource.iterate_assignments():
                if ((assg.get_tstate() & assg.FLAG_DEPLOY != 0) or
                    undeployed_flag):
                        node = assg.get_node()
                        stream.write(" %s" % (node.get_name()))
            stream.write(";\n")
            stream.write(
                "        net {\n"
                "            protocol C;\n"
                "        }\n"
            )
            stream.write("    }\n")
            # end resource/connection

            stream.write("}\n")
            # end resource
        except InvalidMinorNrException:
            logging.critical("DrbdAdmConf: Volume configuration has no "
                             "MinorNr object")
        except Exception as exc:
            logging.error("Cannot generate configuration file, "
                          "unhandled exception: %s" % str(exc))


    def write_excerpt(self, stream, assignment, nodes, vol_states):
        """
        Writes an excerpt of the configuration file to a stream.
        Used for adjusting resources to an intermediate state
        (a state somewhere between current state and target state),
        for example, when deploying or undeploying multiple volumes
        or when removing or deploying peer nodes.
        In all of these cases, not-yet-deployed or not-yet-undeployed
        objects may be selected to be included or excluded from the
        configuration passed to drbdadm in order to adjust the DRBD
        resource's state while multiple operations are in progress
        (some of which may have failed, too, and the corresponding
        objects excluded from the configuration for that reason).
        """
        try:
            resource = assignment.get_resource()
            secret   = resource.get_secret()
            if secret is None:
                secret = ""

            # begin resource
            stream.write(
                "resource %s {\n"
                "    net {\n"
                "        cram-hmac-alg sha1;\n"
                "        shared-secret \"%s\";\n"
                "    }\n"
                % (resource.get_name(), secret)
            )

            # begin resource/nodes
            local_node = assignment.get_node()
            for node in nodes:
                assg = node.get_assignment(resource.get_name())
                if assg is not None:
                    stream.write(
                        "    on %s {\n"
                        "        node-id %s;\n"
                        "        address %s:%d;\n"
                        % (node.get_name(), assg.get_node_id(),
                           node.get_addr(), resource.get_port())
                    )
                    # begin resource/nodes/volumes
                    node_vol_states = vol_states.get(node.get_name())
                    for vol_state in node_vol_states:
                        volume  = vol_state.get_volume()
                        minor   = volume.get_minor()
                        if minor is None:
                            raise InvalidMinorNrException
                        bd_path = vol_state.get_bd_path()
                        if bd_path is None:
                            if node is local_node:
                                # If the local node has no backend storage,
                                # configure it as a DRBD client
                                bd_path = "none"
                            else:
                                # If a remote node has no backend storage
                                # (probably because it is not deployed yet),
                                # pretend that there is backend storage
                                # on that node. This should prevent a
                                # situation where drbdadm refuses to adjust
                                # the configuration because none of the nodes
                                # seems to have some backend storage
                                bd_path = "/dev/null"

                        stream.write(
                            "        volume %d {\n"
                            "            device minor %d;\n"
                            "            disk %s;\n"
                            "            meta-disk internal;\n"
                            "        }\n"
                            % (volume.get_id(), minor.get_value(),
                               bd_path)
                        )
                    # end resource/nodes/volumes
                    stream.write("    }\n")
            # end resource/nodes

            # begin resource/connection
            stream.write(
                "    connection-mesh {\n"
                "        hosts"
            )
            for node in nodes:
                stream.write(" %s" % (node.get_name()))
            stream.write(";\n")
            stream.write(
                "        net {\n"
                "            protocol C;\n"
                "        }\n"
            )
            stream.write("    }\n")
            # end resource/connection

            stream.write("}\n")
            # end resource
        except InvalidMinorNrException:
            logging.critical("DrbdAdmConf: Volume configuration has no "
                             "MinorNr object")
        except Exception as exc:
            logging.error("Cannot generate configuration file, "
                          "unhandled exception: %s" % str(exc))


    def read_drbdctrl_params(self, stream):
        # parameters that contain specific information
        # FIXME: these parameters should be read from the section of the
        #        current node
        params = [
            ["shared-secret",   DrbdAdmConf.KEY_SECRET],
            ["disk",            DrbdAdmConf.KEY_BDEV],
            ["address",         DrbdAdmConf.KEY_ADDRESS]
        ]
        fields   = {}
        while True:
            confline = stream.readline()
            if len(confline) == 0:
                break
            for keypair in params:
                p_name = keypair[0]
                confline = confline.lstrip()
                if confline.startswith(p_name):
                    confline = confline[len(p_name):]
                    fields[keypair[1]] = self._extract_field(confline)
        return fields


    def _extract_field(self, value):
        value = value.strip()
        idx = value.find("\"")
        if idx != -1:
            value = value[idx + 1:]
            idx = value.find("\"")
            if idx != -1:
                value = value[:idx]
        elif value.endswith(";"):
            value = value[:len(value) - 1]
        return value


    def write_drbdctrl(self, stream, nodes, bdev, port, secret):
        stream.write(
            "resource .drbdctrl {\n"
            "    net {\n"
            "        cram-hmac-alg   sha256;\n"
            "        shared-secret   \"" + secret + "\";\n"
            "    }\n"
            "    volume 0 {\n"
            "        device      minor 0;\n"
            "        disk        " + bdev + ";\n"
            "        meta-disk   internal;\n"
            "    }\n"
        )
        if len(nodes) > 0:
            for node in nodes.itervalues():
                stream.write(
                    "    on " + node.get_name() + " {\n"
                    "        node-id     " + str(node.get_node_id())
                    + ";\n"
                    "        address     " + node.get_addr()
                    + ":" + str(port) + ";\n"
                    "    }\n"
                )
            stream.write(
                "    connection-mesh {\n"
                "        hosts"
            )
            for node in nodes.itervalues():
                stream.write(" " + node.get_name())
            stream.write(
                ";\n"
                "        net {\n"
                "            protocol C;\n"
                "        }\n"
                "    }\n"
            )
        stream.write("}\n")
