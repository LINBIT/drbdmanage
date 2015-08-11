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

import logging
import drbdmanage.utils as dmutils

from drbdmanage.exceptions import InvalidMinorNrException
from drbdmanage.propscontainer import PropsContainer

is_set   = dmutils.is_set
is_unset = dmutils.is_unset


class DrbdAdmConf(object):

    KEY_SECRET = "secret"
    KEY_ADDRESS = "port"
    KEY_BDEV = "blockdevice"

    def __init__(self, objects_root):
        self.indentwidth = 3
        self.objects_root = objects_root

    def _get_setup_props(self, item, subnamespace):
        """
        Returns the props for this item for the given subnamespace on
        success, otherwise 0. Return value is something len() can handle
        """
        import os
        if subnamespace.startswith('/'):
            subnamespace = subnamespace[1:]

        try:
            props = item.get_props()
            if props:
                # this part of drbdmanage is GNU/Linux only and our namespaces
                # follow a unix-like path structure, therfore os.path.join is
                # the way to go
                ns = os.path.join(PropsContainer.NAMESPACES["setupopt"], subnamespace)
                ns = os.path.normpath(ns) + '/'
                opts = props.get_all_props(ns)
        except:
            return {}

        return opts if len(opts) else {}

    def _write_section(self, section, curstream, opts, indentlevel=0):
        # opts are k, v pairs

        if len(opts):
            curstream.write("%s%s {\n" % (' ' * indentlevel * self.indentwidth,
                                          section))

            spaces = ' ' * self.indentwidth * (indentlevel + 1)
            for k, v in opts.iteritems():
                curstream.write("%s %s %s;\n" % (spaces, k, v))

            curstream.write("%s}\n" % (' ' * indentlevel * self.indentwidth))

    def _write_global_stream(self, globalstream):
        wrote_global = False

        if self.objects_root:
            common = self.objects_root["common"]
            if common:
                diskopts = self._get_setup_props(common, "/disko/")
                peerdiskopts = self._get_setup_props(common, "/peerdisko/")
                for k, v in peerdiskopts.items():
                    diskopts[k] = v
                netopts = self._get_setup_props(common, "/neto/")
                if globalstream:
                    globalstream.write('common {\n')
                    if diskopts or netopts:
                        if diskopts:
                            self._write_section('disk', globalstream, diskopts, 1)
                        if netopts:
                            self._write_section('net', globalstream, netopts, 1)
                    else:
                        globalstream.write('# currently empty\n')
                    globalstream.write('}\n')
                    wrote_global = True
                    globalstream.close()

        return wrote_global

# TODO: only have a single writer that returns a template and a dict of substitutions,
# and use that in all the functions below.
    def write(self, stream, assignment, undeployed_flag, globalstream=False):
        try:
            wrote_global = self._write_global_stream(globalstream)

            resource = assignment.get_resource()
            secret = resource.get_secret()
            if secret is None:
                secret = ""

            # begin resource
            stream.write("resource %s {\n" % (resource.get_name()))

            if wrote_global:
                stream.write('template-file "%s";\n\n' % (globalstream.name))

            # begin resource/net-options
            netopts = self._get_setup_props(resource, "neto/")
            netopts['cram-hmac-alg'] = 'sha1'
            netopts['shared-secret'] = '"%s"' % (secret)
            self._write_section('net', stream, netopts, 1)

            resopts = self._get_setup_props(resource, "/reso/")
            self._write_section('options', stream, resopts, 1)

            # begin resource/disk options
            diskopts = self._get_setup_props(resource, "/disko/")
            peerdiskopts = self._get_setup_props(resource, "/peerdisko/")
            for k, v in peerdiskopts.items():
                diskopts[k] = v
            self._write_section('disk', stream, diskopts, 1)
            # end resource/disk options

            # begin resource/nodes
            local_node = assignment.get_node()
            for assg in resource.iterate_assignments():
                diskless = is_set(assg.get_tstate(), assg.FLAG_DISKLESS)
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
                                volume = vol_state.get_volume()
                                minor = volume.get_minor()
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
                                    % (volume.get_id(), minor.get_value(),
                                       bd_path)
                                )
                                if not diskless:
                                    diskopts = self._get_setup_props(volume, "/disko/")
                                    peerdiskopts = self._get_setup_props(volume, "/peerdisko/")
                                    for k, v in peerdiskopts.items():
                                        diskopts[k] = v
                                    diskopts['size'] = str(volume.get_size_kiB()) + 'k'
                                    self._write_section('disk', stream, diskopts, 4)
                                    # end volume/disk options
                                stream.write(
                                    "            meta-disk internal;\n"
                                    "        }\n"
                                )
                        stream.write("    }\n")
            # end resource/nodes

            # begin resource/connection
            servers = []
            clients = []
            for assg in resource.iterate_assignments():
                tstate = assg.get_tstate()
                if (is_set(tstate, assg.FLAG_DEPLOY) or
                    undeployed_flag):
                        node = assg.get_node()
                        if is_unset(tstate, assg.FLAG_DISKLESS):
                            servers.append(node)
                        else:
                            clients.append(node)

            if len(servers) > 0:
                stream.write(
                    "    connection-mesh {\n"
                    "        hosts"
                )
                for node in servers:
                    stream.write(" %s" % (node.get_name()))
                stream.write(";\n")
                stream.write("    }\n")

            # connect each client to every server, but not to other clients
            for client_node in clients:
                client_name = client_node.get_name()
                for server_node in servers:
                    stream.write(
                        "    connection {\n"
                        "        host %s;\n"
                        "        host %s;\n"
                        "    }\n"
                        % (client_name, server_node.get_name())
                    )
            # end resource/connection

            stream.write("}\n")
            # end resource
        except InvalidMinorNrException:
            logging.critical("DrbdAdmConf: Volume configuration has no "
                             "MinorNr object")
        except Exception as exc:
            logging.error("Cannot generate configuration file, "
                          "unhandled exception: %s" % str(exc))


    def write_excerpt(self, stream, assignment, nodes, vol_states, globalstream=False):
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
            wrote_global = self._write_global_stream(globalstream)

            resource = assignment.get_resource()
            secret   = resource.get_secret()
            if secret is None:
                secret = ""

            # begin resource
            stream.write("resource %s {\n" % (resource.get_name()))

            if wrote_global:
                stream.write('template-file "%s";\n\n' % (globalstream.name))

            # begin resource/net-options
            netopts = self._get_setup_props(resource, "neto/")
            netopts['cram-hmac-alg'] = 'sha1'
            netopts['shared-secret'] = '"%s"' % (secret)
            self._write_section('net', stream, netopts, 1)

            resopts = self._get_setup_props(resource, "/reso/")
            self._write_section('options', stream, resopts, 1)

            # begin resource/disk options
            diskopts = self._get_setup_props(resource, "/disko/")
            self._write_section('disk', stream, diskopts, 1)
            # end resource/disk options

            # begin resource/nodes
            local_node = assignment.get_node()
            clients = []
            servers = []
            for node in nodes:
                assg = node.get_assignment(resource.get_name())
                if assg is not None:
                    diskless = is_set(assg.get_tstate(), assg.FLAG_DISKLESS)
                    if not diskless:
                        servers.append(node)
                    else:
                        clients.append(node)
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
                            % (volume.get_id(), minor.get_value(),
                               bd_path)
                        )
                        if not diskless:
                            diskopts = self._get_setup_props(volume, "/disko/")
                            diskopts['size'] = str(volume.get_size_kiB()) + 'k'
                            self._write_section('disk', stream, diskopts, 4)
                        stream.write(
                            "            meta-disk internal;\n"
                            "        }\n"
                        )
                    # end resource/nodes/volumes
                    stream.write("    }\n")
            # end resource/nodes

            # If any hosts are left in the configuration, generate the
            # connection mesh section
            if len(servers) > 0:
                # begin resource/connection
                stream.write(
                    "    connection-mesh {\n"
                    "        hosts"
                )
                for server_node in servers:
                    stream.write(" %s" % (server_node.get_name()))
                stream.write(";\n")
                stream.write("    }\n")

                # connect each client to every server, but not to other clients
                for client_node in clients:
                    client_name = client_node.get_name()
                    for server_node in servers:
                        stream.write(
                            "    connection {\n"
                            "        host %s;\n"
                            "        host %s;\n"
                            "    }\n"
                            % (client_name, server_node.get_name())
                        )
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
        for confline in dmutils.read_lines(stream):
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
            "        cram-hmac-alg       sha256;\n"
            "        shared-secret       \"" + secret + "\";\n"
            "        allow-two-primaries no;\n"
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
