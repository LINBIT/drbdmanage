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

import drbdmanage.consts as consts

from drbdmanage.exceptions import InvalidMinorNrException
from drbdmanage.propscontainer import PropsContainer

is_set   = dmutils.is_set
is_unset = dmutils.is_unset


class DrbdConnectionConf(object):
    def __init__(self, servers, clients, objects_root, stream, target_node=None):
        self.objects_root = objects_root
        self.servers = servers
        self.clients = clients
        self.stream = stream
        self.target_node = target_node

        self._all_nodes = set(servers + clients)
        self._meshes = []  # list of sets, every set is list of nodes, a set represents a mesh
        self._nodes_interesting = set()  # nodes we have a connection to (+ self)

        self._indentwidth = 3

    def _is_part_of_site(self, node):
        ns = PropsContainer.NAMESPACES[PropsContainer.KEY_DMCONFIG]
        site_name = node.get_props().get_prop('site', ns)
        return site_name

    def _is_diskless(self, node):
        return node in self.clients

    def _get_other_site_nodes(self, node, site_name):
        others = []

        for n in self._all_nodes:
            other_site = self._is_part_of_site(n)
            if other_site and other_site == site_name and n != node:
                others.append(n)

        return others

    def _gen_mesh(self, base_node, others):
        if self._is_diskless(base_node):
            return

        others = [o for o in others if not self._is_diskless(o)]

        # existing mesh?
        for m in self._meshes:
            if base_node in m:
                m.update(others)
                return

        # new mesh
        s = set([base_node] + others)
        if s:
            self._meshes.append(s)

    def _gen_meshes(self):
        for n in self._all_nodes:
            n_site = self._is_part_of_site(n)
            if n_site:
                others = self._get_other_site_nodes(n, n_site)
                if others:
                    self._gen_mesh(n, others)

    def _get_net_opts(self, site_a, site_b):
        common = self.objects_root["common"]
        ns = PropsContainer.NAMESPACES[PropsContainer.KEY_SITES]
        # should be symmetric, but...
        netopts = common.get_props().get_all_props(ns + site_a + ':' + site_b + '/neto')
        if netopts:
            return netopts

        # fallback
        netopts = common.get_props().get_all_props(ns + site_b + ':' + site_a + '/neto')
        return netopts

    def _get_server_instance(self):
        if self.target_node is not None:
            return self.target_node
        else:
            try:
                return self.objects_root[consts.KEY_SERVER_INSTANCE].get_instance_node()
            except:
                return None

    def _gen_mesh_conf(self, mesh, have_same_site=False):
        site = None
        s = self.stream
        inst_node = self._get_server_instance()

        if inst_node and inst_node not in mesh:
            return

        self._nodes_interesting.update([n for n in mesh])

        if have_same_site:
            site = self._is_part_of_site(list(mesh)[0])
        s.write(' ' * self._indentwidth + 'connection-mesh {\n')
        s.write(' ' * self._indentwidth * 2 + 'hosts ' + ' '.join([n.get_name() for n in mesh]) + ';\n')

        if site:
            netopts = self._get_net_opts(site, site)
            if netopts:
                s.write(' ' * self._indentwidth * 2 + 'net {\n')
                for k, v in netopts.items():
                    s.write(' ' * self._indentwidth * 3 + '%s %s;\n' % (k, v))
                s.write(' ' * self._indentwidth * 2 + '}\n')
        s.write(' ' * self._indentwidth + '}\n')

    def _gen_connection_conf(self, node_a, node_b, netopts=False):
        s = self.stream
        inst_node = self._get_server_instance()

        if inst_node and inst_node not in [node_a, node_b]:
            return

        self._nodes_interesting.update([node_a, node_b])

        s.write(' ' * self._indentwidth + 'connection {\n')
        s.write(' ' * self._indentwidth * 2 + 'host %s;\n' % node_a.get_name())
        s.write(' ' * self._indentwidth * 2 + 'host %s;\n' % node_b.get_name())
        if netopts:
            s.write(' ' * self._indentwidth * 2 + 'net {\n')
            for k, v in netopts.items():
                s.write(' ' * self._indentwidth * 3 + '%s %s;\n' % (k, v))
            s.write(' ' * self._indentwidth * 2 + '}\n')
        s.write(' ' * self._indentwidth + '}\n')

    def _two_in_site_cfg(self, node_a, node_b):
        site_a = self._is_part_of_site(node_a)
        site_b = self._is_part_of_site(node_b)
        if not site_a or not site_b:
            return False

        netopts = self._get_net_opts(site_a, site_b)
        if netopts:
            return netopts

        return False

    def _connect_between_meshes(self, mesh_list):
        for idx, mesh in enumerate(mesh_list):
            meshes_right = mesh_list[idx+1:]
            for node in mesh:
                for mesh_r in meshes_right:
                    for node_mr in mesh_r:
                        netopts = self._two_in_site_cfg(node, node_mr)
                        self._gen_connection_conf(node, node_mr, netopts)

    def generate_conf(self):
        # check if it is a single node cluster
        if len(self._all_nodes) == 1:
            self._gen_mesh_conf(self._all_nodes, False)
            return self._all_nodes
        # or consists only of clients
        if len(self.servers) == 0:
            self._all_nodes = [self._get_server_instance()]
            self._gen_mesh_conf(self._all_nodes, False)
            return self._all_nodes

        # populate self._meshes with nodes that can be added to 'connection-mesh'
        # these are nodes that have the same site config and are not diskless
        # these meshes contain at least 2 nodes.
        self._gen_meshes()

        servers_without_mesh = self.servers[:]
        for mesh in self._meshes:
            self._gen_mesh_conf(mesh, True)
            for mesh_node in mesh:
                servers_without_mesh.remove(mesh_node)

        # we can form a virtual mesh of config less nodes currently outside a mesh, that:
        # are not part of a site, or
        # are part of a site but we could not connect it to another node with special site settings
        virtual_mesh = servers_without_mesh[:]

        for idx, node in enumerate(servers_without_mesh):
            nodes_right = servers_without_mesh[idx+1:]
            for node_r in nodes_right:
                netopts = self._two_in_site_cfg(node, node_r)
                if netopts:  # we can set a specific config, therefore remove from virtual mesh
                    self._gen_connection_conf(node, node_r, netopts)
                    if node_r in virtual_mesh:
                        virtual_mesh.remove(node_r)
                    if node in virtual_mesh:
                        virtual_mesh.remove(node)

        # nodes in virtual mesh are now these that don't have any special settings in common with others
        # but it might be a single node!
        if len(virtual_mesh) > 1:
            self._gen_mesh_conf(virtual_mesh, False)

        # connections between meshes (virtual and real)
        mesh_list = [list(mesh) for mesh in self._meshes]
        if virtual_mesh:
            mesh_list.append(virtual_mesh)

        self._connect_between_meshes(mesh_list)

        # flatten mesh_list (now including virtual mesh)
        in_mesh = [node for mesh in mesh_list for node in mesh]
        # out are all nodes (servers and diskless clients not in real meshes or virtual meshes)
        out_mesh = [node for node in self._all_nodes if node not in in_mesh]

        # interconnect outsiders with insiders
        self._connect_between_meshes(list((in_mesh, out_mesh)))

        return self._nodes_interesting


class DrbdAdmConf(object):

    def __init__(self, objects_root, target_node=None):
        self.indentwidth = 3
        self.objects_root = objects_root
        self.target_node = target_node

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
                resopts = self._get_setup_props(common, "/reso/")
                handlers = self._get_setup_props(common, "/handlers/")
                if globalstream:
                    globalstream.write('common {\n')
                    if diskopts or netopts or resopts or handlers:
                        if diskopts:
                            self._write_section('disk', globalstream, diskopts, 1)
                        if netopts:
                            self._write_section('net', globalstream, netopts, 1)
                        if resopts:
                            self._write_section('options', globalstream, resopts, 1)
                        if handlers:
                            self._write_section('handlers', globalstream, handlers, 1)
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

            # stream = open('/tmp/bla', 'w')

            resource = assignment.get_resource()
            secret = resource.get_secret()
            if secret is None:
                secret = ""

            # begin resource
            stream.write("# This file was generated by drbdmanage(8), do not edit manually.\n")
            stream.write("resource %s {\n" % (resource.get_name()))

            if wrote_global:
                stream.write('template-file "%s";\n\n' % (self._get_stream_final_path(globalstream)))

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

            # begin resource/handlers
            handlers = self._get_setup_props(resource, "/handlers/")
            self._write_section('handlers', stream, handlers, 1)
            # end resource/disk options

            # begin resource/connection
            servers = []
            clients = []
            for assg in resource.iterate_assignments():
                tstate = assg.get_tstate()
                if (is_set(tstate, assg.FLAG_DEPLOY) or undeployed_flag):
                    node = assg.get_node()
                    if is_unset(tstate, assg.FLAG_DISKLESS):
                        servers.append(node)
                    else:
                        clients.append(node)

            # Generate connections configuration
            conn_conf = DrbdConnectionConf(servers, clients, self.objects_root, stream, self.target_node)
            nodes_interesting = conn_conf.generate_conf()

            # begin resource/nodes
            local_node = assignment.get_node()

            for assg in resource.iterate_assignments():
                diskless = is_set(assg.get_tstate(), assg.FLAG_DISKLESS)
                if ((assg.get_tstate() & assg.FLAG_DEPLOY != 0) or undeployed_flag):
                    node = assg.get_node()
                    if node not in nodes_interesting:
                        continue
                    fam_label = node.get_addrfam_label()
                    addr = node.get_addr()
                    if fam_label == consts.AF_IPV6_LABEL:
                        addr = '[%s]' % addr
                    stream.write(
                        "    on %s {\n"
                        "        node-id %s;\n"
                        "        address %s %s:%d;\n"
                        % (node.get_name(), assg.get_node_id(),
                           fam_label, addr, resource.get_port())
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
                                if diskless or node is local_node:
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
                stream.write('template-file "%s";\n\n' % (self._get_stream_final_path(globalstream)))

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
                    fam_label = node.get_addrfam_label()
                    addr = node.get_addr()
                    if fam_label == consts.AF_IPV6_LABEL:
                        addr = '[%s]' % addr
                    stream.write(
                        "    on %s {\n"
                        "        node-id %s;\n"
                        "        address %s %s:%d;\n"
                        % (node.get_name(), assg.get_node_id(),
                           fam_label, addr, resource.get_port())
                    )
                    # begin resource/nodes/volumes
                    node_vol_states = vol_states.get(node.get_name())
                    for vol_state in node_vol_states:
                        volume  = vol_state.get_volume()
                        minor   = volume.get_minor()
                        if minor is None:
                            raise InvalidMinorNrException
                        bd_path = vol_state.get_bd_path()
                        if diskless or bd_path is None:
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
            else:
                stream.write(
                    "    connection-mesh {\n"
                    "        hosts " + local_node.get_name() + ";\n"
                    "    }\n"
                )

            stream.write("}\n")
            # end resource
        except InvalidMinorNrException:
            logging.critical("DrbdAdmConf: Volume configuration has no "
                             "MinorNr object")
        except Exception as exc:
            logging.error("Cannot generate configuration file, "
                          "unhandled exception: %s" % str(exc))

    def _get_stream_final_path(self, stream):
        path = stream.name
        if path.endswith(".tmp"):
            path = path[:-4]
        return path


    def read_drbdctrl_params(self, stream):
        # parameters that contain specific information
        # FIXME: these parameters should be read from the section of the
        #        current node
        fields   = {}
        conf_node_name = None
        conf_volume    = None
        for confline in dmutils.read_lines(stream):
            key, confline = self._next_word(confline)
            if key == "on":
                conf_node_name, confline = self._next_word(confline)
            elif key == "volume":
                conf_volume, confline = self._next_word(confline)
            elif key == "shared-secret":
                fields[consts.NODE_SECRET] = self._extract_field(confline)
            elif key == "address":
                a = self._extract_field(confline)
                # maybe ipv6 with '[ipv6adr]'
                a = a.strip().lstrip('[').rstrip(']')
                fields[consts.NODE_ADDRESS] = a
            elif key == "disk":
                try:
                    conf_volume_nr = int(conf_volume)
                    if conf_volume_nr == 0:
                        fields[consts.NODE_VOL_0] = self._extract_field(confline)
                    elif conf_volume_nr == 1:
                        fields[consts.NODE_VOL_1] = self._extract_field(confline)
                except (ValueError, TypeError):
                    pass
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


    def _next_word(self, value):
        value = value.lstrip()
        space_idx = value.find(" ")
        tab_idx = value.find("\t")
        idx = None
        if space_idx != -1 and tab_idx != -1:
            idx = space_idx if space_idx < tab_idx else tab_idx
        elif space_idx != -1:
            idx = space_idx
        else:
            idx = tab_idx
        word = value[:idx]
        value = value[idx:]
        if word.endswith("\n"):
            word = word[:-1]
        return word, value


    def write_drbdctrl(self, stream, nodes, bdev_0, bdev_1, port, secret):
        stream.write(
            "resource .drbdctrl {\n"
            "    net {\n"
            "        cram-hmac-alg       sha256;\n"
            "        shared-secret       \"" + secret + "\";\n"
            "        allow-two-primaries no;\n"
            "    }\n"
            "    volume 0 {\n"
            "        device      minor 0;\n"
            "        disk        " + bdev_0 + ";\n"
            "        meta-disk   internal;\n"
            "    }\n"
            "    volume 1 {\n"
            "        device      minor 1;\n"
            "        disk        " + bdev_1 + ";\n"
            "        meta-disk   internal;\n"
            "    }\n"
        )
        if len(nodes) > 0:
            for node in nodes.itervalues():
                fam_label = node.get_addrfam_label()
                addr = node.get_addr()
                if fam_label == consts.AF_IPV6_LABEL:
                    addr = '[%s]' % addr
                stream.write(
                    "    on " + node.get_name() + " {\n"
                    "        node-id     " + str(node.get_node_id())
                    + ";\n"
                    "        address     " + fam_label + ' ' + addr
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
