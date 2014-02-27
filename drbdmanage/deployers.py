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

import drbdmanage.utils

from drbdmanage.exceptions import DM_SUCCESS, DM_ENOSPC


class BalancedDeployer(object):

    """
    Balanced deployment strategy - deploy resources on nodes that have
    the greatest amount of free memory
    """


    def __init__(self):
        pass


    def deploy_select(self, nodes, result, count, size_kiB, deploy_unknown):
        """
        Find nodes that have enough memory to deploy the resource
        if deploy_unknown is set and there are not enough nodes that have
        enough memory, also use nodes that do not know the state of their
        storage pool

        @param   nodes: nodes that may be eligible for deploying the resource
        @type    nodes: dict of DrbdNode objects
        @param   size_kiB: amount of free storage required on a node for
                   deploying the resource
        @param   count: number of required nodes
        @param   deploy_unknown: allow deploying to nodes with unknown
                   storage status
        @type    deploy_unknown: bool
        """
        fn_rc = DM_SUCCESS
        selected = []
        # nodes with unknown free memory
        wildcat = []
        for node in nodes.itervalues():
            poolfree = node.get_poolfree()
            if poolfree != -1:
                if poolfree >= size_kiB:
                    selected.append(node)
            else:
                wildcat.append(node)
        selected = sorted(selected,
          key=lambda node: node.get_poolfree(), reverse=True)
        drbdmanage.utils.fill_list(selected, result, count)
        if len(result) < count:
            if deploy_unknown:
                """
                Since there are not enough nodes that are expected
                to have enough free memory to deploy the resource,
                integrate those nodes that could possibly have
                enough memory
                """
                drbdmanage.utils.fill_list(wildcat, result, count)
        """
        If there are still not enough nodes in the result list, return an error
        indicating that there are not enough nodes that may have enough free
        storage to deploy the resource
        """
        if len(result) < count:
            fn_rc = DM_ENOSPC
        return fn_rc


    def undeploy_select(self, nodes, result, count, unknown_first):
        """
        Balanced deployment strategy - undeploy resource from nodes that have
        the least amount of free memory

        If unkown_first is set, then if there are nodes that have the resource
        deployed and have unknown storage status, undeploy from those nodes
        first.

        @param   nodes: nodes that may be eligible for deploying the resource
        @type    nodes: dict of DrbdNode objects
        @param   count: number of required nodes
        @param   deploy_unknown: first undeploy from nodes with unknown
                   storage status
        @type    undeploy_unknown: bool
        """
        selected = []
        # nodes with unknown free memory
        wildcat = []
        for node in nodes.itervalues():
            poolfree = node.get_poolfree()
            if poolfree != -1:
                selected.append(node)
            else:
                wildcat.append(node)
        if unknown_first:
            drbdmanage.utils.fill_list(wildcat, result, count)
        if len(result) < count:
            selected = sorted(selected, key=lambda node: node.get_poolfree())
            drbdmanage.utils.fill_list(selected, result, count)
        if not unknown_first:
            drbdmanage.utils.fill_list(wildcat, result, count)
