#!/usr/bin/python

import sys

import utils
import drbd.drbdcore
from exceptions import *

class BalancedDeployer(object):
    
    """
    Balanced deployment strategy - deploy resources on nodes that have
    the greatest amount of free memory
    """
    
    
    def __init__(self):
        pass
    
    
    def deploy_select(self, nodes, result, count, size_MiB, deploy_unknown):
        """
        Find nodes that have enough memory to deploy the resource
        if deploy_unknown is set and there are not enough nodes that have
        enough memory, also use nodes that do not know the state of their
        storage pool
        
        @param   nodes: nodes that may be eligible for deploying the resource
        @type    nodes: dict of DrbdNode objects
        @param   size_MiB: amount of free storage required on a node for
                   deploying the resource
        @param   count: number of required nodes
        @param   deploy_unknown: allow deploying to nodes with unknown
                   storage status
        @type    deploy_unknown: bool
        """
        rc = DM_SUCCESS
        selected = []
        # nodes with unknown free memory
        wildcat = []
        for node in nodes.itervalues():
            poolfree = node.get_poolfree()
            if poolfree != -1:
                if poolfree >= size_MiB:
                    selected.append(node)
            else:
                wildcat.append(node)
        selected = sorted(selected,
          key=lambda node: node.get_poolfree(), reverse=True)
        utils.fill_list(selected, result, count)
        if len(result) < count:
            if deploy_unknown:
                """
                Since there are not enough nodes that are expected
                to have enough free memory to deploy the resource,
                integrate those nodes that could possibly have
                enough memory
                """
                utils.fill_list(wildcat, result, count)
        """
        If there are still not enough nodes in the result list, return an error
        indicating that there are not enough nodes that may have enough free
        storage to deploy the resource
        """
        if len(result) < count:
            rc = DM_ENOSPC
        return rc
    
    
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
            utils.fill_list(wildcat, result, count)
        if len(result) < count:
            selected = sorted(selected, key=lambda node: node.get_poolfree())
            utils.fill_list(selected, result, count)
        if not unknown_first:
            utils.fill_list(wildcat, result, count)
