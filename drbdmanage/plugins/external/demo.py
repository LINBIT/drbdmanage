"""
    drbdmanage - demo plugin
    Copyright (C) 2015 - 2017 LINBIT HA-Solutions GmbH
    Author: Roland Kammerer <roland.kammerer@linbit.com>

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


class ExternalPlugin(object):
    # Module configuration defaults
    CONF_DEFAULTS = {
        'foo': 'foov',
        'bar': 'barv',
        'baz': 'bazv',
    }

    # all the follwing methods are mandatory

    # probably called multiple times, just setup config and server ref
    # do NOT call back to the server from __init__
    def __init__(self, server):
        self._server = server
        # start with a copy of the default values
        self._conf = self.CONF_DEFAULTS.copy()

    # has to return a dict, can be {}
    def get_default_config(self):
        return self.CONF_DEFAULTS.copy()

    # has to return a dict, can be {}
    def get_config(self):
        return self._conf

    # has to return a bool
    def set_config(self, config):
        self._conf = config
        return True

    # here you can assume the server and all its objects are set up
    # run is the entry point for external plugins
    # has to return a dict, can be {}
    def run(self):
        # do whatever is neccessary
        self._list_node_objects()
        return {'demo': 'plugin'}

    # demo-plugin specific methods, not mandatory
    def _list_node_objects(self):
        # for node in self._server.iterate_nodes():
        #     print 'nodes object at', node
        pass
