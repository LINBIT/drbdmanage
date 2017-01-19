#!/usr/bin/env python2
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2013 - 2017  LINBIT HA-Solutions GmbH
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

import json


class GenericPersistence(object):
    _obj = None


    def __init__(self, obj):
        self._obj = obj


    def get_object(self):
        return self._obj


    def load_dict(self, serializable):
        """
        Load a dictionary with serializable variables of an object

        @param   serializable: list of object variable names to add to
                 the dictionary
        @return: object variables for serialization
        @rtype:  dict
        """
        properties = {}
        for key in serializable:
            try:
                val = self._obj.__dict__[key]
                properties[key] = val
            except KeyError:
                pass
        return properties


    def serialize(self, properties):
        """
        Serialize a dictionary (dict) into a JSON string

        @param   properties: dictionary of serializable variables
        @return: JSON string of serialized data
        @rtype:  str
        """
        return json.dumps(properties, indent=4, sort_keys=True)
