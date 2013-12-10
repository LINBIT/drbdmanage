#!/usr/bin/python

__author__="raltnoeder"
__date__ ="$Sep 30, 2013 12:17:49 PM$"

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
        properties = dict()
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
