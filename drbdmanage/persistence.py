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
        properties = dict()
        for key in serializable:
            try:
                val = self._obj.__dict__[key]
                properties[key] = val
            except KeyError:
                pass
        return properties
    
    
    def serialize(self, properties):
        return json.dumps(properties, indent=4, sort_keys=True)
