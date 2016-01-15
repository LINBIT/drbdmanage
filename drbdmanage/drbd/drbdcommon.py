#!/usr/bin/env python2

import drbdmanage.propscontainer as propscon
import drbdmanage.exceptions as dmexc
from drbdmanage.utils import check_name

class GenericDrbdObject(object):

    """
    Super class of Drbd* objects with a property list
    """

    _props = None


    def __init__(self, get_serial_fn, init_serial, init_props):
        self._props = propscon.PropsContainer(
            get_serial_fn, init_serial, init_props
        )


    @staticmethod
    def name_check(name, min_length, max_length, valid_chars, valid_inner_chars):
        """
        Check the validity of a string for use as a name for
        objects like nodes or volumes.
        A valid name must match these conditions:
          * must at least be 1 byte long
          * must not be longer than specified by the caller
          * contains a-z, A-Z, 0-9, and the characters allowed
            by the caller only
          * contains at least one alpha character (a-z, A-Z)
          * must not start with a numeric character
          * must not start with a character allowed by the caller as
            an inner character only (valid_inner_chars)
        @param name         the name to check
        @param max_length   the maximum permissible length of the name
        @param valid_chars  list of characters allowed in addition to
                            [a-zA-Z0-9]
        @param valid_inner_chars    list of characters allowed in any
                            position in the name other than the first,
                            in addition to [a-zA-Z0-9] and the characters
                            already specified in valid_chars
        """
        # might raise a TypeError
        name = check_name(name, min_length, max_length, valid_chars, valid_inner_chars)
        return name


    def get_props(self):
        """
        Returns a reference to the DRBD object's properties container
        """
        return self._props


    def properties_match(self, filter_props):
        """
        Returns True if any of the criteria match the object's properties

        If any of the key/value pair entries in filter_props match the
        corresponding entry in an object's properties map (props), this
        function returns True; otherwise it returns False.
        """
        match = False
        for (key, val) in filter_props.iteritems():
            prop_val = self._props.get_prop(key)
            if prop_val is not None:
                if prop_val == val:
                    match = True
                    break
        return match


    def special_properties_match(self, special_props_list, filter_props):
        """
        Returns True if any of the criteria match the list properties

        If any of the key/value pair entries in filter_props match the
        corresponding entry in the supplied list of an object's special
        properties, this function returns True; otherwise it returns False
        """
        match = False
        for key, val in special_props_list.iteritems():
            filter_props_val = filter_props.get(key)
            if (filter_props_val is not None
                and val == filter_props_val):
                    match = True
                    break
        return match
