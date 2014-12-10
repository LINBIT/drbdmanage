#!/usr/bin/python

import drbdmanage.consts as consts

class PropsContainer(object):
    """
    Container for managing property dictionary
    """

    _props      = None
    _get_serial = None


    def __init__(self, get_serial_fn, init_serial, ins_props):
        """
        Initializes a new properties container
        """
        self._get_serial = get_serial_fn
        self._props      = {}

        # Load initial properties, if present
        if ins_props is not None:
            for (key, val) in ins_props.iteritems():
                self._props[str(key)] = str(val)

        # Set the initial serial number
        checked_serial = None
        if init_serial is not None:
            try:
                checked_serial = int(init_serial)
            except ValueError:
                pass
        if checked_serial is not None:
            self._props[consts.SERIAL] = str(checked_serial)
        else:
            current_serial = self._props.get(consts.SERIAL)
            if current_serial is not None:
                try:
                    checked_serial = int(current_serial)
                except ValueError:
                    pass
            if checked_serial is None:
                self._props[consts.SERIAL] = str(self._get_serial())


    def get_prop(self, key):
        """
        Retrieves a property from the dictionary
        """
        return str(self._props[str(key)])


    def get_selected_props(self, keys):
        """
        Retrieves a dictionary of multiple properties

        Returns a new dictionary with all existing properties in the
        dictionary. Non-existent keys are ignored and will be missing in
        the new dictionary that is returned.
        """
        sel_props = {}
        for key in keys:
            val = self._props.get(str(key))
            if val is not None:
                sel_props[str(key)] = str(val)
        return sel_props


    def get_all_props(self):
        """
        Retrieves a dictionary of all properties
        """
        all_props = self._props.copy()
        return all_props


    def set_prop(self, key, val):
        """
        Sets the value of an existing property or adds a new property
        """
        if key != consts.SERIAL:
            self._props[str(key)] = str(val)
        self.new_serial()


    def set_selected_props(self, keys, ins_props):
        """
        Sets or multiple values and/or adds properties

        Sets the value of existing properties selected by the argument 'keys'
        to the value of the corresponding property in 'ins_props', if the
        property exists in 'ins_props'. If the property does not exist in
        'ins_props', the property in the container's dictionary is left
        unchanged. If the property does not exist in the container's
        dictionary, but does exist in 'ins_props', then the property is
        added to the container's dictionary.
        Properties that exist in ins_props but are not selected by 'keys'
        are ignored.
        """
        for key in keys:
            val = ins_props.get(str(key))
            if val is not None:
                self._props[str(key)] = str(val)
        self.new_serial()


    def remove_prop(self, key):
        """
        Removes a property from the dictionary
        """
        del self._props[str(key)]
        self.new_serial()


    def remove_selected_props(self, keys):
        """
        Removes multiple properties from the dictionary
        """
        for key in keys:
            try:
                del self._props[str(key)]
            except KeyError:
                pass
        self.new_serial()


    def merge_props(self, ins_props):
        """
        Merges the supplied dictionary into the container's dictionary

        The properties from 'ins_props' are merged into the container's
        dictionary, either replacing the value of existing properties in the
        container's dictionary with corresponing values from 'ins_props', or
        adding property entries to the container's dictionary
        """
        for (key, val) in ins_props.iteritems():
            self._props[str(key)] = str(val)
        self.new_serial()


    def merge_gen(self, gen_obj):
        """
        Merges data from the supplied generator into the container's dictionary

        The properties fetched from 'gen_obj' are merged into the container's
        dictionary, either replacing the value of existing properties in the
        container's dictionary with corresponing values fetched from
        'gen_obj', or adding property entries to the container's dictionary
        """
        for (key, val) in gen_obj:
            self._props[str(key)] = str(val)
        self.new_serial()


    def iterkeys(self):
        """
        Returns an iterator over the keys of the container's dictionary
        """
        return self._props.iterkeys()


    def itervalues(self):
        """
        Returns an iterator over the values of the container's dictionary
        """
        return self._props.itervalues()


    def iteritems(self):
        """
        Returns an iterator over the items of the container's dictionary
        """
        return self._props.iteritems()


    def new_serial(self):
        """
        Begins or continues a new generation of the container's data

        This function can be called to update the container's serial number
        whether or not the container's data changed. Any change of the
        container's data will result in an update of the container's serial
        number, therefor an explicit call of this function for marking
        changes of the container's data is unnecessary.
        """
        self._props[consts.SERIAL] = str(self._get_serial())
