#!/usr/bin/env python2
"""
Module for the PropsContainer class and related classes
"""

import drbdmanage.consts as consts


class Props(object):
    _props = None

    """
    Namespaces:
    """
    KEY_SETUPOPT = 'setupopt'
    KEY_DMCONFIG = 'dmconfig'
    KEY_PLUGINS = 'plugins'
    KEY_SITES = 'siteopt'
    KEY_SATELLITES = 'satellites'

    NAMESPACES = {
        KEY_SETUPOPT: '/dso/',          # drbdsetup options like net options
        KEY_DMCONFIG: '/dmconf/',       # drbdmanage config like 'min-minor-nr'
        KEY_PLUGINS: '/plugins/',       # plugin config
        KEY_SITES: '/sites/',           # site specific options (currently net options)
        KEY_SATELLITES: '/satellites/'  # site specific options (currently net options)
    }

    def __init__(self, ins_props):
        self._props = {}

        # Load initial properties, if present
        if ins_props is not None:
            for (key, value) in ins_props.iteritems():
                self._props[str(key)] = str(value)

    def _normalize_namespace(self, namespace):
        """
        Namespace has to be a string, but can be an empty string ("")
        """
        import os
        if namespace is not None and len(namespace) >= 1:
            namespace = namespace.strip()
            namespace = os.path.normpath(namespace)
            namespace = namespace if namespace[0] == '/' else '/' + namespace
            namespace = namespace if namespace[-1] == '/' else namespace + '/'
        return namespace

    def _normalize_key(self, key, namespace):
        """
        Returns concatenation of namespace and key
        """
        return self._normalize_namespace(namespace) + str(key)

    def get_prop(self, key, namespace=""):
        """
        Retrieves a property from the dictionary
        """
        key = self._normalize_key(key, namespace)
        value = self._props.get(key)
        if value is not None:
            value = str(value)
        return value

    def get_selected_props(self, keys, namespace=""):
        """
        Retrieves a dictionary of multiple properties

        Returns a new dictionary with all existing properties in the
        dictionary. Non-existent keys are ignored and will be missing in
        the new dictionary that is returned.
        """
        sel_props = {}
        for key in keys:
            norm_key = self._normalize_key(key, namespace)
            value = self._props.get(norm_key)
            if value is not None:
                sel_props[norm_key] = str(value)
        return sel_props

    def get_all_props(self, namespace=""):
        """
        Retrieves a dictionary of all properties
        """
        return dict([(key, value) for key, value in self.iteritems(namespace)])

    def set_prop(self, key, value, namespace=""):
        """
        Sets the value of an existing property or adds a new property
        """
        if namespace or key != consts.SERIAL:
            key = self._normalize_key(key, namespace)
            self._props[key] = str(value)

    def set_selected_props(self, keys, ins_props, namespace=""):
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
            norm_key = self._normalize_key(key, namespace)
            value = ins_props.get(norm_key)
            if value is not None:
                self._props[norm_key] = str(value)

    def remove_prop(self, key, namespace=""):
        """
        Removes a property from the dictionary
        """
        try:
            del self._props[self._normalize_key(key, namespace)]
        except KeyError:
            pass

    def remove_selected_props(self, keys, namespace=""):
        """
        Removes multiple properties from the dictionary
        """
        for key in keys:
            norm_key = self._normalize_key(key, namespace)
            try:
                del self._props[norm_key]
            except KeyError:
                pass

    def merge_props(self, ins_props, namespace=""):
        """
        Merges the supplied dictionary into the container's dictionary

        The properties from 'ins_props' are merged into the container's
        dictionary, either replacing the value of existing properties in the
        container's dictionary with corresponing values from 'ins_props', or
        adding property entries to the container's dictionary
        """
        for (key, value) in ins_props.iteritems():
            norm_key = self._normalize_key(key, namespace)
            self._props[norm_key] = str(value)

    def merge_gen(self, gen_obj, namespace=""):
        """
        Merges data from the supplied generator into the container's dictionary

        The properties fetched from 'gen_obj' are merged into the container's
        dictionary, either replacing the value of existing properties in the
        container's dictionary with corresponing values fetched from
        'gen_obj', or adding property entries to the container's dictionary
        """
        for (key, value) in gen_obj:
            norm_key = self._normalize_key(key, namespace)
            self._props[norm_key] = str(value)

    def iterkeys(self, namespace=""):
        """
        Returns an iterator over the keys of the container's dictionary
        """
        norm_namespace = self._normalize_namespace(namespace)
        for norm_key in self._props:
            if norm_key.startswith(norm_namespace):
                yield norm_key[len(norm_namespace):]

    def itervalues(self, namespace=""):
        """
        Returns an iterator over the values of the container's dictionary
        """
        norm_namespace = self._normalize_namespace(namespace)
        for key, value in self._props.iteritems():
            if key.startswith(norm_namespace):
                yield value

    def iteritems(self, namespace=""):
        """
        Returns an iterator over the items of the container's dictionary
        """
        norm_namespace = self._normalize_namespace(namespace)
        for key, value in self._props.iteritems():
            if key.startswith(norm_namespace):
                yield (key[len(norm_namespace):], value)


class PropsContainer(Props):
    """
    Container for managing property dictionary
    """

    _get_serial = None

    def __init__(self, get_serial_fn, init_serial, ins_props):
        """
        Initializes a new properties container
        """
        super(PropsContainer, self).__init__(ins_props)
        self._get_serial = get_serial_fn

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
                if self._get_serial is not None:
                    self._props[consts.SERIAL] = str(self._get_serial())
                else:
                    self._props[consts.SERIAL] = str(1)

    def set_prop(self, key, value, namespace=""):
        super(PropsContainer, self).set_prop(key, value, namespace)
        self.new_serial()

    def set_selected_props(self, keys, ins_props, namespace=""):
        super(PropsContainer, self).set_selected_props(keys, ins_props, namespace)
        self.new_serial()

    def remove_prop(self, key, namespace=""):
        super(PropsContainer, self).remove_prop(key, namespace)
        self.new_serial()

    def remove_selected_props(self, keys, namespace=""):
        super(PropsContainer, self).remove_selected_props(keys, namespace)
        self.new_serial()

    def merge_props(self, ins_props, namespace=""):
        super(PropsContainer, self).merge_props(ins_props, namespace)
        self.new_serial()

    def merge_gen(self, gen_obj, namespace=""):
        super(PropsContainer, self).merge_gen(gen_obj, namespace)
        self.new_serial()

    def new_serial(self):
        """
        Begins or continues a new generation of the container's data

        This function can be called to update the container's serial number
        whether or not the container's data changed. Any change of the
        container's data will result in an update of the container's serial
        number, therefor an explicit call of this function for marking
        changes of the container's data is unnecessary.
        """
        serial = self._get_serial()
        self._props[consts.SERIAL] = str(serial)
        return serial

    def new_serial_gen(self):
        """
        Creates a new instance of the SerialNrGen class

        The new instance of the serial number generator can be used
        to create one new serial number per change generation for
        this PropsContainer
        """
        serial_gen = SerialGen(self._props)
        self._get_serial = serial_gen.get_serial
        return serial_gen


class SerialGen(object):

    _props_store = None
    _change_open = False

    def __init__(self, props_store_ref):
        """
        Initializes the serial number generator
        """
        self._props_store = props_store_ref

    def get_serial(self):
        """
        Returns a serial number for configuration changes

        Upon the first call of this function in a sequence of changes, a
        new serial number is generated and returned. Upon subsequent calls,
        the same serial number is returned until the change generation is
        closed by calling close_serial().
        """
        serial = 0
        try:
            serial_str = self._props_store[consts.SERIAL]
            if serial_str is not None:
                serial = int(serial_str)
        except TypeError:
            pass
        if not self._change_open:
            self._change_open = True
            serial += 1
        return serial

    def close_serial(self):
        """
        Closes the current generation of configuration changes

        After a generation of configuration changes has been closed,
        the next call of get_serial() will open a new change generation and
        will return a new serial number.
        """
        self._change_open = False
