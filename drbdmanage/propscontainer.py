#!/usr/bin/env python2

import drbdmanage.consts as consts


class PropsContainer(object):
    """
    Container for managing property dictionary
    """

    _props = None
    _get_serial = None

    """
    Namespaces:
    """
    NS = {"setupopt": "/dso/"}

    def __init__(self, get_serial_fn, init_serial, ins_props):
        """
        Initializes a new properties container
        """
        self._get_serial = get_serial_fn
        self._props = {}

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

    def _normalize_NS(self, ns):
        """
        ns has to be a string, can be ""
        """
        if ns:
            ns = ns.strip()
            ns = ns if ns[0] == '/' else '/' + ns
            ns = ns if ns[-1] == '/' else ns + '/'
        return ns

    def _normalize_key(self, key, ns):
        """
        returns concat of ns and key
        """
        return self._normalize_NS(ns) + str(key)

    def get_prop(self, key, namespace=""):
        """
        Retrieves a property from the dictionary
        """
        key = self._normalize_key(key, namespace)
        val = self._props.get(key)
        if val is not None:
            val = str(val)
        return val

    def get_selected_props(self, keys, namespace=""):
        """
        Retrieves a dictionary of multiple properties

        Returns a new dictionary with all existing properties in the
        dictionary. Non-existent keys are ignored and will be missing in
        the new dictionary that is returned.
        """
        sel_props = {}
        for key in keys:
            k = self._normalize_key(key, namespace)
            val = self._props.get(k)
            if val is not None:
                sel_props[k] = str(val)
        return sel_props

    def get_all_props(self, namespace=""):
        """
        Retrieves a dictionary of all properties
        """
        return dict([(k, v) for k, v in self.iteritems(namespace)])

    def set_prop(self, key, val, namespace=""):
        """
        Sets the value of an existing property or adds a new property
        """
        if namespace or key != consts.SERIAL:
            key = self._normalize_key(key, namespace)
            self._props[key] = str(val)
        self.new_serial()

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
            k = self._normalize_key(key, namespace)
            val = ins_props.get(k)
            if val is not None:
                self._props[k] = str(val)
        self.new_serial()

    def remove_prop(self, key, namespace=""):
        """
        Removes a property from the dictionary
        """
        try:
            del self._props[self._normalize_key(key, namespace)]
        except KeyError:
            pass
        self.new_serial()

    def remove_selected_props(self, keys, namespace=""):
        """
        Removes multiple properties from the dictionary
        """
        for key in keys:
            k = self._normalize_key(key, namespace)
            try:
                del self._props[k]
            except KeyError:
                pass
        self.new_serial()

    def merge_props(self, ins_props, namespace=""):
        """
        Merges the supplied dictionary into the container's dictionary

        The properties from 'ins_props' are merged into the container's
        dictionary, either replacing the value of existing properties in the
        container's dictionary with corresponing values from 'ins_props', or
        adding property entries to the container's dictionary
        """
        for (key, val) in ins_props.iteritems():
            k = self._normalize_key(key, namespace)
            self._props[k] = str(val)
        self.new_serial()

    def merge_gen(self, gen_obj, namespace=""):
        """
        Merges data from the supplied generator into the container's dictionary

        The properties fetched from 'gen_obj' are merged into the container's
        dictionary, either replacing the value of existing properties in the
        container's dictionary with corresponing values fetched from
        'gen_obj', or adding property entries to the container's dictionary
        """
        for (key, val) in gen_obj:
            k = self._normalize_key(key, namespace)
            self._props[k] = str(val)
        self.new_serial()

    def iterkeys(self, namespace=""):
        """
        Returns an iterator over the keys of the container's dictionary
        """
        ns = self._normalize_NS(namespace)
        for k in self._props:
            if k.startswith(ns):
                yield k[len(ns):]

    def itervalues(self, namespace=""):
        """
        Returns an iterator over the values of the container's dictionary
        """
        ns = self._normalize_NS(namespace)
        for k, v in self._props.iteritems():
            if k.startswith(ns):
                yield v

    def iteritems(self, namespace=""):
        """
        Returns an iterator over the items of the container's dictionary
        """
        ns = self._normalize_NS(namespace)
        for k, v in self._props.iteritems():
            if k.startswith(ns):
                yield (k[len(ns):], v)

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
