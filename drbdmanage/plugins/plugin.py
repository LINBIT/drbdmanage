#!/usr/bin/env python2
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2015 LINBIT HA-Solutions GmbH
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

try:
    import importlib
except ImportError:
    import drbdmanage.importlib as importlib


class PluginManager():
    KEY_PLUGIN_PATH, KEY_PLUGIN_NAME = range(2)

    def __init__(self, server):
        self._server = server
        self._known = [
            ('drbdmanage.deployers.BalancedDeployer', 'balanced-deployer'),
            ('drbdmanage.storage.lvm.Lvm', 'LVM'),
            ('drbdmanage.storage.lvm_thinlv.LvmThinLv', 'ThinLV'),
            ('drbdmanage.storage.lvm_thinpool.LvmThinPool', 'ThinPool'),
        ]

        self._loaded = dict([(k[self.KEY_PLUGIN_PATH], None) for k in self._known])

    def get_known_plugins(self):
        return self._known

    def get_loaded_plugins(self):
        return [p for p, loaded in self._loaded if loaded is not None]

    def _get_new_instance(self, plugin_path):
        instance = None
        try:
            module_name, class_name = plugin_path.rsplit(".", 1)
            class_ = getattr(importlib.import_module(module_name), class_name)
            instance = class_(self._server)
        except:
            pass

        return instance

    def _get_name(self, plugin_path):
        for i in self._known:
            if i[self.KEY_PLUGIN_PATH] == plugin_path:
                return i[self.KEY_PLUGIN_NAME]

        return ''

    def _get_plugin_default_config(self, plugin_path):
        """
        Used to query the default config. This always creates a temporary instance
        Returns valid config or {}
        """
        config = {}

        instance = self._get_new_instance(plugin_path)
        if instance:
            config = instance.get_default_config()
            if config is not None:  # could be an empty dict
                config['name'] = self._get_name(plugin_path)

        return config

    def get_plugin_default_config(self, plugin_path=None):
        """
        Returns [] of plugin configs
        """
        if plugin_path:
            ret = self._get_plugin_default_config(plugin_path)
            if ret:
                return [ret]

        configs = []
        for plugin in self._known:
            ret = self._get_plugin_default_config(plugin[0])
            if ret:
                configs.append(ret)
        return configs

    def get_plugin_config(self, plugin_path):
        plugin_config = None
        if self._loaded[plugin_path] is not None:
            instance = self.get_plugin_instance(plugin_path)
            plugin_config = instance.get_config()
        return plugin_config

    def set_plugin_config(self, plugin_path, config):
        """
        Sets the configuration of a plugin if this plugin is loaded AND the configuration changed
        """
        current_config = self.get_plugin_config(plugin_path)
        if current_config != config:
            self.get_plugin_instance(plugin_path).set_config(config)

    def get_plugin_instance(self, plugin_path):
        """
        Returns existing plugin instance, or registers a new instance
        """
        instance = None
        if self._loaded[plugin_path] is not None:
            instance = self._loaded[plugin_path]
        else:
            instance = self._get_new_instance(plugin_path)
            self._loaded[plugin_path] = instance
        return instance
