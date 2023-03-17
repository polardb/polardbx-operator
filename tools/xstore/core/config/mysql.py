# Copyright 2021 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections
import configparser
import fcntl
import os
from typing import AnyStr, Any

from . import ConfigManager


class MySQLConfigManager(ConfigManager):
    """
    Config manager for MySQL (my.cnf).
    """

    opt_canonical_white_list = {"loose_performance_schema_instrument"}

    def __init__(self, config_path):
        super().__init__(config_path)

    @classmethod
    def _canonical_option_key(cls, opt_key: str):
        return opt_key.replace('-', '_')

    @classmethod
    def canonical_options(cls, config: configparser.ConfigParser):
        for section, proxy in config.items():
            for opt, value in proxy.items():
                c_opt = cls._canonical_option_key(opt)
                if c_opt != opt and c_opt not in MySQLConfigManager.opt_canonical_white_list:
                    proxy.pop(opt)
                    proxy[c_opt] = value
        return config

    @classmethod
    def load_config(cls, config_path) -> configparser.ConfigParser:
        """
        Load config from file.

        :param config_path: config file.
        :return: config.
        """

        config = configparser.ConfigParser(
            allow_no_value=True, dict_type=collections.OrderedDict)

        if config_path:
            config.read(config_path)

            # Before return, canonicalize the keys.
            cls.canonical_options(config)
        return config

    @classmethod
    def _hack_sort_config(cls, config: configparser.ConfigParser) -> configparser.ConfigParser:
        for section in config._sections:
            # noinspection PyUnresolvedReferences
            config._sections[section] = collections.OrderedDict(
                sorted(config._sections[section].items(), key=lambda t: t[0]))
        config._sections = collections.OrderedDict(sorted(config._sections.items(), key=lambda t: t[0]))
        return config

    @classmethod
    def write_config(cls, path, config: configparser.ConfigParser, sort: bool = True):
        if sort:
            config = cls._hack_sort_config(config)
        with open(path, 'w') as f:
            config.write(fp=f)

    @classmethod
    def check_config_version(cls, config_version_path, override_version_path) -> bool:
        if not os.path.exists(override_version_path):
            return False
        with open(override_version_path, 'r') as o:
            override_version = o.read()

        if not os.path.exists(config_version_path):
            return True
        else:
            with open(config_version_path, 'r+') as c:
                config_version = c.read()
                if int(override_version) > int(config_version):
                    return True
        return False

    @classmethod
    def write_config_version(cls, config_version_path, override_version_path):
        if not os.path.exists(override_version_path):
            override_version = "0"
        else:
            with open(override_version_path, 'r') as o:
                override_version = o.read()

        if not os.path.exists(config_version_path):
            with open(config_version_path, 'w+') as c:
                fcntl.flock(c.fileno(), fcntl.LOCK_EX)
                try:
                    c.write(override_version)
                finally:
                    fcntl.flock(c.fileno(), fcntl.LOCK_UN)
        else:
            with open(config_version_path, 'r+') as c:
                fcntl.flock(c.fileno(), fcntl.LOCK_EX)
                try:
                    config_version = c.read()
                    if int(override_version) > int(config_version):
                        c.seek(0)
                        c.write(override_version)
                finally:
                    fcntl.flock(c.fileno(), fcntl.LOCK_UN)

    def _update_config(self, config: configparser.ConfigParser):
        # Write to resource first.
        config_tmp = os.path.join(os.path.dirname(self._config_path), 'my.cnf.tmp')
        self.write_config(config_tmp, config)

        # Atomic rename.
        os.rename(config_tmp, self._config_path)

    @classmethod
    def _override_configs(cls, target: configparser.ConfigParser, overrides: [configparser.ConfigParser]):
        for override in overrides:
            for section, proxy in override.items():
                if section != target.default_section and \
                        not target.has_section(section):
                    target.add_section(section)
                for opt, value in proxy.items():
                    target.set(section, opt, value)

    def update(self, template_file: None or AnyStr, overrides: None or [Any], create_new: bool = True):
        """
        Parse and update MySQL config.
        
        :param template_file: my.cnf resource file.
        :param overrides: my.cnf override files or configs.
        :param create_new: create new file if True.
        """

        # Exit quickly when file not exists and not going to create a new one.
        if not os.path.exists(self._config_path) and not create_new:
            return

        # Load resource configs.
        template_config = self.load_config(template_file)

        # If any override files provided, load and do override.
        if overrides:
            override_configs = [
                self.canonical_options(o) if isinstance(o, configparser.ConfigParser) else self.load_config(o)
                for o in overrides
            ]
            self._override_configs(template_config, override_configs)

        # Do update.
        self._update_config(template_config)
