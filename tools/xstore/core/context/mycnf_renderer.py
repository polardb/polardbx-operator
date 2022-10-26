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

# coding=utf-8
import collections
import configparser
import io
import sys
from enum import Enum
from typing import Dict, Iterable, Tuple


class OptionAction(Enum):
    ADD = 1
    DELETE = 2
    MODIFY = 3


class OptionDiff(object):
    def __init__(self, value: str, action: OptionAction, *, old_value=None):
        self.value = value
        self.old_value = old_value
        self.action = action


def sort_config(config: configparser.ConfigParser) -> configparser.ConfigParser:
    for section in config._sections:
        # noinspection PyUnresolvedReferences
        config._sections[section] = collections.OrderedDict(
            sorted(config._sections[section].items(), key=lambda t: t[0]))
    config._sections = collections.OrderedDict(sorted(config._sections.items(), key=lambda t: t[0]))
    return config


class MycnfRenderer(object):
    def __init__(self, template_file: str):
        self._template_file = template_file

    @staticmethod
    def _diff_configs(new, old) -> Dict[str, Dict[str, OptionDiff]]:
        diff = {}

        # Modify and add
        for section, proxy in new.items():
            section_not_found = section != old.default_section and not old.has_section(section)
            section_diff = {}

            for opt, value in proxy.items():
                if section_not_found:
                    section_diff[opt] = OptionDiff(value=value, action=OptionAction.ADD)
                else:
                    if old.has_section(section, opt):
                        old_value = old.get(section, opt)
                        if value != old_value:
                            section_diff[opt] = OptionDiff(value=value, action=OptionAction.MODIFY,
                                                           old_value=old_value)
                    else:
                        section_diff[opt] = OptionDiff(value=value, action=OptionAction.ADD)

            if len(section_diff) > 0:
                diff[section] = section_diff

        # Delete
        for section, proxy in old.items():
            section_not_found = section != new.default_section and not new.has_section(section)
            section_diff = {}

            if section_not_found:
                for opt, value in proxy.items():
                    section_diff[opt] = OptionDiff(value=value, action=OptionAction.DELETE)
            else:
                for opt, value in proxy.items():
                    if not new.has_option(section, opt):
                        section_diff[opt] = OptionDiff(value=value, action=OptionAction.DELETE)

            if len(section_diff) > 0:
                diff[section] = section_diff

        return diff

    @staticmethod
    def canonical_option(opt: str):
        return opt.replace('-', '_')

    @staticmethod
    def canonical_options(config: configparser.ConfigParser):
        for section, proxy in config.items():
            for opt, value in proxy.items():
                c_opt = MycnfRenderer.canonical_option(opt)
                if c_opt != opt:
                    proxy.pop(opt)
                    proxy[c_opt] = value

    def render(self, extras: configparser.ConfigParser or Iterable[configparser.ConfigParser], *,
               fp: None or io.IOBase = sys.stdout,
               old: None or configparser.ConfigParser = None) \
            -> Tuple[None or configparser.ConfigParser, None or Dict[str, Dict[str, OptionAction]]]:
        # Load the my.cnf template
        config = configparser.ConfigParser(allow_no_value=True, dict_type=collections.OrderedDict)
        config.read(self._template_file)

        # Make all options canonical
        self.canonical_options(config)

        # Render the new my.cnf
        if extras:
            if isinstance(extras, configparser.ConfigParser):
                extras = [extras]
            for e in extras:
                for section, proxy in e.items():
                    if section != config.default_section and \
                            not config.has_section(section):
                        config.add_section(section)
                    for opt, value in proxy.items():
                        opt = self.canonical_option(opt)
                        # if override option ends with '-' or '_', then try
                        # to delete the option from config.
                        if opt[-1] == '_':
                            opt = opt[:-1]
                            # skip invalid 'option_'
                            if len(opt) == 0 or opt[-1] == '_':
                                continue
                            config.remove_option(section, opt)
                        else:
                            config.set(section, opt, value)

        # Write to the given fp if provided
        if fp:
            # Hack, order the sections
            config = sort_config(config)

            config.write(fp=fp, space_around_delimiters=True)

        # If old config is provided, return the diff
        if old:
            return config, self._diff_configs(config, old)

        return config, None


# Test
if __name__ == '__main__':
    system_configs = configparser.ConfigParser(allow_no_value=True)
    system_configs['mysqld'] = {
        'user': 'mysql',
        'cluster-id': 1,
        'server-id': 123,
        'innodb-buffer-pool-size': (8 << 27) * 5
    }
    r = MycnfRenderer('/tmp/my.cnf')
    r.render(extras=[system_configs, {'mysqld': {'server-id-': None}}])
