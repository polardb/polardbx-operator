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

import os
from importlib import import_module

from .store import register_engine_class


def register_found_supported():
    module_root_dir = os.path.dirname(__file__)
    engine_parent_package = '.'.join(str(__name__).split(sep='.')[:-1])
    engine_modules = []
    with os.scandir(module_root_dir) as it:
        for entry in it:
            name = entry.name
            if name == '.' or not entry.is_dir():
                continue
            if not os.path.exists(os.path.join(module_root_dir, name, '__init__.py')):
                continue
            engine_modules.append(name)

    loaded_modules = []
    for module_name in engine_modules:
        module = import_module(name='.' + module_name, package=engine_parent_package)
        try:
            engine_name = getattr(module, 'ENGINE_NAME')
            engine_class = getattr(module, 'ENGINE_CLASS')
            register_engine_class(engine_name, engine_class)
            loaded_modules.append(module_name)
        except AttributeError:
            # ignore the module
            pass
