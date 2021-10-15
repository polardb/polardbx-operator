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

from typing import ClassVar

from core import Context
from .engine import Engine

_engine_cls_map = {}


def register_engine_class(engine: str, cls: ClassVar[Engine]):
    if engine in _engine_cls_map:
        raise RuntimeError('duplicate engine_name: ' + engine)
    _engine_cls_map[engine] = cls


def new_engine(engine: str, context: Context) -> Engine:
    """
    Construct a new name object of target engine_name.

    :param engine: engine_name name.
    :param context: current context.
    :return:
    """
    engine_cls = _engine_cls_map[engine]
    if not engine_cls:
        raise ValueError('engine_name not supported: ' + engine)
    return engine_cls(context)
