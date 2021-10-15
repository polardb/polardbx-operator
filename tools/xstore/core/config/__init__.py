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

from abc import ABC, abstractmethod
from typing import AnyStr, Any


class ConfigManager(ABC):
    """
    Config manager.
    """

    def __init__(self, config_path: str):
        self._config_path = config_path

    @abstractmethod
    def update(self, template_file: AnyStr, overrides: None or [Any], create_new: bool):
        """
        Update config.
        :param template_file: config resource file.
        :param overrides: config overrides, either file or str or other objects.
        :param create_new: create new if True.
        """
        pass
