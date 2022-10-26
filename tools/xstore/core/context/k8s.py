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
from typing import Dict


class PodInfo(object):
    """
    PodInfo represents the current pod environment, loading from envs and
    /etc/podinfo which is mounted automatically by Kubernetes.
    """

    def __init__(self, mount_path='/etc/podinfo', env: Dict[str, str] = os.environ):
        self._mount_path = mount_path
        self._name = None
        self._namespace = None
        self._labels = None
        self._annotations = None
        self._env = env

    def is_mounted(self):
        """
        Determine if pod info can be loaded.
        :return: True if mounted, False otherwise.
        """
        return os.path.exists(self._mount_path)

    def _sub_path(self, p):
        return os.path.join(self._mount_path, p)

    def _check_mount(self):
        if not self.is_mounted():
            raise RuntimeError('pod info not mounted')

    def name(self) -> str:
        """
        Get pod's name.
        :return: pod's name.
        """
        self._check_mount()
        if not self._name:
            with open(self._sub_path('name'), 'r') as f:
                self._name = f.read().strip()
        return self._name


    def namespace(self) -> str:
        """
        Get pod's namespace.
        :return: pod's namespace.
        """
        self._check_mount()

        if not self._namespace:
            with open(self._sub_path('namespace'), 'r') as f:
                return f.read().strip()
        return self._namespace

    def labels(self) -> Dict[str, str]:
        """
        List pod's labels.
        :return: pod's labels.
        """
        self._check_mount()

        if not self._labels:
            labels = {}
            with open(self._sub_path('labels'), 'r') as f:
                for line in f.readlines():
                    line = line.strip()
                    if len(line) == 0:
                        continue
                    kv = line.split('=', maxsplit=2)
                    labels[kv[0]] = kv[1][1:-1]
            self._labels = labels

        return self._labels

    def label(self, key: str) -> None or str:
        """
        Get value of target label.
        :param key: label key.
        :return: value of label if found, or None otherwise.
        """
        return self.labels().get(key)

    def annotations(self) -> Dict[str, str]:
        """
        List pod's annotations.
        :return: pod's annotations.
        """
        self._check_mount()

        if not self._annotations:
            annotations = {}
            with open(self._sub_path('annotations'), 'r') as f:
                for line in f.readlines():
                    line = line.strip()
                    if len(line) == 0:
                        continue
                    kv = line.split('=', maxsplit=2)
                    annotations[kv[0]] = kv[1][1:-1]
            self._annotations = annotations

        return self._annotations

    def annotation(self, key: str) -> None or str:
        """
        Get value of target annotation.
        :param key: annotation key.
        :return: value of annotation if found, or None otherwise.
        """
        return self.annotations().get(key)

    def ip(self):
        """
        Get pod's IP (in cluster).
        :return: pod's IP.
        """
        return self._env['POD_IP']

    def pod_name_from_env(self):
        """
        Get pod's name from env, used by restore job
        :return: pod's name
        """
        return self._env['POD_NAME']

    def node_name(self):
        """
        Get pod's node name.
        :return: pod's node name.
        """
        return self._env['NODE_NAME']

    def node_ip(self):
        """
        Get pod's node IP.
        :return: pod's node IP.
        """
        return self._env['NODE_IP']

    def cpu_limit(self) -> int:
        """
        Get pod's cpu limit.
        :return: pod's cpu limit.
        """
        return int(self._env['LIMITS_CPU'])

    def memory_limit(self) -> int:
        """
        Get pod's memory limit in bytes.
        :return: pod's memory limit.
        """
        return int(self._env['LIMITS_MEM'])


class MockPodInfo(PodInfo):

    def __init__(self, env: Dict[str, str]):
        super().__init__(env=env)

    def is_mounted(self):
        return True

    def name(self) -> str:
        return 'mock'

    def namespace(self) -> str:
        return 'default'

    def labels(self) -> Dict[str, str]:
        return {}

    def label(self, key: str) -> None or str:
        return None

    def annotations(self) -> Dict[str, str]:
        return {}

    def annotation(self, key: str) -> None or str:
        return None


MOCK_POD_INFO = MockPodInfo(env={
    'NODE_IP': '127.0.0.1',
    'NODE_NAME': 'mock',
    'POD_IP': '127.0.0.1',
    'POD_NAME': 'mock',
    'LIMITS_CPU': '4',
    'LIMITS_MEM': '4294967296',
})
