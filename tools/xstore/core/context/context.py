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
from typing import AnyStr, Set, Dict

import pymysql

from .k8s import PodInfo, MOCK_POD_INFO
from .. import convention, channel


class Context(object):
    """
       Context for actions.
    """

    def __init__(self):
        # Setup env.
        self._env = os.environ

        # Set engine name, used to load engine.
        self._engine_name = self._env[convention.ENV_ENGINE]

        # Setup pod info.
        self._pod_info = PodInfo()
        if self._engine_name == 'mock':
            self._pod_info = MOCK_POD_INFO

        self._local_ip = self._pod_info.ip()

        # Set volumes.
        self._volumes = {
            convention.VOLUME_DATA: self._env.get(convention.ENV_VOLUME_DATA, '/data/mysql'),
            convention.VOLUME_CONFIG: self._env.get(convention.ENV_VOLUME_CONFIG, '/data/config'),
            convention.VOLUME_SHARED: self._env.get(convention.ENV_VOLUME_SHARED, '/data/shared'),
        }

        # Set ports.
        self._access_port = int(self._env.get(convention.ENV_PORT_ACCESS, 3306))

        # Set node role.
        self._node_role = self._env.get(convention.ENV_NODE_ROLE, convention.NODE_ROLE_CANDIDATE)

    def env(self):
        return self._env

    def pod_info(self) -> PodInfo:
        """
        Get current pod info.
        :return: pod info.
        """
        return self._pod_info

    def engine_name(self) -> str:
        """
        Get engine_name name.
        :return: engine_name name.
        """
        return self._engine_name

    def node_role(self) -> str:
        """
        Get node role (Consensus).
        :return: node role.
        """
        return self._node_role

    def volume_path(self, volume_type: str, *sub_paths: AnyStr) -> str:
        """
        Get path or sub path of volume.
        :param volume_type: volume type, either 'data', 'config' or 'shared'.
        :param sub_paths: sub paths.
        :return: path.
        """
        vol_root = self._volumes[volume_type]
        if not vol_root:
            raise ValueError('invalid volume type: ' + volume_type)
        return os.path.join(vol_root, *sub_paths)

    def port_access(self) -> int:
        """
        Get access port.
        :return: access port.
        """
        return self._access_port

    def port_paxos(self) -> int or None:
        """
        Get paxos port (if supported).
        :return: paxos port.
        """
        return self.port('paxos')

    def port(self, name: str) -> int or None:
        """
        Get port value of name.
        :param name: port name.
        """
        val = self.env().get(convention.env_port(name))
        return int(val) if val else None

    def get_controller_indicates(self) -> None or Set[str]:
        """
        Get current controller indicates.
        :return: controller indicates.
        """
        val = self.pod_info().annotation(convention.ANNOTATION_XSTORE_INDICATE)
        return None if not val else set(x.strip() for x in val.split(','))

    def is_bootstrap_blocked(self) -> bool:
        """
        Determine if blocking bootstrapping. Returns true only if annotation
        "xstore/indicate" found and value contains "block".

        :return: True if blocked, False otherwise.
        """
        indicates = self.get_controller_indicates()
        return indicates and 'block' in indicates

    def run_mode(self):
        """
        :return: current run mode.
        """
        return self.pod_info().annotation(convention.ANNOTATION_RUNMODE)

    def new_connection(self, *, sock_file: None or str = None, use_local_ip: bool = False) -> pymysql.Connection:
        """
        Create a new connection.

        :param sock_file: use socket file if provided.
        :param use_local_ip: use local ip if True.
        :return: a new connection.
        """
        args = dict(
            user='root',
            connect_timeout=1,
            read_timeout=1,
            write_timeout=1,
        )
        if sock_file:
            args['socket'] = sock_file
        else:
            args['host'] = 'localhost' if not use_local_ip else self.pod_info().ip()
            args['port'] = self.port_access()

        return pymysql.connect(**args)

    def shared_channel(self) -> channel.SharedFileChannel:
        """
        Get the shared channel for global cluster info. Only used for consensus related modules.

        :return: a shared channel for global cluster info.
        """
        shared_channel_file = self.volume_path(convention.VOLUME_SHARED, 'shared-channel.json')
        return channel.SharedFileChannel(shared_channel_file)

    @staticmethod
    def _unquote(s: str) -> str:
        if len(s) < 2:
            return s
        if s[0] == "'" and s[-1] == "'":
            return s[1:-1]
        if s[0] == '"' and s[-1] == '"':
            return s[1:-1]
        return s

    def subprocess_envs(self) -> Dict[str, str] or None:
        """
        Read subprocess environments from file.

        :return: env dict if found, otherwise None.
        """
        env_file = self.volume_path(convention.VOLUME_DATA, 'envs')
        if not os.path.exists(env_file):
            return None
        envs = dict(os.environ)
        with open(env_file, 'r') as f:
            for l in f.readlines():
                kv = l.split('=')
                if len(kv) != 2:
                    continue
                envs[kv[0].strip()] = self._unquote(kv[1].strip())
        return envs
