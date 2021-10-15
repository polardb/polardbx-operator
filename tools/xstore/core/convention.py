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

# Common environments

ENV_ENGINE = 'ENGINE'
ENV_ENGINE_HOME = 'ENGINE_HOME'
ENV_ENGINE_BACKUP_TOOL_HOME = 'ENGINE_BACKUP_TOOL_HOME'

ENV_VOLUME_DATA = 'VOLUME_DATA'
ENV_VOLUME_CONFIG = 'VOLUME_CONFIG'
ENV_VOLUME_SHARED = 'VOLUME_SHARED'

ENV_PORT_ACCESS = 'PORT_MYSQL'
ENV_PORT_PAXOS = 'PORT_PAXOS'

ENV_NODE_ROLE = 'NODE_ROLE'

# Common Volumes

VOLUME_DATA = 'data'
VOLUME_CONFIG = 'config'
VOLUME_SHARED = 'shared'

# Common Ports

PORT_ACCESS = 'mysql'
PORT_PAXOS = 'paxos'

# Common Annotations

ANNOTATION_XSTORE_INDICATE = 'xstore/indicate'
ANNOTATION_RUNMODE = 'runmode'

# Node roles.

NODE_ROLE_CANDIDATE = 'str'
NODE_ROLE_VOTER = 'voter'
NODE_ROLE_LEARNER = 'learner'


def env_port(name: str) -> str:
    """
    Get env key of port.
    :param name: port name.
    :return: env key.
    """
    return 'PORT_' + name.upper()
