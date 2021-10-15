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

import json

import jsons


class Node(object):
    def __init__(self, host: str, port: int, pod: str, domain: str or None, role: str or None):
        self.host = host
        self.port = port
        self.domain = domain
        self.pod = pod
        self.role = role

    def addr(self) -> str:
        return '%s:%d' % (self.host, self.port)

    def __eq__(self, other):
        return isinstance(other, Node) and \
               other is not None and \
               self.pod == other.pod and \
               self.host == other.host and \
               self.port == other.port and \
               self.domain == other.domain and \
               self.role == other.role

    def __str__(self):
        return jsons.dump(self)


class SharedFileChannel(object):
    def __init__(self, file: str):
        self._load(file)

    def _load(self, file: str):
        nodes = []  # [Node]
        with open(file, 'r') as f:
            v = json.load(f)

            # Set generation.
            self.generation = int(v.get('generation', -1))

            # Set status.
            self.status = v.get('status')

            # Set last backup log index
            self.last_backup_log_index = v.get('last_backup_log_index')

            # Load nodes info.
            for nv in v.get('nodes', []):
                nodes.append(Node(
                    host=nv['host'],
                    port=int(nv['port']),
                    pod=nv['pod'],
                    domain=nv.get('domain', None),
                    role=nv.get('role', None)
                ))

        self._nodes = nodes
        self._pod_map = dict([(n.pod, n) for n in nodes])
        self._addr_map = dict([(n.host + ':' + str(n.port), n) for n in nodes])

    def get_node_by_addr(self, addr: str) -> Node:
        return self._addr_map.get(addr)

    def get_node_by_pod_name(self, pod_name: str) -> Node:
        return self._pod_map.get(pod_name)

    def get_node_index(self, pod_name: str) -> int:
        for i, node in enumerate(self._nodes):
            if node.pod == pod_name:
                return i
        raise ValueError('node not found for: ' + pod_name)

    def list_nodes(self) -> [Node]:
        return self._nodes

    def is_blocked(self) -> bool:
        return 'blocked' == self.status
