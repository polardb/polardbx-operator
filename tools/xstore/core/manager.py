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

import time
from typing import Callable

import pymysql

from . import Context, consensus, channel, convention, engine
from .consensus import AbstractConsensusManager


class Manager(object):
    def __init__(self, context: Context):
        self._context = context
        self._engine = engine.new_engine(context.engine_name(), context)

    def context(self) -> Context:
        return self._context

    def engine(self) -> engine.Engine:
        """
        Return name.
        :return: name.
        """
        return self._engine

    @staticmethod
    def _print_and_flush(*args, **kwargs):
        print(*args, **kwargs, flush=True)

    @staticmethod
    def _block_until(is_blocked: Callable[[], bool], interval=1):
        while True:
            if is_blocked():
                time.sleep(interval)
            else:
                return

    def wait_for_unblock(self):
        def is_blocked():
            try:
                shared = self.context().shared_channel()
                return shared.is_blocked()
            except:
                return True

        self._block_until(is_blocked)

    def handle_indicates(self):
        """
        Handle xstore indicates.
        """

        indicates = self.context().get_controller_indicates()
        if not indicates:
            return

        for ind in indicates:
            if ind == 'block':
                continue
            self.engine().handle_indicate(ind)

        self._block_until(self.context().is_bootstrap_blocked)

    def new_connection(self, *, sock_file: None or str = None, use_local_ip: bool = False) -> pymysql.Connection:
        """
        Create a new connection.

        :param sock_file: use socket file if provided.
        :param use_local_ip: use local ip if True.
        :return: a new connection.
        """
        return self.context().new_connection(sock_file=sock_file, use_local_ip=use_local_ip)

    def consensus_manager(self) -> AbstractConsensusManager:
        """
        New a consensus manager.
        :return: consensus manager.
        :raises: NotSupportedError if not supported.
        """
        mgr_class = self.engine().consensus_manager_class()
        if not mgr_class:
            raise consensus.NotSupportedError()

        port_paxos = self.context().port(convention.PORT_PAXOS)
        if not port_paxos:
            raise consensus.NotSupportedError('paxos port not found')

        current_addr = '%s:%d' % (self.context().pod_info().ip(), port_paxos)
        return mgr_class(self.new_connection(), current_addr)

    def shared_channel(self) -> channel.SharedFileChannel:
        """
        Get the shared channel for global cluster info. Only used for consensus related modules.

        :return: a shared channel for global cluster info.
        """
        return self.context().shared_channel()
