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
import hashlib
import logging
import shlex
import subprocess
import time
from abc import ABC, abstractmethod
from typing import ClassVar, Sequence, AnyStr

from core import consensus
from core.consensus import AbstractConsensusManager
from core.context import Context


class Engine(ABC):
    """
    Engine abstract class.
    """

    def __init__(self, context: Context):
        self.context = context

    @abstractmethod
    def name(self) -> str:
        """
        Get target engine_name.
        :return: target engine_name name.
        """

    @abstractmethod
    def bootstrap(self):
        """
        Bootstrap engine_name.
        """

    @abstractmethod
    def initialize(self):
        """
        Initialize engine_name. Must be idempotent.
        """

    @abstractmethod
    def is_initialized(self) -> bool:
        """
        Determine if it's initialized.
        :return: True if already initialized.
        """

    @abstractmethod
    def update_config(self, **override):
        """
        Create or update config file used by engine_name.
        """

    @abstractmethod
    def check_health(self, check_leader_readiness) -> bool:
        """
        Check health.

        :param check_leader_readiness: check leader status, such as ready for read-write.
        :return: True if leader is ready, False other wise.
        """

    @abstractmethod
    def handle_indicate(self, indicate: str):
        """
        Handle indicate.
        :param indicate: indicate from operator.
        """

    @abstractmethod
    def consensus_manager_class(self) -> ClassVar[AbstractConsensusManager]:
        """
        Consensus manager class.

        :return: a consensus manager for this engine, or None if not supported.
        """

    @abstractmethod
    def engine_process_id(self) -> int or None:
        """
        Get the engine pid.

        :return: pid or None if not found.
        """


class Mock(Engine):
    """
    Mock engine. Used for test.
    """

    def __init__(self, context: Context):
        self._initialized = False
        self._running = False
        super().__init__(context)

    def name(self) -> str:
        return "mock"

    def bootstrap(self):
        if self._initialized:
            self._running = True
        raise Exception('bootstrap before initialized')

    def initialize(self):
        self._initialized = True

    def is_initialized(self) -> bool:
        return self._initialized

    def update_config(self, **override):
        pass

    def check_health(self, check_leader_readiness) -> bool:
        return self._initialized and self._running

    def handle_indicate(self, indicate: str):
        pass

    def consensus_manager_class(self) -> ClassVar[AbstractConsensusManager]:
        raise consensus.NotSupportedError()

    def engine_process_id(self) -> int or None:
        return


class EngineCommon(Engine, ABC):
    def __init__(self, context: Context):
        super(EngineCommon, self).__init__(context)
        self._setup_logger()

    def _setup_logger(self):
        self.logger = logging.getLogger(type(self).__name__)
        self.logger.propagate = False

        formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    def _log_command(self, cmd: Sequence[AnyStr], *, cwd=None, message: str = None):
        cmd_str = ' '.join([shlex.quote(s) for s in cmd])

        prefix = []
        if cwd:
            prefix.append('cwd=%s' % cwd)

        prefix = '(' + ','.join(prefix) + ')'

        if message:
            prefix += ' ' + message

        self.logger.info('%s: %s' % (prefix, cmd_str))

    def check_run_process(self, cmd: Sequence[AnyStr], *, cwd=None, stdout=None, stderr=None):
        self._log_command(cmd, cwd=cwd, message='execute command')
        return subprocess.check_call(cmd, cwd=cwd, stdout=stdout, stderr=stderr, env=self.context.subprocess_envs())

    def check_run_process_output(self, cmd: Sequence[AnyStr], *, cwd=None):
        self._log_command(cmd, cwd=cwd, message='execute command')
        return subprocess.check_output(cmd, cwd=cwd, env=self.context.subprocess_envs())

    # noinspection DuplicatedCode
    def start_process(self, cmd: Sequence[AnyStr], *, cwd=None, stdout=None, stderr=None) -> subprocess.Popen:
        self._log_command(cmd, cwd=cwd, message='start command')
        return subprocess.Popen(cmd, cwd=cwd, stdout=stdout, stderr=stderr, env=self.context.subprocess_envs())

    # noinspection PyBroadException
    def simple_daemon(self, cmd: Sequence[AnyStr], pid_file: str, *, interval=1, max_retry_interval=300,
                      limit=None):
        retry_interval = 1

        # try write to the pid file to avoid permission problems.
        with open(pid_file, 'w') as f:
            f.write('')

        retry_count = 0
        while limit is None or retry_count < limit:
            self.logger.info('starting process...')
            p = self.start_process(cmd)

            # try-my-best to write pid to file
            try:
                with open(pid_file, 'w') as f:
                    f.write(str(p.pid))
            except Exception:
                self.logger.error('failed to write pid file: ' + str(p.pid))

            # monitor the process
            start_time = time.time()
            while True:
                try:
                    retcode = p.wait(timeout=interval)
                    if retcode:
                        self.logger.warning('process exit with retcode: %d' % retcode)
                        break
                    elif retcode == 0:
                        self.logger.warning('process exit normally')
                        break
                except subprocess.TimeoutExpired:
                    pass
                except:  # Including KeyboardInterrupt, wait handled that.
                    p.kill()
                    # We don't call p.wait() again as p.__exit__ does that for us.
                    raise
            alive_time = time.time() - start_time
            self.logger.warning('last process exit, wait and re-run...')
            time.sleep(retry_interval)

            if alive_time >= 600:
                retry_interval = 1
                retry_count = 0
            else:
                retry_interval *= 2
                if retry_interval >= max_retry_interval:
                    retry_interval = max_retry_interval
                retry_count += 1

    def consensus_manager(self) -> AbstractConsensusManager:
        """
        New a consensus manager.
        :return: consensus manager.
        :raises: NotSupportedError if not supported.
        """
        mgr_class = self.consensus_manager_class()
        if not mgr_class:
            raise consensus.NotSupportedError()

        port_paxos = self.context.port('paxos')
        if not port_paxos:
            raise consensus.NotSupportedError('paxos port not found')

        current_addr = '%s:%d' % (self.context.pod_info().ip(), port_paxos)
        return mgr_class(self.context.new_connection(), current_addr)

    def get_server_id(self, port, *, ip=None):
        """ MD5 is a sequence of 32 hexadecimal digits.
            use the md5 of ip and port to get the server id"""

        ip = ip or self.context.pod_info().ip()
        digest = hashlib.md5(('%s:%d' % (ip, port)).encode('utf-8')).hexdigest()
        server_id = ''
        for c in str(int(digest, 16)):
            if len(server_id) == 10:
                break
            # if the first digit is more than 4, in order to avoid the computed server is more than 4294967295,
            # map the first digit of final server id to 0
            if len(server_id) == 0 and c >= '4':
                server_id = '0'
            # considering of compatibility, the forth digit of server id can not be 3 to avoid the same of old server id
            elif len(server_id) == 3 and c == '3':
                continue
            else:
                server_id += c
        return int(server_id)
