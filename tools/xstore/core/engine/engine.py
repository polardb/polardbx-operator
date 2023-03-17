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
import os
import shlex
import shutil
import subprocess
import time
from abc import ABC, abstractmethod
from core import consensus, convention
from core.consensus import AbstractConsensusManager
from core.context import Context, PodInfo
from typing import ClassVar, Sequence, AnyStr


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
    def is_marked_log_separated(self):
        """
        Determine if it's marked log separated.
        """

    @abstractmethod
    def try_move_log_file(self):
        """
        move log file when log_data_separation changes
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

    @abstractmethod
    def engine_safe_process_id(self) -> int or None:
        """
        Get the engine safe pid.

        :return: pid or None if not found.
        """

    @abstractmethod
    def set_enable_engine(self, enable_engine):
        """
        set engine enable

        """

    @abstractmethod
    def is_enable_engine(self) -> bool:
        """
        is engine enabled

        """

    @abstractmethod
    def wait_for_enable(self):
        """
        wait for enable

        """

    @abstractmethod
    def clean_data_log(self):
        """
        clean data and log dir

        """

    @abstractmethod
    def set_recover_index_filepath(self, filepath):
        """
        set recover index filepath

        """

    @abstractmethod
    def set_cluster_start_index(self, cluster_start_index):
        """
        set cluster_start_index

        """

    @abstractmethod
    def set_restore_prepare(self, restore_prepare):
        """
        set restore_prepare

        """

    @abstractmethod
    def is_restore_prepare(self) -> bool:
        """
        check if restore_prepare

        """

    @abstractmethod
    def shutdown(self):
        """
        shutdown mysql
        """

    @abstractmethod
    def prepare_handle_indicate(self, action):
        """
        prepare indicate
        """

    @abstractmethod
    def try_handle_indicate(self):
        """
        check if it is necessary to handle indicate
        try to handle indicate
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

    def is_marked_log_separated(self):
        pass

    def check_health(self, check_leader_readiness) -> bool:
        return self._initialized and self._running

    def handle_indicate(self, indicate: str):
        pass

    def consensus_manager_class(self) -> ClassVar[AbstractConsensusManager]:
        raise consensus.NotSupportedError()

    def engine_process_id(self) -> int or None:
        return

    def try_move_log_file(self):
        return

    def engine_safe_process_id(self) -> int or None:
        return

    def set_enable_engine(self, enable_engine):
        return

    def is_enable_engine(self) -> bool:
        return False

    def wait_for_enable(self):
        return

    def clean_data_log(self):
        return

    def set_recover_index_filepath(self, filepath):
        return

    def set_cluster_start_index(self, cluster_start_index):
        return

    def set_restore_prepare(self, restore_prepare):
        return

    def is_restore_prepare(self) -> bool:
        return

    def shutdown(self):
        return

    def prepare_handle_indicate(self, action):
        return

    def try_handle_indicate(self):
        return


class EngineCommon(Engine, ABC):
    def __init__(self, context: Context):
        super(EngineCommon, self).__init__(context)
        self._setup_logger()
        self.file_log_data_separated = None
        self.log_data_separation = None
        self.vol_log_path_with_separation = None
        self.vol_log_path_without_separation = None
        self.path_log_with_separation = None
        self.path_log_without_separation = None
        self.path_data = None
        self.path_log = None
        self.path_run = None
        self.path_tmp = None
        self.path_conf = None
        self.vol_data_path = None
        self.recover_index_filepath = None
        self.cluster_start_index = None
        self.restore_prepare = None

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

    def exec_cmd(self, cmd: Sequence[AnyStr], *, cwd=None, stdout=None, stderr=None, interval=10):
        p = self.start_process(cmd, cwd=cwd, stdout=stdout, stderr=stderr)
        try:
            ret = p.wait(timeout=interval)
            if ret == 0:
                self.logger.info("succeeding to exec cmd %s " % cmd)
            else:
                raise Exception("ErrorCode = %d, Failed to exec cmd %s" % (ret, cmd))
        except subprocess.TimeoutExpired:
            self.logger.error("Exec cmd Timeout, cmd %s" % cmd)
            raise
        except BaseException:
            p.kill()
            raise

    def shutdown(self):
        self.exec_cmd(
            cmd=convention.SHELL_CMD['SHUTDOWN_MYSQL']("--socket=" + os.path.join(self.path_run, 'mysql.sock')))

    def simple_daemon(self, cmd: Sequence[AnyStr], pid_file: str, *, interval=1, max_retry_interval=300,
                      limit=None):
        retry_interval = 1

        # try write to the pid file to avoid permission problems.
        with open(pid_file, 'w') as f:
            f.write('')

        retry_count = 0
        while limit is None or retry_count < limit or 'debug' == PodInfo().annotation(convention.ANNOTATION_RUNMODE):
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

    def _make_dirs(self, remote_if_exists=True):
        path_log = self.path_log_with_separation if self.log_data_separation else self.path_log_without_separation
        for p in [self.path_data, path_log, self.path_run, self.path_tmp, self.path_conf]:
            if os.path.exists(p):
                if remote_if_exists:
                    shutil.rmtree(p)
                else:
                    continue
            os.makedirs(p)
            shutil.chown(p, 'mysql', 'mysql')

        # change data path's owner to mysql:mysql
        shutil.chown(self.vol_data_path, 'mysql', 'mysql')

    def _mark_log_separated(self):
        with open(self.file_log_data_separated, 'w') as f:
            f.write('ok')

    def _try_mark_log_separated(self):
        if self.log_data_separation:
            self._mark_log_separated()

    def is_marked_log_separated(self):
        return os.path.exists(self.file_log_data_separated)

    def _delete_log_separated(self):
        os.remove(self.file_log_data_separated)

    def try_move_log_file(self):
        source_path = self.path_log_without_separation
        destination_path = self.path_log_without_separation
        if self.is_marked_log_separated():
            source_path = self.path_log_with_separation
        if self.log_data_separation:
            destination_path = self.path_log_with_separation
        self.logger.info("log_data_separtion %s, source_path %s, destination_path %s" % (self.log_data_separation,
                                                                                         source_path, destination_path))
        if source_path == destination_path:
            self.logger.info("It not necessary to mv data because the source_path equals the destination_path")
            return
        if os.path.islink(destination_path):
            os.remove(destination_path)
        self.exec_cmd(cmd=convention.SHELL_CMD['MK_DIRECTORY'](destination_path))
        self.exec_cmd(cmd=convention.SHELL_CMD['CP_ALL'](source_path, destination_path))
        self.exec_cmd(cmd=convention.SHELL_CMD['CHMOD_OWN_MYSQL'](destination_path))
        self.exec_cmd(cmd=convention.SHELL_CMD['RM_DIRECTORY'](source_path))
        if self.log_data_separation:
            self._mark_log_separated()
        else:
            self._delete_log_separated()
        self._try_create_symbolic_link()

    def _try_create_symbolic_link(self):
        if self.log_data_separation and not os.path.exists(self.path_log_without_separation):
            # add symbolic link
            self.exec_cmd(cmd=convention.SHELL_CMD['LINK_DIRECTORY'](self.path_log_with_separation,
                                                                     self.path_log_without_separation))

    def engine_safe_process_id(self) -> int or None:
        mysqld_safe_pid_file = self.context.volume_path(convention.VOLUME_DATA, 'run', 'mysqld_safe.pid')
        if not os.path.exists(mysqld_safe_pid_file):
            return None
        with open(mysqld_safe_pid_file, 'r') as f:
            return int(f.read())

    def set_enable_engine(self, enable_engine):
        disable_engine_filepath = self.context.volume_path(convention.VOLUME_DATA, 'disable_engine')
        if self.is_enable_engine() == enable_engine:
            return
        if not enable_engine:
            with open(disable_engine_filepath, 'w') as f:
                f.write('ok')
        else:
            if os.path.exists(disable_engine_filepath):
                os.remove(disable_engine_filepath)
        return

    def is_enable_engine(self) -> bool:
        disable_engine_filepath = self.context.volume_path(convention.VOLUME_DATA, 'disable_engine')
        return not os.path.exists(disable_engine_filepath)

    def wait_for_enable(self, interval=5):
        self.logger.info("begin wait for enable")
        while True:
            time.sleep(interval)
            if self.is_enable_engine():
                break
        self.logger.info("finish wait for enable")
        return

    def clean_data_log(self):
        self.exec_cmd(cmd=convention.SHELL_CMD['RM_DIRECTORY_CONTENT']('/data/mysql/data'))
        self.exec_cmd(cmd=convention.SHELL_CMD['RM_DIRECTORY_CONTENT']('/data/mysql/log'))

    def set_recover_index_filepath(self, filepath):
        self.recover_index_filepath = filepath

    def set_cluster_start_index(self, cluster_start_index):
        self.cluster_start_index = cluster_start_index

    def set_restore_prepare(self, restore_prepare: bool):
        self.restore_prepare = restore_prepare

    def is_restore_prepare(self) -> bool:
        return self.restore_prepare

    def prepare_handle_indicate(self, action):
        indicate_file = self.context.volume_path(convention.VOLUME_DATA, 'handle_indicate')
        with open(indicate_file, "w") as f:
            f.write(action)

    def try_handle_indicate(self):
        indicate_file = self.context.volume_path(convention.VOLUME_DATA, 'handle_indicate')
        if os.path.exists(indicate_file):
            with open(indicate_file, "r") as f:
                action = f.readline()
                self.handle_indicate(action)
            os.remove(indicate_file)
