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

import collections
import configparser
import os
import shutil
from typing import ClassVar, Dict, AnyStr

from core import Context, convention
from core.consensus import AbstractConsensusManager, ConsensusManager
from ..engine import EngineCommon
from ...config.mysql import MySQLConfigManager

ENGINE_NAME = 'galaxy'


# noinspection PyMissingOrEmptyDocstring
class GalaxyEngine(EngineCommon):
    """
    Engine implementation for Galaxy Engine.
    """

    def __init__(self, context: Context):
        super().__init__(context)

        # Files and paths.
        self.vol_data_path = self.context.volume_path(convention.VOLUME_DATA)
        self.file_initialize_mark = os.path.join(self.vol_data_path, 'initialized')

        self.file_config_template = self.context.volume_path(convention.VOLUME_CONFIG, 'my.cnf.template')
        self.file_config_override = self.context.volume_path(convention.VOLUME_CONFIG, 'my.cnf.override')

        self.path_conf = os.path.join(self.vol_data_path, 'conf')
        self.file_config = os.path.join(self.path_conf, 'my.cnf')
        self.file_config_dynamic = os.path.join(self.path_conf, 'dynamic.cnf')

        self.path_data = os.path.join(self.vol_data_path, 'data')
        self.path_log = os.path.join(self.vol_data_path, 'log')
        self.path_run = os.path.join(self.vol_data_path, 'run')
        self.path_tmp = os.path.join(self.vol_data_path, 'tmp')

        self.path_home = self.context.env()['ENGINE_HOME']

    def consensus_manager_class(self) -> ClassVar[AbstractConsensusManager]:
        return ConsensusManager

    def engine_process_id(self) -> int or None:
        mysqld_pid_file = self.context.volume_path(convention.VOLUME_DATA, '/run', 'mysqld.pid')
        if not os.path.exists(mysqld_pid_file):
            return None
        with open(mysqld_pid_file, 'r') as f:
            return int(f.read())

    def name(self) -> str:
        return ENGINE_NAME

    def _mysqld_safe_command(self):
        return self._command_mysqld(binary='mysqld_safe')

    def bootstrap(self):
        self.simple_daemon(
            cmd=self._mysqld_safe_command(),
            pid_file=os.path.join(self.path_run, 'mysqld_safe.pid'),
            limit=3
        )

    def _make_dirs(self, remote_if_exists=True):
        for p in [self.path_data, self.path_log, self.path_run, self.path_tmp, self.path_conf]:
            if os.path.exists(p):
                if remote_if_exists:
                    shutil.rmtree(p)
                else:
                    continue
            os.makedirs(p)
            shutil.chown(p, 'mysql', 'mysql')

        # change data path's owner to mysql:mysql
        shutil.chown(self.vol_data_path, 'mysql', 'mysql')

    # noinspection DuplicatedCode
    def _command_mysqld(self, binary: str = 'mysqld', *, extra_args: None or Dict[str, AnyStr or int] = None):
        args = collections.OrderedDict({
            'loose-pod-name': self.context.pod_info().name(),
        })

        if extra_args:
            args.update(extra_args)

        if self.context.node_role() == convention.NODE_ROLE_VOTER:
            args['loose-cluster-log-type-node'] = 'ON'
        elif self.context.node_role() == convention.NODE_ROLE_LEARNER:
            args['loose-cluster-learner-node'] = 'ON'

        # build cmd, use --k=v or --k to build the arguments
        cmd = [os.path.join(self.path_home, 'bin', binary), '--defaults-file=' + self.file_config] + [
            '--%s=%s' % (str(k), v) if v else ('--' + str(k)) for k, v in args.items()]

        return cmd

    def _get_cluster_info(self, learner: bool = False, local: bool = False):
        shared_channel = self.context.shared_channel()

        if local:
            return '%s:%d@1' % (self.context.pod_info().ip(), self.context.port_paxos())

        pod_info = self.context.pod_info()
        if learner:
            node_info = shared_channel.get_node_by_pod_name(pod_info.name())
            return node_info.addr()
        else:
            idx = shared_channel.get_node_index(self.context.pod_info().name())
            return ';'.join([n.addr() for n in shared_channel.list_nodes()]) + '@' + str(idx + 1)

    def _new_initialize_command(self):
        return self._command_mysqld(extra_args={
            'loose-cluster-info': self._get_cluster_info(),
            'initialize-insecure': None,
        })

    def _initialize_data(self):
        self.check_run_process(
            cmd=self._new_initialize_command(),
            cwd=self.path_data,
        )

    def initialize(self):
        if self.is_initialized():
            return

        self._make_dirs()

        self.update_config()

        self._initialize_data()

        self._mark_initialized()

    def _mark_initialized(self):
        with open(self.file_initialize_mark, 'w') as f:
            f.write('ok')

    def is_initialized(self) -> bool:
        return os.path.exists(self.file_initialize_mark)

    def _system_config(self) -> configparser.ConfigParser:
        system_config = configparser.ConfigParser(allow_no_value=True)
        system_config['mysqld'] = {
            'user': 'mysql',
            'port': self.context.port_access(),
            'galaxyx_port': int(self.context.port('polarx')),
            'basedir': self.path_home,
            'datadir': self.path_data,
            'tmpdir': self.path_tmp,
            'log_error': os.path.join(self.path_log, 'alert.log'),
            'slow_query_log_file': os.path.join(self.path_log, 'slow.log'),
            'general_log_file': os.path.join(self.path_log, 'general.log'),
            'socket': os.path.join(self.path_run, 'mysql.sock'),
            'loose_log_sql_fifo': os.path.join(self.path_run, 'mysql.fifo'),
            'innodb_data_home_dir': self.path_data,
            'innodb_log_group_home_dir': self.path_data,
            'master_info_file': os.path.join(self.path_data, 'master.info'),
            'relay_log': os.path.join(self.path_log, 'relay.log'),
            'relay_log_info_file': os.path.join(self.path_log, 'relay_log.info'),
            'relay_log_index': os.path.join(self.path_log, 'mysqld_relay_bin.index'),
            'slave_load_tmpdir': self.path_tmp,
            'log_bin': os.path.join(self.path_log, 'mysql_bin'),
            'server_id': self.get_server_id(self.context.port_access()),
        }

        # Adjust innodb buffer pool according to the environment.
        pod_info = self.context.pod_info()
        if pod_info.memory_limit() <= (1 << 30):  # Less than 1G
            # default unit is 128m
            system_config['mysqld']['innodb_buffer_pool_chunk_size'] = '134217728'
            system_config['mysqld']['innodb_buffer_pool_instances'] = '1'
        elif pod_info.memory_limit() <= (2 << 30):  # Less than 2G:
            # default unit is 256m
            system_config['mysqld']['innodb_buffer_pool_chunk_size'] = '134217728'
            system_config['mysqld']['innodb_buffer_pool_instances'] = '2'

        system_config['mysqld_safe'] = {
            'pid-file': os.path.join(self.path_run, 'mysql.pid')
        }
        return system_config

    def _default_dynamic_config(self) -> configparser.ConfigParser:
        config = configparser.ConfigParser(allow_no_value=True)
        buffer_pool_size = int(self.context.pod_info().memory_limit() * 5 / 8)
        config['mysqld'] = {
            # Default using 5/8 of the memory limit.
            'innodb_buffer_pool_size': str(buffer_pool_size),
            'loose_rds_audit_log_buffer_size': str(int(buffer_pool_size / 100)),
            'loose_innodb_replica_log_parse_buf_size': str(int(buffer_pool_size / 10)),
            'loose_innodb_primary_flush_max_lsn_lag': str(int(buffer_pool_size / 11)),
            'loose_extra_max_connections': str(65535),
            'max_connections': str(65535),
            'max_user_connections': str(65535),
            'mysqlx_max_connections': str(4096),
            'loose_galaxy_max_connections': str(4096),
            'default_time_zone': '+08:00',
        }
        MySQLConfigManager.canonical_options(config)
        return config

    def update_config(self, **override):
        mgr = MySQLConfigManager(self.file_config)
        if not os.path.exists(self.file_config_dynamic):
            override_config = self._default_dynamic_config()
            MySQLConfigManager.write_config(self.file_config_dynamic, override_config)

        template_config_file = self.file_config_template
        if not os.path.exists(template_config_file):
            template_config_file = os.path.join(os.path.dirname(__file__), 'templates', 'my.cnf')

        override_configs = []
        if os.path.exists(self.file_config_override):
            override_configs.extend(self.file_config_override)
        override_configs += [self.file_config_dynamic, self._system_config()]

        mgr.update(template_config_file, overrides=override_configs)

    def check_health(self, check_leader_readiness) -> bool:
        with self.context.new_connection() as conn:
            conn.ping()

        return True

    def _reset_cluster_info(self, learner):
        self.check_run_process(self._command_mysqld(extra_args={
            'cluster-force-change-meta': 'ON',
            'cluster-info': self._get_cluster_info(learner=learner),
        }))

    def handle_indicate(self, indicate: str):
        if 'reset-cluster-info' == indicate:
            self._reset_cluster_info(learner=False)
        elif 'reset-cluster-info-to-learner' == indicate:
            self._reset_cluster_info(learner=True)
