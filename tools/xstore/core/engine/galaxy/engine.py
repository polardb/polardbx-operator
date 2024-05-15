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
import sys
import shutil
from typing import ClassVar, Dict, AnyStr

from core import Context, convention
from core.consensus import AbstractConsensusManager, ConsensusManager
from ..engine import EngineCommon
from ...config.mysql import MySQLConfigManager
from ..util import config_util

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
        self.file_log_data_separated = os.path.join(self.vol_data_path, 'log_data_separated')
        self.log_data_separation = self.context.check_log_data_separation()
        self.vol_log_path_with_separation = self.context.volume_path(convention.VOLUME_LOG)
        self.vol_log_path_without_separation = self.vol_data_path

        self.file_config_template = self.context.volume_path(convention.VOLUME_CONFIG, 'my.cnf.template')
        self.file_config_override = self.context.volume_path(convention.VOLUME_CONFIG, 'my.cnf.override')
        self.file_config_override_version = self.context.volume_path(convention.VOLUME_CONFIG,
                                                                     'my.cnf.override.version')

        self.path_conf = os.path.join(self.vol_data_path, 'conf')
        self.file_config = os.path.join(self.path_conf, 'my.cnf')
        self.file_config_dynamic = os.path.join(self.path_conf, 'dynamic.cnf')
        self.file_config_version = os.path.join(self.path_conf, 'my.cnf.override.version')

        self.path_data = os.path.join(self.vol_data_path, 'data')
        self.path_log = os.path.join(self.vol_data_path, 'log')
        self.path_log_with_separation = os.path.join(self.vol_log_path_with_separation, 'log')
        self.path_log_without_separation = os.path.join(self.vol_log_path_without_separation, 'log')
        self.path_run = os.path.join(self.vol_data_path, 'run')
        self.path_tmp = os.path.join(self.vol_data_path, 'tmp')

        self.path_home = self.context.env()['ENGINE_HOME']

        self.new_rpc_enabled = self.context.new_rpc_protocol_enabled()

    def consensus_manager_class(self) -> ClassVar[AbstractConsensusManager]:
        return ConsensusManager

    def engine_process_id(self) -> int or None:
        mysqld_pid_file = self.context.volume_path(convention.VOLUME_DATA, 'run', 'mysqld.pid')
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

    # noinspection DuplicatedCode
    def _command_mysqld(self, binary: str = 'mysqld', *, extra_args: None or Dict[str, AnyStr or int] = None):
        shared_channel = self.context.shared_channel()

        args = collections.OrderedDict({
            'loose-pod-name': self.context.pod_info().name(),
        })

        if extra_args:
            args.update(extra_args)

        if self.context.node_role() == convention.NODE_ROLE_VOTER:
            args['loose-cluster-log-type-node'] = 'ON'
            if shared_channel.last_backup_log_index != -1:
                args['cluster-start-index'] = shared_channel.last_backup_log_index
        elif self.context.node_role() == convention.NODE_ROLE_LEARNER:
            args['loose-cluster-learner-node'] = 'ON'

        if self.cluster_start_index is not None:
            args['cluster-start-index'] = self.cluster_start_index
        
        # build cmd, use --k=v or --k to build the arguments
        cmd = [os.path.join(self.path_home, 'bin', binary), '--defaults-file=' + self.file_config] + [
            '--%s=%s' % (str(k), v) if v else ('--' + str(k)) for k, v in args.items()]

        return cmd

    def _get_cluster_info(self, learner: bool = False, local: bool = False):
        shared_channel = self.context.shared_channel()
        pod_info = self.context.pod_info()
        node_info = shared_channel.get_node_by_pod_name(pod_info.name())

        if local:
            return '%s@1' % node_info.addr()

        if learner:
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
            stdout=sys.stdout,
            stderr=sys.stderr
        )

    def initialize(self):
        if self.is_initialized():
            return
        self._make_dirs()
        self.update_config()
        self._try_create_symbolic_link()
        self._try_mark_log_separated()
        self._initialize_data()
        self._mark_initialized()

    def _mark_initialized(self):
        with open(self.file_initialize_mark, 'w') as f:
            f.write('ok')
            f.write("\n")
            f.write("undo_redo_to_log")

    def _is_undo_redo_to_log(self):
        with open(self.file_initialize_mark, 'r') as f:
            return "undo_redo_to_log" in f.readlines()

    def is_initialized(self) -> bool:
        return os.path.exists(self.file_initialize_mark)

    def _system_config(self) -> configparser.ConfigParser:
        system_config = configparser.ConfigParser(allow_no_value=True)
        system_config['mysqld'] = {
            'user': 'mysql',
            'port': self.context.port_access(),
            'loose_galaxyx_port': int(self.context.port('polarx')),
            'loose_rpc_port': int(self.context.port('polarx')),
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

        if not self.is_initialized() or self._is_undo_redo_to_log():
            system_config['mysqld']['innodb_log_group_home_dir'] = self.path_log
            system_config['mysqld']['innodb_undo_directory'] = self.path_log

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
        dynamic_config = config_util.get_dynamic_mysql_cnf_by_spec(self.context.pod_info().cpu_limit(),
                                                                   self.context.pod_info().memory_limit())
        config['mysqld'] = {
            'innodb_buffer_pool_size': dynamic_config["innodb_buffer_pool_size"],
            'loose_rds_audit_log_buffer_size': dynamic_config["loose_rds_audit_log_buffer_size"],
            'max_connections': dynamic_config["max_connections"],
            'max_user_connections': dynamic_config["max_user_connections"],
            'loose_mysqlx_max_connections': dynamic_config["mysqlx_max_connections"],
            'loose_galaxy_max_connections': dynamic_config["loose_galaxy_max_connections"],
            'default_time_zone': '+08:00',
            'loose_new_rpc': "on" if self.new_rpc_enabled else "off",
        }
        MySQLConfigManager.canonical_options(config)
        return config

    def _final_config(self):
        config = configparser.ConfigParser(allow_no_value=True)
        config['mysqld'] = {}
        if self.new_rpc_enabled:
            config['mysqld']['loose_new_rpc'] = 'on'
        return config

    def update_config(self, **override):
        mgr = MySQLConfigManager(self.file_config)
        override_config = self._default_dynamic_config()
        MySQLConfigManager.write_config(self.file_config_dynamic, override_config)

        template_config_file = self.file_config_template
        if not os.path.exists(template_config_file):
            template_config_file = os.path.join(os.path.dirname(__file__), 'templates', 'my.cnf')

        override_configs = []
        override_configs += [self.file_config_dynamic, self._system_config()]
        if os.path.exists(self.file_config_override):
            override_configs += [self.file_config_override]
        override_configs += [self._final_config()]

        mgr.update(template_config_file, overrides=override_configs)

        MySQLConfigManager.write_config_version(self.file_config_version, self.file_config_override_version)

    def check_health(self, check_leader_readiness) -> bool:
        with self.context.new_connection() as conn:
            conn.ping()

        return True

    def _reset_cluster_info(self, learner, local=False):
        cluster_info = self._get_cluster_info(learner=learner, local=local)
        pod_info = self.context.pod_info()
        if pod_info.annotation(convention.ANNOTATION_FLUSH_LOCAL) == "true":
            if learner:
                cluster_info = '%s:%s' % (self.context.env().get("POD_IP"), self.context.env().get("PORT_PAXOS"))
            if local:
                cluster_info = '%s:%s@1' % (self.context.env().get("POD_IP"), self.context.env().get("PORT_PAXOS"))
        args = {
            'cluster-force-change-meta': 'ON',
            'loose-cluster-info': cluster_info,
            'user': 'mysql',
        }
        if learner:
            args['cluster-learner-node'] = 'ON'

        if self.recover_index_filepath is not None:
            recover_pos = convention.parse_backup_binlog_info_file(self.recover_index_filepath)
            args['cluster-start-index'] = recover_pos
            args['cluster-force-recover-index'] = recover_pos

        self.check_run_process(self._command_mysqld(extra_args=args))

    def handle_indicate(self, indicate: str):
        if 'reset-cluster-info' == indicate:
            self._reset_cluster_info(learner=False)
        elif 'reset-cluster-info-to-learner' == indicate:
            self._reset_cluster_info(learner=True)
        elif 'reset-cluster-info-to-local' == indicate:
            self._reset_cluster_info(learner=False, local=True)
