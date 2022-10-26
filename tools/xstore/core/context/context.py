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
import hashlib
import configparser
from typing import AnyStr, Set, Dict

import pymysql

from .mycnf_renderer import MycnfRenderer
from .k8s import PodInfo, MOCK_POD_INFO
from .. import convention, channel


class InvalidEngineVersionError(Exception):
    pass


class Context(object):
    """
       Context for actions.
    """

    def __init__(self):
        # Setup env.
        self._env = os.environ

        # Set engine name, used to load engine.
        self._engine_name = self._env[convention.ENV_ENGINE]

        if self.is_galaxy80():
            # galaxy related paths
            self.engine_home = self._env.get('ENGINE_HOME', '/opt/galaxy_engine')
            self.xtrabackup_home = self._env.get('XTRABACKUP_HOME', '/tools/xstore/current/xcluster_xtrabackup80/bin')
            self.xtrabackup = os.path.join(self.xtrabackup_home, "xtrabackup")
        else:
            self.engine_home = self._env.get('ENGINE_HOME', '/u01/xcluster_current')
            self.xtrabackup_home = self._env.get('XTRABACKUP_HOME', '/u01/xcluster_xtrabackup24/bin')
            self.xtrabackup = os.path.join(self.xtrabackup_home, "innobackupex")

        self._tools_home = self._env.get('TOOL_HOME', '/tools/xstore/current')
        self.mysqlbinlogtailor = os.path.join(self.xtrabackup_home, "mysqlbinlogtailor")
        self.bb_home = os.path.join(self._tools_home, "bb")
        self._filestream_client_home = os.path.join(self._tools_home, "bin/polardbx-filestream-client")
        self._hostinfo_path = "/tools/xstore/hdfs-nodes.json"

        # Setup pod info.
        self._pod_info = PodInfo()
        if self._engine_name == 'mock':
            self._pod_info = MOCK_POD_INFO

        self._local_ip = self._pod_info.ip()

        # Set volumes.
        self._volumes = {
            convention.VOLUME_DATA: self._env.get(convention.ENV_VOLUME_DATA, '/data/mysql'),
            convention.VOLUME_LOG: self._env.get(convention.ENV_VOLUME_LOG, '/data-log/mysql'),
            convention.VOLUME_CONFIG: self._env.get(convention.ENV_VOLUME_CONFIG, '/data/config'),
            convention.VOLUME_SHARED: self._env.get(convention.ENV_VOLUME_SHARED, '/data/shared'),
        }
        if not self.check_log_data_separation():
            self._volumes[convention.ENV_VOLUME_LOG] = self._volumes[convention.VOLUME_DATA]

        # mycnf file
        self.mysql_conf = os.path.join(self._volumes['data'], 'conf')
        self.mycnf_path = os.path.join(self.mysql_conf, 'my.cnf')
        self.mycnf_override_path = os.path.join(self.mysql_conf, 'dynamic.cnf')
        if self.is_galaxy80():
            self.mycnf_template_path = os.path.join(self._tools_home, 'core/engine/galaxy/templates', 'my.cnf')
        elif self.is_xcluster57():
            self.mycnf_template_path = os.path.join(self._tools_home, 'core/engine/xcluster/templates', 'xcluster.57.cnf')

        # Set ports.
        self._access_port = int(self._env.get(convention.ENV_PORT_ACCESS, 3306))

        # Set node role.
        self._node_role = self._env.get(convention.ENV_NODE_ROLE, convention.NODE_ROLE_CANDIDATE)

        self._restore_prepare = False

        # set rpc protocol version, value is in ("1", "2")
        self._rpc_protocol_version = self._env.get(convention.ENV_RPC_PROTOCOL_VERSION, "1")

    def set_restore_prepare(self, val):
        self._restore_prepare = val

    def is_restore_prepare(self):
        return self._restore_prepare is True

    def env(self):
        return self._env

    def new_rpc_protocol_enabled(self):
        return self._rpc_protocol_version == "2"

    def check_log_data_separation(self):
        return self._env.get(convention.ENV_LOG_DATA_SEPARATION, 'false') == convention.LOG_DATA_SEPARATION_ON

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

    def port_polarx(self) -> int:
        """
        Get polarx port.
        :return: polarx port
        """
        return self.port('polarx')

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
            for line in f.readlines():
                kv = line.split('=')
                if len(kv) != 2:
                    continue
                envs[kv[0].strip()] = self._unquote(kv[1].strip())
        return envs

    def _get_server_id(self, port, *, ip=None):
        """ MD5 is a sequence of 32 hexadecimal digits.
            use the md5 of ip and port to get the server id"""

        ip = ip or self._local_ip
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

    def mycnf_system_config(self) -> configparser.ConfigParser:
        if self.is_xcluster57():
            return self.xcluster57_mycnf_system_config()
        elif self.is_galaxy80():
            return self.galaxy80_mycnf_system_config()
        else:
            raise InvalidEngineVersionError('unknown version: ' + self._engine_name)

    # noinspection DuplicatedCode
    def xcluster57_mycnf_system_config(self) -> configparser.ConfigParser:
        data_dir = self.volume_path(convention.VOLUME_DATA, 'data')
        run_dir = self.volume_path(convention.VOLUME_DATA, 'run')
        tmp_dir = self.volume_path(convention.VOLUME_DATA, 'tmp')
        log_dir = self.volume_path(convention.VOLUME_DATA, 'log')

        mysqld_system_conf = {
            'user': 'mysql',
            'port': int(self.port_access()),
            'polarx_port': int(self.port_polarx()),
            'basedir': self.engine_home,
            'datadir': data_dir,
            'tmpdir': tmp_dir,
            'log_error': os.path.join(log_dir, 'alert.log'),
            'slow_query_log_file': os.path.join(log_dir, 'slow.log'),
            'general_log_file': os.path.join(log_dir, 'general.log'),
            'socket': os.path.join(run_dir, 'mysql.sock'),
            'loose_log_sql_fifo': os.path.join(run_dir, 'mysql.fifo'),
            'innodb_data_home_dir': data_dir,
            'innodb_log_group_home_dir': data_dir,
            'master_info_file': os.path.join(data_dir, 'master.info'),
            'relay_log': os.path.join(log_dir, 'relay.log'),
            'relay_log_info_file': os.path.join(log_dir, 'relay_log.info'),
            'relay_log_index': os.path.join(log_dir, 'mysqld_relay_bin.index'),
            'slave_load_tmpdir': tmp_dir,
            'log_bin': os.path.join(log_dir, 'mysql_bin'),
            'cluster_id': '1',
            'server_id': self._get_server_id(self.port_access()),
        }

        config = configparser.ConfigParser(allow_no_value=True)
        config['mysqld'] = dict([(k, str(v)) for k, v in mysqld_system_conf.items()])
        config['mysqld_safe'] = {'pid-file': os.path.join(run_dir, 'mysql.pid'), }

        # Adjustments according to the environment
        if self._pod_info.memory_limit() <= (1 << 30):
            # default unit is 128m
            config['mysqld']['innodb_buffer_pool_chunk_size'] = '134217728'
            config['mysqld']['innodb_buffer_pool_instances'] = '1'
        elif self._pod_info.memory_limit() <= (2 << 30):
            # default unit is 256m
            config['mysqld']['innodb_buffer_pool_chunk_size'] = '134217728'
            config['mysqld']['innodb_buffer_pool_instances'] = '2'

        # Return the system config
        MycnfRenderer.canonical_options(config)
        return config

    def galaxy80_mycnf_system_config(self) -> configparser.ConfigParser:
        data_dir = self.volume_path(convention.VOLUME_DATA, 'data')
        run_dir = self.volume_path(convention.VOLUME_DATA, 'run')
        tmp_dir = self.volume_path(convention.VOLUME_DATA, 'tmp')
        log_dir = self.volume_path(convention.VOLUME_DATA, 'log')

        mysqld_system_conf = {
            'user': 'mysql',
            'port': int(self.port_access()),
            'galaxyx_port': int(self.port_polarx()),
            'basedir': self.engine_home,
            'datadir': data_dir,
            'tmpdir': tmp_dir,
            'log_error': os.path.join(log_dir, 'alert.log'),
            'slow_query_log_file': os.path.join(log_dir, 'slow.log'),
            'general_log_file': os.path.join(log_dir, 'general.log'),
            'socket': os.path.join(run_dir, 'mysql.sock'),
            'loose_log_sql_fifo': os.path.join(run_dir, 'mysql.fifo'),
            'innodb_data_home_dir': data_dir,
            'innodb_log_group_home_dir': data_dir,
            'master_info_file': os.path.join(data_dir, 'master.info'),
            'relay_log': os.path.join(log_dir, 'relay.log'),
            'relay_log_info_file': os.path.join(log_dir, 'relay_log.info'),
            'relay_log_index': os.path.join(log_dir, 'mysqld_relay_bin.index'),
            'slave_load_tmpdir': tmp_dir,
            'log_bin': os.path.join(log_dir, 'mysql_bin'),
            'server_id': self._get_server_id(self.port_access()),
        }

        config = configparser.ConfigParser(allow_no_value=True)
        config['mysqld'] = dict([(k, str(v)) for k, v in mysqld_system_conf.items()])
        config['mysqld_safe'] = {'pid-file': os.path.join(run_dir, 'mysql.pid'), }

        # Adjustments according to the environment
        if self._pod_info.memory_limit() <= (1 << 30):
            # default unit is 128m
            config['mysqld']['innodb_buffer_pool_chunk_size'] = '134217728'
            config['mysqld']['innodb_buffer_pool_instances'] = '1'
        elif self._pod_info.memory_limit() <= (2 << 30):
            # default unit is 256m
            config['mysqld']['innodb_buffer_pool_chunk_size'] = '134217728'
            config['mysqld']['innodb_buffer_pool_instances'] = '2'

        # Return the system config
        MycnfRenderer.canonical_options(config)
        return config

    def xcluster_info_argument(self, learner: bool = False, local: bool = False, name_from_env: bool = False):
        if local:
            return '%s:%d@1' % (self._local_ip, self.port_paxos())

        if learner:
            node_info = self.shared_channel().get_node_by_pod_name(self._pod_info.name() if not name_from_env
                                                                   else self._pod_info.pod_name_from_env())
            return node_info.addr()
        else:
            idx = self.shared_channel().get_sort_node_index(self._pod_info.name() if not name_from_env
                                                            else self._pod_info.pod_name_from_env())
            return ';'.join([n.addr() for n in self.shared_channel().list_sort_nodes()]) + '@' + str(idx + 1)

    def mycnf_override_config(self) -> configparser.ConfigParser:
        config = configparser.ConfigParser(allow_no_value=True)
        buffer_pool_size = int(self._pod_info.memory_limit() * 5 / 8)
        config['mysqld'] = {
            # force using 5/8 of memory limits.
            'innodb-buffer-pool-size': str(buffer_pool_size),
            'loose_rds_audit_log_buffer_size': str(int(buffer_pool_size / 100)),
            'loose_innodb_replica_log_parse_buf_size': str(int(buffer_pool_size / 10)),
            'loose_innodb_primary_flush_max_lsn_lag': str(int(buffer_pool_size / 11)),
            'loose_extra_max_connections': str(65535),
            'loose_max_connections': str(65535),
            'loose_max_user_connections': str(65535),
            'loose_mysqlx_max_connections': str(4096),
            'loose_polarx_max_connections': str(4096),
            'default_time_zone': '+08:00',
        }

        MycnfRenderer.canonical_options(config)
        return config

    def current_indicate(self):
        return self.shared_channel().get_indicate(self._pod_info.name())

    def mark_node_initialized(self):
        with open(self.volume_path(convention.VOLUME_DATA, 'initialized'), 'w+') as f:
            f.write('ok')

    def is_xcluster57(self):
        return self._engine_name == "xcluster"

    def is_galaxy80(self):
        return self._engine_name == "galaxy"

    def filestream_client(self):
        """
        get filestream client
        :return: path of filestream client
        """
        return self._filestream_client_home

    def host_info(self):
        """
        get host info
        :return: path of host info file
        """
        return self._hostinfo_path
