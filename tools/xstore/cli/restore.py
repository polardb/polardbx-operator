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
import fcntl
import json
import os
import shutil
import subprocess
import sys
import time
import pymysql as mysql

import click
from core.context import Context
from core.log import LogFactory
from core.convention import *
from core.context.mycnf_renderer import MycnfRenderer
from core.backup_restore.storage.filestream_client import FileStreamClient, BackupStorage
from core.backup_restore.utils import check_run_process
import wget
import requests
from .common import check_parameters_exist, get_parameter_value


RESTORE_TEMP_DIR = "/data/mysql/restore"
CONN_TIMEOUT = 30
INTERNAL_MARK = '/* rds internal mark */ '


@click.group(name="restore")
def restore_group():
    pass


@click.command(name='start')
@click.option('--restore_context', required=True, type=str)
def start(restore_context):
    logger = LogFactory.get_logger("restore.log")

    with open(restore_context, 'r') as f:
        params = json.load(f)
        commit_index = params["backupCommitIndex"]
        backup_file_path = params["backupFilePath"]
        binlog_dir_path = params["binlogDirPath"]
        storage_name = params["storageName"]
        sink = params["sink"]
        pitr_endpoint = params["pitrEndpoint"] if "pitrEndpoint" in params else ""
        pitr_xstore = params["pitrXStore"] if "pitrXStore" in params else ""
        is_pxc_xstore = params["pxcXStore"]
        keyring_path = params["keyringPath"] if "keyringPath" in params else ""
        keyringfile_path = params["keyringFilePath"] if "keyringFilePath" in params else ""

    logger.info('start restore: commit_index=%s, backup_file_path=%s, isPXCXStore:%s, pitr_endpoint=%s,'
                'pitr_xstore=%s,keyring_path=%s'
                % (commit_index, backup_file_path, is_pxc_xstore, pitr_endpoint, pitr_xstore,keyring_path))

    context = Context()
    node_role = context.node_role()
    if node_role != NODE_ROLE_CANDIDATE and node_role != NODE_ROLE_LEARNER:
        logger.info("pod role is %s, no need to download backup." % node_role)
        return

    filestream_client = FileStreamClient(context, BackupStorage[str.upper(storage_name)], sink)

    keyring_path_local = download_keyring_file(keyringfile_path,keyring_path, filestream_client, logger)

    mkdir_needed(context)

    backup_file_name = backup_file_path.split("/")[-1]

    download_backup_file(backup_file_path, backup_file_name, filestream_client, logger)

    decompress_backup_file(backup_file_name, context, logger)

    initialize_local_mycnf(context, logger)

    create_init_file(context, logger)

    apply_backup_file(keyring_path_local, context, logger)

    if is_pxc_xstore or len(pitr_endpoint) != 0:
        mysql_bin_list = download_binlogbackup_file(binlog_dir_path, filestream_client, logger) if len(
            pitr_endpoint) == 0 else download_pitr_binloglist(context, pitr_endpoint, pitr_xstore, logger)

        copy_binlog_to_new_path(mysql_bin_list, context, logger)

        cluster_start_index = get_xtrabackup_binlog_info_from_instance_local(context)
        logger.info("cluster_start_index is: %s" % cluster_start_index)

        chown_data_dir(context, logger)

        last_binlog, first_binlog = show_last_and_first_binlog(context, logger)

        end_index, end_term = xdb_show_binlog_index(last_binlog, context, logger)
        logger.info("end_index:%s;end_term:%s" % (end_index, end_term))

        init_mysqld_metadata(cluster_start_index, commit_index, context, end_term, node_role, logger, is_pxc_xstore,
                             pitr_endpoint)

        p = subprocess.Popen([
            os.path.join(context.engine_home, 'bin', 'mysqld'),
            "--defaults-file=" + context.mycnf_path,
            "--init-file=" + os.path.join(context.mysql_conf, "mysql-init.sql"),
            "--user=mysql"
        ], stdout=sys.stdout)

        wait_binlog_apply_ready(context.port_access(), end_index, logger)

        p.kill()
        p.wait()
    else:
        chown_data_dir(context, logger)
        init_mysqld_metadata(commit_index, commit_index, context, 0, node_role, logger, is_pxc_xstore,
                             pitr_endpoint)

    sync_cluster_metadata(context, logger)
    context.mark_node_initialized()


def mkdir_needed(context):
    if not os.path.exists(RESTORE_TEMP_DIR):
        os.mkdir(RESTORE_TEMP_DIR)
    if not os.path.exists(context.volume_path(VOLUME_DATA, "data")):
        os.mkdir(context.volume_path(VOLUME_DATA, "data"))
    if not os.path.exists(context.volume_path(VOLUME_DATA, "log")):
        os.mkdir(context.volume_path(VOLUME_DATA, "log"))
    if not os.path.exists(context.volume_path(VOLUME_DATA, "tmp")):
        os.mkdir(context.volume_path(VOLUME_DATA, "tmp"))
    if not os.path.exists(context.volume_path(VOLUME_DATA, "run")):
        os.mkdir(context.volume_path(VOLUME_DATA, "run"))
    shutil.chown(context.volume_path(VOLUME_DATA, "data"), "mysql", "mysql")
    shutil.chown(context.volume_path(VOLUME_DATA, "log"), "mysql", "mysql")
    shutil.chown(context.volume_path(VOLUME_DATA, "tmp"), "mysql", "mysql")
    shutil.chown(context.volume_path(VOLUME_DATA, "run"), "mysql", "mysql")
    shutil.chown(context.volume_path(VOLUME_DATA), "mysql","mysql")


def download_keyring_file(keyringfile_path,keyring_path, filestream_client, logger):
    if len(keyring_path) != 0:
        keyring_file_path = os.path.dirname(keyringfile_path)
        logger.info("keyring_file_path:%s",keyring_file_path)
        if not os.path.exists(keyring_file_path):
            os.makedirs(keyring_file_path)
        shutil.chown(keyring_file_path,"mysql","mysql")
        keyring_path_local = os.path.join(keyring_file_path, "keyring")
        filestream_client.download_to_file(remote=keyring_path, local=keyring_path_local, logger=logger)
        logger.info("backup keyring downloaded!")
        return keyring_path_local
    return ""


def download_backup_file(backup_file_path, backup_file_name, filestream_client, logger):
    backup_stream_file = os.path.join(RESTORE_TEMP_DIR, backup_file_name)
    filestream_client.download_to_file(remote=backup_file_path, local=backup_stream_file, logger=logger)
    logger.info("backup file downloaded!")


def download_binlogbackup_file(binlog_dir_path, filestream_client, logger):
    binlog_list_path = os.path.join(RESTORE_TEMP_DIR, "binlog_list")
    filestream_client.download_to_file(remote=os.path.join(binlog_dir_path, "binlog_list"), local=binlog_list_path,
                                       logger=logger)
    with open(binlog_list_path, 'r') as f:
        mysql_binlog_list = f.read().splitlines()
    for binlog in mysql_binlog_list:
        filestream_client.download_to_file(remote=os.path.join(binlog_dir_path, binlog),
                                           local=os.path.join(RESTORE_TEMP_DIR, binlog), logger=logger)
    logger.info("binlog backup file download")
    logger.info("mysql_binlog_list:%s" % mysql_binlog_list)
    return mysql_binlog_list


def download_pitr_binloglist(context, pitrEndpoint, xstore, logger):
    binlogListUrl = "/".join([pitrEndpoint, "binlogs"]) + ("?xstore=%s" % xstore)
    response = requests.get(binlogListUrl)
    mysql_binlog_list = []
    pitr_ts = ""
    if response.status_code == 200:
        logger.info("binlogs http response %s" % response.content)
        binlogs = json.loads(response.content)
        for binlog in binlogs:
            mysql_binlog_list.append(binlog['filename'])
    else:
        raise Exception("failed to get binlogs url = %s" % binlogListUrl)
    for binlog in mysql_binlog_list:
        downloadUrl = "/".join([pitrEndpoint, "download", "binlog"]) + ("?xstore=%s&only_meta=true" % xstore) + "&" + (
                "filename=%s" % binlog)
        response = requests.get(downloadUrl)
        if response.status_code == 200:
            binlog_datasource = response.content.decode("utf-8")
            cmd = " ".join(
                [os.path.join("/tools/xstore/current/bin", "polardbx-job"), "-job-type=PitrDownloadFile",
                 "-output=" + os.path.join(RESTORE_TEMP_DIR, binlog), "-binlog-source='%s'" % binlog_datasource])
            logger.info("binlog_datasource %s" % response.content)
            p = subprocess.Popen(cmd, shell=True, stdout=sys.stdout)
            p.wait()

            if p.returncode > 0:
                raise Exception("failed to get download binlog url = %s " % downloadUrl)
        # logger.info("apply backup")
        else:
            raise Exception("failed to get download binlog url = %s" % downloadUrl)
    return mysql_binlog_list


def copy_binlog_to_new_path(mysql_bin_list, context, logger):
    # copy backup binlog to new binlog path
    log_dir = context.volume_path(VOLUME_DATA, "log")
    index_file = os.path.join(log_dir, "mysql_bin.index")
    with open(index_file, 'w+') as f:
        for binlog in mysql_bin_list:
            binlog_file_path = os.path.join(RESTORE_TEMP_DIR, binlog)
            new_binlog_path = os.path.join(log_dir, binlog)
            shutil.copy(binlog_file_path, new_binlog_path)
            shutil.chown(new_binlog_path, "mysql", "mysql")
            f.write(new_binlog_path)
            f.write('\n')
            logger.info("binlog_file_path:%s;new_binlog_path:%s" % (binlog_file_path, new_binlog_path))
    shutil.chown(index_file, "mysql", "mysql")
    logger.info("copy binlog to log_path")


def decompress_backup_file(backup_file_name, context, logger):
    decompress_cmd = "%s/xbstream --decompress -x < %s -C %s" % (
        context.xtrabackup_home, os.path.join(RESTORE_TEMP_DIR, backup_file_name),
        context.volume_path(VOLUME_DATA, "data"))
    logger.info("decompress_cmd:%s" % decompress_cmd)
    with subprocess.Popen(decompress_cmd, shell=True, stdout=sys.stdout):
        logger.info("decompress!")


def sort_config(config: configparser.ConfigParser) -> configparser.ConfigParser:
    for section in config._sections:
        # noinspection PyUnresolvedReferences
        config._sections[section] = collections.OrderedDict(
            sorted(config._sections[section].items(), key=lambda t: t[0]))
    config._sections = collections.OrderedDict(sorted(config._sections.items(), key=lambda t: t[0]))
    return config


def create_init_file(context: Context, logger):
    init_filepath = os.path.join(context.mysql_conf, 'mysql-init.sql')
    with open(init_filepath, 'w') as init_file:
        init_file.write("set sql_log_bin=OFF;\n")
        init_file.write("set force_revise=ON;\n")
        init_file.write("update mysql.user set user='root' , host = 'localhost' , authentication_string = '' where user = 'aliyun_root' or user = 'root' ;\n")
        init_file.write("flush privileges;\n")

def initialize_local_mycnf(context: Context, logger):
    indicate = context.current_indicate()
    force = indicate and indicate.reset_config
    if not os.path.exists(context.mysql_conf):
        os.mkdir(context.mysql_conf)

    with open(context.mycnf_path, 'w') as mycnf_file:
        # Acquire the file lock
        fcntl.flock(mycnf_file.fileno(), fcntl.LOCK_EX)

        # Render and write.
        if force or not os.path.exists(context.mycnf_override_path):
            override_config = context.mycnf_override_config()
            with open(context.mycnf_override_path, 'w') as f:
                override_config = sort_config(override_config)
                override_config.write(fp=f)

        override_config = configparser.ConfigParser(allow_no_value=True)
        override_config.read(context.mycnf_override_path)

        overrides = [context.mycnf_system_config(), override_config]
        if os.path.exists(context.file_config_override):
            # override file has the highest priority
            override_file_config = configparser.ConfigParser(allow_no_value=True)
            override_file_config.read(context.file_config_override)
            overrides += [override_file_config]

        r = MycnfRenderer(context.mycnf_template_path)
        r.render(extras=overrides, fp=mycnf_file)

        # Release the lock
        fcntl.flock(mycnf_file.fileno(), fcntl.LOCK_UN)
    logger.info("local mycnf initialized!")


def apply_backup_file(keyring_path_local, context, logger):
    # 应用全量备份集
    apply_backup_cmd = ""
    if context.is_galaxy80():
        apply_backup_cmd = "%s --defaults-file=%s --prepare --target-dir=%s --xtrabackup-plugin-dir=%s --keyring-file-data=%s 2> %s/applybackup.log" \
                           % (context.xtrabackup, context.mycnf_path, context.volume_path(VOLUME_DATA, 'data'),
                            context.xtrabackup_plugin, keyring_path_local,context.volume_path(VOLUME_DATA, "log"))
    elif context.is_xcluster57():
        apply_backup_cmd = "%s --defaults-file=%s --apply-log  %s 2> %s/applybackup.log" \
                           % (context.xtrabackup, context.mycnf_path, context.volume_path(VOLUME_DATA, 'data'),
                              context.volume_path(VOLUME_DATA, "log"))
    logger.info("apply_backup_cmd:%s" % apply_backup_cmd)
    with subprocess.Popen(apply_backup_cmd, shell=True, stdout=sys.stdout):
        logger.info("apply backup")


def chown_data_dir(context, logger):
    check_run_process(['chown', '-R', 'mysql:mysql', context.volume_path(VOLUME_DATA, "data")], logger=logger)


def init_mysqld_metadata(cluster_start_index, commit_index, context, end_term, pod_role, logger, is_pxc_xstore,
                         pitr_endpoint):
    if pod_role == NODE_ROLE_VOTER:
        cluster_start_index = commit_index
        if cluster_start_index is None:
            raise Exception("can NOT get logger commit index")
        logger.info("got logger commit index :%s" % cluster_start_index)

    # mysqld 元数据初始化（标准版和企业版）
    if is_pxc_xstore or len(pitr_endpoint) != 0:
        init_metadata_cmd = [os.path.join(context.engine_home, 'bin', 'mysqld'),
                             "--defaults-file=" + context.mycnf_path,
                             "--cluster-current-term=" + str(end_term),
                             "--cluster-info=" + context.xcluster_info_argument(local=True),
                             "--cluster-force-change-meta=ON",
                             "--cluster-force-single-mode=ON",
                             "--loose-cluster-force-recover-index=" + str(cluster_start_index),
                             "--cluster-start-index=" + str(cluster_start_index)
                             ]
    else:
        init_metadata_cmd = [os.path.join(context.engine_home, 'bin', 'mysqld'),
                             "--defaults-file=" + context.mycnf_path,
                             "--cluster-info=" + context.xcluster_info_argument(local=True),
                             "--cluster-force-change-meta=ON",
                             "--cluster-force-single-mode=ON",
                             "--loose-cluster-force-recover-index=" + str(cluster_start_index),
                             "--cluster-start-index=" + str(cluster_start_index)
                             ]
    logger.info("init_metadata_cmd" + str(init_metadata_cmd))
    check_run_process(init_metadata_cmd, logger=logger)


# 同步集群元数据
def sync_cluster_metadata(context, logger):
    sync_metadata_cmd = [os.path.join(context.engine_home, 'bin', 'mysqld'),
                         "--defaults-file=" + context.mycnf_path,
                         "--cluster-info=" + context.xcluster_info_argument(name_from_env=True),
                         "--cluster-force-change-meta=ON"
                         ]
    logger.info("sync_metadata_cmd" + str(sync_metadata_cmd))
    check_run_process(sync_metadata_cmd, logger=logger)


def get_xtrabackup_binlog_info_from_instance_local(context: Context):
    data_dir = context.volume_path(VOLUME_DATA, "data")
    xtrabackup_binlog_info_path = os.path.join(data_dir, "xtrabackup_binlog_info")
    if not os.path.exists(xtrabackup_binlog_info_path):
        return None
    with open(xtrabackup_binlog_info_path, "r") as f:
        binlog_info = f.read().strip()
        if binlog_info:
            str_list = binlog_info.split()
            return str_list[1] if len(str_list) >= 2 else str_list[0]


def wait_binlog_apply_ready(mysql_port, end_log_index, logger):
    timeout = 48 * 60 * 60
    deadline = time.time() + timeout
    cnt = 0
    while time.time() < deadline:
        logger.info("wait applying binlog")
        try:
            time.sleep(10)
            if check_binlog_apply_index_status(mysql_port, end_log_index, logger, False):
                return
            if cnt > 10:
                # 兼容DN空binlog的bug
                if check_binlog_apply_index_status(mysql_port, end_log_index, logger, True):
                    return
            cnt = cnt + 1
        except Exception as e:
            logger.info(e)
    raise TimeoutError("binlog apply timeout!")


def check_binlog_apply_index_status(mysql_port, end_log_index, logger, isEmptyBinlog):
    sql_list = "select * from information_schema.alisql_cluster_local"
    logger.info("Execute SQL: %s" % sql_list)

    stat, output = execute_mysqlcmd(mysql_port, sql_list, db='information_schema')
    logger.info("get local node info: %s, %s", stat, output)
    if not output:
        raise Exception("can not get xdb full health info")

    rows = output.split("\n")

    for row in rows:
        columns = row.split("\t")
        logger.info("columns: %s" % columns)
        logger.info("last apply index: %s" % columns[-3])
        logger.info("end_log_index: %s" % end_log_index)
        if isEmptyBinlog and int(columns[-3]) < int(end_log_index) - 1:
            return False
        if not isEmptyBinlog and int(columns[-3]) < int(end_log_index):
            return False
    return True


def execute_mysqlcmd(port, cmd, db=None, host='127.0.0.1', user='root', autocommit=False, **kwargs):
    kwargs_base = {
        'db': db,
        'init_command': '',
        'connect_timeout': CONN_TIMEOUT
    }
    kwargs_base.update(kwargs)
    new_kwargs = dict([(k, v) for k, v in kwargs_base.items() if v])
    result = []
    conn = None
    try:
        conn = mysql.connect(host=host, port=int(port), user=user, passwd='', **new_kwargs)
        if isinstance(cmd, str):
            cmd = [cmd]
        row_count = 0
        if autocommit:
            # autocommit default false, unless specify
            conn.autocommit(autocommit)
        cursor = conn.cursor()
        for c in cmd:
            sql = INTERNAL_MARK + c
            row_count += cursor.execute(sql)
        rows = cursor.fetchall()

        for row in rows:
            row_str = '\t'.join([item.strip("'") for item in map(conn.literal, row)])
            result.append(row_str)
        conn.commit()
        result_str = '\n'.join(result)
    except Exception as e:
        return 1, str(e)
    finally:
        if conn:
            conn.close()
    return 0, result_str


def show_last_and_first_binlog(context, logger):
    log_dir = context.volume_path(VOLUME_DATA, "log")
    index_file = os.path.join(log_dir, "mysql_bin.index")
    last_file_index = -1
    first_file_index = 1000000000
    with open(index_file, "r") as f:
        for text_line in f.readlines():
            last_file_index = max(int(text_line.split('.')[-1]), last_file_index)
            first_file_index = min(int(text_line.split('.')[-1]), first_file_index)
    last_binlog = "mysql_bin.%06d" % last_file_index
    first_binlog = "mysql_bin.%06d" % first_file_index
    last_binlog = os.path.join(log_dir, last_binlog)
    first_binlog = os.path.join(log_dir, first_binlog)
    logger.info("get last binlog:%s;first binlog:%s" % (last_binlog, first_binlog))
    return last_binlog, first_binlog


def truncate_last_binlog(binlog_path, endTS, context, logger):
    """
    bb truncate mysql_bin.xxxxx --end-ts  --output cut.xxxxx
    """
    log_dir = context.volume_path(VOLUME_DATA, "log")
    out_file = os.path.join(log_dir, "cut.out")

    truncate_cmd = [context.bb_home, 'truncate', binlog_path,
                    '--end-ts', str(endTS),
                    '--output', out_file]
    check_run_process(truncate_cmd, logger=logger)
    shutil.chown(out_file, "mysql", "mysql")

    os.rename(binlog_path, binlog_path + ".bak")
    os.rename(out_file, binlog_path)


def download_pitr_ts(pitrEndpoint, logger):
    configUrl = "/".join([pitrEndpoint, "config"])
    response = requests.get(configUrl)
    if response.status_code == 200:
        logger.info("config http response %s" % response.content)
        config = json.loads(response.content)
        pitr_ts = config['timestamp']
    else:
        raise Exception("failed to get config url = %s" % configUrl)
    return pitr_ts


def xdb_show_binlog_index(binlog_path, context, logger):
    """
    ../bin/mysqlbinlogtailor --show-index-info mysql-bin.xxxxxx，
    输出格式为 [start_index:start_term, end_index:end_term]
    :param binlog_path:
    :return:
    """
    cmd = [context.mysqlbinlogtailor,
           "--show-index-info",
           binlog_path
           ]

    logger.info("show_binlog_cmd:%s" % cmd)
    with subprocess.Popen(cmd, stdout=subprocess.PIPE) as proc:
        index_info = proc.stdout.read().decode('utf-8')
        logger.info("xdb_show_binlog_index out" + index_info)

    temp = index_info.strip().strip('[[]]').replace(' ', '')
    end_index = temp.split(',')[1].split(':')[0]
    end_term = temp.split(',')[1].split(':')[1]
    logger.info("end_index:%s;end_term:%s" % (end_index, end_term))
    return end_index, end_term


restore_group.add_command(start)
