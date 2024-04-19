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
import re
import subprocess

import click

from core.context import Context
from core.log import LogFactory
from core.convention import *
from core.backup_restore.xstore_binlog import XStoreBinlog
from core.backup_restore.storage.filestream_client import FileStreamClient, BackupStorage


@click.group(name="binlogbackup")
def binbackup_group():
    pass


@click.command(name='start')
@click.option('--backup_context', required=True, type=str)
@click.option('-si', '--start_index', required=True, type=str)
@click.option('-g', '--gms_label', required=True, type=str)
@click.option('-xs', '--xstore_name', required=True, type=str)
def start_binlogbackup(backup_context,  start_index, gms_label, xstore_name):
    logger = LogFactory.get_logger("binlogbackup.log")

    with open(backup_context, 'r') as f:
        params = json.load(f)
        collect_end_index = params["collectEndIndex"]
        indexes_path = params["indexesPath"]
        remote_binlog_backup_dir = params["binlogBackupDir"]
        storage_name = params["storageName"]
        sink = params["sink"]

    logger.info("start binlog backup")
    context = Context()
    mysql_port = context.port_access()
    binlog = XStoreBinlog(mysql_port)
    log_dir = context.volume_path(VOLUME_DATA, "log")
    backup_dir = context.volume_path(VOLUME_DATA, 'backup')
    local_binlog_backup_dir = os.path.join(backup_dir, "binlogbackup")

    filestream_client = FileStreamClient(context, BackupStorage[str.upper(storage_name)], sink)

    os.makedirs(local_binlog_backup_dir, exist_ok=True)

    # 获取binlog的起始文件和最终文件
    min_log_name = get_min_log_name(context, log_dir, start_index, logger)
    logger.info("min_log_name:%s" % min_log_name)
    if gms_label == "true":
        # Todo： optimization cut gms binlog
        max_log_name, max_log_index = collect_end_index.split(':')
    else:
        max_log_name, max_log_index = get_max_log_from_cp(filestream_client, indexes_path, local_binlog_backup_dir,
                                                          xstore_name, logger)
    # 将可上传的binlog上传
    binlog_list = binlog.get_local_binlog(min_binlog_name=min_log_name, max_binglog_name=max_log_name,
                                          left_contain=True, right_contain=False)
    upload_binlog_info(binlog_list, log_dir, remote_binlog_backup_dir, filestream_client, logger)
    truncate_and_upload_binlog_info(context, log_dir, local_binlog_backup_dir, remote_binlog_backup_dir,
                                    filestream_client, max_log_name, max_log_index, logger)

    # 记录所有上传的binlog_name_list，用于后续恢复时下载binlog
    uploaded_binlog_list = [log_name for i, (log_name, start_log_index) in enumerate(binlog_list)]
    if max_log_name not in uploaded_binlog_list:
        uploaded_binlog_list.append(max_log_name)
    filestream_client.upload_from_string(remote=os.path.join(remote_binlog_backup_dir, "binlog_list"),
                                         string='\n'.join(uploaded_binlog_list), logger=logger)
    logger.info("List of uploaded binlog:%s", uploaded_binlog_list)

    logger.info("upload finished")


def get_min_log_name(context, log_dir, start_index, logger):
    logger.info("start_index: %s " % str(start_index))
    for i in range(1,999999):
        binlog_tmp_name = "mysql_bin.%06d" % i
        binlog_tmp_file = os.path.join(log_dir,binlog_tmp_name)
        if os.path.exists(binlog_tmp_file):
            tailor_cmd = [context.mysqlbinlogtailor,"--show-index-info", binlog_tmp_file]
            logger.info("tailor_cmd:%s" % tailor_cmd)
            with subprocess.Popen(tailor_cmd,stdout=subprocess.PIPE) as pipe:
                index_line = pipe.stdout.readline().decode("utf-8").lstrip('[')
                start_log_index = index_line.split(',')[0].split(':')[0]
                end_log_index = index_line.split(',')[1].split(':')[0].strip()
                logger.info("tailer_info:%s" % index_line)
            if int(start_log_index) <= int(start_index) <= int(end_log_index):
                logger.info("start_log_index:%s;end_log_index:%s;" % (str(start_log_index),str(end_log_index)))
                return binlog_tmp_name
            if int(start_log_index) > int(start_index):
                return ""
        else:
            continue


# noinspection DuplicatedCode
def get_max_log_from_cp(filestream_client, indexes_path, binlog_backup_dir, xstore_name, logger):
    indexes_local_path = os.path.join(binlog_backup_dir, "indexes")
    filestream_client.download_to_file(remote=indexes_path, local=indexes_local_path, logger=logger)
    xstore_pattern = xstore_name + ':'  # such as "pxc-dn-1:"
    with open(indexes_local_path, 'r') as f:
        for text_line in f.readlines():
            m = re.search(xstore_pattern, text_line)
            if m:
                max_log_info = text_line.split(xstore_pattern)[-1].strip()
                break
    logger.info("max_log_info:" + max_log_info)
    return max_log_info.split(':')[0], max_log_info.split(':')[1]


def truncate_and_upload_binlog_info(context, log_dir, binlogbackup_dir, binlogbackupdir_path, filestream_client,
                                    max_log_name, max_log_index, logger):
    binlog_file_path = os.path.join(log_dir, max_log_name)
    truncated_log_name = "{}_trunc.{}".format(*max_log_name.split('.'))
    truncate_file_path = os.path.join(binlogbackup_dir, truncated_log_name)
    last_event_timestamp_path = os.path.join(binlogbackup_dir, "last_event_timestamp")
    truncate_cmd = "%s truncate %s --end-offset %s -o %s" % (context.bb_home, binlog_file_path,
                                                             max_log_index, truncate_file_path)
    logger.info("truncate_cmd:" + truncate_cmd)
    with subprocess.Popen(truncate_cmd, shell=True, stdout=subprocess.PIPE) as pipe:
        logger.info("cut max log")
        output = pipe.stdout.readline().decode("utf-8")
        res = re.search(r"LAST EVENT TIMESTAMP: (\d+)", output)
        if res:
            last_event_timestamp = res.group(1)
            logger.info("last event timestamp: " + last_event_timestamp)
            with open(last_event_timestamp_path, 'w') as f:  # use to display in pxb right after binlog backup
                f.write(last_event_timestamp)
            filestream_client.upload_from_file(remote=os.path.join(binlogbackupdir_path, "last_event_timestamp"),
                                               local=last_event_timestamp_path, logger=logger)
    filestream_client.upload_from_file(remote=os.path.join(binlogbackupdir_path, max_log_name),
                                       local=truncate_file_path, logger=logger)


def upload_binlog_info(binlog_list, log_dir, binlog_backup_dir_path, filestream_client, logger):
    for i, (log_name, start_log_index) in enumerate(binlog_list):
        logger.info("log to upload:%s during binlog backup" % log_name)
        binlog_file_path = os.path.join(log_dir, log_name)
        filestream_client.upload_from_file(remote=os.path.join(binlog_backup_dir_path, log_name),
                                           local=binlog_file_path, logger=logger)


binbackup_group.add_command(start_binlogbackup)
