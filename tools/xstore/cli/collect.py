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
from core.backup_restore.storage.filestream_client import BackupStorage, FileStreamClient
from core.backup_restore.utils import check_run_process


@click.group(name="collect")
def collect_group():
    pass


def download_binlog_offset_to_local(offset_file_name, filestream_client, backup_dir):
    offset_local_file = os.path.join(backup_dir, "offset")
    os.makedirs(offset_local_file, exist_ok=True)

    start_binlog_name, start_offset = download_binlog_offset(True, offset_file_name,
                                                             offset_local_file, filestream_client)
    end_binlog_name, end_offset = download_binlog_offset(False, offset_file_name,
                                                         offset_local_file, filestream_client)
    return start_binlog_name, start_offset, end_binlog_name, end_offset


def get_binlog_list(context, start_binlog_name, end_binlog_name):
    mysql_port = context.port_access()
    binlog = XStoreBinlog(mysql_port)
    log_dir = context.volume_path(VOLUME_DATA, "log")
    binlog_list = binlog.get_local_binlog(min_binlog_name=start_binlog_name, max_binglog_name=end_binlog_name,
                                          left_contain=True, right_contain=True)
    binlog_path_list = []
    for i, (logname, start_log_index) in enumerate(binlog_list):
        binlog_path_list.append(os.path.join(log_dir, logname))
    return binlog_list, binlog_path_list


def seekhb_and_upload(filstream_client, file_path, context, binlog_list, heartbeat_name, backup_dir, logger):
    local_hb_path = os.path.join(backup_dir, "heartbeat")
    remote_hb_path = os.path.join('/'.join(file_path.split('/')[:-1]), "heartbeat")
    log_dir = context.volume_path(VOLUME_DATA, "log")
    for i, (log_name, start_log_index) in enumerate(binlog_list):
        binlog_file_path = os.path.join(log_dir, log_name)
        seekhb_cmd = context.bb_home + " seekhb " + binlog_file_path + " --sname " + heartbeat_name
        logger.info("seekhb_cmd:%s" % seekhb_cmd)
        with subprocess.Popen(seekhb_cmd, shell=True, stdout=subprocess.PIPE) as pipe:
            trans_info = pipe.stdout.read().decode("utf-8")
            if re.search(r'TRANSACTION', trans_info):
                logger.info("got heartbeatID:%s" % trans_info)
                with open(local_hb_path, 'w') as f:
                    f.write(trans_info)
                filstream_client.upload_from_string(remote=remote_hb_path, string=trans_info, logger=logger)
                break


def collect_and_upload(context, start_binlog_name, start_offset, end_binlog_name, end_offset, binlog_path_list,
                       file_path, filestream_client, backup_dir, logger):
    collect_local_file = os.path.join(backup_dir, "collect")
    os.makedirs(collect_local_file, exist_ok=True)
    local_collect_file_path = os.path.join(collect_local_file, "collect.evs")
    start_binlog = start_binlog_name + ":" + start_offset
    end_binlog = end_binlog_name + ":" + end_offset
    collect_cmd = [context.bb_home, 'txdump',
                   '--start-offset', start_binlog,
                   '--end-offset', end_binlog,
                   '--bin', '--output', local_collect_file_path
                   ] + binlog_path_list
    check_run_process(collect_cmd, logger=logger)
    filestream_client.upload_from_file(remote=file_path, local=local_collect_file_path, logger=logger)


def download_binlog_offset(is_start, offsetfile_name, offset_local_file, filestream_client):
    if is_start:
        tmp_file = "-start"
    else:
        tmp_file = "-end"
    remote_file = offsetfile_name + tmp_file
    local_file = os.path.join(offset_local_file, "offset" + tmp_file)
    filestream_client.download_to_file(remote=remote_file, local=local_file)
    with open(local_file) as f:
        offset = f.readline().rstrip('\n')
    return offset.split(':')[0], offset.split(":")[-1]


@click.command(name="start")
@click.option('--backup_context', required=True, type=str)
@click.option('-hb', '--heartbeat_name', required=True, type=str)
def collect_binlog_index(backup_context, heartbeat_name):
    logger = LogFactory.get_logger("collect.log")
    context = Context()
    with open(backup_context) as f:
        params = json.load(f)
        collect_file = params["collectFilePath"]
        offsetfile_name = params["offsetFileName"]
        storage_name = params["storageName"]
        sink = params["sink"]

    backup_dir = context.volume_path(VOLUME_DATA, 'backup')
    if not os.path.exists(backup_dir):
        os.mkdir(backup_dir)

    filestream_client = FileStreamClient(context, BackupStorage[str.upper(storage_name)], sink)

    start_binlog_name, start_offset, end_binlog_name, end_offset = download_binlog_offset_to_local(offsetfile_name,
                                                                                                   filestream_client,
                                                                                                   backup_dir)
    # binlog_list only records binlog name, while binlog_path_list contains absolute path for each binlog
    binlog_list, binlog_path_list = get_binlog_list(context, start_binlog_name, end_binlog_name)

    logger.info("start_binlog_name:%s, start_offset:%s, end_binlog_name:%s, end_offset:%s",
                start_binlog_name, start_offset, end_binlog_name, end_offset)
    seekhb_and_upload(filestream_client, collect_file, context, binlog_list, heartbeat_name, backup_dir, logger)

    collect_and_upload(context, start_binlog_name, start_offset, end_binlog_name, end_offset, binlog_path_list,
                       collect_file, filestream_client, backup_dir, logger)


collect_group.add_command(collect_binlog_index)


@click.command(name='upload_offset')
@click.option('-o', '--offset', required=True, type=str)
@click.option('-p', '--path', required=True, type=str)
@click.option('--storage_name', required=True, type=str)
@click.option('--sink', required=True, type=str)
def upload_offset(offset, path, storage_name, sink):
    """
    upload binlog offset to remote storage
    """
    logger = LogFactory.get_logger("collect.log")
    logger.info("Binlog offset: %s", offset)
    context = Context()
    filestream_client = FileStreamClient(context, BackupStorage[str.upper(storage_name)], sink)
    filestream_client.upload_from_string(remote=path, string=offset, logger=logger)


collect_group.add_command(upload_offset)
