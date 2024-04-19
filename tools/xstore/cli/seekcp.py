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

from core.context import Context
import click

from core.log import LogFactory
from core.convention import *
from core.backup_restore.storage.filestream_client import BackupStorage, FileStreamClient


@click.group(name="seekcp")
def seekcp_group():
    pass


@click.command(name="start")
@click.option('--seekcp_context', required=True, type=str)
def start_seekcp(seekcp_context):
    logger = LogFactory.get_logger("seekcp.log")

    with open(seekcp_context) as f:
        params = json.load(f)
        remote_cp_path = params["remoteCpPath"]
        tx_events_dir = params["txEventsDir"]
        indexes_file = params["indexesPath"]
        dn_name_list = params["dnNameList"].split(',')
        storage_name = params["storageName"]
        sink = params["sink"]

    context = Context()

    backup_dir = context.volume_path(VOLUME_DATA, 'backup')
    if not os.path.exists(backup_dir):
        os.mkdir(backup_dir)

    filestream_client = FileStreamClient(context, BackupStorage[str.upper(storage_name)], sink)

    local_tx_dir = os.path.join(backup_dir, "seekcp")
    os.makedirs(local_tx_dir, exist_ok=True)

    cpfile = os.path.join(local_tx_dir, "a.cp")

    # download .evs file
    download_evs_file(filestream_client, local_tx_dir, tx_events_dir, dn_name_list, logger)

    # get heartbeat_id
    hb_tx_id = get_heartbeat_txid(context, filestream_client, tx_events_dir, logger)

    logger.info("Got HeartBeat TX ID:%s" % hb_tx_id)
    seekcp_cmd = [context.bb_home, 'seekcp',
                  '--binary-events',
                  '--heartbeat-txid', hb_tx_id,
                  '-v', local_tx_dir,
                  '--output', cpfile
                  ]
    logger.info("seek_cmd:%s" % seekcp_cmd)
    with subprocess.Popen(seekcp_cmd, stdout=subprocess.PIPE) as pipe:
        get_binlog_indexes(pipe, filestream_client, indexes_file, logger)
    filestream_client.upload_from_file(remote=remote_cp_path, local=cpfile, logger=logger)


def get_binlog_indexes(pipe, filestream_client, indexes_file, logger):
    stdout_text = pipe.stdout.read()
    stdout_text_lines = stdout_text.splitlines()
    binlog_indexes_str_list = []
    for i in reversed(range(len(stdout_text_lines))):
        binlog_indexes_str_list.append(stdout_text_lines[i].decode("utf-8").strip())
        if re.search('BINLOG INDEXES:', stdout_text_lines[i].decode("utf-8")):
            break
    binlog_indexes_str = '\n'.join(binlog_indexes_str_list[::-1])
    logger.info("binlog_indexes_str:%s" % binlog_indexes_str)
    filestream_client.upload_from_string(remote=indexes_file, string=binlog_indexes_str, logger=logger)


def get_heartbeat_txid(context, filestream_client, tx_events_dir, logger):
    local_heartbeat_path = os.path.join(context.volume_path(VOLUME_DATA, "backup", "heartbeat"))
    if not os.path.exists(local_heartbeat_path):  # to deal with oss download issue
        remote_heartbeat_path = os.path.join(tx_events_dir, 'heartbeat')
        filestream_client.download_to_file(remote=remote_heartbeat_path, local=local_heartbeat_path, logger=logger)

    with open(local_heartbeat_path, "r") as file:
        hb_tx_id = file.readline()
    return hb_tx_id.split(':')[-1].strip()


def download_evs_file(filestream_client, local_tx_dir, remote_tx_dir, dn_name_list, logger):
    for dn in dn_name_list:
        if dn.endswith("gms"):  # no need to download evs for gms
            continue
        remote_path = os.path.join(remote_tx_dir, dn + ".evs")
        local_path = os.path.join(local_tx_dir, dn + ".evs")
        filestream_client.download_to_file(remote=remote_path, local=local_path, logger=logger)


seekcp_group.add_command(start_seekcp)
