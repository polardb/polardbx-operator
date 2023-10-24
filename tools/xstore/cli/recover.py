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
import os.path

import click
import wget

from core.context import Context
from core.log import LogFactory
from core.backup_restore.storage.filestream_client import BackupStorage, FileStreamClient
from core.backup_restore.utils import check_run_process

CONN_TIMEOUT = 30
INTERNAL_MARK = '/* rds internal mark */ '
RESTORE_TEMP_DIR = "/data/mysql/restore"


@click.group(name="recover")
def recover_group():
    pass


@click.command(name='start')
@click.option('--restore_context', required=True, type=str)
@click.option('-tp', '--target_pod', required=True, type=str)
@click.option('-p', '--password', required=True, type=str)
def start(restore_context, target_pod, password):
    logger = LogFactory.get_logger("recover.log")
    context = Context()
    with open(restore_context, 'r') as f:
        params = json.load(f)
        remote_cp_path = params["cpfilePath"]
        storage_name = params["storageName"]
        sink = params["sink"]
        pitr_endpoint = params["pitrEndpoint"] if "pitrEndpoint" in params else ""

    local_cp_path = os.path.join(RESTORE_TEMP_DIR, "set.cp")
    if len(pitr_endpoint) == 0:
        filestream_client = FileStreamClient(context, BackupStorage[str.upper(storage_name)], sink)
        filestream_client.download_to_file(remote=remote_cp_path, local=local_cp_path, logger=logger)
    else:
        download_url = "/".join([pitr_endpoint, "download", "recovertxs"])
        wget.download(download_url, local_cp_path)

    recover_cmd = [context.bb_home, 'recover',
                   '-f', local_cp_path,
                   '-h', target_pod,  # TODO(dengli): more elegant way
                   '-P', str(context.port_access()),
                   '-u', 'admin',
                   '-p', password]
    check_run_process(recover_cmd, logger=logger)


recover_group.add_command(start)
