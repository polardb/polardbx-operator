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
import re
import subprocess
import shutil

import click

from core.context import Context
from core.convention import *
from core.log import LogFactory
from core.backup_restore.storage.filestream_client import FileStreamClient, BackupStorage


@click.group(name="backup")
def backup_group():
    pass


@click.command(name='start')
@click.option('--backup_context', required=True, type=str)
@click.option('-j', '--job_name', required=True, type=str)
def start_backup(backup_context, job_name):
    logger = LogFactory.get_logger("fullbackup.log")
    with open(backup_context, 'r') as f:
        params = json.load(f)
        fullbackup_path = params["fullBackupPath"]
        storage_name = params["storageName"]
        sink = params["sink"]

    try:
        logger.info('start backup')
        context = Context()
        sockfile = context.volume_path(VOLUME_DATA, 'run', 'mysql.sock')
        backup_dir = context.volume_path(VOLUME_DATA, 'backup')
        if os.path.exists(backup_dir):
            shutil.rmtree(backup_dir)
        os.mkdir(backup_dir)

        backup_cmd = ""
        if context.is_galaxy80():
            backup_cmd = [context.xtrabackup,
                          "--stream=xbstream",
                          "--socket=" + sockfile,
                          "--slave-info",
                          "--backup", "--lock-ddl"]
        elif context.is_xcluster57():
            backup_cmd = [context.xtrabackup,
                          "--stream=xbstream",
                          "--socket=" + sockfile,
                          backup_dir]
        logger.info("backup_cmd: %s " % backup_cmd)

        stderr_path = backup_dir + '/fullbackup-stderr.out'
        upload_stderr_path = backup_dir + '/upload.out'
        stderr_outfile = open(stderr_path, 'w+')
        upload_stderr_outfile = open(upload_stderr_path, 'w+')
        filestream_client = FileStreamClient(context, BackupStorage[str.upper(storage_name)], sink)
        with subprocess.Popen(backup_cmd, bufsize=8192, stdout=subprocess.PIPE, stderr=stderr_outfile, close_fds=True) as pipe:
            filestream_client.upload_from_stdin(remote_path=fullbackup_path, stdin=pipe.stdout,
                                                stderr=upload_stderr_outfile, logger=logger)
            pipe.stdout.close()
        get_binlog_commit_index(job_name, stderr_path, logger)
        logger.info("backup upload finished")

    except Exception as e:
        logger.info(e)
        raise e


def get_binlog_commit_index(job_name, stderr_path, logger):
    # parse stderr to get commit_index
    with open(stderr_path,'r') as file:
        stderr_text = file.read()
    stderr_text_lines = stderr_text.splitlines()
    logger.info("backup result: %s" % stderr_text)
    binlog_pos = -1
    slave_pos = -1
    for i in reversed(range(len(stderr_text_lines))):
        if re.search('MySQL binlog position:', stderr_text_lines[i]):
            binlog_pos = i
            break
        elif re.search('MySQL slave binlog position:', stderr_text_lines[i]):
            slave_pos = i
    binlog_line = " ".join(stderr_text_lines[binlog_pos:slave_pos])
    # following is for xdb
    slave_status = {}
    m = re.search(r"role: '(\S*)'", binlog_line)
    if m:
        slave_status['ROLE'] = m.group(1)
    m = re.search(r"commit_index:'(\d+)'", binlog_line)
    if m:
        slave_status['COMMIT_INDEX'] = m.group(1)
    m = re.search(r"consensus_apply_index:'(\d+)'", binlog_line)
    if m:
        slave_status['CONSENSUS_APPLY_INDEX'] = m.group(1)
    with open("/data/mysql/tmp/" + job_name + ".idx", mode='w+', encoding='utf-8') as f:
        if slave_status['CONSENSUS_APPLY_INDEX'] == "0" or slave_status['CONSENSUS_APPLY_INDEX'] == "1":
            f.write(slave_status['COMMIT_INDEX'])
        else:
            f.write(slave_status['CONSENSUS_APPLY_INDEX'])
    logger.info("binlog line parsed: %s" % slave_status)


backup_group.add_command(start_backup)
