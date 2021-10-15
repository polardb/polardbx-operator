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
import sys

import click
import pymysql

from .common import global_mgr


def fetchall_with_lowercase_fieldnames(cursor: pymysql.cursors.Cursor):
    """
    Fetch all rows with lowercase field names.

    :param cursor: cursor.
    :return: a list of dict of column names and values.
    """
    # convert to lowercase fields
    field_name = [field[0].lower() for field in cursor.description]
    return [dict(zip(field_name, r)) for r in cursor.fetchall()]


def fetchone_with_lowercase_fieldnames(cursor: pymysql.cursors.Cursor):
    """
    Fetch one row with lowercase field names.

    :param cursor: cursor.
    :return: a dict of column names and values.
    """
    # convert to lowercase fields
    field_name = [field[0].lower() for field in cursor.description]
    return dict(zip(field_name, cursor.fetchone()))


@click.group('log')
def log_group():
    pass


def purge_binary_logs(left):
    """
    Purge binary logs with native MySQL SQL interface.
    :param left:
    :return:
    """
    with global_mgr.new_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('show binary logs')
            rows = fetchall_with_lowercase_fieldnames(cur)
            binary_logs = [r['log_name'] for r in rows]
        if len(binary_logs) <= left:
            print('no need to purge')
            return
        with conn.cursor() as cur:
            cur.execute('purge binary logs to \'%s\'' % binary_logs[-left])


@click.command('purge', help='Purge local logs, such binary logs. Do not use '
                             'this on clustered node. Use consensus log purge instead.')
@click.option('--type', default='binary', type=str)
@click.option('--left', default=5, type=int, show_default=True)
def purge_logs(type, left):
    if type == 'binary':
        purge_binary_logs(left)
    else:
        print('unsupported log type: ' + type)
        sys.exit(1)


log_group.add_command(purge_logs)
