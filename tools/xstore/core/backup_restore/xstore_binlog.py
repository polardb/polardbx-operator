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

# !/usr/bin/env python3
# coding=utf-8
import logging
import re
import time
import pymysql
from core.context import Context
from core.convention import *


class XStoreBinlog():
    BINLOG_PATTERN = 'mysql_bin\.(\d+)$'

    def __init__(self, mysql_port):
        self.host = None
        self.db_port = mysql_port

        context = Context()
        self.binlog_dir = context.volume_path(VOLUME_DATA, 'mysql', 'log')
        self.user = "root"
        self.decrypted_passwd = ""

    # get binlog from  (min_binlog_name, max_binglog_name]
    def get_local_binlog(self, min_binlog_name=None, max_binglog_name=None,left_contain=False,
                         right_contain=False):
        if not max_binglog_name:
            sql = "select LOGNAME from information_schema.consensus_commit_position"
            max_binglog_name = mysql_do_select(self.db_port, sql, host=self.host, user=self.user,
                                               passwd=self.decrypted_passwd,
                                               retry=False, fetchone=True)

        local_binlogs = filter_xdb_binlog_by_pattern(self.BINLOG_PATTERN, self.db_port, self.user,
                                                     self.decrypted_passwd,
                                                     host=self.host, minfilter=min_binlog_name,
                                                     maxfilter=max_binglog_name,left_contain=left_contain,
                                                     right_contain=right_contain)
        return local_binlogs


def connect(host, port, user, password, database, use_unicode=True, charset='utf8', connect_timeout=3, timeout=30,
            autocommit=False):
    password = '' if password is None else password
    context = Context()
    sockfile = context.volume_path(VOLUME_DATA, 'run', 'mysql.sock')

    return pymysql.connect(host=host, port=int(port), user=user, password=password, database=database,
                           use_unicode=use_unicode, charset=charset or 'utf8', connect_timeout=connect_timeout,
                           read_timeout=timeout, write_timeout=timeout, autocommit=autocommit, unix_socket=sockfile)


def mysql_do_select(port, sqlstr, params=(), host=None, user=None, passwd=None, db=None, use_unicode=True,
                    charset='utf8',
                    fetchone=False, as_dict=False, connect_timeout=3, timeout=10, retry=True, raise_error=True):
    host = host or '127.0.0.1'
    user = user or 'root'
    timeout_at = int(time.time()) + timeout
    need_retry_errors = ['Can\'t connect to MySQL server',
                         'running with the --read-only option',
                         'Too many connections',
                         '--rds-deny-access']
    no_warn_sqls = ["show failover status"]
    sleep_time = 1
    while time.time() < timeout_at:
        conn = None
        cursor = None
        try:
            conn = connect(host=host, port=int(port), user=user, password=passwd, database=db, use_unicode=use_unicode,
                           charset=charset, connect_timeout=connect_timeout, timeout=timeout, autocommit=True)
            if as_dict:
                conn.cursorclass = pymysql.cursors.DictCursor
            cursor = conn.cursor()
            cursor.execute(sqlstr, params)
            if fetchone:
                ret = cursor.fetchone()
                if ret is None or len(ret) == 0:
                    ret = None
                elif len(ret) == 1 and not as_dict:
                    ret = ret[0]
            else:
                ret = cursor.fetchall()
            return ret
        except Exception as e:
            error_string = str(e).lower()
            if retry and any(x.lower() in error_string for x in need_retry_errors):
                sleep_time = min(5, sleep_time + 1)
                time.sleep(sleep_time)
                continue
            elif any(x.lower() in sqlstr for x in no_warn_sqls):
                return None
            else:
                print('do_select sqlstr=%r for host=%r port=%r final error: %r' % (sqlstr, host, port, e))
                if not raise_error:
                    return None
                raise e
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


def filter_xdb_binlog_by_pattern(pattern, port, user, passwd, host=None, minfilter=None, maxfilter=None,
                                 left_contain=False,
                                 right_contain=False, retry=False, raise_error=True):
    """ filter file according to pattern which group name > minfilter and < maxfilter"""
    sqlstr = "show consensus logs"
    try:
        binlog_list = mysql_do_select(port, sqlstr, host=host, user=user, passwd=passwd, retry=retry,
                                      raise_error=raise_error)
    except Exception as e:
        if '--rds-deny-access' in str(e):
            binlog_list = []
        else:
            raise
    if not binlog_list:
        return []

    compile_pattern = re.compile(pattern)
    filter_list = []
    logging.info("minfilter:%s" % minfilter)
    min_index = None if not minfilter else int(compile_pattern.match(minfilter).group(1))
    max_index = None if not maxfilter else int(compile_pattern.match(maxfilter).group(1))
    for logname, file_size, start_log_index in binlog_list:
        m = compile_pattern.match(logname)
        if not m:
            continue
        file_index = int(m.group(1))
        if min_index is not None:
            if left_contain and min_index > file_index:
                continue
            if not left_contain and min_index >= file_index:
                continue
        if max_index is not None:
            if right_contain and file_index > max_index:
                continue
            if not right_contain and file_index >= max_index:
                continue
        filter_list.append((logname, start_log_index))
    return sorted(filter_list, key=lambda one: int(compile_pattern.match(one[0]).group(1)))


def main():
    print("test binlog fetch")


if __name__ == "__main__":
    main()
