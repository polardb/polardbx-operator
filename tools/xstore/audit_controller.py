#!/usr/bin/env python3


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
import logging
import subprocess
import threading
import time
import os

from core import Manager, Context
from core.context.k8s import PodInfo

from mysql.connector.pooling import MySQLConnectionPool
from mysql.connector.errors import Error, DatabaseError, PoolError, InterfaceError

SECOND = 1
LABEL_CHECK_INTERVAL = 10 * SECOND
LOG_CHECK_INTERVAL = 1 * SECOND
MAX_AUDIT_LOG_FILE_NUM = 50
MAX_SLOW_LOG_FILE_NUM = 50
POOL_SIZE = 3

AUDIT_DIR = "/data/mysql/tmp"
SLOW_LOG_DIR = "/data/mysql/data/mysql"
PYTHON_PATH = "/tools/xstore/current/venv/bin/python3"

AUDIT_ENABLED_VAR_MYSQL80 = "rds_audit_log_enabled"
AUDIT_ENABLED_VAR_MYSQL57 = "opt_rds_audit_log_enabled"
AUDIT_FLUSH_CMD_MYSQL80 = "SET GLOBAL rds_audit_log_flush = ON"
AUDIT_FLUSH_CMD_MYSQL57 = "flush rds_audit logs"
SQL_BINLOG = "sql_log_bin"
SQL_INTERNAL_MARK = "/* rds internal mark */ "

LOG_OUTPUT_VAR = "log_output" # option: FILE, TABLE, NONE
LOG_ROTATE_CMD = "set session rotate_log_table = on;"
SLOW_LOG_FLUSH_CMD = "flush slow logs;"
SLOW_LOG_ENABELD_VAR = "slow_query_log"

AUDIT_LABEL_KEY = "polardbx/enableAuditLog"


class AuditController:
    """ Audit Controller enables pxc operator to switch on/off dn audit log.
    It sets mysql's variables according to pod label 'AUDIT_LABEL_KEY'.
    It will rotate dn audit log and slow query log periodically if they are enabeld.
        
    A audit controller has two threads to do two things periodically.
    - sql_checker:
      - Reading mysql's audit log enabled variables and setting it to the same as pod's label 'AUDIT_LABEL_KEY'.
      - Reading mysql's slow log enabled variables.
    - log_flusher:
        - When audit log enabled, log_flusher executes mysql commands to rotate log file every LOG_CHECK_INTERVAL to prevent single file growing too large.
        - When slow log enabled, log_flusher rotates log file every LOG_CHECK_INTERVAL.
        - log_flusher deletes old files to keep file number <= MAX_XXXX_LOG_FILE_NUM
        
    Audit Controller has a mysql connection pool with size POOL_SIZE.
        
    """
    def __init__(self) -> None:
        self.mgr = Manager(Context())
        self._setup_logger()
        self.port = self.mgr.context().port_access()
        self.mysql_pool_config = {
            "pool_name": "mysql_pool",
            "pool_size": POOL_SIZE,
            "host": "localhost",
            "port": self.port,
            "user": "root",
        }
        self.pool: MySQLConnectionPool = None # type: ignore
        self.audit_enbaled = False
        self.slow_enabled = False

    def _setup_logger(self):
        self.logger = logging.getLogger(type(self).__name__)
        self.logger.propagate = False
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        
        # mysql connector logger
        logger = logging.getLogger("mysql.connector")
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s- %(message)s")
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    # a wrapper to handle exceptions
    def _sql_check_wrapper(self):
        self.logger.info("Starting a Label Check Thread to listen k8s' audit label every %d second."%LABEL_CHECK_INTERVAL)
        desp = "SQL Checker"
        while True:
            try:
                self._check_sql()
            except DatabaseError as e:
                self.logger.error(f"{desp} met database error: {e}")
            except Error as e:
                self.logger.error(f"{desp} met mysql/connector/pool error: {e}")
            except Exception as e:
                self.logger.error(f"{desp} met error: {e}")
            time.sleep(LABEL_CHECK_INTERVAL - int(time.time())%LABEL_CHECK_INTERVAL)


    # a wrapper to handle exceptions
    def _log_flush_wrapper(self):
        self.logger.info("Starting a Log Flush Thread to flush audit log every %d second."%LOG_CHECK_INTERVAL)
        desp = "Log Flusher"
        while True:
            try:
                self._flush_log()
            except DatabaseError as e:
                self.logger.error(f"{desp} met database error: {e}")
            except Error as e:
                self.logger.error(f"{desp} met mysql/connector/pool error: {e}")
            except Exception as e:
                self.logger.error(f"{desp} met error: {e}")
            time.sleep(LOG_CHECK_INTERVAL)
            
    def _check_sql(self):
        audit_label = PodInfo().label(AUDIT_LABEL_KEY)
        audit_label = False if (audit_label is None or audit_label.lower()=="false") else True

        audit_enabled_var = AUDIT_ENABLED_VAR_MYSQL80 if self.mgr.context().is_galaxy80() else AUDIT_ENABLED_VAR_MYSQL57
        slow_enabled_var = SLOW_LOG_ENABELD_VAR

        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            execute = lambda cmd : cursor.execute(SQL_INTERNAL_MARK + cmd)
            execute(f"set {SQL_BINLOG} = 0")
            
            execute(f"SELECT @@{audit_enabled_var}, @@{slow_enabled_var}")
            results = cursor.fetchone()
            audit_enabled = bool(results[0])
            slow_enabled = bool(results[1])

            # set audit enabled according to audit label
            if audit_enabled != audit_label:
                self.logger.info(f"Label {AUDIT_LABEL_KEY}({audit_label}) != SQL {audit_enabled_var}({audit_enabled}). Trying to change {audit_enabled_var} from {audit_enabled} to {audit_label}.")
                execute(f"SET GLOBAL {audit_enabled_var} = {audit_label}")
                audit_enabled = audit_label
            self.audit_enbaled = audit_label

            # set general,slow log output type to TABLE
            self.slow_enabled = slow_enabled

            conn.commit()

    # body of log_flusher thread
    def _flush_log(self):

        if self.audit_enbaled==False and self.slow_enabled==False:
            return
        
        audit_flush_cmd = None
        if self.audit_enbaled:
            if self.mgr.context().is_galaxy80():
                audit_flush_cmd = AUDIT_FLUSH_CMD_MYSQL80
            elif self.mgr.context().is_xcluster57():
                audit_flush_cmd = AUDIT_FLUSH_CMD_MYSQL57

        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            execute = lambda cmd : cursor.execute(SQL_INTERNAL_MARK + cmd)
            execute(f"set {SQL_BINLOG} = 0")
            
            if self.audit_enbaled:
                execute(audit_flush_cmd)
            if self.slow_enabled:
                execute(LOG_ROTATE_CMD)
                execute(SLOW_LOG_FLUSH_CMD)

            conn.commit()

        if self.audit_enbaled:
            # *.alog
            audit_log_files = [os.path.join(AUDIT_DIR, f) for f in os.listdir(AUDIT_DIR) if f.endswith('.alog')]
            self._maintain_file_num(audit_log_files, MAX_AUDIT_LOG_FILE_NUM)

        if self.slow_enabled:
            # slow_log_*.csv
            slow_log_files = [os.path.join(SLOW_LOG_DIR, f) for f in os.listdir(SLOW_LOG_DIR) if f.startswith('slow_log_')]
            self._maintain_file_num(slow_log_files, MAX_SLOW_LOG_FILE_NUM)
    
    def _maintain_file_num(self, files, max_num):
        file_nums = len(files)
        if file_nums > max_num:
            file_time_tuples = [(f, os.stat(f).st_mtime) for f in files]
            file_time_tuples = sorted(file_time_tuples, key=lambda x: x[1])
            old_num = file_nums - max_num
            for i in range(old_num):
                os.remove(file_time_tuples[i][0])

    def _create_connection_pool(self):
        self.logger.info("Creating mysql connection pool...")
        pool = None
        while True:
            try:
                pool = MySQLConnectionPool(**self.mysql_pool_config)
                break
            except Error as e:
                self.logger.error("Creating mysql connection pool failed: %s"%e)
                time.sleep(5)
        
        self.logger.info("Created a mysql connection pool with size = %d."%POOL_SIZE)
        return pool

    def start(self):
        """ Start a Audit Controller in current process.
        """
        self.pool = self._create_connection_pool()
        sql_checker = threading.Thread(target=self._sql_check_wrapper)
        log_flusher = threading.Thread(target=self._log_flush_wrapper)
        
        sql_checker.start()
        log_flusher.start()

    def start_process(self):
        """ Start a Audit Controller in a new process.
        
        AuditController will be run in a process. So we can manually start a new controller when it's down.
        """
        module_path = os.path.abspath(__file__)
        cmd = [PYTHON_PATH, module_path]
        self.logger.info("Start Audit Controller Process: %s"%cmd)
        subprocess.Popen(cmd)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - AuditController - %(levelname)s - %(message)s')
    audit_controller = AuditController()
    audit_controller.start()

