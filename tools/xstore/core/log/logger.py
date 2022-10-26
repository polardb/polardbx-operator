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
import os
from logging import handlers

from core.context import Context
from core.convention import VOLUME_DATA


class LogFactory(object):

    level_relations = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'crit': logging.CRITICAL
    }

    @staticmethod
    def get_logger(filename, level='info', when='D', back_count=3,
                   fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
        context = Context()
        backup_log_dir = context.volume_path(VOLUME_DATA, 'backuplog')
        if not os.path.exists(backup_log_dir):
            os.mkdir(backup_log_dir)
        log_path = os.path.join(backup_log_dir, filename)

        logger = logging.getLogger()
        format_str = logging.Formatter(fmt)
        logger.setLevel(LogFactory.level_relations.get(level))
        sh = logging.StreamHandler()
        sh.setFormatter(format_str)
        th = handlers.TimedRotatingFileHandler(filename=log_path, when=when, backupCount=back_count, encoding='utf-8')
        th.setFormatter(format_str)
        logger.addHandler(sh)
        logger.addHandler(th)
        return logger
