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
import subprocess
import shlex
from typing import Sequence, AnyStr


def check_run_process(cmd: Sequence[AnyStr] or AnyStr, *, cwd=None, stdout=None, stderr=None, logger=None):
    prefix = []
    if cwd:
        prefix.append('cwd=%s' % cwd)

    prefix = '(' + ','.join(prefix) + ')'

    if logger:
        logger.info('%s execute command: %s' % (
            prefix, ' '.join([shlex.quote(s) for s in cmd]) if not isinstance(cmd, str) else cmd))
    return subprocess.check_call(cmd, shell=isinstance(cmd, str), cwd=cwd, stdout=stdout, stderr=stderr)
