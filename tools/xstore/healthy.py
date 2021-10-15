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

import sys
import traceback

import click

from core import Context, Manager


# noinspection PyBroadException
@click.command(name='health', help='Report health status.')
@click.option('--details', is_flag=True)
@click.option('--startup', is_flag=True)
@click.option('--check-leader', is_flag=True)
def _check_health(details, startup, check_leader):
    mgr = Manager(Context())

    if not startup and 'debug' == mgr.context().run_mode():
        print('runmode debug, skip')
        return

    if mgr.context().is_bootstrap_blocked():
        print('blocked')
        sys.exit(1)

    engine = mgr.engine()
    if not engine.is_initialized():
        print('uninitialized')
        sys.exit(2)

    try:
        if engine.check_health(check_leader):
            print('success')
        else:
            print('not ready')
    except Exception:
        if details:
            traceback.print_exc(file=sys.stderr)
        print('fail')
        sys.exit(10)


if __name__ == '__main__':
    _check_health()
