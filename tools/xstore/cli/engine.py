# Copyright 2021-2022 Alibaba Group Holding Limited.
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

import click

from .common import global_mgr


@click.group(name='engine')
def engine_group():
    pass


@click.command(name='version')
def version():
    with global_mgr.new_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('select version()')
            row = cur.fetchone()
            print(row[0])


engine_group.add_command(version)