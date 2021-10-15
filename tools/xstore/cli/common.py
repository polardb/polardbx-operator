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

from typing import List, Sequence

# Global context.
import click

from core import Context, Manager
from .root import root_group

global_mgr = Manager(Context())


def print_rows(rows: List[Sequence[str]], sep=' ', *, header: List[str] or None = None):
    if not rows or len(rows) == 0 or len(rows[0]) == 0:
        return

    # convert to string list
    rows = [[str(x) for x in r] for r in rows]

    # patch header if present
    if header and len(header) == len(rows[0]):
        rows = [header] + rows

    row_max_width = [0] * len(rows[0])
    for r in rows:
        for i, x in enumerate(r):
            row_max_width[i] = max(row_max_width[i], len(x))

    for r in rows:
        print(sep.join([x.ljust(row_max_width[i]) for i, x in enumerate(r)]))


@click.command(name='ping')
def ping():
    with global_mgr.new_connection() as conn:
        conn.ping()


root_group.add_command(ping)
