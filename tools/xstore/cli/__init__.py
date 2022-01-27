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

from . import account, consensus, process, variables, log
from .common import global_mgr
from .root import root_group
from .engine import engine_group

root_group.add_command(account.account_group)
root_group.add_command(consensus.consensus_group)
root_group.add_command(process.process_group)
root_group.add_command(variables.variables_group)
root_group.add_command(log.log_group)
root_group.add_command(engine_group)


def run():
    root_group()
