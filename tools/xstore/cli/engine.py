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


@click.command(name='parameter')
@click.option('-k', '--key', required=True, multiple=True)
@click.option('-v', '--value', required=True, multiple=True)
def set_global(key, value):
    with global_mgr.new_connection() as conn:
        with conn.cursor() as cur:
            cmd = "SET GLOBAL "
            n = 0
            for k in key:
                cmd += k + " = " + value[n] + ", "
                n += 1
            cmd = cmd[0:-2]
            cur.execute(cmd)
        conn.commit()


engine_group.add_command(set_global)


@click.command(name='set_engine_enable')
@click.option('--enable', is_flag=True)
@click.option('--disable', is_flag=True)
def set_engine_enable(enable, disable):
    if enable:
        global_mgr.engine().set_enable_engine(True)
    if disable:
        global_mgr.engine().set_enable_engine(False)


engine_group.add_command(set_engine_enable)


@click.command(name='reset_meta')
@click.option('--learner', is_flag=True)
@click.option('--recover-index-filepath', type=str)
def reset_meta(learner, recover_index_filepath):
    if recover_index_filepath is not None:
        global_mgr.engine().set_recover_index_filepath(recover_index_filepath)
    if learner:
        global_mgr.engine().handle_indicate('reset-cluster-info-to-learner')
    else:
        global_mgr.engine().handle_indicate('reset-cluster-info')


engine_group.add_command(reset_meta)


@click.command(name='shutdown')
def shutdown():
    global_mgr.engine().shutdown()
    pass


engine_group.add_command(shutdown)


@click.command(name="xtrabackup")
def xtrabackup_home():
    xtrabackup = global_mgr.engine().context.xtrabackup
    print(xtrabackup)
    return


engine_group.add_command(xtrabackup_home)
