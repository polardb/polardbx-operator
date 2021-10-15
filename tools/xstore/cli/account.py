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

import click

from .common import print_rows, global_mgr


@click.group(name='account')
def account_group():
    pass


@click.command(name='reset')
@click.option('-u', '--user', required=True, type=str)
@click.option('-p', '--passwd', required=True, type=str)
@click.option('-h', '--host', default='%', required=False, type=str, show_default=True)
def reset_account_passwd(user, passwd, host):
    with global_mgr.new_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("ALTER USER %s@%s IDENTIFIED BY %s", args=(user, host, passwd))
            cur.execute("FLUSH PRIVILEGES")
        conn.commit()


account_group.add_command(reset_account_passwd)


@click.command(name='create')
@click.option('-u', '--user', required=True, type=str)
@click.option('-p', '--passwd', required=True, type=str)
@click.option('-h', '--host', default='%', required=False, type=str, show_default=True)
def create_account(user, passwd, host):
    with global_mgr.new_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE USER IF NOT EXISTS %s@%s IDENTIFIED BY %s", args=(user, host, passwd))
            cur.execute("GRANT ALL PRIVILEGES ON *.* TO %s@%s", args=(user, host))
        conn.commit()


account_group.add_command(create_account)


@click.command(name='drop')
@click.option('-u', '--user', required=True, type=str)
@click.option('-h', '--host', default='%', required=False, type=str, show_default=True)
def delete_account(user, host):
    with global_mgr.new_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP USER IF EXISTS %s@%s", args=(user, host))
        conn.commit()


account_group.add_command(delete_account)


# noinspection SqlResolve, SqlNoDataSourceInspection,SqlDialectInspection
@click.command(name='list')
@click.option('-h', '--host', required=False, type=str)
def list_accounts(host):
    with global_mgr.new_connection() as conn:
        with conn.cursor() as cur:
            if host:
                cur.execute('select user, host from mysql.user where host = "%s"' % host)
            else:
                cur.execute('select user, host from mysql.user')
            rows = cur.fetchall()

            print_rows(rows, sep=' | ')


account_group.add_command(list_accounts)
