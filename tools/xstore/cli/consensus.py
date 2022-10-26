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
import re
import sys

import click
import pymysql

from core import channel, convention
from core.consensus import ConsensusRole, ConsensusExtRole, ConsensusNode, SlaveStatus
from .common import global_mgr, print_rows


@click.group(name='consensus')
def consensus_group():
    pass


@click.command(name='role')
@click.option('--report-leader', is_flag=True)
def report_role(report_leader):
    with global_mgr.consensus_manager() as mgr:
        current_node = mgr.current_node()
        if current_node.role == ConsensusRole.FOLLOWER:
            if current_node.ext_role == ConsensusExtRole.LOGGER:
                print('logger')
            else:
                print('follower')
        else:
            print(current_node.role.value)

        if report_leader:
            if current_node.role == ConsensusRole.LEADER:
                print(global_mgr.context().pod_info().name())
            else:
                leader_addr = current_node.local_info.current_leader
                if leader_addr != '':
                    channel = global_mgr.shared_channel()
                    print(channel.get_node_by_addr(leader_addr).pod)
                else:
                    print('')


consensus_group.add_command(report_role)


@click.command(name='this')
@click.option('--full', '-f', is_flag=True)
def print_current_info(full):
    chan = global_mgr.shared_channel()

    def get_role(consensus_node: ConsensusNode, node_info: channel.Node):
        if consensus_node.role == ConsensusRole.FOLLOWER and \
                node_info.role == convention.NODE_ROLE_VOTER:
            return 'logger'
        return consensus_node.role.value

    with global_mgr.consensus_manager() as mgr:
        current_node = mgr.current_node()
        current_node_info = chan.get_node_by_pod_name(global_mgr.context().pod_info().name())
        leader_addr = current_node.local_info.current_leader
        leader_node_info = chan.get_node_by_addr(leader_addr) if leader_addr != '' else None

        if full:
            print_rows(sep=' | ', header=(
                'pod',
                'addr',
                'server_id',
                'role',
                'leader_pod',
                'leader_addr',
                'current_term',
                'last_log_index',
                'applied_index',
                'commit_index',
            ), rows=[(
                current_node_info.pod,
                current_node.addr,
                current_node.server_id,
                get_role(current_node, current_node_info),
                leader_node_info.pod if leader_node_info else '',
                current_node.local_info.current_leader,
                current_node.local_info.current_term,
                current_node.local_info.last_log_index,
                current_node.local_info.last_apply_index,
                current_node.local_info.commit_index,
            )])
        else:
            print_rows(sep=' | ', header=(
                'pod',
                'addr',
                'role',
                'leader_pod',
                'leader_addr',
            ), rows=[(
                current_node_info.pod,
                current_node.addr,
                get_role(current_node, current_node_info),
                leader_node_info.pod if leader_node_info else '',
                current_node.local_info.current_leader,
            )])


consensus_group.add_command(print_current_info)


@click.command(name='list')
@click.option('--full', '-f', is_flag=True)
def list_nodes(full):
    chan = global_mgr.shared_channel()

    def get_role(consensus_node: ConsensusNode, node_info: channel.Node):
        if consensus_node.role == ConsensusRole.FOLLOWER and \
                node_info.role == convention.NODE_ROLE_VOTER:
            return 'logger'
        return consensus_node.role.value

    with global_mgr.consensus_manager() as mgr:
        current_node = mgr.current_node()

        if current_node.role != ConsensusRole.LEADER:
            print('not allowed outside leader')
            sys.exit(1)

        all_nodes = mgr.list_consensus_nodes()

        def to_print_tuple(consensus_node: ConsensusNode):
            node_info = chan.get_node_by_addr(consensus_node.addr)
            if full:
                return node_info.pod, consensus_node.server_id, consensus_node.addr, \
                       get_role(consensus_node, node_info), consensus_node.global_info.match_index, \
                       consensus_node.global_info.applied_index, consensus_node.global_info.election_weight, \
                       consensus_node.global_info.force_sync, consensus_node.global_info.learner_source
            else:
                return node_info.pod, consensus_node.addr, get_role(consensus_node, node_info)

        if full:
            print_rows(sep=' | ', header=(
                'pod',
                'server_id',
                'addr',
                'role',
                'match_index',
                'applied_index',
                'election_weight',
                'force_sync',
                'learner_source'
            ), rows=[to_print_tuple(n) for n in all_nodes])
        else:
            print_rows(sep=' | ', header=(
                'pod', 'addr', 'role'
            ), rows=[to_print_tuple(n) for n in all_nodes])


consensus_group.add_command(list_nodes)


def _get_addr_from_argument(arg: str, shared_channel: channel.SharedFileChannel) -> str:
    # If in the format of "IP:port", return arg.
    if re.match('\\d+\\.\\d+\\.\\d+\\.\\d+:\\d+', arg):
        return arg

    # Otherwise query the shared channel.
    node = shared_channel.get_node_by_pod_name(arg)
    if not node:
        raise ValueError('node not found: ' + arg)
    return node.addr()


@click.command(name='configure')
@click.option('--weight', required=True, type=int)
@click.argument('nodes', required=True, nargs=-1)
def configure_election_weight(weight, nodes):
    if not nodes or len(nodes) == 0:
        return

    shared_channel = global_mgr.shared_channel()
    weights = []
    with global_mgr.consensus_manager() as mgr:
        consensus_nodes = dict([(n.addr, n) for n in mgr.list_consensus_nodes()])

        for node in nodes:
            addr = _get_addr_from_argument(node, shared_channel)
            consensus_node = consensus_nodes[addr]
            mgr.configure_follower(
                target=consensus_node,
                election_weight=weight,
                force_sync=consensus_node.global_info.force_sync
            )
            weights.append(str(consensus_node.global_info.election_weight))
    print(','.join(weights))


consensus_group.add_command(configure_election_weight)


@click.command(name='change-leader')
@click.argument('node')
def change_leader_to(node):
    shared_channel = global_mgr.shared_channel()
    addr = _get_addr_from_argument(node, shared_channel)

    with global_mgr.consensus_manager() as mgr:
        mgr.change_leader(addr)


consensus_group.add_command(change_leader_to)


@click.command(name='add-learner')
@click.argument('node')
def add_learner(node):
    shared_channel = global_mgr.shared_channel()
    addr = _get_addr_from_argument(node, shared_channel)

    with global_mgr.consensus_manager() as mgr:
        consensus_nodes = mgr.list_consensus_nodes()
        # check if the leaner node already existed
        for consensus_node in consensus_nodes:
            if consensus_node.role == ConsensusRole.LEARNER and consensus_node.addr == addr:
                return
        mgr.add_learner(addr)


consensus_group.add_command(add_learner)


@click.command(name='drop-learner')
@click.argument('node')
def drop_learner(node):
    shared_channel = global_mgr.shared_channel()
    addr = _get_addr_from_argument(node, shared_channel)

    with global_mgr.consensus_manager() as mgr:
        consensus_nodes = mgr.list_consensus_nodes()
        # check if the leaner node exists
        for consensus_node in consensus_nodes:
            if consensus_node.role == ConsensusRole.LEARNER and consensus_node.addr == addr:
                mgr.drop_learner(addr)
                return


consensus_group.add_command(drop_learner)


@click.command(name='slave-status')
def show_status():
    with global_mgr.consensus_manager() as mgr:
        slave_status = mgr.show_slave_status()
        print_rows(sep=' | ', header=(
            'relay_log_file',
            'relay_log_pos',
            'slave_io_running',
            'slave_sql_running',
            'slave_sql_running_state',
            'seconds_behind_master',
        ), rows=[(slave_status.relay_log_file, slave_status.relay_log_pos, slave_status.slave_io_running,
                  slave_status.slave_sql_running, slave_status.slave_sql_running_state,
                  slave_status.seconds_behind_master)])


consensus_group.add_command(show_status)


@click.group(name='log')
def consensus_log_group():
    pass


consensus_group.add_command(consensus_log_group)


@click.command(name='purge')
@click.option('--local', is_flag=True)
@click.option('--force', is_flag=True)
@click.option('--left', default=5, type=int, show_default=True)
def purge_consensus_log(local, force, left):
    shared_channel = global_mgr.shared_channel()

    before_index = shared_channel.last_backup_log_index

    with global_mgr.consensus_manager() as mgr:
        # at least left 5 files if not specified
        if not before_index or before_index < 0:
            logs = mgr.list_consensus_logs()
            if len(logs) <= left:
                print('not enough to purge')
                return
            before_index = list(logs)[-left].start_log_index

        mgr.purge_consensus_log_to(before_index, local=local, force=force)


consensus_log_group.add_command(purge_consensus_log)
