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

import time

from .manager import *


def fetchall_with_lowercase_fieldnames(cursor: pymysql.cursors.Cursor):
    """
    Fetch all rows with lowercase field names.

    :param cursor: cursor.
    :return: a list of dict of column names and values.
    """
    # convert to lowercase fields
    field_name = [field[0].lower() for field in cursor.description]
    return [dict(zip(field_name, r)) for r in cursor.fetchall()]


def fetchone_with_lowercase_fieldnames(cursor: pymysql.cursors.Cursor):
    """
    Fetch one row with lowercase field names.

    :param cursor: cursor.
    :return: a dict of column names and values.
    """
    # convert to lowercase fields
    field_name = [field[0].lower() for field in cursor.description]
    return dict(zip(field_name, cursor.fetchone()))


# noinspection SqlResolve,SqlResolve,SqlNoDataSourceInspection,SqlDialectInspection
def _slave_status_from_row(r):
    return SlaveStatus(
        relay_log_file=r['relay_log_file'],
        relay_log_pos=int(r['relay_log_pos']),
        slave_io_running=r['slave_io_running'],
        slave_sql_running=r['slave_sql_running'],
        slave_sql_running_state=r['slave_sql_running_state'],
        seconds_behind_master=r['seconds_behind_master'],
        last_errno=r['last_errno'],
        last_error=r['last_error'],
        last_io_errno=r['last_io_errno'],
        last_io_error=r['last_io_error'],
        last_sql_errno=r['last_sql_errno'],
        last_sql_error=r['last_sql_error']
    )


class LegacyConsensusManager(AbstractConsensusManager):
    """
    Consensus manager for legacy implementation. Interfaces are customized
    SQLs, which is unstable and problematic.
    """

    def __init__(self, conn: pymysql.Connection, current_addr: str):
        super(LegacyConsensusManager, self).__init__(conn, current_addr)

    @staticmethod
    def _consensus_global_from(r) -> ConsensusGlobal:
        return ConsensusGlobal(
            match_index=int(r['match_index']),
            next_index=int(r['next_index']),
            force_sync=r['force_sync'].lower() == 'yes',
            election_weight=int(r['election_weight']),
            learner_source=int(r['learner_source']),
            applied_index=int(r['applied_index']),
        )

    @staticmethod
    def _consensus_local_from(r) -> ConsensusLocal:
        return ConsensusLocal(
            current_term=int(r['current_term']),
            current_leader=r['current_leader'],
            commit_index=int(r['commit_index']),
            last_log_term=int(r['last_log_term']),
            last_log_index=int(r['last_log_index']),
            last_apply_index=int(r['last_apply_index']),
            ready_for_rw=r['server_ready_for_rw'].lower() == 'yes',
            instance_type=r['instance_type']
        )

    @staticmethod
    def _consensus_node_from_global_row(r) -> ConsensusNode:
        return ConsensusNode(
            server_id=int(r['server_id']),
            addr=r['ip_port'],
            role=ConsensusRole(r['role'].lower()),
            ext_role=ConsensusExtRole.UNKNOWN,
            global_info=ConsensusGlobal(
                match_index=int(r['match_index']),
                next_index=int(r['next_index']),
                force_sync=r['force_sync'].lower() == 'yes',
                election_weight=int(r['election_weight']),
                learner_source=int(r['learner_source']),
                applied_index=int(r['applied_index']),
            ),
            local_info=ConsensusLocal(),
        )

    def _consensus_node_from_local_row(self, r) -> ConsensusNode:
        local_info = LegacyConsensusManager._consensus_local_from(r)

        return ConsensusNode(
            server_id=int(r['server_id']),
            addr=self._current_addr,
            role=ConsensusRole(r['role'].lower()),
            ext_role=ConsensusExtRole.LOGGER if local_info.instance_type == 'Log' else ConsensusExtRole.UNKNOWN,
            global_info=ConsensusGlobal(),
            local_info=local_info,
        )

    def __enter__(self):
        return self

    def current_leader_address(self) -> str:
        node = self._get_current_node()
        return node.local_info.current_leader

    def list_consensus_nodes(self) -> Collection[ConsensusNode]:
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute('select * from information_schema.alisql_cluster_global')
            rows = fetchall_with_lowercase_fieldnames(cur)
            return [self._consensus_node_from_global_row(r) for r in rows]

    def get_consensus_node(self, target_addr: str) -> ConsensusNode or None:
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute('select * from information_schema.alisql_cluster_global where ip_port = "%s"' % target_addr)

            if cur.rowcount == 0:
                return None

            r = fetchone_with_lowercase_fieldnames(cur)
            return self._consensus_node_from_global_row(r)

    def current_node(self) -> ConsensusNode:
        with self._conn.cursor() as cur:
            cur.execute('select * from information_schema.alisql_cluster_local')
            r = fetchone_with_lowercase_fieldnames(cur)
            return self._consensus_node_from_local_row(r)

    def change_leader(self, target: Union[str, ConsensusNode]):
        target_server_id = self._get_server_id(target)
        with self._conn.cursor() as cur:
            cur.execute('change consensus_leader to %d' % target_server_id)

    def configure_follower(self, target: Union[str, ConsensusNode], election_weight: int, force_sync: bool):
        target_server_id = self._get_server_id(target)
        with self._conn.cursor() as cur:
            cur.execute('change consensus_node %d consensus_force_sync %s consensus_election_weight %d' % (
                target_server_id, "true" if force_sync else "false", election_weight
            ))

    def modify_cluster_id(self, cluster_id: int):
        self.check_current_role(ConsensusRole.FOLLOWER)

        with self._conn.cursor() as cur:
            cur.execute('change consensus_cluster_id to %d' % cluster_id)

    def force_single_mode(self):
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute('change consensus_node to consensus_force_single_mode')

    def fix_match_index(self, target: Union[str, ConsensusNode], match_index: int):
        self.check_current_role(ConsensusRole.LEADER)

        target_server_id = self._get_server_id(target)
        with self._conn.cursor() as cur:
            cur.execute('change consensus_node %d consensus_matchindex %d' % (
                target_server_id, match_index
            ))

    def add_learner(self, target_addr: str) -> ConsensusNode:
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute('add consensus_learner "%s"' % target_addr)

        return self.get_consensus_node(target_addr)

    def drop_learner(self, target_addr: str):
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            if target_addr.isdigit():
                cur.execute('drop consensus_learner %s' % target_addr)
            else:
                cur.execute('drop consensus_learner "%s"' % target_addr)

    def add_follower(self, target_addr: str) -> ConsensusNode:
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute('add consensus_follower "%s"' % target_addr)

        return self.get_consensus_node(target_addr)

    def drop_follower(self, target: Union[str, ConsensusNode]):
        self.check_current_role(ConsensusRole.LEADER)

        target_server_id = self._get_server_id(target)
        with self._conn.cursor() as cur:
            cur.execute('drop consensus_follower %d' % target_server_id)

    def upgrade_learner_to_follower(self, addr) -> ConsensusNode:
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute("change consensus_learner '%s' to consensus_follower" % addr)

        return self.get_consensus_node(addr)

    def downgrade_follower_to_learner(self, addr) -> ConsensusNode:
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            if addr.isdigit():
                cur.execute('change consensus_follower %s to consensus_learner' % addr)
            else:
                cur.execute('change consensus_follower "%s" to consensus_learner' % addr)

        return self.get_consensus_node(addr)

    def configure_learner_source(self, learner: Union[str, ConsensusNode], source: Union[str, ConsensusNode], *,
                                 applied_index: bool):
        self.check_current_role(ConsensusRole.LEADER)

        # change consensus_node "127.0.0.1:15700" consensus_learner_source "127.0.0.1:15701"
        # [consensus_use_applyindex true/false]
        learner_server_id = self._get_server_id(learner)
        source_server_id = self._get_server_id(source)
        with self._conn.cursor() as cur:
            if applied_index is not None:
                cur.execute('change consensus_node %d consensus_learner_source %d consensus_use_applyindex %s' % (
                    learner_server_id, source_server_id, 'true' if applied_index else 'false'
                ))
            else:
                cur.execute('change consensus_node %d consensus_learner_source %d' % (
                    learner_server_id, source_server_id
                ))

    def refresh_learner_meta(self):
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute('change consensus_learner for consensus_meta')

    def enable_follower_election(self):
        current_node = self._get_current_node()
        if current_node.role != ConsensusRole.FOLLOWER:
            return

        with self._conn.cursor() as cur:
            cur.execute("set force_revise=ON")
            cur.execute("set sql_log_bin=OFF")
            cur.execute("set global consensus_disable_election = OFF")

    def disable_follower_election(self):
        current_node = self._get_current_node()
        if current_node.role != ConsensusRole.FOLLOWER:
            return

        with self._conn.cursor() as cur:
            cur.execute("set force_revise=ON")
            cur.execute("set sql_log_bin=OFF")
            cur.execute("set global consensus_disable_election = ON")

    def enable_weak_consensus_mode(self):
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute('set global weak_consensus_mode = ON')

    def disable_weak_consensus_mode(self):
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute('set global weak_consensus_mode = OFF')

    @staticmethod
    def _consensus_log_from_row(r):
        return ConsensusLog(
            log_name=r['log_name'],
            file_size=int(r['file_size']),
            start_log_index=int(r['start_log_index']),
            start_lsn=None,
        )

    def list_consensus_logs(self) -> Collection[ConsensusLog]:
        with self._conn.cursor() as cur:
            cur.execute('show consensus logs')
            rows = fetchall_with_lowercase_fieldnames(cur)
            return [self._consensus_log_from_row(r) for r in rows]

    def purge_consensus_log_to(self, target_log_index: int = None, *, local: bool = False,
                               force: bool = False):
        if not target_log_index:
            target_log_index = 9999999999999999999999999999999

        if force:
            stmt = 'purge force consensus_log before %d' % target_log_index
        elif local:
            stmt = 'purge local consensus_log before %d' % target_log_index
        else:
            stmt = 'purge consensus_log before %d' % target_log_index

        with self._conn.cursor() as cur:
            cur.execute(stmt)

    def show_slave_status(self):
        with self._conn.cursor() as cur:
            cur.execute('show slave status')
            row = fetchone_with_lowercase_fieldnames(cur)
            return _slave_status_from_row(row)

    def update_cluster_info(self, cluster_info):
        with self._conn.cursor() as cur:
            cur.execute("set force_revise=ON")
            cur.execute("set sql_log_bin=OFF")
            cur.execute('update mysql.consensus_info set cluster_info="%s"' % cluster_info)

    def set_readonly(self):
        with self._conn.cursor() as cur:
            cur.execute("FLUSH TABLES WITH READ LOCK")
            cur.execute("SET GLOBAL read_only = 1")


class ConsensusManager(AbstractConsensusManager):
    """
    Consensus manager implementation that leverages the stored procedure.
    """

    def __init__(self, conn: pymysql.Connection, current_addr: str):
        super(ConsensusManager, self).__init__(conn, current_addr)

    def __enter__(self):
        return self

    @staticmethod
    def _consensus_global_from(r) -> ConsensusGlobal:
        return ConsensusGlobal(
            match_index=int(r['match_index']),
            next_index=int(r['next_index']),
            force_sync=r['force_sync'].lower() == 'yes',
            election_weight=int(r['election_weight']),
            learner_source=int(r['learner_source']),
            applied_index=int(r['applied_index']),
        )

    @staticmethod
    def _consensus_local_from(r) -> ConsensusLocal:
        return ConsensusLocal(
            current_term=int(r['current_term']),
            current_leader=r['current_leader'],
            commit_index=int(r['commit_index']),
            last_log_term=int(r['last_log_term']),
            last_log_index=int(r['last_log_index']),
            last_apply_index=int(r['applied_index']),
            ready_for_rw=True,  # currently polardb 8.0 will not output any value.
            instance_type=r['instance_type']
        )

    @staticmethod
    def _consensus_node_from_global_row(r) -> ConsensusNode:
        return ConsensusNode(
            server_id=int(r['id']),
            addr=r['ip_port'],
            role=ConsensusRole(r['role'].lower()),
            ext_role=ConsensusExtRole.UNKNOWN,
            global_info=ConsensusGlobal(
                match_index=int(r['match_index']),
                next_index=int(r['next_index']),
                force_sync=r['force_sync'].lower() == 'yes',
                election_weight=int(r['election_weight']),
                learner_source=int(r['learner_source']),
                applied_index=int(r['applied_index']),
            ),
            local_info=ConsensusLocal()
        )

    def _consensus_node_from_local_row(self, r) -> ConsensusNode:
        local_info = ConsensusManager._consensus_local_from(r)

        return ConsensusNode(
            server_id=int(r['id']),
            addr=self._current_addr,
            role=ConsensusRole(r['role'].lower()),
            ext_role=ConsensusExtRole.LOGGER if local_info.instance_type == 'Log' else ConsensusExtRole.UNKNOWN,
            global_info=ConsensusGlobal(),
            local_info=local_info,
        )

    def current_leader_address(self) -> str:
        node = self._get_current_node()
        return node.local_info.current_leader

    def list_consensus_nodes(self) -> Collection[ConsensusNode]:
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute('call dbms_consensus.show_cluster_global()')
            rows = fetchall_with_lowercase_fieldnames(cur)
            return [self._consensus_node_from_global_row(r) for r in rows]

    def get_consensus_node(self, target_addr: str) -> ConsensusNode or None:
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute('call dbms_consensus.show_cluster_global()')
            rows = fetchall_with_lowercase_fieldnames(cur)
            nodes = [self._consensus_node_from_global_row(r) for r in rows]

            for n in nodes:
                if n.addr == target_addr:
                    return n
            return None

    def current_node(self) -> ConsensusNode:
        with self._conn.cursor() as cur:
            cur.execute('call dbms_consensus.show_cluster_local()')
            r = fetchone_with_lowercase_fieldnames(cur)
            return self._consensus_node_from_local_row(r)

    def change_leader(self, target: Union[str, ConsensusNode]):
        with self._conn.cursor() as cur:
            cur.execute('call dbms_consensus.change_leader("%s")' % target)

    def configure_follower(self, target: Union[str, ConsensusNode], election_weight: int, force_sync: bool):
        addr = self._get_address(target)
        with self._conn.cursor() as cur:
            cur.execute('call dbms_consensus.configure_follower("%s", %d, %d)' % (
                addr, election_weight, 1 if force_sync else 0
            ))

    def modify_cluster_id(self, cluster_id: int):
        self.check_current_role(ConsensusRole.FOLLOWER)

        with self._conn.cursor() as cur:
            cur.execute('call dbms_consensus.fix_cluster_id(%d)' % cluster_id)

    def force_single_mode(self):
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute('call dbms_consensus.force_single_mode()')

    def fix_match_index(self, target: Union[str, ConsensusNode], match_index: int):
        self.check_current_role(ConsensusRole.LEADER)

        target_server_id = self._get_server_id(target)
        with self._conn.cursor() as cur:
            cur.execute('call dbms_consensus.fix_matchindex(%d, %d)' % (
                target_server_id, match_index
            ))

    def add_learner(self, target_addr: str) -> ConsensusNode:
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute('call dbms_consensus.add_learner("%s")' % target_addr)

        return self.get_consensus_node(target_addr)

    def drop_learner(self, target_addr: str):
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            if target_addr.isdigit():
                cur.execute('call dbms_consensus.drop_learner(%s)' % target_addr)
            else:
                cur.execute('call dbms_consensus.drop_learner("%s")' % target_addr)

    def add_follower(self, target_addr: str) -> ConsensusNode:
        node = self.add_learner(target_addr)

        timeout = 10
        while (node.global_info.match_index - node.global_info.applied_index) > 100 and timeout > 0:
            time.sleep(1)
            timeout -= 1
            node = self.get_consensus_node(target_addr)

        # if timeout, just drop the learner
        if timeout <= 0:
            self.drop_learner(target_addr)

        return self.upgrade_learner_to_follower(node)

    def drop_follower(self, target: Union[str, ConsensusNode]):
        node = self.downgrade_follower_to_learner(target)
        return self.drop_learner(node)

    def upgrade_learner_to_follower(self, addr) -> ConsensusNode:
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute("call dbms_consensus.upgrade_learner('%s')" % addr)

        return self.get_consensus_node(addr)

    def downgrade_follower_to_learner(self, addr) -> ConsensusNode:
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            if addr.isdigit():
                cur.execute('call dbms_consensus.downgrade_follower(%s)' % addr)
            else:
                cur.execute('call dbms_consensus.downgrade_follower("%s")' % addr)

        return self.get_consensus_node(addr)

    def configure_learner_source(self, learner: Union[str, ConsensusNode], source: Union[str, ConsensusNode], *,
                                 applied_index: bool):
        self.check_current_role(ConsensusRole.LEADER)

        learner_server_id = self._get_server_id(learner)
        source_server_id = self._get_server_id(source)
        with self._conn.cursor() as cur:
            if applied_index is not None:
                cur.execute('call dbms_consensus.configure_learner(%d, %d, %d)' % (
                    learner_server_id, source_server_id, 1 if applied_index else 0
                ))
            else:
                cur.execute('call dbms_consensus.configure_learner(%d, %d)' % (
                    learner_server_id, source_server_id
                ))

    def refresh_learner_meta(self):
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute('call dbms_consensus.refresh_learner_meta()')

    def enable_follower_election(self):
        current_node = self._get_current_node()
        if current_node.role != ConsensusRole.FOLLOWER:
            return

        with self._conn.cursor() as cur:
            cur.execute("set force_revise=ON")
            cur.execute("set sql_log_bin=OFF")
            cur.execute("set global consensus_disable_election = OFF")

    def disable_follower_election(self):
        current_node = self._get_current_node()
        if current_node.role != ConsensusRole.FOLLOWER:
            return

        with self._conn.cursor() as cur:
            cur.execute("set force_revise=ON")
            cur.execute("set sql_log_bin=OFF")
            cur.execute("set global consensus_disable_election = ON")

    def enable_weak_consensus_mode(self):
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute('set global weak_consensus_mode = ON')

    def disable_weak_consensus_mode(self):
        self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute('set global weak_consensus_mode = OFF')

    @staticmethod
    def _consensus_log_from_row(r):
        return ConsensusLog(
            log_name=r['log_name'],
            file_size=int(r['file_size']),
            start_log_index=int(r.get('start_log_index', 0)),
            start_lsn=int(r.get('start_lsn', 0))
        )

    def list_consensus_logs(self) -> Collection[ConsensusLog]:
        with self._conn.cursor() as cur:
            cur.execute('call dbms_consensus.show_logs()')
            rows = fetchall_with_lowercase_fieldnames(cur)
            return [self._consensus_log_from_row(r) for r in rows]

    def purge_consensus_log_to(self, target_log_index: int, *, local: bool = False, force: bool = False):
        if not local:
            self.check_current_role(ConsensusRole.LEADER)

        with self._conn.cursor() as cur:
            cur.execute('call dbms_consensus.show_logs()')
            log_rows = fetchall_with_lowercase_fieldnames(cur)
            logs = [self._consensus_log_from_row(r) for r in log_rows]

            # find the logs that has start log index < target_log_index
            to_purge_logs = [l for l in logs if l.start_log_index < target_log_index]

            if len(to_purge_logs) == 0:
                return

            if not local:
                cur.execute('call dbms_consensus.purge_log(%d)' % to_purge_logs[-1].start_log_index)
            else:
                cur.execute('call dbms_consensus.local_purge_log(%d)' % to_purge_logs[-1].start_log_index)

    def show_slave_status(self) -> SlaveStatus:
        with self._conn.cursor() as cur:
            cur.execute('show slave status')
            row = fetchone_with_lowercase_fieldnames(cur)
            return _slave_status_from_row(row)

    def update_cluster_info(self, cluster_info):
        with self._conn.cursor() as cur:
            cur.execute("set force_revise=ON")
            cur.execute("set sql_log_bin=OFF")
            cur.execute('update mysql.consensus_info set cluster_info="%s"' % cluster_info)

    def set_readonly(self):
        with self._conn.cursor() as cur:
            cur.execute("FLUSH TABLES WITH READ LOCK")
            cur.execute("SET GLOBAL read_only = 1")
