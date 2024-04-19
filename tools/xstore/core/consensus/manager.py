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

import pymysql
from abc import abstractmethod
from contextlib import AbstractContextManager
from enum import Enum
from typing import NamedTuple, Union, Collection

CONSENSUS_ROLE_LEADER = 'leader'
CONSENSUS_ROLE_FOLLOWER = 'follower'
# special type of follower
CONSENSUS_ROLE_LOGGER = 'logger'
CONSENSUS_ROLE_LEARNER = 'learner'


class ConsensusRole(Enum):
    """
    Legal consensus roles.
    """
    CANDIDATE = 'candidate'
    LEADER = 'leader'
    FOLLOWER = 'follower'
    LEARNER = 'learner'
    PREPARED = 'prepared'


class ConsensusExtRole(Enum):
    """
    Legal consensus ext roles.
    """
    LOGGER = 'logger'
    UNKNOWN = 'unknown'


class ConsensusGlobal(NamedTuple):
    """
    Global consensus info.
    """
    # Match index.
    match_index: int = 0
    # Next index.
    next_index: int = 0
    # Applied index.
    applied_index: int = 0
    # Election weight.
    election_weight: int = 0
    # Force sync.
    force_sync: bool = False
    # Learner source.
    learner_source: int = 0


class ConsensusLocal(NamedTuple):
    """
    Local consensus info.
    """

    # Current term.
    current_term: int = 0

    # Current leader.
    current_leader: str = ''

    # Commit index.
    commit_index: int = 0

    # Last log term.
    last_log_term: int = 0

    # Last log index.
    last_log_index: int = 0

    # Last apply index.
    last_apply_index: int = 0

    # Ready for rw. (Leader)
    ready_for_rw: bool = False

    # Instance type.
    instance_type: str = ''


class ConsensusNode(NamedTuple):
    """
    Consensus node.
    """

    # Server id.
    server_id: int
    # Address, consensus ip:port.
    addr: str
    # Consensus role.
    role: ConsensusRole
    # Ext role, e.g. logger. If not recognized, keep None.
    ext_role: ConsensusExtRole

    # From global
    global_info: ConsensusGlobal

    # From local
    local_info: ConsensusLocal


class ConsensusLog(NamedTuple):
    """
    Consensus log info.
    """

    log_name: str
    file_size: int
    start_log_index: int
    # Only for redo logs.
    start_lsn: int or None


class RoleMismatchError(Exception):
    """
    Role mismatch when operating.
    """
    pass


    # last_errno=r['last_errno'],
    # last_error=r['last_error'],
    # last_io_errno=r['last_io_errno'],
    # last_io_error=r['last_io_error'],
    # last_sql_errno=r['last_sql_errno'],
    # last_sql_error=r['last_sql_error']

class SlaveStatus(NamedTuple):
    """
    the result of show slave status
    """
    relay_log_file: str
    relay_log_pos: int
    slave_io_running: str
    slave_sql_running: str
    slave_sql_running_state: str
    seconds_behind_master: float
    last_errno: str
    last_error: str
    last_io_errno: str
    last_io_error: str
    last_sql_errno: str
    last_sql_error: str

class AbstractConsensusManager(AbstractContextManager):
    def __init__(self, conn: pymysql.Connection, current_addr: str):
        if not conn or not isinstance(conn, pymysql.Connection):
            raise ValueError('connection not valid')

        self._conn = conn
        self._current_node = None
        self._current_addr = current_addr

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.__exit__(exc_type, exc_val, exc_tb)

    @staticmethod
    def _get_address(target: Union[str, ConsensusNode]) -> str:
        if isinstance(target, ConsensusNode):
            return target.addr
        return target

    def _get_server_id(self, target: Union[str, ConsensusNode]) -> int:
        if isinstance(target, ConsensusNode):
            return target.server_id

        node = self.get_consensus_node(target)  # ConsensusNode
        if not node:
            raise ValueError('node not found: ' + target)
        return node.server_id

    # get current node with cache.
    def _get_current_node(self):
        if not self._current_node:
            self._current_node = self.current_node()
        return self._current_node

    def check_current_role(self, role: ConsensusRole):
        current_node = self._get_current_node()
        if current_node.role != role:
            raise RoleMismatchError('role mismatch, expect %s, but is %s' % (role, current_node.role))

    @abstractmethod
    def current_leader_address(self) -> str:
        """
        Return current leader's consensus address.
        :return: current leader's consensus address.
        """

    @abstractmethod
    def list_consensus_nodes(self) -> Collection[ConsensusNode]:
        """
        List consensus nodes in current configuration.

        :return: a collection of ConsensusNode objects.
        """

    @abstractmethod
    def get_consensus_node(self, target_addr: str) -> ConsensusNode or None:
        """
        Get the ConsensusNode object for specified address.

        :param target_addr: address of target node.
        :return: a ConsensusNode object representing the current status the target node.
        """

    @abstractmethod
    def current_node(self) -> ConsensusNode:
        """
        Get the current consensus node info.

        :return: a ConsensusNode object.
        """

    # FIXME: ConsensusNode -> ip:port
    @abstractmethod
    def change_leader(self, target: Union[str, ConsensusNode]):
        """
        Change leadership to specified node

        :param target: address or ConsensusNode object of the target consensus
            node to be elected as leader.
        :return: None
        """

    # FIXME: ConsensusNode -> ip:port
    @abstractmethod
    def configure_follower(self, target: Union[str, ConsensusNode], *,
                           election_weight: int, force_sync: bool):
        """
        Configure the target follower's election weight and state of force sync.

        :param target: address or Consensus Node object of the target consensus
            follower node.
        :param election_weight: election weight, range from 0 to 9
        :param force_sync: the state of force sync.
        """

    @abstractmethod
    def modify_cluster_id(self, cluster_id: int):
        """
        Configure current cluster id. Commonly used to stop receiving new consensus logs.

        :param cluster_id: target cluster id.
        """

    @abstractmethod
    def force_single_mode(self):
        """
        Force current consensus node (must be leader) to remove all followers from
        configuration and change to single node mode.

        """

    # FIXME: ConsensusNode -> ip:port
    @abstractmethod
    def fix_match_index(self, target: Union[str, ConsensusNode], match_index: int):
        """
        Fix match index of target consensus node. It breaks the consensus protocol
        and must be used with extreme care.

        :param target: address or ConsensusNode object of target node.
        :param match_index: new match index.
        """

    @abstractmethod
    def add_learner(self, target_addr: str) -> ConsensusNode:
        """
        Add learner to configuration.

        :param target_addr: address of target node.
        :return: a ConsensusNode object of the new learner.
        """

    @abstractmethod
    def drop_learner(self, target_addr: str):
        """
        Drop learner from configuration.

        :param target_addr: address of target consensus node.
        """

    @abstractmethod
    def add_follower(self, target_addr: str) -> ConsensusNode:
        """
        Add follower to configuration.

        :param target_addr: address of target node.
        :return: a ConsensusNode object of the new follower.
        """

    # FIXME: ConsensusNode -> ip:port
    @abstractmethod
    def drop_follower(self, target: Union[str, ConsensusNode]):
        """
        Drop follower from configuration.

        :param target: address or Consensus Node of target consensus node.
        """

    # FIXME: ConsensusNode -> ip:port
    @abstractmethod
    def upgrade_learner_to_follower(self, addr) -> ConsensusNode:
        """
        Upgrade the learner to follower.

        :param target: address or Consensus Node of target consensus node.
        :return: a new ConsensusNode represents the target node.
        """

    # FIXME: ConsensusNode -> ip:port
    @abstractmethod
    def downgrade_follower_to_learner(self, addr) -> ConsensusNode:
        """
        Downgrade the follower to learner.

        :param target: address or Consensus Node of target consensus node.
        :return: a new ConsensusNode represents the target node.
        """

    # FIXME: ConsensusNode -> ip:port
    @abstractmethod
    def configure_learner_source(self, learner: Union[str, ConsensusNode], source: Union[str, ConsensusNode],
                                 *, applied_index: bool):
        """
        Configure the learner's source.

        :param learner: address or Consensus Node of learner consensus node.
        :param source: address or Consensus Node of source consensus node.
        :param applied_index: use applied index or not.
        """

    @abstractmethod
    def refresh_learner_meta(self):
        """
        Refresh the learner's metadata.

        """

    @abstractmethod
    def enable_follower_election(self):
        """
        Follower only. Enable current follower node to propose election of itself.
        """

    @abstractmethod
    def disable_follower_election(self):
        """
        Follower only. Disable current follower node to propose election of itself.
        """

    @abstractmethod
    def enable_weak_consensus_mode(self):
        """
        Leader only. Enable weak consensus mode.
        """

    @abstractmethod
    def disable_weak_consensus_mode(self):
        """
        Leader only. Disable weak consensus mode.
        """

    @abstractmethod
    def list_consensus_logs(self) -> Collection[ConsensusLog]:
        """
        Get the list of consensus logs.
        :return: a list of consensus logs objects.
        """

    @abstractmethod
    def purge_consensus_log_to(self, target_log_index: int, *, local: bool = False, force: bool = False):
        """
        Purge logs to target log index. This method supports any consensus log with the conception of
        log index, e.g. binlog or redo log. When not local purge, the method can only be executed on
        leader and will purge all logs on it and it's followers/learners.

        :param target_log_index: purge logs to target log index.
        :param local: purge only local logs.
        :param force: force purge. (may not work on some versions)
        :return:
        """

    @abstractmethod
    def show_slave_status(self) -> SlaveStatus:
        """
        return query result of "show slave status"
        """

    @abstractmethod
    def update_cluster_info(self, cluster_info):
        """
          update cluster info by SQL
        """

    @abstractmethod
    def set_readonly(self):
        """
         set readonly
        """

    @abstractmethod
    def start_xpaxos_replication(self):
        """
        Follower only. Start xpaxos replication.
        """
