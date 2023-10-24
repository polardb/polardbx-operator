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

mem_1G = 1 << 30
mem_10G = 10 * mem_1G
mem_32G = 32 * mem_1G
mem_128G = 128 * mem_1G


def get_dynamic_mysql_cnf_by_spec(cpu, mem):
    result = {}
    buffer_pool_size = 0.7 * mem
    if mem <= mem_10G:
        buffer_pool_size = 0.3 * mem
    elif mem <= mem_32G:
        buffer_pool_size = 0.5 * mem
    elif mem <= mem_128G:
        buffer_pool_size = 0.625 * mem
    buffer_pool_size = int(buffer_pool_size)
    result["innodb_buffer_pool_size"] = str(buffer_pool_size)
    result["loose_rds_audit_log_buffer_size"] = str(16777216)
    loose_rds_kill_connections = 20
    result["loose_rds_kill_connections"] = str(loose_rds_kill_connections)
    maintain_max_connections = 512
    result["loose_rds_reserved_connections"] = str(maintain_max_connections)
    result["loose_maintain_max_connections"] = str(maintain_max_connections)
    max_user_connections = int(cpu)
    if max_user_connections < 8000:
        max_user_connections = 8000
    result["max_user_connections"] = str(max_user_connections)
    max_connections = loose_rds_kill_connections + maintain_max_connections + max_user_connections
    result["max_connections"] = str(max_connections)
    result["default_time_zone"] = "+08:00"
    result["polarx_max_allowed_packet"] = str(1073741824)
    result["polarx_max_connections"] = str(max_user_connections)
    result["loose_polarx_max_connections"] = str(max_user_connections)
    result["loose_galaxy_max_connections"] = str(max_user_connections)
    result["mysqlx_max_connections"] = str(max_user_connections)
    return result
