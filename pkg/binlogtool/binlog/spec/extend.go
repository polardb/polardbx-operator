/*
Copyright 2022 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package spec

// Extended event types.
const (
	SEQUENCE_EVENT                     uint8 = 80
	PREVIOUS_PREPARED_XIDS_EVENT             = 99
	GROUP_UPDATE_ROWS_EVENT                  = 100
	CONSENSUS_LOG_EVENT                      = 101
	PREVIOUS_CONSENSUS_INDEX_LOG_EVENT       = 102
	CONSENSUS_CLUSTER_INFO_EVENT             = 103
	CONSENSUS_EMPTY_EVENT                    = 104
	GCN_LOG_EVENT                            = 105
	ANNOTATE_ROWS_EVENT                      = 160
	BINLOG_CHECKPOINT_EVENT                  = 161
	GTID_EVENT                               = 162
	GTID_LIST_EVENT                          = 163
	START_ENCRYPTION_EVENT                   = 164
)

// Extended status var codes.
const (
	Q_LIZARD_COMMIT_GCN  uint8 = 200
	Q_LIZARD_PREPARE_GCN       = 201
	Q_HRNOW                    = 128
)

// Sequence types.
const (
	INVALID_SEQUENCE  uint8 = 0
	SNAPSHOT_SEQUENCE       = 1
	COMMIT_SEQUENCE         = 2
	HEARTBEAT               = 3
)
