/*
Copyright 2021 Alibaba Group Holding Limited.

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

package channel

import "encoding/json"

type ChannelStatus string

const (
	ChannelBlocked ChannelStatus = "blocked"
	ChannelOpen    ChannelStatus = ""
)

type Node struct {
	Pod    string `json:"pod,omitempty"`
	Host   string `json:"host,omitempty"`
	Domain string `json:"domain,omitempty"`
	Port   int    `json:"port,omitempty"`
	// Node name in k8s.
	HostName string `json:"node_name,omitempty"`
	// Pod role.
	Role string `json:"role,omitempty"`
}

type Indicate struct {
	ResetMeta      bool `json:"reset_meta,omitempty"`
	ResetAsLearner bool `json:"reset_as_learner,omitempty"`
	RecoverIndex   *int `json:"recover_index,omitempty"`
	Block          bool `json:"block,omitempty"`
	ResetConfig    bool `json:"reset_config,omitempty"`

	// used when bootstrap cluster.
	BlockGatePrepareData bool `json:"block_gate_prepare_data,omitempty"`
}

type SharedChannel struct {
	Generation            int64               `json:"generation,omitempty"`
	Nodes                 []Node              `json:"nodes,omitempty"`
	Status                ChannelStatus       `json:"status,omitempty"`
	Indicates             map[string]Indicate `json:"indicates,omitempty"`
	LastBackupBinlogIndex *int64              `json:"last_backup_log_index,omitempty"`
}

func (sc *SharedChannel) IsBlocked() bool {
	return sc.Status == ChannelBlocked
}

func (sc *SharedChannel) Unblock() {
	sc.Status = ChannelOpen
}

func (sc *SharedChannel) Block() {
	sc.Status = ChannelBlocked
}

func (sc *SharedChannel) UpdateLastBackupBinlogIndex(commitIndex *int64) {
	sc.LastBackupBinlogIndex = commitIndex
}
func (sc *SharedChannel) String() string {
	b, _ := json.MarshalIndent(sc, "", "  ")
	return string(b)
}

func (sc *SharedChannel) Load(s string) error {
	return json.Unmarshal([]byte(s), sc)
}

const SharedChannelKey = "shared-channel.json"
