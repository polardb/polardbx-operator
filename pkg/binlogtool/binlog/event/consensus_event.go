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

package event

import (
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"
)

type ConsensusEvent struct {
	Flag    uint32 `json:"flag,omitempty"`
	Term    uint64 `json:"term,omitempty"`
	Index   uint64 `json:"index,omitempty"`
	Length  uint64 `json:"length,omitempty"`
	Reserve uint64 `json:"reserve,omitempty"`
}

func (e *ConsensusEvent) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	return layout.Decl(
		layout.Number(&e.Flag),
		layout.Number(&e.Term),
		layout.Number(&e.Index),
		layout.Number(&e.Length),
		layout.Number(&e.Reserve),
	)
}
