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

const (
	LAST_INSERT_ID_EVENT uint8 = 1
	INSERT_ID_EVENT            = 2
)

type IntvarEvent struct {
	VariableType uint8  `json:"variable_type,omitempty"`
	Value        uint64 `json:"value,omitempty"`
}

func (e *IntvarEvent) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	return layout.Decl(
		layout.Number(&e.VariableType),
		layout.Number(&e.Value),
	)
}
