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
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/str"
)

type UserVarEvent struct {
	Name    str.Str `json:"name,omitempty"`
	IsNull  bool    `json:"is_null,omitempty"`
	Type    byte    `json:"type,omitempty"`
	Charset uint32  `json:"charset,omitempty"`
	Value   str.Str `json:"value,omitempty"`
}

func (e *UserVarEvent) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	var nameLen uint32

	return layout.Decl(
		layout.Number(&nameLen),
		layout.Bytes(&nameLen, &e.Name),
		layout.Bool(&e.IsNull),
		layout.Area(layout.Infinite(), func(data []byte) (int, error) {
			if !e.IsNull {
				var valueLen uint32
				return layout.Decl(
					layout.Number(&e.Type),
					layout.Number(&e.Charset),
					layout.Number(&valueLen),
					layout.Bytes(&valueLen, &e.Value),
				).FromBlock(data)
			}
			return 0, nil
		}),
	)
}
