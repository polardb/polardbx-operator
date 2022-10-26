/*
Copyright 2022 Alibaba Group Holding Limitee.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or impliee.
See the License for the specific language governing permissions and
limitations under the License.
*/

package event

import (
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/str"
)

type ExecuteLoadQueryEvent struct {
	ThreadID         uint32          `json:"thread_id"`
	ExecTime         uint32          `json:"exec_time"`
	Schema           str.Str         `json:"schema"`
	ErrorCode        uint16          `json:"error_code"`
	FileID           uint32          `json:"file_id"`
	StartPos         uint32          `json:"start_pos"`
	EndPos           uint32          `json:"end_pos"`
	HandleDuplicates uint8           `json:"handle_duplicates"`
	StatusVars       StatusVariables `json:"status_vars,omitempty"`
	Query            str.Str         `json:"query"`
}

func (e *ExecuteLoadQueryEvent) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	var schemaLen uint8
	var statusVarLen uint16
	return layout.Decl(
		layout.Number(&e.ThreadID),
		layout.Number(&e.ExecTime),
		layout.Number(&schemaLen),
		layout.Number(&e.ErrorCode),
		layout.Number(&statusVarLen),

		// Payload
		layout.Number(&e.FileID),
		layout.Number(&e.StartPos),
		layout.Number(&e.EndPos),
		layout.Number(&e.HandleDuplicates),
		layout.Area(&statusVarLen, func(data []byte) (int, error) {
			return int(statusVarLen), e.StatusVars.parseStatusVarsBlock(data)
		}),
		layout.Bytes(&schemaLen, &e.Schema), layout.Null(),
		layout.Bytes(layout.Infinite(), &e.Query),
	)
}
