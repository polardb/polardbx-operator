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
	"errors"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/str"
)

type NewLoadEvent struct {
	ThreadID          uint32    `json:"thread_id,omitempty"`
	ExecTime          uint32    `json:"exec_time,omitempty"`
	SkipLines         uint32    `json:"skip_lines,omitempty"`
	Schema            str.Str   `json:"schema,omitempty"`
	Table             str.Str   `json:"table,omitempty"`
	FieldTerminatedBy str.Str   `json:"field_terminated_by,omitempty"`
	FieldEnclosedBy   str.Str   `json:"field_enclosed_by,omitempty"`
	LineTerminatedBy  str.Str   `json:"line_terminated_by,omitempty"`
	LineStartingBy    str.Str   `json:"line_starting_by,omitempty"`
	FieldEscapedBy    str.Str   `json:"field_escaped_by,omitempty"`
	OptFlags          uint8     `json:"opt_flags,omitempty"`
	Fields            []str.Str `json:"fields,omitempty"`
	File              str.Str   `json:"file,omitempty"`
}

func (e *NewLoadEvent) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	var tableNameLength, schemaNameLength uint8
	var numFields uint32
	var fieldNameLength []uint8

	var fieldTermLen, enclosedByLen, lineTermLen, lineStartLen, escapedByLen uint8
	return layout.Decl(
		layout.Number(&e.ThreadID),
		layout.Number(&e.ExecTime),
		layout.Number(&e.SkipLines),
		layout.Number(&tableNameLength),
		layout.Number(&schemaNameLength),
		layout.Number(&numFields),

		layout.Number(&fieldTermLen),
		layout.Bytes(&fieldTermLen, &e.FieldTerminatedBy),
		layout.Number(&enclosedByLen),
		layout.Bytes(&enclosedByLen, &e.FieldEnclosedBy),
		layout.Number(&lineTermLen),
		layout.Bytes(&lineTermLen, &e.LineTerminatedBy),
		layout.Number(&lineStartLen),
		layout.Bytes(&lineStartLen, &e.LineStartingBy),
		layout.Number(&escapedByLen),
		layout.Bytes(&escapedByLen, &e.FieldEscapedBy),
		layout.Number(&e.OptFlags),

		layout.Bytes(&numFields, &fieldNameLength),
		layout.Area(layout.Infinite(), func(data []byte) (int, error) {
			fields := make([][]byte, numFields)
			off := 0
			for i := range fields {
				length := int(fieldNameLength[i])
				if len(data) < off+length+1 {
					return 0, errors.New("not enough bytes")
				}
				if data[off+length] != 0 {
					return 0, errors.New("not null")
				}
				fields[i] = make([]byte, length)
				copy(fields[i], data[off:off+length])
				off += length + 1
			}
			return off, nil
		}),
		layout.BytesEndWithNull(&tableNameLength, &e.Table), layout.Null(),
		layout.BytesEndWithNull(&schemaNameLength, &e.Schema), layout.Null(),
		layout.Bytes(layout.Infinite(), &e.File),
	)
}
