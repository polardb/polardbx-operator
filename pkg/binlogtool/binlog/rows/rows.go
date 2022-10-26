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

package rows

import (
	"errors"
	"fmt"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/str"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/bitmap"
)

type Column struct {
	event.Column `json:",inline"`
	Nullable     bool `json:"nullable"`
	Value        any  `json:"value"`
}

func newColumn(colDef event.Column, nullable bool, value any) Column {
	return Column{
		Column:   colDef,
		Nullable: nullable,
		Value:    value,
	}
}

type Row []Column

// DecodeRow decodes one row from the block.
// Note that MySQL allows date/time/datetime like '0000-00-00' which is not allowed in go, these values will be converted to '0001-01-01'.
// Note that bytes are always reused.
func DecodeRow(columns bitmap.Bitmap, rows []byte, table *event.TableMapEvent) (Row, int, error) {
	columnsPresent := columns.NumBitsSet()

	bs := rows
	nullBitmapLen := (columnsPresent + 7) / 8
	if len(bs) < nullBitmapLen {
		return nil, 0, errors.New("not enough bytes")
	}
	nullBitmap := bitmap.NewBitmap(bs[:nullBitmapLen], columnsPresent)
	off := nullBitmapLen
	row := make(Row, 0, columnsPresent)
	for i := 0; i < table.ColumnCount; i++ {
		if !columns.Get(i) { // not present
			continue
		}
		columnDef := table.Columns[i]
		if nullBitmap.Get(len(row)) {
			row = append(row, newColumn(columnDef, table.NullBitmap.Get(i), nil))
			continue
		}
		v, n, err := ExtractColumnValueFromBlock(bs[off:], columnDef)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to extract value for type %s: %w", columnDef.String(), err)
		}
		off += n
		row = append(row, newColumn(columnDef, table.NullBitmap.Get(i), v))
	}

	return row, off, nil
}

type DecodedRowsEvent interface {
	event.RowsEventVariants
	GetSchema() str.Str
	GetTable() str.Str
	GetDecodedRows() []Row
}

type DecodedWriteRowsEvent struct {
	event.WriteRowsEvent `json:",inline"`
	Schema               str.Str `json:"schema,omitempty"`
	Table                str.Str `json:"table,omitempty"`
	DecodedRows          []Row   `json:"rows,omitempty"`
}

func (e *DecodedWriteRowsEvent) GetSchema() str.Str {
	return e.Schema
}

func (e *DecodedWriteRowsEvent) GetTable() str.Str {
	return e.Table
}

func (e *DecodedWriteRowsEvent) GetDecodedRows() []Row {
	return e.DecodedRows
}

func (e *DecodedWriteRowsEvent) AsRowsEvent() *event.RowsEvent {
	return e.WriteRowsEvent.AsRowsEvent()
}

type DecodedDeleteRowsEvent struct {
	event.DeleteRowsEvent `json:",inline"`
	Schema                str.Str `json:"schema,omitempty"`
	Table                 str.Str `json:"table,omitempty"`
	DecodedRows           []Row   `json:"rows,omitempty"`
}

func (e *DecodedDeleteRowsEvent) GetSchema() str.Str {
	return e.Schema
}

func (e *DecodedDeleteRowsEvent) GetTable() str.Str {
	return e.Table
}

func (e *DecodedDeleteRowsEvent) GetDecodedRows() []Row {
	return e.DecodedRows
}

func (e *DecodedDeleteRowsEvent) AsRowsEvent() *event.RowsEvent {
	return e.DeleteRowsEvent.AsRowsEvent()
}

func DecodeWriteRowsEvent(event *event.WriteRowsEvent, table *event.TableMapEvent) (*DecodedWriteRowsEvent, error) {
	rows := make([]Row, 0, 1)
	off := 0
	for off < len(event.Rows) {
		row, n, err := DecodeRow(event.Columns, event.Rows[off:], table)
		if err != nil {
			return nil, err
		}
		off += n
		rows = append(rows, row)
	}
	return &DecodedWriteRowsEvent{
		WriteRowsEvent: *event,
		Schema:         table.Schema,
		Table:          table.Table,
		DecodedRows:    rows,
	}, nil
}

func DecodeDeleteRowsEvent(event *event.DeleteRowsEvent, table *event.TableMapEvent) (*DecodedDeleteRowsEvent, error) {
	rows := make([]Row, 0, 1)
	off := 0
	for off < len(event.Rows) {
		row, n, err := DecodeRow(event.Columns, event.Rows[off:], table)
		if err != nil {
			return nil, err
		}
		off += n
		rows = append(rows, row)
	}
	return &DecodedDeleteRowsEvent{
		DeleteRowsEvent: *event,
		Schema:          table.Schema,
		Table:           table.Table,
		DecodedRows:     rows,
	}, nil
}

type DecodedUpdateRowsEvent struct {
	event.UpdateRowsEvent `json:",inline"`
	Schema                str.Str `json:"schema,omitempty"`
	Table                 str.Str `json:"table,omitempty"`
	DecodedRows           []Row   `json:"rows,omitempty"`
	DecodedChangedRows    []Row   `json:"changed_rows,omitempty"`
}

func (e *DecodedUpdateRowsEvent) GetSchema() str.Str {
	return e.Schema
}

func (e *DecodedUpdateRowsEvent) GetTable() str.Str {
	return e.Table
}

func (e *DecodedUpdateRowsEvent) GetDecodedRows() []Row {
	return e.DecodedRows
}

func (e *DecodedUpdateRowsEvent) AsRowsEvent() *event.RowsEvent {
	return e.UpdateRowsEvent.AsRowsEvent()
}

func DecodeUpdateRowsEvent(event *event.UpdateRowsEvent, table *event.TableMapEvent) (*DecodedUpdateRowsEvent, error) {
	rows := make([]Row, 0, 1)
	changedRows := make([]Row, 0, 1)
	off := 0
	for off < len(event.Rows) {
		row, n, err := DecodeRow(event.Columns, event.Rows[off:], table)
		if err != nil {
			return nil, err
		}
		off += n
		rows = append(rows, row)
		changedRow, n, err := DecodeRow(event.ChangedColumns, event.Rows[off:], table)
		if err != nil {
			return nil, err
		}
		off += n
		changedRows = append(changedRows, changedRow)
	}
	return &DecodedUpdateRowsEvent{
		UpdateRowsEvent:    *event,
		Schema:             table.Schema,
		Table:              table.Table,
		DecodedRows:        rows,
		DecodedChangedRows: changedRows,
	}, nil
}
