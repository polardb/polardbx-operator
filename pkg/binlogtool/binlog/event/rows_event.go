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
	"fmt"
	"io"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/str"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/bitmap"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

type RowsEventExtraDataExtraInfo struct {
	Format  uint8   `json:"format,omitempty"`
	Payload str.Str `json:"payload,omitempty"`
}

type RowsEventExtraData struct {
	Type uint8 `json:"type,omitempty"`
	Data any   `json:"data,omitempty"`
}

type RowsEvent struct {
	TableID        uint64               `json:"table_id,omitempty"`
	Flags          uint16               `json:"flags,omitempty"`
	ExtraData      []RowsEventExtraData `json:"extra_data,omitempty"`
	Columns        bitmap.Bitmap        `json:"columns,omitempty"`
	ChangedColumns bitmap.Bitmap        `json:"changed_columns,omitempty"`
	Rows           []byte               `json:"rows,omitempty"`
}

func (e *RowsEvent) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	postHeaderLen := fde.GetPostHeaderLength(code, 6)

	var extraDataLen uint16
	var columnCnt uint64

	isVersionV2 := isRowsEventV2(code)
	isUpdateV1OrV2 := code == spec.UPDATE_ROWS_EVENT_V1 ||
		code == spec.UPDATE_ROWS_EVENT_V2 ||
		code == spec.GROUP_UPDATE_ROWS_EVENT
	return layout.Decl(
		layout.Conditional(postHeaderLen == 6,
			layout.Area(layout.Const(4), func(data []byte) (int, error) {
				var v uint32
				utils.ReadNumberLittleEndianHack(&v, data)
				e.TableID = uint64(v)
				return 4, nil
			}),
			layout.Area(layout.Const(6), func(data []byte) (int, error) {
				var v [8]byte
				copy(v[:6], data[:6])
				utils.ReadNumberLittleEndianHack(&e.TableID, v[:])
				return 6, nil
			})),
		layout.Number(&e.Flags),
		layout.If(isVersionV2, layout.Number(&extraDataLen)),
		layout.If(isVersionV2, layout.Area(layout.Infinite(), func(r io.Reader) error {
			if extraDataLen < 2 {
				return errors.New("invalid extra data length: must be larger than 2")
			}
			extraBlockLen := extraDataLen - 2
			bs := make([]byte, extraBlockLen)
			_, err := io.ReadFull(r, bs)
			if err != nil {
				return fmt.Errorf("unable to read extra data block: %w", err)
			}

			off := 0
			for off < len(bs) {
				extraType := bs[off]
				off++
				switch extraType {
				case spec.RW_V_EXTRAINFO_TAG:
					var length uint8
					var extraInfo RowsEventExtraDataExtraInfo
					n, err := layout.Decl(
						layout.Number(&length),
						layout.Number(&extraInfo.Format),
						layout.Bytes(&length, &extraInfo.Payload),
					).FromBlock(bs[off:])
					if err != nil {
						return err
					}
					e.ExtraData = append(e.ExtraData, RowsEventExtraData{
						Type: extraType,
						Data: &extraInfo,
					})
					off += n
				default:
					// Skip and set offset to block size.
					off = len(bs)
				}
			}

			return nil
		})),

		// Body
		layout.PackedInt(&columnCnt),
		layout.BitSet(&columnCnt, &e.Columns),
		layout.If(isUpdateV1OrV2, layout.BitSet(&columnCnt, &e.ChangedColumns)),

		// Rows
		layout.Bytes(layout.Infinite(), &e.Rows),
	)
}

func isRowsEventV0(code uint8) bool {
	return code == spec.PRE_GA_DELETE_ROWS_EVENT || code == spec.PRE_GA_WRITE_ROWS_EVENT || code == spec.PRE_GA_UPDATE_ROWS_EVENT
}

func isRowsEventV1(code uint8) bool {
	return code == spec.DELETE_ROWS_EVENT_V1 || code == spec.WRITE_ROWS_EVENT_V1 || code == spec.UPDATE_ROWS_EVENT_V1
}

func isRowsEventV2(code uint8) bool {
	return code == spec.DELETE_ROWS_EVENT_V2 || code == spec.WRITE_ROWS_EVENT_V2 || code == spec.UPDATE_ROWS_EVENT_V2
}

type RowsEventVariants interface {
	AsRowsEvent() *RowsEvent
}

func (e *RowsEvent) AsRowsEvent() *RowsEvent {
	return e
}

type DeleteRowsEvent struct {
	RowsEvent `json:",inline"`
}

func (e *DeleteRowsEvent) AsRowsEvent() *RowsEvent {
	return &e.RowsEvent
}

type WriteRowsEvent struct {
	RowsEvent `json:",inline"`
}

func (e *WriteRowsEvent) AsRowsEvent() *RowsEvent {
	return &e.RowsEvent
}

type UpdateRowsEvent struct {
	RowsEvent `json:",inline"`
}

func (e *UpdateRowsEvent) AsRowsEvent() *RowsEvent {
	return &e.RowsEvent
}

type PartialUpdateRowsEvent struct {
	RowsEvent `json:",inline"`
}

func (e *PartialUpdateRowsEvent) AsRowsEvent() *RowsEvent {
	return &e.RowsEvent
}

type GroupUpdateRows struct {
	RowsEvent `json:",inline"`
}

func (e *GroupUpdateRows) AsRowsEvent() *RowsEvent {
	return &e.RowsEvent
}
