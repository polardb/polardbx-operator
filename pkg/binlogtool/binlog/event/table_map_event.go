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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/str"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/bitmap"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

type Column struct {
	Type byte   `json:"type"`
	Meta uint16 `json:"meta"`
}

func (c *Column) String() string {
	v, _ := json.Marshal(c)
	return string(v)
}

type TableMapEvent struct {
	TableID     uint64        `json:"table_id,omitempty"`
	Schema      str.Str       `json:"schema,omitempty"`
	Table       str.Str       `json:"table,omitempty"`
	ColumnCount int           `json:"column_count,omitempty"`
	Columns     []Column      `json:"columns,omitempty"`
	NullBitmap  bitmap.Bitmap `json:"null_bitmap,omitempty"`

	// Optional metadata.

	// SignednessBitmap stores signedness info for numeric columns.
	SignednessBitmap bitmap.Bitmap `json:"signedness_bitmap,omitempty"`

	// DefaultCharset/ColumnCharset stores collation info for character columns.

	// DefaultCharset[0] is the default collation of character columns.
	// For character columns that have different charset,
	// (character column index, column collation) pairs follows
	DefaultCharset []uint64 `json:"default_charset,omitempty"`

	// ColumnCharset contains collation sequence for all character columns
	ColumnCharset []uint64 `json:"column_charset,omitempty"`

	// SetStrValues stores values for set columns.
	SetStrValues [][]str.Str `json:"set_str_values,omitempty"`

	// EnumStrValues stores values for enum columns.
	EnumStrValues [][]str.Str `json:"enum_str_values,omitempty"`

	// ColumnNames list all column names.
	ColumnNames []str.Str `json:"column_names,omitempty"`

	// GeometryType stores real type for geometry columns.
	GeometryType []uint64 `json:"geometry_type,omitempty"`

	// PrimaryKey is a sequence of column indexes of primary key.
	PrimaryKey []uint64 `json:"primary_key,omitempty"`

	// PrimaryKeyPrefix is the prefix length used for each column of primary key.
	// 0 means that the whole column length is used.
	PrimaryKeyPrefix []uint64 `json:"primary_key_prefix,omitempty"`

	// EnumSetDefaultCharset/EnumSetColumnCharset is similar to DefaultCharset/ColumnCharset but for enum/set columns.
	EnumSetDefaultCharset []uint64 `json:"enum_set_default_charset,omitempty"`
	EnumSetColumnCharset  []uint64 `json:"enum_set_column_charset,omitempty"`
}

func fieldMetaSize(t uint8) uint8 {
	return spec.FieldMetaLength[t]
}

func parseEnumOrSetCandidateStrValues(bs []byte) ([][]str.Str, error) {
	var r [][]str.Str
	off := 0
	for off < len(bs) {
		var valueCnt uint64
		n, err := layout.PackedInt(&valueCnt).FromBlock(bs[off:])
		if err != nil {
			return nil, err
		}
		off += n

		values := make([]str.Str, 0, int(valueCnt))
		for i := 0; i < int(valueCnt); i++ {
			var valueLen uint64
			var value []byte

			n, err := layout.Decl(
				layout.PackedInt(&valueLen),
				layout.Bytes(&valueLen, &value),
			).FromBlock(bs[off:])
			if err != nil {
				return nil, err
			}

			values = append(values, value)

			off += n
		}

		r = append(r, values)
	}

	if off != len(bs) {
		return nil, fmt.Errorf("enum or set candidates block size not match, expect %d, actual %d", len(bs), off)
	}

	return r, nil
}

func parsePackedIntSequence(bs []byte) ([]uint64, error) {
	s := make([]uint64, 0)
	off := 0
	for off < len(bs) {
		var v uint64
		n, err := layout.PackedInt(&v).FromBlock(bs[off:])
		if err != nil {
			return nil, err
		}
		s = append(s, v)
		off += n
	}
	if off != len(bs) {
		return nil, fmt.Errorf("packed int sequence block size not match, expect %d, actual %d", len(bs), off)
	}
	return s, nil
}

func (e *TableMapEvent) parsePrimaryKeyWithPrefix(bs []byte) error {
	off := 0
	for off < len(bs) {
		var columnIndex uint64
		n, err := layout.PackedInt(&columnIndex).FromBlock(bs[off:])
		if err != nil {
			return err
		}
		e.PrimaryKey = append(e.PrimaryKey, columnIndex)
		off += n

		var prefixLength uint64
		n, err = layout.PackedInt(&prefixLength).FromBlock(bs[off:])
		if err != nil {
			return err
		}
		e.PrimaryKeyPrefix = append(e.PrimaryKeyPrefix, prefixLength)
		off += n
	}

	if off != len(bs) {
		return fmt.Errorf("primary key with prefix block size not match, expect %d, actual %d", len(bs), off)
	}

	return nil
}

func (e *TableMapEvent) parseSimplePrimaryKey(bs []byte) error {
	off := 0
	for off < len(bs) {
		var columnIndex uint64
		n, err := layout.PackedInt(&columnIndex).FromBlock(bs[off:])
		if err != nil {
			return err
		}
		e.PrimaryKey = append(e.PrimaryKey, columnIndex)
		e.PrimaryKeyPrefix = append(e.PrimaryKeyPrefix, 0)
		off += n
	}

	if off != len(bs) {
		return fmt.Errorf("simple primary key block size not match, expect %d, actual %d", len(bs), off)
	}

	return nil
}

func (e *TableMapEvent) parseColumnNames(bs []byte) error {
	e.ColumnNames = make([]str.Str, len(e.Columns))
	off := 0
	for i := 0; i < len(e.Columns); i++ {
		if off >= len(bs) {
			return errors.New("not enough bytes")
		}
		l := int(bs[off])
		off++
		if off+l >= len(bs) {
			return errors.New("not enough bytes")
		}
		e.ColumnNames[i] = bs[off : off+l]
		off += l
	}
	if off != len(bs) {
		return fmt.Errorf("column names block size not match, expect %d, actual %d", len(bs), off)
	}
	return nil
}

func (e *TableMapEvent) parseOptionalMetadata(block []byte) (int, error) {
	off := 0
	for off < len(block) {
		// Type (1 byte), Length (packed int), Value (Length bytes)
		t := block[off]
		off++

		var length uint64
		n, err := layout.PackedInt(&length).FromBlock(block[off:])
		if err != nil {
			return 0, err
		}
		off += n

		if off+int(length) > len(block) {
			return 0, errors.New("not enough bytes")
		}

		switch t {
		case spec.TABLE_MAP_OPT_META_SIGNEDNESS:
			var buf []byte
			if _, err = layout.Bytes(&length, &buf).FromBlock(block[off:]); err != nil {
				return 0, err
			}
			e.SignednessBitmap = bitmap.NewBitmap(buf, int(length*8))
		case spec.TABLE_MAP_OPT_META_DEFAULT_CHARSET:
			e.DefaultCharset, err = parsePackedIntSequence(block[off : off+int(length)])
			if err != nil {
				return 0, err
			}
			if len(e.DefaultCharset)%2 != 1 {
				return 0, fmt.Errorf("expect odd items in default charset, but got %d", len(e.ColumnCharset))
			}

		case spec.TABLE_MAP_OPT_META_COLUMN_CHARSET:
			e.ColumnCharset, err = parsePackedIntSequence(block[off : off+int(length)])

		case spec.TABLE_MAP_OPT_META_COLUMN_NAME:
			_, err = layout.Area(&length, func(bs []byte) (int, error) {
				if err := e.parseColumnNames(bs); err != nil {
					return 0, err
				}
				return len(bs), nil
			}).FromBlock(block[off:])

		case spec.TABLE_MAP_OPT_META_SET_STR_VALUE:
			e.SetStrValues, err = parseEnumOrSetCandidateStrValues(block[off : off+int(length)])

		case spec.TABLE_MAP_OPT_META_ENUM_STR_VALUE:
			e.EnumStrValues, err = parseEnumOrSetCandidateStrValues(block[off : off+int(length)])

		case spec.TABLE_MAP_OPT_META_GEOMETRY_TYPE:
			e.GeometryType, err = parsePackedIntSequence(block[off : off+int(length)])

		case spec.TABLE_MAP_OPT_META_SIMPLE_PRIMARY_KEY:
			err = e.parseSimplePrimaryKey(block[off : off+int(length)])

		case spec.TABLE_MAP_OPT_META_PRIMARY_KEY_WITH_PREFIX:
			err = e.parsePrimaryKeyWithPrefix(block[off : off+int(length)])

		case spec.TABLE_MAP_OPT_META_ENUM_AND_SET_DEFAULT_CHARSET:
			e.EnumSetDefaultCharset, err = parsePackedIntSequence(block[off : off+int(length)])
			if err != nil {
				return 0, err
			}
			if len(e.EnumSetDefaultCharset)%2 != 1 {
				return 0, fmt.Errorf("expect odd items in enum and set default charset, but got %d", len(e.EnumSetDefaultCharset))
			}

		case spec.TABLE_MAP_OPT_META_ENUM_AND_SET_COLUMN_CHARSET:
			e.EnumSetColumnCharset, err = parsePackedIntSequence(block[off : off+int(length)])

		default:
			// Skip
		}

		if err != nil {
			return 0, err
		}

		off += int(length)
	}
	return off, nil
}

func (e *TableMapEvent) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	postHeaderLen := fde.GetPostHeaderLength(code, 6)

	var schemaLen, tableLen uint8
	var columnCnt uint64
	var metaBlockLen uint64
	var columnTypes []byte

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
		layout.Skip(layout.Const(2)),

		// Payload
		layout.Number(&schemaLen),
		layout.Bytes(&schemaLen, &e.Schema), layout.Null(),
		layout.Number(&tableLen),
		layout.Bytes(&tableLen, &e.Table), layout.Null(),
		layout.PackedInt(&columnCnt),
		layout.Bytes(&columnCnt, &columnTypes),
		layout.PackedInt(&metaBlockLen),
		layout.Area(&metaBlockLen, func(block []byte) (int, error) {
			e.ColumnCount = int(columnCnt)
			e.Columns = make([]Column, columnCnt)
			off := 0
			for i := 0; i < int(columnCnt); i++ {
				columnType := columnTypes[i]
				length := fieldMetaSize(columnType)
				if len(block) < off+int(length) {
					return 0, errors.New("not enough bytes")
				}
				if length == 0 {
					e.Columns[i] = Column{Type: columnType, Meta: 0}
				} else if length == 1 {
					e.Columns[i] = Column{Type: columnType, Meta: uint16(block[off])}
				} else if length == 2 {
					var meta uint16
					switch columnType {
					case spec.MYSQL_TYPE_VARCHAR,
						spec.MYSQL_TYPE_BIT: // { bits length, bytes length }
						utils.ReadNumberLittleEndianHack(&meta, block[off:off+2])
					case spec.MYSQL_TYPE_NEWDECIMAL, // { precision, decimals }
						spec.MYSQL_TYPE_STRING, spec.MYSQL_TYPE_VAR_STRING: // { real type, pack or field length }
						utils.ReadNumber(binary.BigEndian, &meta, block[off:off+2])
					default:
						utils.ReadNumberLittleEndianHack(&meta, block[off:off+2])
					}
					e.Columns[i] = Column{Type: columnType, Meta: meta}
				} else {
					panic("never reach here, meta length must not be > 2")
				}

				off += int(length)
			}
			return off, nil
		}),
		layout.BitSet(&columnCnt, &e.NullBitmap),
		layout.Area(layout.Infinite(), func(block []byte) (int, error) {
			n, err := e.parseOptionalMetadata(block)
			if err != nil {
				return 0, err
			}
			if n != len(block) {
				return 0, errors.New("optional metadata block not used up")
			}
			return n, nil
		}),
	)
}
