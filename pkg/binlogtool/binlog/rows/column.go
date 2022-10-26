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
	"strconv"
	"time"

	"github.com/shopspring/decimal"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/str"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/bitmap"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

// Reference: libbinlogevents/include/rows_event.h

func readNumberFromBlock(bs []byte, numBytes int) (uint64, error) {
	switch numBytes {
	case 1:
		x, err := ExtractNumberFromBlock[uint8](bs)
		return uint64(x), err
	case 2:
		x, err := ExtractNumberFromBlock[uint16](bs)
		return uint64(x), err
	case 3:
		x, err := ExtractInt24FromBlock(bs)
		return uint64(x), err
	case 4:
		x, err := ExtractNumberFromBlock[uint32](bs)
		return uint64(x), err
	case 8:
		x, err := ExtractNumberFromBlock[uint64](bs)
		return x, err
	default:
		return 0, errors.New("invalid bytes number: " + strconv.Itoa(numBytes))
	}
}

func ExtractColumnValueFromBlock(bs []byte, col event.Column) (val any, n int, err error) {
	switch col.Type {
	case spec.MYSQL_TYPE_NEWDECIMAL:
		var x decimal.Decimal
		if x, n, err = ExtractNewDecimalFromBlock(bs, byte(col.Meta>>8), byte(col.Meta)); err != nil {
			return nil, 0, err
		}
		return x, n, nil
	case spec.MYSQL_TYPE_TINY:
		var x uint8
		if x, err = ExtractNumberFromBlock[uint8](bs); err != nil {
			return nil, 0, err
		}
		return x, 1, nil
	case spec.MYSQL_TYPE_SHORT:
		var x uint16
		if x, err = ExtractNumberFromBlock[uint16](bs); err != nil {
			return nil, 0, err
		}
		return x, 2, nil
	case spec.MYSQL_TYPE_LONG:
		var x uint32
		if x, err = ExtractNumberFromBlock[uint32](bs); err != nil {
			return nil, 0, err
		}
		return x, 4, nil
	case spec.MYSQL_TYPE_FLOAT:
		var x float32
		if x, err = ExtractNumberFromBlock[float32](bs); err != nil {
			return nil, 0, err
		}
		return x, 4, nil
	case spec.MYSQL_TYPE_DOUBLE:
		var x float64
		if x, err = ExtractNumberFromBlock[float64](bs); err != nil {
			return nil, 0, err
		}
		return x, 8, nil
	case spec.MYSQL_TYPE_NULL:
		return nil, 0, nil
	case spec.MYSQL_TYPE_TIMESTAMP:
		// Big endian 4 bytes.
		if len(bs) < 4 {
			return nil, 0, errors.New("not enough bytes")
		}
		var ts uint32
		utils.ReadNumberBigEndianHack(&ts, bs)
		return time.Unix(int64(ts), 0), 4, nil
	case spec.MYSQL_TYPE_LONGLONG:
		var x uint64
		if x, err = ExtractNumberFromBlock[uint64](bs); err != nil {
			return nil, 0, err
		}
		return x, 8, nil
	case spec.MYSQL_TYPE_INT24:
		var x uint32
		if x, err = ExtractInt24FromBlock(bs); err != nil {
			return nil, 0, err
		}
		return x, 3, nil
	case spec.MYSQL_TYPE_DATE:
		var x time.Time
		if x, err = ExtractDateFromBlock(bs); err != nil {
			return nil, 0, err
		}
		return x, 3, nil
	case spec.MYSQL_TYPE_TIME:
		var x time.Duration
		if x, err = ExtractTimeFromBlock(bs); err != nil {
			return nil, 0, err
		}
		return x, 3, nil
	case spec.MYSQL_TYPE_DATETIME:
		var x time.Time
		if x, err = ExtractDatetimeFromBlock(bs); err != nil {
			return nil, 0, err
		}
		return x, 8, nil
	case spec.MYSQL_TYPE_YEAR:
		var x uint16
		if x, err = ExtractYearFromBlock(bs); err != nil {
			return nil, 0, err
		}
		return x, 1, nil
	case spec.MYSQL_TYPE_VARCHAR:
		if col.Meta < 256 {
			var l uint8
			if l, err = ExtractNumberFromBlock[uint8](bs); err != nil {
				return nil, 0, err
			}
			var x str.Str
			if x, err = ExtractStringFromBlock(bs[1:], uint64(l)); err != nil {
				return nil, 0, err
			}
			return x, 1 + int(l), nil
		} else {
			var l uint16
			if l, err = ExtractNumberFromBlock[uint16](bs); err != nil {
				return nil, 0, err
			}
			var x str.Str
			if x, err = ExtractStringFromBlock(bs[2:], uint64(l)); err != nil {
				return nil, 0, err
			}
			return x, 2 + int(l), nil
		}
	case spec.MYSQL_TYPE_BIT:
		nBits := (int(col.Meta)>>8)*8 + int(col.Meta&0xff)
		length := (nBits + 7) / 8
		x := bitmap.NewBitmap(bs[:length], nBits)
		return x, length, nil
	case spec.MYSQL_TYPE_TIMESTAMP2:
		var x time.Time
		if x, err = ExtractTimestamp2FromBlock(bs, byte(col.Meta)); err != nil {
			return nil, 0, err
		}
		return x, 4 + int(fspLen(byte(col.Meta))), nil
	case spec.MYSQL_TYPE_DATETIME2:
		var x time.Time
		if x, err = ExtractDatetime2FromBlock(bs, byte(col.Meta)); err != nil {
			return nil, 0, err
		}
		return x, 5 + int(fspLen(byte(col.Meta))), nil
	case spec.MYSQL_TYPE_TIME2:
		var x time.Duration
		if x, err = ExtractTime2FromBlock(bs, byte(col.Meta)); err != nil {
			return nil, 0, err
		}
		return x, 3 + int(fspLen(byte(col.Meta))), nil
	case spec.MYSQL_TYPE_TINY_BLOB, spec.MYSQL_TYPE_MEDIUM_BLOB, spec.MYSQL_TYPE_LONG_BLOB, spec.MYSQL_TYPE_BLOB:
		// Only MYSQL_TYPE_BLOB can appear in binary logs.
		numBytes := int(col.Meta & 0xff)
		var length uint64
		if length, err = readNumberFromBlock(bs, numBytes); err != nil {
			return nil, 0, err
		}
		var x str.Blob
		if x, err = ExtractBlobFromBlock(bs[numBytes:], length); err != nil {
			return nil, 0, err
		}
		return x, numBytes + int(length), nil
	case spec.MYSQL_TYPE_STRING:
		// Real type may be
		// 	253 (MYSQL_TYPE_VAR_STRING)
		// 	247 (MYSQL_TYPE_ENUM)
		//  248 (MYSQL_TYPE_SET)
		byte0 := uint8(col.Meta >> 8)
		realType := byte0
		charLen := int(col.Meta & 0xff)
		if (byte0 & 0x30) != 0x30 {
			/* a long CHAR() field: see #37426 */
			realType |= 0x30
			charLen = charLen | int((byte0&0x30)^0x30)<<4
		}
		switch realType {
		case spec.MYSQL_TYPE_VAR_STRING, spec.MYSQL_TYPE_STRING:
			numBytes := 1
			if charLen > 255 {
				numBytes = 2
			}
			var length uint64
			if length, err = readNumberFromBlock(bs, numBytes); err != nil {
				return nil, 0, err
			}
			var x str.Str
			if x, err = ExtractStringFromBlock(bs[numBytes:], length); err != nil {
				return nil, 0, err
			}
			return x, numBytes + int(length), nil
		case spec.MYSQL_TYPE_ENUM:
			numBytes := int(col.Meta & 0xff)
			if numBytes > 2 {
				return nil, 0, errors.New("invalid enum type")
			}
			var enumIdx uint64
			if enumIdx, err = readNumberFromBlock(bs, numBytes); err != nil {
				return nil, 0, err
			}
			return uint16(enumIdx), numBytes, nil
		case spec.MYSQL_TYPE_SET:
			numBytes := int(col.Meta & 0xff)
			if numBytes > 2 {
				return nil, 0, errors.New("invalid enum type")
			}
			// Each bit represents one value, e.g.
			// 	red = 0b0001, green = 0b0010, blue = 0b0100, yellow = 0b1000
			//  { red, blue } = 0b0101
			var setPackedVal uint64
			if setPackedVal, err = readNumberFromBlock(bs, numBytes); err != nil {
				return nil, 0, err
			}
			return setPackedVal, numBytes, nil
		default:
			return nil, 0, errors.New("invalid (var) string type")
		}
	case spec.MYSQL_TYPE_GEOMETRY, spec.MYSQL_TYPE_JSON:
		// TODO: return some structure instead of raw bytes.
		numBytes := int(col.Meta & 0xff)
		var length uint64
		if length, err = readNumberFromBlock(bs, numBytes); err != nil {
			return nil, 0, err
		}
		var x str.Str
		if x, err = ExtractStringFromBlock(bs[numBytes:], length); err != nil {
			return nil, 0, err
		}
		return x, numBytes + int(length), nil
	case spec.MYSQL_TYPE_DECIMAL, spec.MYSQL_TYPE_NEWDATE, // should not appear in binary logs
		spec.MYSQL_TYPE_VAR_STRING, spec.MYSQL_TYPE_ENUM, spec.MYSQL_TYPE_SET, // appear as MYSQL_TYPE_STRING
		spec.MYSQL_TYPE_TYPED_ARRAY, // replication only
		spec.MYSQL_TYPE_INVALID,     // invalid
		spec.MYSQL_TYPE_BOOL:        // now just placeholder
		return nil, 0, errors.New("unexpected column type: " + spec.FieldTypeName(col.Type))
	default:
		return nil, 0, errors.New("unexpected column type: " + strconv.Itoa(int(col.Type)))
	}
}
