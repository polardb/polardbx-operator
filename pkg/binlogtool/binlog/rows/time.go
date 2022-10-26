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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

// MYSQL_TYPE_YEAR

func ExtractYearFromBlock(bs []byte) (uint16, error) {
	if len(bs) < 1 {
		return 0, errors.New("not enough bytes")
	}
	if bs[0] == 0 {
		return 0, nil
	}
	return 1900 + uint16(bs[0]), nil
}

// MYSQL_TYPE_DATE

func ExtractDateFromBlock(bs []byte) (time.Time, error) {
	x, err := ExtractInt24FromBlock(bs)
	if err != nil {
		return time.Time{}, err
	}

	// YYYY×16×32 + MM×32 + DD
	day := int(x & 0x1f)
	if day > 31 {
		return time.Time{}, errors.New("invalid date: day overflow")
	}
	month := int((x >> 5) & 0xf)
	if month > 12 {
		return time.Time{}, errors.New("invalid date: month overflow")
	}
	year := int(x >> 9)
	if year > 9999 {
		return time.Time{}, errors.New("invalid date: year overflow")
	}

	// zero value (tweak)
	if year == 0 {
		year = 1
	}
	if month == 0 {
		month = 1
	}
	if day == 0 {
		day = 1
	}

	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC), nil
}

// MYSQL_TYPE_TIME

func ExtractTimeFromBlock(bs []byte) (time.Duration, error) {
	x, err := ExtractInt24FromBlock(bs)
	if err != nil {
		return 0, err
	}

	// DD×24×3600 + HH×3600 + MM×60 + SS
	return time.Duration(x), nil
}

// MYSQL_TYPE_TIME2

func ExtractTime2FromBlock(bs []byte, precision byte) (time.Duration, error) {
	fspLen := fspLen(precision)
	if len(bs) < 3+fspLen {
		return 0, errors.New("not enough bytes")
	}

	// 1 bit sign    (1= non-negative, 0= negative)
	// 1 bit unused  (reserved for future extensions)
	// 10 bits hour   (0-838)
	// 6 bits minute (0-59)
	// 6 bits second (0-59)
	// ---------------------
	// 24 bits = 3 bytes
	// time value = abs| uint24 - 0x800000|, fractional part is also the distance.

	x := int64(bs[0])<<16 + int64(bs[1])<<8 + int64(bs[2])

	if fspLen > 0 {
		var fracPart uint32
		var fsp [4]byte
		copy(fsp[4-fspLen:], bs[3:3+fspLen])
		utils.ReadNumber(binary.BigEndian, &fracPart, fsp[:])
		x = x<<(fspLen*8) + int64(fracPart)
	}

	x -= 1 << (23 + fspLen*8)
	negative := x < 0
	if x < 0 {
		x = -x
	}

	// frac part
	microSeconds := x & (1<<(fspLen*8) - 1)
	microSeconds *= int64(fspTimes[fspLen])
	if microSeconds > 999999 {
		return 0, errors.New("invalid time: microsecond overflow")
	}

	// int part
	x >>= fspLen * 8

	hours := x >> 12
	if hours > 838 || hours < -838 {
		return 0, errors.New("invalid time: hour overflow")
	}
	minutes := (x >> 6) & 0x3f
	if minutes > 59 {
		return 0, errors.New("invalid time: minute overflow")
	}
	seconds := x & 0x3f
	if seconds > 59 {
		return 0, errors.New("invalid time: second overflow")
	}

	d := time.Duration(hours)*time.Hour + time.Duration(minutes)*time.Minute + time.Duration(seconds)*time.Second
	if negative {
		d = -d
	}

	return d, nil
}

// MYSQL_TYPE_TIMESTAMP

func ExtractTimestampFromBlock(bs []byte) (time.Time, error) {
	if len(bs) < 4 {
		return time.Time{}, errors.New("not enough bytes")
	}
	var ts uint32
	utils.ReadNumberLittleEndianHack(&ts, bs) // little endian
	return time.Unix(int64(ts), 0), nil
}

// MYSQL_TYPE_TIMESTAMP2

func ExtractTimestamp2FromBlock(bs []byte, precision byte) (time.Time, error) {
	fspLen := fspLen(precision)
	if len(bs) < 4+fspLen {
		return time.Time{}, errors.New("not enough bytes")
	}
	var ts uint32
	utils.ReadNumberBigEndianHack(&ts, bs) // big endian

	microSeconds, err := parseFractionalSecondPart(bs[4:], fspLen)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(int64(ts), int64(microSeconds)*1000), nil
}

// MYSQL_TYPE_DATETIME

func ExtractDatetimeFromBlock(bs []byte) (time.Time, error) {
	if len(bs) < 8 {
		return time.Time{}, errors.New("not enough bytes")
	}
	var x uint64
	utils.ReadNumberLittleEndianHack(&x, bs)
	second := int(x % 100)
	x /= 100
	if second > 59 {
		return time.Time{}, errors.New("invalid datetime: second overflow")
	}

	minute := int(x % 100)
	x /= 100
	if minute > 59 {
		return time.Time{}, errors.New("invalid datetime: minute overflow")
	}

	hour := int(x % 100)
	if hour > 12 {
		return time.Time{}, errors.New("invalid datetime: hour overflow")
	}

	x /= 100
	day := int(x % 100)
	if day > 31 {
		return time.Time{}, errors.New("invalid datetime: day overflow")
	}

	x /= 100
	month := int(x % 100)
	if month > 12 {
		return time.Time{}, errors.New("invalid datetime: month overflow")
	}

	x /= 100
	year := int(x % 100)
	if year > 9999 {
		return time.Time{}, errors.New("invalid date: year overflow")
	}

	// zero value (tweak)
	if year == 0 {
		year = 1
	}
	if month == 0 {
		month = 1
	}
	if day == 0 {
		day = 1
	}

	return time.Date(year, time.Month(month), day, hour, minute, second, 0, time.Local), nil
}

// MYSQL_TYPE_DATETIME2

var fspTimes = []uint32{0, 10000, 100, 1}

func fspLen(precision byte) int {
	return int(precision+1) / 2
}

func parseFractionalSecondPart(bs []byte, len int) (uint32, error) {
	// fractional part
	var microSeconds uint32
	if len > 0 {
		var fsp [4]byte
		copy(fsp[4-len:], bs[:len])
		utils.ReadNumber(binary.BigEndian, &microSeconds, fsp[:])
		microSeconds *= fspTimes[len]
	}

	if microSeconds > 999999 {
		return 0, errors.New("invalid fractional second part: overflow")
	}
	return microSeconds, nil
}

func ExtractDatetime2FromStream(r io.Reader, precision byte) (time.Time, error) {
	fspLen := fspLen(precision)
	bs := make([]byte, 5+fspLen)
	if _, err := io.ReadFull(r, bs); err != nil {
		return time.Time{}, fmt.Errorf("unable to read: %w", err)
	}
	return ExtractDatetime2FromBlock(bs, precision)
}

func ExtractDatetime2FromBlock(bs []byte, precision byte) (time.Time, error) {
	fspLen := fspLen(precision)
	if len(bs) < 5+fspLen {
		return time.Time{}, errors.New("not enough bytes")
	}

	// 1 bit sign + 17 bits year
	negative := (bs[0] & 0x80) == 0
	if negative {
		// now negative is reserved, should not happen
		return time.Time{}, errors.New("invalid datetime: negative is reserved")
	}
	year := int(bs[0] & 0x7f)
	year = (year << 10) + int(bs[1])<<2 + int(bs[2]>>6)
	month := year % 13
	year = year / 13
	if year > 9999 {
		return time.Time{}, errors.New("invalid datetime: year overflow")
	}
	if month > 12 {
		return time.Time{}, errors.New("invalid datetime: month overflow")
	}

	// 5 bits day
	day := int(bs[2]&0x3f) >> 1
	if day > 31 {
		return time.Time{}, errors.New("invalid datetime: day overflow")
	}

	// 5 bits hour
	hour := (int(bs[2]&0x1) << 4) + int(bs[3]>>4)
	if hour > 23 {
		return time.Time{}, errors.New("invalid datetime: hour overflow")
	}

	// 6 bits minute
	minute := (int(bs[3]&0xf) << 2) + int(bs[4]>>6)
	if minute > 59 {
		return time.Time{}, errors.New("invalid datetime: minute overflow")
	}
	// 6 bits second
	second := int(bs[4] & 0x3f)
	if second > 59 {
		return time.Time{}, errors.New("invalid datetime: second overflow")
	}

	// fractional part
	microSeconds, err := parseFractionalSecondPart(bs[5:], fspLen)
	if err != nil {
		return time.Time{}, err
	}

	// zero value (tweak)
	if year == 0 {
		year = 1
	}
	if month == 0 {
		month = 1
	}
	if day == 0 {
		day = 1
	}

	return time.Date(year, time.Month(month), day, hour, minute, second, int(microSeconds*1000), time.Local), nil
}
