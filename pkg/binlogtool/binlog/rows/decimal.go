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
	"math/big"

	"github.com/shopspring/decimal"
)

// MYSQL_TYPE_NEWDECIMAL
var digitsBytesNumMap = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

func decimalBytesNumber(numDigits int) int {
	return 4*(numDigits/9) + digitsBytesNumMap[numDigits%9]
}

func readBigEndianNumber(bs []byte, numBytes int) uint64 {
	switch numBytes {
	case 1:
		return uint64(bs[0])
	case 2:
		return uint64(binary.BigEndian.Uint16(bs))
	case 3:
		return uint64(uint32(bs[0])<<16 | uint32(bs[1])<<8 | uint32(bs[2]))
	case 4:
		return uint64(binary.BigEndian.Uint32(bs))
	case 8:
		return binary.BigEndian.Uint64(bs)
	default:
		panic("never reach here")
	}
}

func parseDecimalPart(bs []byte) *big.Int {
	oneBillion := big.NewInt(1000000000)
	r := big.NewInt(0)
	left := len(bs) & 0x3
	if left > 0 {
		r.Add(r, big.NewInt(0).SetUint64(readBigEndianNumber(bs, left)))
	}
	for i := left; i < len(bs); i += 4 {
		x := binary.BigEndian.Uint32(bs[i:])
		r.Mul(r, oneBillion)
		r.Add(r, big.NewInt(0).SetUint64(uint64(x)))
	}
	return r
}

func fastExp(x *big.Int, exp byte) *big.Int {
	r := big.NewInt(1)
	a := big.NewInt(0).Set(x)
	for exp > 0 {
		if exp&0x1 != 0 {
			r.Mul(r, a)
		}
		a.Mul(a, a)
		exp >>= 1
	}
	return r
}

func ExtractNewDecimalFromBlock(bs []byte, precision byte, scale byte) (decimal.Decimal, int, error) {
	intPartLen, fracPartLen := decimalBytesNumber(int(precision-scale)), decimalBytesNumber(int(scale))
	bytesLen := intPartLen + fracPartLen
	if len(bs) < bytesLen {
		return decimal.Decimal{}, 0, errors.New("not enough bytes")
	}

	negative := (bs[0] & 0x80) == 0
	buf := make([]byte, bytesLen)
	copy(buf, bs[:bytesLen])
	if negative {
		for i := range buf {
			buf[i] = ^buf[i]
		}
	}
	buf[0] &= 0x7f

	intPart := parseDecimalPart(buf[:intPartLen])
	fracPart := parseDecimalPart(buf[intPartLen:bytesLen])
	r := big.NewInt(0).Set(intPart)
	r.Mul(r, fastExp(big.NewInt(10), scale))
	r.Add(r, fracPart)

	if negative {
		r.Neg(r)
	}

	return decimal.NewFromBigInt(r, -int32(scale)), bytesLen, nil
}
