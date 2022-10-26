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

package utils

import (
	"encoding/binary"
	"unsafe"

	"github.com/dolmen-go/endian"
	"golang.org/x/exp/constraints"
)

func forceConvert[T constraints.Integer | constraints.Float, R constraints.Integer | constraints.Float](x T) R {
	return *(*R)(unsafe.Pointer(&x))
}

func init() {
	_ = endian.Native
}

func ReadNumber[T constraints.Integer | constraints.Float](order binary.ByteOrder, data *T, bs []byte) {
	size := int(unsafe.Sizeof(T(0)))

	switch size {
	case 1:
		*data = T(bs[0])
	case 2:
		*data = T(order.Uint16(bs))
	case 4:
		*data = forceConvert[uint32, T](order.Uint32(bs))
	default:
		*data = forceConvert[uint64, T](order.Uint64(bs))
	}
}

// Two more endian specific functions are implemented:
//
//   + func ReadNumberLittleEndianHack[T constraints.Integer | constraints.Float](data *T, bs []byte)
//   + func ReadNumberBigEndianHack[T constraints.Integer | constraints.Float](data *T, bs []byte)
//
// Commonly they have better than or at least the same performance with the ReadNumber.
//
// The endianness is determined by script and for now. You can find the latest
// build tags for different endianness in the following repo:
// https://github.com/dolmen-go/endian
