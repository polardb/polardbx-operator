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
	"io"
	"unsafe"

	"golang.org/x/exp/constraints"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

// MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_LONG, MYSQL_TYPE_LONGLONG
// MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE

func ExtractNumberFromStream[T constraints.Integer | constraints.Float](r io.Reader) (T, error) {
	bs := make([]byte, unsafe.Sizeof(T(0)))
	if _, err := io.ReadFull(r, bs[:]); err != nil {
		return T(0), fmt.Errorf("unable to read: %w", err)
	}
	return ExtractNumberFromBlock[T](bs[:])
}

func ExtractNumberFromBlock[T constraints.Integer | constraints.Float](bs []byte) (T, error) {
	if unsafe.Sizeof(T(0)) > uintptr(len(bs)) {
		return T(0), errors.New("not enough bytes")
	}

	var t T
	utils.ReadNumberLittleEndianHack(&t, bs)
	return t, nil
}

// MYSQL_TYPE_INT24

func ExtractInt24FromBlock(bs []byte) (uint32, error) {
	if len(bs) < 3 {
		return 0, errors.New("not enough bytes")
	}

	return (uint32(bs[2]) << 16) + (uint32(bs[1]) << 8) + uint32(bs[0]), nil
}

func ExtractInt24FromStream(r io.Reader) (uint32, error) {
	var bs [3]byte
	if _, err := io.ReadFull(r, bs[:]); err != nil {
		return 0, fmt.Errorf("unable to read: %w", err)
	}
	return ExtractInt24FromBlock(bs[:])
}
