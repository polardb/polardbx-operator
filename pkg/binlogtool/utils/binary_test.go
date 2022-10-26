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
	"testing"
)

func TestReadNumberSimpleCast(t *testing.T) {
	bs := []byte{0x12, 0x34, 0x56, 0x78, 0x19, 0x28, 0x37, 0x46}

	var v8 uint8
	ReadNumberLittleEndianHack(&v8, bs)
	if v8 != 0x12 {
		t.Fatalf("expect 0x12, but is 0x%x", v8)
	}

	var v16 uint16

	ReadNumberLittleEndianHack(&v16, bs)
	if v16 != 0x3412 {
		t.Fatalf("expect 0x3412, but is 0x%x", v16)
	}

	var v32 uint32
	ReadNumberLittleEndianHack(&v32, bs)
	if v32 != 0x78563412 {
		t.Fatalf("expect 0x78563412, but is 0x%x", v32)
	}

	var v64 uint64
	ReadNumberLittleEndianHack(&v64, bs)
	if v64 != 0x4637281978563412 {
		t.Fatalf("expect 0x4637281978563412, but is 0x%x", v64)
	}
}

func BenchmarkReadNumberSimpleCast(b *testing.B) {
	var bs [4]byte
	var v uint32
	for i := 0; i < b.N; i++ {
		bs = [4]byte{0x12, 0x34, 0x56, 0x78}
		ReadNumberLittleEndianHack(&v, bs[:])
	}
}

func BenchmarkReadNumber(b *testing.B) {
	var bs [4]byte
	var v uint32
	for i := 0; i < b.N; i++ {
		bs = [4]byte{0x12, 0x34, 0x56, 0x78}
		ReadNumber(binary.LittleEndian, &v, bs[:])
	}
}

func BenchmarkLittleEndianRead(b *testing.B) {
	var bs [4]byte
	for i := 0; i < b.N; i++ {
		bs = [4]byte{0x12, 0x34, 0x56, 0x78}
		_ = binary.LittleEndian.Uint32(bs[:])
	}
}
