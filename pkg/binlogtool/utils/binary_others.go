//go:build !386 && !amd64 && !amd64p32 && !arm && !arm64 && !arm64be && !armbe && !loong64 && !mips && !mips64 && !mips64le && !mips64p32 && !mips64p32le && !mipsle && !ppc && !ppc64 && !ppc64le && !riscv && !riscv64 && !s390 && !s390x && !sparc && !sparc64 && !wasm && !generate

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

	"golang.org/x/exp/constraints"
)

func ReadNumberLittleEndianHack[T constraints.Integer](data *T, bs []byte) {
	ReadNumber(binary.BigEndian, data, bs)
}

func ReadNumberBigEndianHack[T constraints.Integer](data *T, bs []byte) {
	ReadNumber(binary.BigEndian, data, bs)
}
