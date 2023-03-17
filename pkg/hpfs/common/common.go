/*
Copyright 2021 Alibaba Group Holding Limited.

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

package common

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	AffectedFiles = "AffectedFiles"
)

func ReadBytes(reader io.Reader, len uint64) ([]byte, error) {
	bytes := make([]byte, len)
	for readLen := uint64(0); readLen < len; {
		nowReadLen, err := reader.Read(bytes[readLen:])
		if nowReadLen == 0 {
			return bytes[:readLen], err
		}
		readLen += uint64(nowReadLen)
	}
	return bytes, nil
}

func ReadInt64(reader io.Reader) (int64, error) {
	bytes, err := ReadBytes(reader, 8)
	if err != nil {
		return int64(0), err
	}
	return int64(binary.BigEndian.Uint64(bytes)), nil
}

func ParseNetAddr(addr string) (string, int) {
	strs := strings.Split(strings.Trim(addr, " "), ":")
	if len(strs) != 2 {
		panic(fmt.Sprintf("invalid addr %s", addr))
	}
	port, err := strconv.Atoi(strs[1])
	if err != nil {
		panic(fmt.Sprintf("invalid addr %s %v", addr, err))
	}
	return strs[0], port
}
