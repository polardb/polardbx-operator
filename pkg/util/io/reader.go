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

package io

import "io"

func ReadBytes(reader io.Reader, len int, bytes []byte) (int, error) {
	readLen := 0
	for readLen = 0; readLen < len; {
		nowReadLen, err := reader.Read(bytes[readLen:])
		if nowReadLen == 0 {
			return readLen, err
		}
		readLen += nowReadLen
	}
	return readLen, nil
}
