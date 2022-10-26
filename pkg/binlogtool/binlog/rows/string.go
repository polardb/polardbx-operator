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

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/str"
)

// MYSQL_TYPE_VARCHAR, MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING

func ExtractStringFromBlock(bs []byte, length uint64) (str.Str, error) {
	if uint64(len(bs)) < length {
		return nil, errors.New("not enough bytes")
	}
	return bs[:length], nil
}

func ExtractStringFromStream(r io.Reader, length uint64) (str.Str, error) {
	bs := make([]byte, length)
	if _, err := io.ReadFull(r, bs); err != nil {
		return nil, fmt.Errorf("unable to read: %w", err)
	}
	return bs, nil
}
