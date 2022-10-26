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
	"io"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/str"
)

// MYSQL_TYPE_BLOB, MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB

func ExtractBlobFromBlock(bs []byte, length uint64) (str.Blob, error) {
	r, err := ExtractStringFromBlock(bs, length)
	return str.Blob(r), err
}

func ExtractBlobFromStream(r io.Reader, length uint64) (str.Blob, error) {
	x, err := ExtractStringFromStream(r, length)
	return str.Blob(x), err
}
