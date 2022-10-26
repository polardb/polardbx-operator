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

package str

import (
	"encoding/json"

	"golang.org/x/text/encoding"
)

// DefaultEncoding is UTF-8 (replacement). It replaces invalid UTF-8
// bytes to '\uFFFD'.
var DefaultEncoding = encoding.Replacement

// Str but bytes. We know nothing about the charset binlog uses and
// golang does not support charset convert directly. Thus use this as
// a trick to show UTF-8 encoded strings by default.
type Str []byte

func (s Str) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func (s Str) String() string {
	b, err := DefaultEncoding.NewEncoder().Bytes(s)
	if err != nil {
		return string('\uFFFD')
	}
	return string(b)
}
