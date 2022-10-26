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

import "encoding/json"

// DecodeBlob controls if blob is decoded into string with default encoding.
var DecodeBlob = false

type Blob []byte

func (blob Blob) MarshalJSON() ([]byte, error) {
	if DecodeBlob {
		var s string
		if b, err := DefaultEncoding.NewEncoder().Bytes(blob); err != nil {
			s = string('\uFFFD')
		} else {
			s = string(b)
		}
		return json.Marshal(s)
	} else {
		return json.Marshal([]byte(blob))
	}
}
