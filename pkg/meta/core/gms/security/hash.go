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

package security

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"hash/fnv"
)

// Sha1Hash performs the SHA-1 hash algorithm and returns the
// hash result in hex.
func Sha1Hash(s string) (string, error) {
	h := sha1.New()

	_, err := h.Write([]byte(s))
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func Sha1HashBytes(data []byte) (string, error) {
	h := sha1.New()
	_, err := h.Write(data)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func MustSha1Hash(s string) string {
	hash, err := Sha1Hash(s)
	if err != nil {
		panic(err)
	}
	return hash
}

func Hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func HashObj(object interface{}) (string, error) {
	if data, err := json.Marshal(object); err == nil {
		return Sha1HashBytes(data)
	} else {
		return "", err
	}
}
