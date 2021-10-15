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

package bundle

import (
	"bytes"
	"io"
	"strings"
)

type kvBundle struct {
	parentKey string
	kv        map[string][]byte
}

func (b *kvBundle) ListPaths() ([]string, error) {
	subKeys := make(map[string]int)
	for k := range b.kv {
		if strings.HasPrefix(k, b.parentKey+"/") {
			k = k[len(b.parentKey):]
			if !strings.Contains(k, "/") {
				continue
			}
			subKeys[strings.Split(k, "/")[0]] = 0
		}
	}
	keys := make([]string, 0, len(subKeys))
	for k := range subKeys {
		keys = append(keys, k)
	}
	return keys, nil
}

func (b *kvBundle) List() ([]string, error) {
	subKeys := make(map[string]int)
	for k := range b.kv {
		if strings.HasPrefix(k, b.parentKey+"/") {
			k = k[len(b.parentKey):]
			if strings.Contains(k, "/") {
				continue
			}
			subKeys[k] = 0
		}
	}
	keys := make([]string, 0, len(subKeys))
	for k := range subKeys {
		keys = append(keys, k)
	}
	return keys, nil
}

func (b *kvBundle) Get(key string) (io.Reader, error) {
	if b.parentKey != "" {
		key = strings.Join([]string{b.parentKey, key}, "/")
	}
	content, ok := b.kv[key]
	if !ok {
		return nil, ErrNotFound
	}
	return bytes.NewBuffer(content), nil
}

func (b *kvBundle) Path(subPath string) (Bundle, error) {
	var targetPath string
	if b.parentKey != "" {
		targetPath = b.parentKey + "/" + subPath
	} else {
		targetPath = subPath
	}

	return &kvBundle{
		parentKey: targetPath,
		kv:        b.kv,
	}, nil
}

func NewKvBundle(kv map[string][]byte) Bundle {
	return &kvBundle{
		parentKey: "",
		kv:        kv,
	}
}
