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

import "io"

func GetOneFromBundle(b Bundle, keys ...string) (io.Reader, error) {
	for _, key := range keys {
		r, err := b.Get(key)
		if err == nil {
			return r, nil
		}
		if err == ErrNotFound {
			continue
		}
		return nil, err
	}
	return nil, ErrNotFound
}
