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

func OneKey[K comparable, V any](m map[K]V) K {
	for k := range m {
		return k
	}
	var k K
	return k
}

func MapKeys[K comparable, V any](m map[K]V) []K {
	if m == nil {
		return nil
	}

	i := 0
	keys := make([]K, len(m))
	for k := range m {
		keys[i] = k
		i++
	}
	return keys
}
