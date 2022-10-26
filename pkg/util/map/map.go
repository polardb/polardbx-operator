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

package _map

import (
	"fmt"
	"reflect"
)

func Equals(m1, m2 *map[string]interface{}) bool {
	if SizeOf(m1) != SizeOf(m2) {
		return false
	}
	if m1 != nil && m2 != nil && len(*m1) == len(*m2) {
		for key1, val1 := range *m1 {
			val2, ok := (*m2)[key1]
			if !ok {
				return false
			}
			if val2 != val1 {
				return false
			}
		}
		return true
	}
	return false
}

func SizeOf(m *map[string]interface{}) int {
	size := 0
	if m != nil {
		size = len(*m)
	}
	return size
}

func MergeMap(target interface{}, source interface{}, overwrite bool) interface{} {
	if target == nil || source == nil {
		panic(fmt.Sprintf("nil value, isTargetNil %v isSourceNil %v", target == nil, source == nil))
	}
	targetMap := reflect.ValueOf(target)
	sourceMap := reflect.ValueOf(source)
	if targetMap.Kind() == reflect.Map && sourceMap.Kind() == reflect.Map {
		for _, mapKey := range sourceMap.MapKeys() {
			targetMapValue := targetMap.MapIndex(mapKey)
			if targetMapValue.IsValid() && !overwrite {
				panic("overwriting key is not allowed")
			}
			soureMapValue := sourceMap.MapIndex(mapKey)
			targetMap.SetMapIndex(mapKey, soureMapValue)
		}
	}
	return target
}
