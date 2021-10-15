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

package dict

import (
	"reflect"
	"sort"
)

func Keys(m interface{}) interface{} {
	mapValue := reflect.ValueOf(m)
	if mapValue.Kind() != reflect.Map {
		panic("not a map")
	}

	keySlice := reflect.MakeSlice(reflect.SliceOf(mapValue.Type().Key()), mapValue.Len(), mapValue.Len())
	for i, keyValue := range mapValue.MapKeys() {
		keySlice.Index(i).Set(keyValue)
	}
	return keySlice.Interface()
}

func StringKeys(m interface{}) []string {
	return Keys(m).([]string)
}

func SortedStringKeys(m interface{}) []string {
	keys := StringKeys(m)
	sort.Strings(keys)
	return keys
}

func IntKeys(m interface{}) []int {
	return Keys(m).([]int)
}

func SortedIntKeys(m interface{}) []int {
	keys := IntKeys(m)
	sort.Ints(keys)
	return keys
}

func DiffStringMap(newMap, oldMap map[string]string) map[string]string {
	if oldMap == nil || newMap == nil {
		return newMap
	}

	diff := make(map[string]string)

	for k, vNew := range newMap {
		if vOld, ok := oldMap[k]; !ok || vOld != vNew {
			diff[k] = vNew
		}
	}

	return diff
}

func MergeStringMap(dst, src map[string]string) map[string]string {
	if dst == nil {
		return src
	}
	if src == nil {
		return dst
	}

	for k, v := range src {
		dst[k] = v
	}
	return dst
}
