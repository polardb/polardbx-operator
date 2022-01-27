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

package common

import (
	"reflect"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

func iterateOverObjects(objList interface{}, f func(obj client.Object) bool) {
	if objList == nil {
		return
	}

	v := reflect.ValueOf(objList)
	if v.Kind() != reflect.Slice {
		panic("invalid object list")
	}
	for i := 0; i < v.Len(); i++ {
		// Always get the pointer.
		vi := v.Index(i).Addr()
		vo := vi.Interface().(client.Object)
		if f(vo) {
			return
		}
	}
}

func GetObjectFromObjectList(objList interface{}, name string) client.Object {
	var r client.Object
	iterateOverObjects(objList, func(obj client.Object) bool {
		if obj.GetName() == name {
			r = obj
			return true
		}
		return false
	})
	return r
}

func mapContains(m, t map[string]string) bool {
	if m == nil || len(m) == 0 {
		return t == nil || len(t) == 0
	}
	for k, v := range t {
		mv, ok := m[k]
		if !ok || mv != v {
			return false
		}
	}
	return true
}

func MapObjectsFromObjectListByLabel(objList interface{}, label string) map[string][]client.Object {
	r := make(map[string][]client.Object)
	iterateOverObjects(objList, func(vo client.Object) bool {
		lv := vo.GetLabels()[label]
		if _, ok := r[lv]; !ok {
			r[lv] = []client.Object{vo}
		} else {
			r[lv] = append(r[lv], vo)
		}
		return false
	})
	return r
}

func GetObjectList(objList interface{}) []client.Object {
	return GetObjectsFromObjectListByLabels(objList, nil)
}

func GetObjectsFromObjectListByLabels(objList interface{}, labels map[string]string) []client.Object {
	r := make([]client.Object, 0)
	iterateOverObjects(objList, func(vo client.Object) bool {
		if mapContains(vo.GetLabels(), labels) {
			r = append(r, vo)
		}
		return false
	})
	return r
}

func GetObjectMapFromObjectListByName(objList interface{}) map[string]client.Object {
	r := make(map[string]client.Object)
	iterateOverObjects(objList, func(obj client.Object) bool {
		r[obj.GetName()] = obj
		return false
	})
	return r
}
