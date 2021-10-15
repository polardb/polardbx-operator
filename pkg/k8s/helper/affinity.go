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

package helper

import (
	"reflect"

	corev1 "k8s.io/api/core/v1"
)

const LabelNodeRole = "kubernetes.io/role"

const (
	NodeRoleMaster = "master"
)

func patchAffinity(t, p interface{}) interface{} {
	tType, pType := reflect.TypeOf(t), reflect.TypeOf(p)

	if tType != pType {
		panic("types must be the same, but one is " + tType.Name() + ", another is " + pType.Name())
	}

	tValue, pValue := reflect.ValueOf(t), reflect.ValueOf(p)
	switch tType.Kind() {
	case reflect.Slice:
		if tValue.IsNil() {
			return pValue.Interface()
		} else if !pValue.IsNil() {
			return reflect.AppendSlice(tValue, pValue).Interface()
		}
	case reflect.Map:
		if tValue.IsNil() {
			return pValue.Interface()
		} else if !pValue.IsNil() {
			for _, key := range pValue.MapKeys() {
				tValue.SetMapIndex(key, pValue.MapIndex(key))
			}
			return tValue.Interface()
		}
	case reflect.Ptr:
		if tValue.IsNil() {
			return pValue.Interface()
		} else if !pValue.IsNil() {
			elemType := tType.Elem()
			switch elemType.Kind() {
			case reflect.Struct:
				for i := 0; i < elemType.NumField(); i++ {
					tValue.Elem().Field(i).Set(reflect.ValueOf(patchAffinity(tValue.Elem().Field(i).Interface(),
						pValue.Elem().Field(i).Interface())))
				}
			case reflect.Slice, reflect.Array, reflect.Chan, reflect.Map, reflect.Ptr:
				panic("pointer type of slice, array, chan, map and pointer is not patchable")
			default:
				tValue.Elem().Set(pValue.Elem())
			}
			return tValue.Interface()
		}
	default:
		panic("type kind not patchable")
	}

	return t
}

// PatchAffinity is only valid for pod (anti) affinity. Node affinity uses OR among different terms.
func PatchAffinity(affinity *corev1.Affinity, patches ...*corev1.Affinity) *corev1.Affinity {
	t := interface{}(affinity)

	for _, p := range patches {
		if p == nil {
			continue
		}
		t = patchAffinity(t, p)
	}

	return t.(*corev1.Affinity)
}
