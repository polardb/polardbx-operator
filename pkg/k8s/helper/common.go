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

	"sigs.k8s.io/controller-runtime/pkg/client"
)

func ToObjectInterfaces(slice interface{}) []client.Object {
	rv := reflect.ValueOf(slice)
	if rv.Kind() != reflect.Slice {
		panic(reflect.ValueError{
			Method: "ToObjectInterfaces",
			Kind:   rv.Kind(),
		})
	}

	if rv.IsNil() {
		return nil
	}

	length := rv.Len()
	result := make([]client.Object, 0, length)
	for i := 0; i < length; i++ {
		object := rv.Index(i)
		if object.Kind() != reflect.Ptr {
			result = append(result, object.Addr().Interface().(client.Object))
		} else {
			result = append(result, object.Interface().(client.Object))
		}
	}

	return result
}

func ToObjectNames(slice interface{}) []string {
	objects := ToObjectInterfaces(slice)
	names := make([]string, 0, len(objects))
	for _, obj := range objects {
		names = append(names, obj.GetName())
	}
	return names
}

func ToObjectNameSet(slice interface{}) map[string]struct{} {
	objects := ToObjectInterfaces(slice)
	names := make(map[string]struct{})
	for _, obj := range objects {
		names[obj.GetName()] = struct{}{}
	}
	return names
}
