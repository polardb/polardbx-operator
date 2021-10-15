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

func patchStringMap(origin, patch map[string]string, overwrite bool) map[string]string {
	if origin == nil {
		return patch
	}
	if patch == nil {
		return origin
	}
	for k, v := range patch {
		_, ok := origin[k]
		if overwrite || !ok {
			origin[k] = v
		}
	}
	return origin
}

func patchStringMaps(origin map[string]string, overwrite bool, patches ...map[string]string) map[string]string {
	for _, patch := range patches {
		origin = patchStringMap(origin, patch, overwrite)
	}
	return origin
}

func DeepCopyStrMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	c := make(map[string]string)
	for k, v := range m {
		c[k] = v
	}
	return c
}

func PatchLabels(labels map[string]string, patches ...map[string]string) map[string]string {
	return patchStringMaps(labels, true, patches...)
}

func PatchAnnotations(annotations map[string]string, patches ...map[string]string) map[string]string {
	return patchStringMaps(annotations, true, patches...)
}
