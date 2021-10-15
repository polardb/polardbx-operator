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
	"strconv"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newPodSlice(size int) []corev1.Pod {
	pods := make([]corev1.Pod, 0)
	for i := 0; i < size; i++ {
		pods = append(pods, corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-" + strconv.Itoa(i),
			},
		})
	}
	return pods
}

func TestToObjectInterfaces(t *testing.T) {
	pods := newPodSlice(10)
	objects := ToObjectInterfaces(pods)
	if len(pods) != len(objects) {
		t.Fatal()
	}
	for i, obj := range objects {
		podRef := obj.(*corev1.Pod)
		if podRef != &pods[i] {
			t.Fatal()
		}
	}
}

func TestToObjectNames(t *testing.T) {
	pods := newPodSlice(10)
	names := ToObjectNames(pods)
	if len(pods) != len(names) {
		t.Fatal()
	}
	for i, name := range names {
		if name != pods[i].Name {
			t.Fatal()
		}
	}
}
