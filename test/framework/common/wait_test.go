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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func Test_IsObjectListEmpty(t *testing.T) {
	testcases := map[string]struct {
		list   client.ObjectList
		expect bool
	}{
		"pod-list-not-empty": {
			list: &corev1.PodList{
				Items: []corev1.Pod{{}},
			},
			expect: false,
		},
		"pod-list-empty": {
			list:   &corev1.PodList{},
			expect: true,
		},
		"service-list-empty": {
			list:   &corev1.ServiceList{},
			expect: true,
		},
		"service-list-not-empty": {
			list: &corev1.ServiceList{
				Items: []corev1.Service{{}},
			},
			expect: false,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			r, err := isObjectListEmpty(tc.list)
			if err != nil {
				t.Fatal("unexpected error: ", err)
			}
			if r != tc.expect {
				t.Fatalf("expect %v, but is %v", tc.expect, r)
			}
		})
	}
}
