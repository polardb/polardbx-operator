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

package guide

import (
	"errors"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func newNode(cpu int64, memory int64) *corev1.Node {
	return &corev1.Node{
		Status: corev1.NodeStatus{
			Capacity: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU:    *resource.NewQuantity(cpu, resource.DecimalSI),
				corev1.ResourceMemory: *resource.NewQuantity(memory*GigaByte, resource.BinarySI),
			},
		},
	}
}

func TestTopologyModeFactory_computeResourcesForMinimumMode(t *testing.T) {
	factory := NewTopologyModeFactory(nil)
	testcases := map[string]struct {
		nodeList *corev1.NodeList
		err      error
		config   map[nodeType]*nodeConfig
	}{
		"nodes-less-than-three": {
			nodeList: &corev1.NodeList{
				Items: []corev1.Node{
					*newNode(16, 64),
					*newNode(16, 32),
					*newNode(8, 64),
				},
			},
			config: nil,
			err:    errors.New("performance mode is not supported for nodes with different resources"),
		},
		"nodes-with-different-resources": {
			nodeList: &corev1.NodeList{},
			config:   nil,
			err:      errors.New("nodes insufficient for performance mode"),
		},
		"normal-16c64g": {
			nodeList: &corev1.NodeList{
				Items: []corev1.Node{
					*newNode(16, 64),
					*newNode(16, 64),
					*newNode(16, 64),
				},
			},
			config: map[nodeType]*nodeConfig{
				CN: {
					replica:  3,
					resource: factory.newResourceRequirements(4, 23957864448),
				},
				DN: {
					replica:      3,
					paxosReplica: 3,
					resource:     factory.newResourceRequirements(3, 15971909632),
				},
				GMS: {
					replica:      1,
					paxosReplica: 3,
					resource:     factory.newResourceRequirements(4, 8*GigaByte, 1, GigaByte),
				},
			},
			err: nil,
		},
		"normal-32c256g": {
			nodeList: &corev1.NodeList{
				Items: []corev1.Node{
					*newNode(32, 256),
					*newNode(32, 256),
					*newNode(32, 256),
				},
			},
			config: map[nodeType]*nodeConfig{
				CN: {
					replica:  3,
					resource: factory.newResourceRequirements(10, 101267275776),
				},
				DN: {
					replica:      3,
					paxosReplica: 3,
					resource:     factory.newResourceRequirements(7, 67511517184),
				},
				GMS: {
					replica:      1,
					paxosReplica: 3,
					resource:     factory.newResourceRequirements(4, 8*GigaByte, 1, GigaByte),
				},
			},
			err: nil,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			config, err := factory.computeResourcesForPerformanceMode(tc.nodeList)
			if err != nil {
				if tc.err == nil {
					t.Fatal("unexpected error", err)
				}
				if err.Error() != tc.err.Error() {
					t.Fatal("unexpected error", err)
				}
			}
			if !reflect.DeepEqual(config, tc.config) {
				t.Fatal("unexpected config")
			}
		})
	}
}
