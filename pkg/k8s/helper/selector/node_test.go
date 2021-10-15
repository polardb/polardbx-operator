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

package selector

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var node = &corev1.Node{
	TypeMeta: metav1.TypeMeta{
		Kind: "Node",
	},
	ObjectMeta: metav1.ObjectMeta{
		Name: "test-node",
		Labels: map[string]string{
			"a": "a",
		},
		Generation: 10,
	},
	Status: corev1.NodeStatus{
		Phase: corev1.NodeRunning,
		NodeInfo: corev1.NodeSystemInfo{
			KernelVersion: "3.19",
		},
		Config: nil,
	},
}

func BenchmarkGetStringValueOfField(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = stringValueOfField(reflect.ValueOf(node), "metadata.name")
	}
}

func TestIsFieldsMatchesRequirement(t *testing.T) {
	testcases := map[string]struct {
		requirement *corev1.NodeSelectorRequirement
		expected    bool
		err         error
	}{
		"node-name-should-match": {
			requirement: &corev1.NodeSelectorRequirement{
				Key:      "metadata.name",
				Operator: "In",
				Values: []string{
					"test-node",
				},
			},
			expected: true,
		},
		"node-name-should-not-match": {
			requirement: &corev1.NodeSelectorRequirement{
				Key:      "metadata.name",
				Operator: "In",
				Values:   []string{},
			},
			expected: false,
		},
		"node-label-exists-should-match": {
			requirement: &corev1.NodeSelectorRequirement{
				Key:      "metadata.labels.a",
				Operator: "Exists",
			},
			expected: true,
		},
		"node-label-exists-should-not-match": {
			requirement: &corev1.NodeSelectorRequirement{
				Key:      "metadata.labels.a",
				Operator: "DoesNotExist",
			},
			expected: false,
		},
		"node-label-not-exists-should-not-match": {
			requirement: &corev1.NodeSelectorRequirement{
				Key:      "metadata.labels.b",
				Operator: "Exists",
			},
			expected: false,
		},
		"node-label-not-exists-should-match": {
			requirement: &corev1.NodeSelectorRequirement{
				Key:      "metadata.labels.b",
				Operator: "DoesNotExist",
			},
			expected: true,
		},
		"node-generation-gt-9-should-match": {
			requirement: &corev1.NodeSelectorRequirement{
				Key:      "metadata.generation",
				Operator: "Gt",
				Values:   []string{"9"},
			},
			expected: true,
		},
		"node-generation-lt-11-should-match": {
			requirement: &corev1.NodeSelectorRequirement{
				Key:      "metadata.generation",
				Operator: "Lt",
				Values:   []string{"11"},
			},
			expected: true,
		},
		"node-generation-gt-10-should-not-match": {
			requirement: &corev1.NodeSelectorRequirement{
				Key:      "metadata.generation",
				Operator: "Gt",
				Values:   []string{"10"},
			},
			expected: false,
		},
		"node-generation-lt-9-should-not-match": {
			requirement: &corev1.NodeSelectorRequirement{
				Key:      "metadata.generation",
				Operator: "Lt",
				Values:   []string{"9"},
			},
			expected: false,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)

			matches, err := IsFieldsMatchesRequirement(node, tc.requirement)
			if err != nil {
				g.Expect(err).To(gomega.Equal(tc.err))
			} else {
				g.Expect(matches).To(gomega.Equal(tc.expected))
			}
		})
	}
}

func TestGetStringValueOfField(t *testing.T) {
	testcases := map[string]struct {
		node        *corev1.Node
		labelKey    string
		expected    string
		expectedErr error
	}{
		"empty-node": {
			node:        nil,
			labelKey:    "metadata.name",
			expectedErr: errNil,
		},
		"inline-kind": {
			node:     node,
			labelKey: "kind",
			expected: "Node",
		},
		"metadata.name": {
			node:     node,
			expected: "test-node",
		},
		"metadata.labels.a": {
			node:     node,
			expected: "a",
		},
		"metadata.labels.b": {
			node:        node,
			expectedErr: errNil,
		},
		"metadata.namespace": {
			node:     node,
			expected: "",
		},
		"status.phase": {
			node:     node,
			expected: "Running",
		},
		"status.nodeInfo.kernelVersion": {
			node:     node,
			expected: "3.19",
		},
		"status.config": {
			node:        node,
			expectedErr: errNil,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			labelKey := tc.labelKey
			if len(labelKey) == 0 {
				labelKey = name
			}
			val, err := stringValueOfField(reflect.ValueOf(tc.node), labelKey)
			if err != nil {
				if err == errInvalidField {
					fmt.Println("not a valid field: " + labelKey)
				} else if err == errNil {
					fmt.Println("field not found: " + labelKey)
				} else {
					fmt.Println("unexpected error: " + err.Error())
				}
				g.Expect(err).To(gomega.Equal(tc.expectedErr))
			} else {
				fmt.Printf("value of field \"%s\": %s\n", labelKey, val)
				g.Expect(val).To(gomega.Equal(tc.expected))
			}
		})
	}
}

func TestIsNodeMatches(t *testing.T) {
	testcases := map[string]struct {
		nodeSelector *corev1.NodeSelector
		expected     bool
	}{
		"label-not-match-should-not-match": {
			expected: false,
			nodeSelector: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      "b",
								Operator: corev1.NodeSelectorOpExists,
							},
						},
					},
				},
			},
		},
		"label-match-should-match": {
			expected: true,
			nodeSelector: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      "a",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{"a"},
							},
						},
					},
				},
			},
		},
		"label-not-match-but-name-match-should-match": {
			expected: true,
			nodeSelector: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      "b",
								Operator: corev1.NodeSelectorOpExists,
							},
						},
					},
					{
						MatchFields: []corev1.NodeSelectorRequirement{
							{
								Key:      "metadata.name",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{"test-node"},
							},
						},
					},
				},
			},
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)

			matches, err := IsNodeMatches(node, tc.nodeSelector)
			g.Expect(err).Should(gomega.BeNil())
			g.Expect(matches).To(gomega.Equal(tc.expected))
		})
	}
}
