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
	"bytes"
	"errors"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/alibaba/polardbx-operator/pkg/util/json"
)

func isCompareRequirementMatches(val string, req *corev1.NodeSelectorRequirement, compare func(a, b int64) bool) (bool, error) {
	if len(req.Values) != 1 {
		return false, errors.New("invalid requirement, compare requires value length to be 1")
	}

	intVal, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return false, errors.New("compare field isn't an integer")
	}
	requiredVal, err := strconv.ParseInt(req.Values[0], 10, 64)
	if err != nil {
		return false, errors.New("requirement value isn't an integer")
	}

	return compare(intVal, requiredVal), nil
}

func IsFieldsMatchesRequirement(obj interface{}, req *corev1.NodeSelectorRequirement) (bool, error) {
	strVal, err := json.GetStringValueOfFieldByJsonKey(obj, req.Key)
	if err != nil {
		if err == json.ErrNilOrNotExists {
			if req.Operator == corev1.NodeSelectorOpDoesNotExist {
				return true, nil
			}
			return false, nil
		}
		return false, err
	}

	switch req.Operator {
	case corev1.NodeSelectorOpIn:
		for _, v := range req.Values {
			if strVal == v {
				return true, nil
			}
		}
		return false, nil
	case corev1.NodeSelectorOpNotIn:
		for _, v := range req.Values {
			if strVal == v {
				return false, nil
			}
		}
		return true, nil
	case corev1.NodeSelectorOpExists:
		return true, nil
	case corev1.NodeSelectorOpDoesNotExist:
		return false, nil
	case corev1.NodeSelectorOpGt:
		return isCompareRequirementMatches(strVal, req, func(a, b int64) bool {
			return a > b
		})
	case corev1.NodeSelectorOpLt:
		return isCompareRequirementMatches(strVal, req, func(a, b int64) bool {
			return a < b
		})
	default:
		return false, errors.New("invalid selector operator")
	}
}

func IsLabelsMatchesRequirement(labels map[string]string, req *corev1.NodeSelectorRequirement) (bool, error) {
	if req == nil {
		return true, nil
	}
	if req.Key == "" {
		return false, errors.New("invalid selector, empty key")
	}
	val, exists := labels[req.Key]
	switch req.Operator {
	case corev1.NodeSelectorOpIn:
		for _, v := range req.Values {
			if exists && val == v {
				return true, nil
			}
		}
		return false, nil
	case corev1.NodeSelectorOpNotIn:
		for _, v := range req.Values {
			if exists && val == v {
				return false, nil
			}
		}
		return exists, nil
	case corev1.NodeSelectorOpDoesNotExist:
		return !exists, nil
	case corev1.NodeSelectorOpExists:
		return exists, nil
	default:
		return false, errors.New("invalid selector operator")
	}
}

func IsNodeMatchesTerm(node *corev1.Node, term *corev1.NodeSelectorTerm) (bool, error) {
	if term == nil {
		return true, nil
	}

	for _, req := range term.MatchExpressions {
		ok, err := IsLabelsMatchesRequirement(node.Labels, &req)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}

	for _, req := range term.MatchFields {
		ok, err := IsFieldsMatchesRequirement(node, &req)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}

	return true, nil
}

func IsNodeMatches(node *corev1.Node, nodeSelector *corev1.NodeSelector) (bool, error) {
	if nodeSelector == nil || len(nodeSelector.NodeSelectorTerms) == 0 {
		return true, nil
	}

	for _, term := range nodeSelector.NodeSelectorTerms {
		ok, err := IsNodeMatchesTerm(node, &term)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func isNodeSelectorTermEmpty(t *corev1.NodeSelectorTerm) bool {
	if t == nil || (len(t.MatchExpressions) == 0 && len(t.MatchFields) == 0) {
		return true
	}
	return false
}

func isNodeSelectorEmpty(s *corev1.NodeSelector) bool {
	if s == nil || len(s.NodeSelectorTerms) == 0 {
		return true
	}

	for _, term := range s.NodeSelectorTerms {
		if !isNodeSelectorTermEmpty(&term) {
			return false
		}
	}
	return true
}

func stableRequirementStr(r *corev1.NodeSelectorRequirement) string {
	buf := &bytes.Buffer{}

	buf.WriteString(r.Key)
	buf.WriteString(",")
	buf.WriteString(string(r.Operator))

	if r.Values != nil {
		buf.WriteString(",")
		valuesC := make([]string, len(r.Values))
		copy(valuesC, r.Values)
		sort.Strings(valuesC)
		for i := range valuesC {
			valuesC[i] = strings.ReplaceAll(valuesC[i], "|", "\\|")
		}
		buf.WriteString(strings.Join(valuesC, "|"))
	}

	return buf.String()
}

type nodeSelectorRequirementSet map[string]*corev1.NodeSelectorRequirement

func (s nodeSelectorRequirementSet) covers(o nodeSelectorRequirementSet) bool {
	for k := range s {
		_, ok := o[k]
		if !ok {
			return false
		}
	}
	return true
}

func buildStableRequirementSet(a []corev1.NodeSelectorRequirement) nodeSelectorRequirementSet {
	m := make(nodeSelectorRequirementSet)
	for i := range a {
		r := &a[i]
		m[stableRequirementStr(r)] = r
	}
	return m
}

func doesRequirementsCovers(a, b []corev1.NodeSelectorRequirement) bool {
	as := buildStableRequirementSet(a)
	bs := buildStableRequirementSet(b)
	return as.covers(bs)
}

// DoesNodeSelectorTermCoversAnother determines if the first node selector term covers (or equals to)
// the second in pure logic.
func DoesNodeSelectorTermCoversAnother(a, b *corev1.NodeSelectorTerm) bool {
	// a covers b means that any requirement in b's stricter than that in a's.
	// This method simplifies the comparison by only determining if any requirement in
	// a exists in b for both fields and labels.

	if isNodeSelectorTermEmpty(a) {
		return true
	}

	if isNodeSelectorTermEmpty(b) {
		return false
	}

	return doesRequirementsCovers(a.MatchFields, b.MatchFields) && doesRequirementsCovers(a.MatchExpressions, b.MatchExpressions)
}

// DoesNodeSelectorCoversAnother determines if the first node selector covers (or equals to)
// the second in pure logic. Important for testing when generating pod's affinity.
func DoesNodeSelectorCoversAnother(s, t *corev1.NodeSelector) bool {
	// Empty s only covers any.
	if isNodeSelectorEmpty(s) {
		return true
	}

	// Non-empty s never covers empty.
	if isNodeSelectorEmpty(t) {
		return false
	}

	// Stricter than logical. But simpler.
	// Any term in t is in some term in s.
	for _, tt := range t.NodeSelectorTerms {
		r := false
		for _, ts := range s.NodeSelectorTerms {
			if DoesNodeSelectorTermCoversAnother(&ts, &tt) {
				r = true
				break
			}
		}
		if !r {
			return false
		}
	}
	return true
}
