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
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var (
	errInvalidField = errors.New("invalid field")
	errNil          = errors.New("nil")
)

type fieldValueGetter func(value reflect.Value) (reflect.Value, error)
type compileResult struct {
	fieldValueGetter
	error
}

var (
	cachedFieldValueGetter = map[reflect.Type]map[string]compileResult{}
	mu                     sync.RWMutex
)

func getFieldValueGetter(valueType reflect.Type, fieldKey string) (fieldValueGetter, error) {
	fieldKey = strings.TrimSpace(fieldKey)

	mu.RLock()

	if getters, ok := cachedFieldValueGetter[valueType]; ok {
		if g, ok := getters[fieldKey]; ok {
			defer mu.RUnlock()
			return g.fieldValueGetter, g.error
		}
	}

	// Unlock read
	mu.RUnlock()

	// Lock write
	mu.Lock()
	defer mu.Unlock()

	getters, ok := cachedFieldValueGetter[valueType]
	if !ok {
		getters = make(map[string]compileResult)
		cachedFieldValueGetter[valueType] = getters
	}

	if g, ok := getters[fieldKey]; ok {
		return g.fieldValueGetter, g.error
	}

	getter, err := newFieldValueGetter(valueType, fieldKey)
	getters[fieldKey] = compileResult{fieldValueGetter: getter, error: err}
	return getter, err
}

func newFieldPath(valueType reflect.Type, partKeys []string) ([]intstr.IntOrString, error) {
	// Search field by JSON tag.
	fieldPath := make([]intstr.IntOrString, 0)
	if valueType.Kind() == reflect.Ptr {
		valueType = valueType.Elem()
	}

	for partKeyIndex, partKey := range partKeys {
		switch valueType.Kind() {
		case reflect.Map:
			keyType := valueType.Key()
			if keyType.Kind() != reflect.String {
				return nil, errInvalidField
			}

			fieldPath = append(fieldPath, intstr.FromString(partKey))

			valueType = valueType.Elem()
			if valueType.Kind() == reflect.Ptr {
				valueType = valueType.Elem()
			}
		case reflect.Struct:
			found := false
			for i := 0; i < valueType.NumField(); i++ {
				field := valueType.Field(i)
				tag := field.Tag.Get("json")
				tagParts := strings.Split(tag, ",")
				nameTag := tagParts[0]
				if len(nameTag) > 0 {
					if nameTag == partKey {
						fieldPath = append(fieldPath, intstr.FromInt(i))

						// update next type.
						valueType = field.Type
						if valueType.Kind() == reflect.Ptr {
							valueType = valueType.Elem()
						}
						found = true
						break
					}
				} else {
					// Handle if inline.
					inlineValueType := field.Type

					if len(tagParts) == 2 && tagParts[1] == "inline" {
						subFieldPath, err := newFieldPath(inlineValueType, partKeys[partKeyIndex:])
						if err == nil {
							fieldPath = append(fieldPath, intstr.FromInt(i))
							fieldPath = append(fieldPath, subFieldPath...)
							return fieldPath, nil
						} else {
							if err == errInvalidField {
								continue
							}
							return nil, err
						}
					}

					// Ignore if not inline.
				}
			}

			if !found {
				// Such field not found.
				return nil, errInvalidField
			}
		default:
			return nil, errInvalidField
		}
	}

	return fieldPath, nil
}

func newFieldValueGetter(valueType reflect.Type, fieldKey string) (fieldValueGetter, error) {
	if len(fieldKey) == 0 {
		return nil, errors.New("invalid field key")
	}

	partKeys := strings.Split(fieldKey, ".")
	if len(partKeys) == 0 {
		return nil, errors.New("invalid field key")
	}

	for _, partKey := range partKeys {
		if len(partKey) == 0 {
			return nil, errors.New("invalid field key")
		}
	}

	fieldPath, err := newFieldPath(valueType, partKeys)
	if err != nil {
		return nil, err
	}

	// Get the value following the path.
	getter := func(value reflect.Value) (reflect.Value, error) {
		for _, i := range fieldPath {
			if value.Kind() == reflect.Ptr {
				if value.IsNil() {
					return reflect.Value{}, errNil
				}
				value = value.Elem()
			}
			switch i.Type {
			case intstr.Int:
				value = value.Field(i.IntValue())
			case intstr.String:
				value = value.MapIndex(reflect.ValueOf(i.String()))
				if !value.IsValid() {
					return reflect.Value{}, errNil
				}
			}
		}
		if value.Kind() == reflect.Ptr {
			if value.IsNil() {
				return reflect.Value{}, errNil
			}
		}
		return value, nil
	}

	return getter, nil
}

func stringValueOfField(value reflect.Value, fieldKey string) (string, error) {
	fieldGetter, err := getFieldValueGetter(value.Type(), fieldKey)
	if err != nil {
		return "", err
	}

	value, err = fieldGetter(value)
	if err != nil {
		return "", err
	}

	if value.Kind() == reflect.Ptr {
		// Must not be nil
		value = value.Elem()
	}

	// Found filed, try extract value.
	switch value.Kind() {
	case reflect.String:
		return value.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatInt(value.Int(), 10), nil
	case reflect.Bool:
		return strconv.FormatBool(value.Bool()), nil
	default:
		return "", errInvalidField
	}
}

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
	value := reflect.ValueOf(obj)
	strVal, err := stringValueOfField(value, req.Key)
	if err != nil {
		if err == errNil {
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
