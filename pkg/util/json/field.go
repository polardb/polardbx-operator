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

package json

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/util/intstr"
)

var (
	ErrInvalidField   = errors.New("invalid field")
	ErrNilOrNotExists = errors.New("nil")
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
				return nil, ErrInvalidField
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
							if err == ErrInvalidField {
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
				return nil, ErrInvalidField
			}
		default:
			return nil, ErrInvalidField
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
					return reflect.Value{}, ErrNilOrNotExists
				}
				value = value.Elem()
			}
			switch i.Type {
			case intstr.Int:
				value = value.Field(i.IntValue())
			case intstr.String:
				value = value.MapIndex(reflect.ValueOf(i.String()))
				if !value.IsValid() {
					return reflect.Value{}, ErrNilOrNotExists
				}
			}
		}
		if value.Kind() == reflect.Ptr {
			if value.IsNil() {
				return reflect.Value{}, ErrNilOrNotExists
			}
		}
		return value, nil
	}

	return getter, nil
}

func getFieldValueByJsonKey(value reflect.Value, key string) (reflect.Value, error) {
	g, err := getFieldValueGetter(value.Type(), key)
	if err != nil {
		return reflect.Value{}, err
	}
	return g(value)
}

func GetFieldValueByJsonKey(obj interface{}, key string) (interface{}, error) {
	v, err := getFieldValueByJsonKey(reflect.ValueOf(obj), key)
	if err != nil {
		return nil, err
	}
	return v.Interface(), nil
}

func GetStringValueOfFieldByJsonKey(obj interface{}, fieldKey string) (string, error) {
	value, err := getFieldValueByJsonKey(reflect.ValueOf(obj), fieldKey)
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
		return "", ErrInvalidField
	}
}
