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
	"context"
	"errors"
	"reflect"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/alibaba/polardbx-operator/pkg/util/json"
)

func WaitForObjectToDisappear(c client.Client, name, namespace string, poll, timeout time.Duration, obj client.Object) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return wait.PollImmediate(poll, timeout, func() (bool, error) {
		err := c.Get(ctx, types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		}, obj)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return true, err // stop wait with error
		}
		return false, nil
	})
}

func isObjectListEmpty(objList client.ObjectList) (bool, error) {
	if objList == nil {
		return false, nil
	}

	items, err := json.GetFieldValueByJsonKey(objList, "items")
	if err != nil {
		return false, err
	}
	v := reflect.ValueOf(items)
	if v.Kind() != reflect.Slice {
		return false, errors.New("items not slice")
	}
	return v.IsNil() || v.Len() == 0, nil
}

func WaitForObjectsWithLabelsToDisappear(c client.Client, labels map[string]string, namespace string, poll, timeout time.Duration, objList client.ObjectList) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return wait.PollImmediate(poll, timeout, func() (bool, error) {
		err := c.List(ctx, objList, client.InNamespace(namespace), client.MatchingLabels(labels))
		if err != nil {
			return true, err // stop wait with error
		}
		return isObjectListEmpty(objList)
	})
}
