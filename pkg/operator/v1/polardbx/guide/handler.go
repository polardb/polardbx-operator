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
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
)

type Handler interface {
	Handle(rc *polardbxv1reconcile.Context, obj *polardbxv1.PolarDBXCluster) error
}

var (
	handlers = map[string]Handler{}
)

func registerHandler(annotation string, handler Handler) {
	handlers[annotation] = handler
}

func HandleGuides(rc *polardbxv1reconcile.Context, obj *polardbxv1.PolarDBXCluster) (bool, error) {
	guideFound := false
	for annotation, handler := range handlers {
		if _, ok := obj.Annotations[annotation]; ok {
			guideFound = true
			err := handler.Handle(rc, obj)
			if err != nil {
				return false, err
			}
		}
	}
	return guideFound, nil
}
