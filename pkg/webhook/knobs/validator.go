/*
Copyright 2022 Alibaba Group Holding Limited.

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

package knobs

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/webhook/extension"
)

type Validator struct {
}

func (v *Validator) validateObject(ctx context.Context, obj *polardbxv1.PolarDBXClusterKnobs) error {
	var errList field.ErrorList

	if len(obj.Spec.ClusterName) == 0 {
		errList = append(errList, field.Required(field.NewPath("spec", "clusterName"), "cluster name must be specified"))
	}

	if len(errList) > 0 {
		return apierrors.NewInvalid(obj.GroupVersionKind().GroupKind(), obj.Name, errList)
	}
	return nil
}

func (v *Validator) ValidateCreate(ctx context.Context, obj runtime.Object) error {
	return v.validateObject(ctx, obj.(*polardbxv1.PolarDBXClusterKnobs))
}

func (v *Validator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) error {
	oldKnobs, newKnobs := oldObj.(*polardbxv1.PolarDBXClusterKnobs), newObj.(*polardbxv1.PolarDBXClusterKnobs)
	gvk := oldKnobs.GroupVersionKind()

	if err := v.validateObject(ctx, newKnobs); err != nil {
		return err
	}

	if oldKnobs.Spec.ClusterName != newKnobs.Spec.ClusterName {
		return apierrors.NewForbidden(
			schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, newKnobs.Name,
			field.Forbidden(field.NewPath("spec", "clusterName"), "immutable field"))
	}

	return nil
}

func (v *Validator) ValidateDelete(ctx context.Context, obj runtime.Object) error {
	return nil
}

func NewValidator() extension.CustomValidator {
	return &Validator{}
}
