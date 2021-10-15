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
	"errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func CheckControllerReference(obj, owner metav1.Object) error {
	ref := metav1.GetControllerOfNoCopy(obj)
	if ref == nil {
		return errors.New("no owner found for " + obj.GetNamespace() + "/" + obj.GetName())
	}
	if ref.UID == owner.GetUID() {
		return nil
	}
	return errors.New(string("unexpected owner reference, expect " + owner.GetUID() + ", found " + ref.UID))
}
