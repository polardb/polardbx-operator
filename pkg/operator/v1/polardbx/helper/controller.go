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
	"strings"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
)

func IsOperatorHintFound(polardbx *polardbxv1.PolarDBXCluster, hint string) bool {
	hints := strings.Split(polardbx.Annotations[polardbxmeta.AnnotationControllerHints], ",")

	for _, h := range hints {
		if hint == strings.TrimSpace(h) {
			return true
		}
	}
	return false
}
