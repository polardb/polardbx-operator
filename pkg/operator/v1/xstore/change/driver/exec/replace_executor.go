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

package exec

import (
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

type ReplaceExec struct {
	baseExec
}

func (exec *ReplaceExec) Execute(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
	// TODO implement me
	panic("implement me")
}
