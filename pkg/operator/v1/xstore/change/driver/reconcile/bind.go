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

package reconcile

import (
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/change/driver/context"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/change/driver/exec"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

func BuildExecutionBlock(ec *context.ExecutionContext) control.BindFunc {
	executors := exec.NewExecutorsForPlan(ec, *ec.Plan)

	binds := make([]control.BindFunc, len(executors))
	for i := range executors {
		executor := executors[i]
		binds[i] = xstorev1reconcile.NewStepBinder(
			fmt.Sprintf("ChangeDriver-Plan-Step-%d-%s", i, executor.Step().Description()),
			func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
				return executor.Execute(rc, flow)
			},
		)
	}
	return control.Block(binds...)
}
