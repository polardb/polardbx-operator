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

package polardbxparameter

import (
	"time"

	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/test/framework"
	"github.com/alibaba/polardbx-operator/test/framework/log"
	pxmframework "github.com/alibaba/polardbx-operator/test/framework/polardbxparameter"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func CreatePolarDBXParameterAndWaitUntilRunningOrFail(f *framework.Framework, polardbxparameter *polardbxv1.PolarDBXParameter,
	timeout time.Duration) {
	log.Logf("Starting to create a PolarDBXParameter for test: %s ...", polardbxparameter.Name)

	// Create the given polardbx cluster.
	err := f.Client.Create(f.Ctx, polardbxparameter)
	framework.ExpectNoError(err)

	// Wait until in modifying status.
	pxmframework.WaitPolarDBXParameterInPhase(f.Client,
		polardbxparameter.Name, polardbxparameter.Namespace,
		[]polardbxv1polardbx.ParameterPhase{
			polardbxv1polardbx.ParameterStatusFinished,
		},
		timeout)
	framework.ExpectNoError(err)
}

func CreatePolarDBXParameterTemplate(f *framework.Framework, polardbxparametertemplate *polardbxv1.PolarDBXParameterTemplate,
	timeout time.Duration) {
	log.Logf("Starting to create a PolarDBXParameterTemplate for test: %s ...", polardbxparametertemplate.Name)

	// Create the given polardbx cluster.
	err := f.Client.Create(f.Ctx, polardbxparametertemplate)
	framework.ExpectNoError(err)
}

func DeletePolarDBXParameterAndWaitUntilItDisappear(f *framework.Framework, polardbxparameter *polardbxv1.PolarDBXParameter, timeout time.Duration) {
	log.Logf("Cleaning PolarDBXParameter: %s, should complete in 1 minute...", polardbxparameter.Name)

	err := f.Client.Delete(f.Ctx, polardbxparameter, client.PropagationPolicy(metav1.DeletePropagationBackground))
	if !apierrors.IsNotFound(err) {
		framework.ExpectNoError(err, "cleanup failed")
	}
	err = pxmframework.WaitForPolarDBXParameterToDisappear(f.Client, polardbxparameter.Name, polardbxparameter.Namespace, timeout)

	framework.ExpectNoError(err, "failed to wait for PolarDBXParameter object to disappear")
}

func DeletePolarDBXParameterTemplateAndWaitUntilItDisappear(f *framework.Framework, polardbxparametertemplate *polardbxv1.PolarDBXParameterTemplate, timeout time.Duration) {
	log.Logf("Cleaning PolarDBXParameter: %s, should complete in 1 minute...", polardbxparametertemplate.Name)

	err := f.Client.Delete(f.Ctx, polardbxparametertemplate, client.PropagationPolicy(metav1.DeletePropagationBackground))
	if !apierrors.IsNotFound(err) {
		framework.ExpectNoError(err, "cleanup failed")
	}
	err = pxmframework.WaitForPolarDBXParameterTemplateToDisappear(f.Client, polardbxparametertemplate.Name, polardbxparametertemplate.Namespace, timeout)

	framework.ExpectNoError(err, "failed to wait for PolarDBXParameterTemplate object to disappear")
}
