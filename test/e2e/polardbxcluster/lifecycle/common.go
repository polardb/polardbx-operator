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

package lifecycle

import (
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/test/framework"
	"github.com/alibaba/polardbx-operator/test/framework/log"
	pxcframework "github.com/alibaba/polardbx-operator/test/framework/polardbxcluster"
)

func DeletePolarDBXClusterAndWaitUntilItDisappear(f *framework.Framework, polardbxcluster *polardbxv1.PolarDBXCluster, timeout time.Duration) {
	log.Logf("Cleaning PolarDB-X cluster: %s, should complete in 1 minute...", polardbxcluster.Name)

	err := f.Client.Delete(f.Ctx, polardbxcluster, client.PropagationPolicy(metav1.DeletePropagationBackground))
	if !apierrors.IsNotFound(err) {
		framework.ExpectNoError(err, "cleanup failed")
	}
	err = pxcframework.WaitForPolarDBXClusterToDisappear(f.Client, polardbxcluster.Name, polardbxcluster.Namespace, 1*time.Minute)
	framework.ExpectNoError(err, "failed to wait for polardbxcluster object to disappear in one minute")
}

func CreatePolarDBXClusterAndWaitUntilRunningOrFail(f *framework.Framework, polardbxcluster *polardbxv1.PolarDBXCluster, timeout time.Duration) {
	log.Logf("Starting to create a PolarDB-X cluster for test: %s ...", polardbxcluster.Name)

	// Create the given polardbx cluster.
	err := f.Client.Create(f.Ctx, polardbxcluster)
	framework.ExpectNoError(err)

	// Wait until not in new / creating / restoring phases.
	obj, err := pxcframework.WaitUntilPolarDBXClusterToNotInPhases(f.Client,
		polardbxcluster.Name, polardbxcluster.Namespace,
		[]polardbxv1polardbx.Phase{
			polardbxv1polardbx.PhaseNew,
			polardbxv1polardbx.PhasePending,
			polardbxv1polardbx.PhaseCreating,
			polardbxv1polardbx.PhaseRestoring,
		},
		timeout)
	framework.ExpectNoError(err)

	// Expect to be running.
	pxcframework.ExpectBeInPhase(obj, polardbxv1polardbx.PhaseRunning)
}
