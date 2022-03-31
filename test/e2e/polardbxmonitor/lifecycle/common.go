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
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/test/framework"
	"github.com/alibaba/polardbx-operator/test/framework/log"
	pxmframework "github.com/alibaba/polardbx-operator/test/framework/polardbxmonitor"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

func CreatePolarDBXMonitorAndWaitUntilMonitoringOrFail(f *framework.Framework, polardbxmonitor *polardbxv1.PolarDBXMonitor,
	timeout time.Duration) {
	log.Logf("Starting to create a PolarDBXMonitor for test: %s ...", polardbxmonitor.Name)

	// Create the given polardbx cluster.
	err := f.Client.Create(f.Ctx, polardbxmonitor)
	framework.ExpectNoError(err)

	// Wait until in monitoring status.
	pxmframework.WaitPolarDBXMonitorInStatus(f.Client,
		polardbxmonitor.Name, polardbxmonitor.Namespace,
		[]polardbxv1polardbx.MonitorStatus{
			polardbxv1polardbx.MonitorStatusMonitoring,
		},
		timeout)
	framework.ExpectNoError(err)
}

func DeletePolarDBXMonitorAndWaitUntilItDisappear(f *framework.Framework, polardbxmonitor *polardbxv1.PolarDBXMonitor, timeout time.Duration) {
	log.Logf("Cleaning PolarDBXMonitor: %s, should complete in 1 minute...", polardbxmonitor.Name)

	err := f.Client.Delete(f.Ctx, polardbxmonitor, client.PropagationPolicy(metav1.DeletePropagationBackground))
	if !apierrors.IsNotFound(err) {
		framework.ExpectNoError(err, "cleanup failed")
	}
	err = pxmframework.WaitForPolarDBXMonitorToDisappear(f.Client, polardbxmonitor.Name, polardbxmonitor.Namespace, timeout)

	framework.ExpectNoError(err, "failed to wait for PolarDBXMonitor object to disappear")
}

func UpdatePolarDBXMonitorAndWaitMonitoringOrFail(f *framework.Framework, polardbxmonitor *polardbxv1.PolarDBXMonitor, timeout time.Duration,
	opts ...pxmframework.MonitorFactoryOption) {

	f.Client.Get(f.Ctx, types.NamespacedName{
		Namespace: polardbxmonitor.Namespace,
		Name:      polardbxmonitor.Name,
	}, polardbxmonitor)

	for _, opt := range opts {
		opt(polardbxmonitor)
	}

	err := f.Client.Update(f.Ctx, polardbxmonitor)
	framework.ExpectNoError(err, "Update PolarDBXMonitor failed")

	// Wait until in monitoring status.
	pxmframework.WaitPolarDBXMonitorInStatus(f.Client,
		polardbxmonitor.Name, polardbxmonitor.Namespace,
		[]polardbxv1polardbx.MonitorStatus{
			polardbxv1polardbx.MonitorStatusMonitoring,
		},
		timeout)
	framework.ExpectNoError(err)
}
