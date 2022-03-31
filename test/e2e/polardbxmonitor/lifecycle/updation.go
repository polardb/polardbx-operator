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
	"github.com/alibaba/polardbx-operator/test/framework"
	pxcframework "github.com/alibaba/polardbx-operator/test/framework/polardbxcluster"
	pxmframework "github.com/alibaba/polardbx-operator/test/framework/polardbxmonitor"
	"github.com/onsi/ginkgo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"time"

	pxcmanager "github.com/alibaba/polardbx-operator/test/e2e/polardbxcluster/lifecycle"
)

var _ = ginkgo.Describe("[PolarDBXMonitor] [Lifecycle:Update]", func() {
	f := framework.NewDefaultFramework(framework.TestContext)

	ginkgo.It("should monitorInterval and scrapeTimeout be modified", func() {
		pxc := pxcframework.NewPolarDBXCluster(
			"e2e-test-monitor-update",
			f.Namespace,
			pxcframework.ProtocolVersion(5),
			pxcframework.TopologyModeGuide("quick-start"),
		)

		pxm := pxmframework.NewPolarDBXMonitor(pxc.Name, pxc.Namespace)

		// Always run clean up to make sure objects are cleaned.
		defer pxcmanager.DeletePolarDBXClusterAndWaitUntilItDisappear(f, pxc, 1*time.Minute)
		defer DeletePolarDBXMonitorAndWaitUntilItDisappear(f, pxm, 1*time.Minute)

		// Do create and verify.
		pxcmanager.CreatePolarDBXClusterAndWaitUntilRunningOrFail(f, pxc, 10*time.Minute)

		// Update object.
		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: pxc.Name, Namespace: f.Namespace,
		}, pxc))

		CreatePolarDBXMonitorAndWaitUntilMonitoringOrFail(f, pxm, 5*time.Minute)

		pxcframework.NewExpectation(f, pxc).ExpectServiceMonitorsOK()

		// Update monitorInterval and scrapeTimeout
		UpdatePolarDBXMonitorAndWaitMonitoringOrFail(f, pxm, 2*time.Minute,
			pxmframework.MonitorInterval(metav1.Duration{Duration: 60 * time.Second}),
			pxmframework.ScrapeTimeout(metav1.Duration{Duration: 20 * time.Second}),
		)

		pxcframework.NewExpectation(f, pxc).ExpectServiceMonitorsOK()

	})
})
