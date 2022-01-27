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

	"github.com/onsi/ginkgo"

	"github.com/alibaba/polardbx-operator/test/framework"
	objframework "github.com/alibaba/polardbx-operator/test/framework/object"
	pxcframework "github.com/alibaba/polardbx-operator/test/framework/polardbxcluster"
	xstoreframework "github.com/alibaba/polardbx-operator/test/framework/xstore"
)

var _ = ginkgo.Describe("[PolarDBXCluster] [Lifecycle:Delete]", func() {
	f := framework.NewDefaultFramework(framework.TestContext)

	ginkgo.It("should all sub-resources related be removed", func() {
		obj := pxcframework.NewPolarDBXCluster("e2e-test-delete-subr", f.Namespace,
			pxcframework.ProtocolVersion(5),
			pxcframework.TopologyModeGuide("quick-start"),
		)

		// Defer deletion and checks.
		defer func() {
			DeletePolarDBXClusterAndWaitUntilItDisappear(f, obj, 5*time.Minute)

			// Any sub-resources with labels should be removed.
			labels := map[string]string{
				"polardbx/name": obj.Name,
			}

			framework.ExpectNoError(objframework.WaitForConfigMapsWithLabelsToDisappear(f.Client, f.Namespace, labels, 1*time.Minute))
			framework.ExpectNoError(objframework.WaitForServicesWithLabelsToDisappear(f.Client, f.Namespace, labels, 1*time.Minute))
			framework.ExpectNoError(objframework.WaitForSecretsWithLabelsToDisappear(f.Client, f.Namespace, labels, 1*time.Minute))
			framework.ExpectNoError(objframework.WaitForDeploymentsWithLabelsToDisappear(f.Client, f.Namespace, labels, 1*time.Minute))
			framework.ExpectNoError(objframework.WaitForJobsWithLabelsToDisappear(f.Client, f.Namespace, labels, 1*time.Minute))

			framework.ExpectNoError(objframework.WaitForPodsWithLabelsToDisappear(f.Client, f.Namespace, labels, 2*time.Minute))
			framework.ExpectNoError(xstoreframework.WaitForXStoresWithLabelsToDisappear(f.Client, f.Namespace, labels, 2*time.Minute))
		}()

		// Create and wait to running.
		CreatePolarDBXClusterAndWaitUntilRunningOrFail(f, obj, 10*time.Minute)
	})
})
