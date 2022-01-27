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
	"github.com/onsi/ginkgo"
)

var _ = ginkgo.Describe("[PolarDBXCluster] [Lifecycle:Maintain]", func() {
	// f := framework.NewDefaultFramework(framework.TestContext)

	// This testcase is skipped.

	// ginkgo.It("should cn configurations be as expected", func() {
	// 	resources := corev1.ResourceRequirements{
	// 		Limits: corev1.ResourceList{
	// 			corev1.ResourceCPU:    resource.MustParse("2"),
	// 			corev1.ResourceMemory: resource.MustParse("2Gi"),
	// 		},
	// 		Requests: corev1.ResourceList{
	// 			corev1.ResourceCPU:    resource.MustParse("100m"),
	// 			corev1.ResourceMemory: resource.MustParse("100Mi"),
	// 		},
	// 	}
	// 	obj := pxcframework.NewPolarDBXCluster(
	// 		"e2e-test-upgrade-cn-configurations",
	// 		f.Namespace,
	// 		pxcframework.ProtocolVersion(5),
	// 		pxcframework.TopologyNode("cn", 2, "", "", false, resources),
	// 		pxcframework.TopologyNode("dn", 2, "", "", false, resources),
	// 	)
	//
	// 	// Always run clean up to make sure objects are cleaned.
	// 	defer DeletePolarDBXClusterAndWaitUntilItDisappear(f, obj, 1*time.Minute)
	//
	// 	// Do create and verify.
	// 	CreatePolarDBXClusterAndWaitUntilRunningOrFail(f, obj, 10*time.Minute)
	//
	// 	// Update object.
	// 	framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
	// 		Name: obj.Name, Namespace: f.Namespace,
	// 	}, obj))
	//
	// 	// Expect sub-resources (especially deployments and xstores ok)
	// 	exp := pxcframework.NewExpectation(f, obj)
	// 	exp.ExpectDeploymentsOk()
	// 	exp.ExpectXStoresOk()
	//
	// 	obj.Spec.Config.CN.Dynamic = map[string]intstr.IntOrString{
	// 		"ConfigKeyEnableLocalMode": intstr.FromString("true"),
	// 	}
	// 	err := f.Client.Update(f.Ctx, obj)
	// 	framework.ExpectNoError(err)
	//
	// 	framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
	// 		Name: obj.Name, Namespace: f.Namespace,
	// 	}, obj))
	//
	// 	obj, err = pxcframework.WaitUntilPolarDBXClusterUpgradeCompleteOrFail(f.Client, obj.Name, obj.Namespace, 10*time.Minute)
	// 	framework.ExpectNoError(err)
	// 	pxcframework.ExpectBeInPhase(obj, polardbxv1polardbx.PhaseRunning)
	//
	// 	pxcframework.NewExpectation(f, obj).ExpectCNDynamicConfigurationsOk()
	// })

})
