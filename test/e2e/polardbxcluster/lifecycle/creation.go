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
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"

	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/onsi/ginkgo"
	"k8s.io/apimachinery/pkg/types"

	"github.com/alibaba/polardbx-operator/test/framework"
	pxcframework "github.com/alibaba/polardbx-operator/test/framework/polardbxcluster"
)

var _ = ginkgo.Describe("[PolarDBXCluster] [Lifecycle:Create]", func() {
	f := framework.NewDefaultFramework(framework.TestContext)
	//f.Namespace = "development"
	ginkgo.It("should polardbx cluster with paxos be in running in ten minutes after creation and sub-resources set", func() {
		obj := pxcframework.NewPolarDBXCluster(
			"e2e-test-quick-start-paxos",
			f.Namespace,
			pxcframework.ProtocolVersion(5),
			pxcframework.TopologyModeGuide("quick-start-paxos"),
		)

		// Always run clean up to make sure objects are cleaned.
		defer DeletePolarDBXClusterAndWaitUntilItDisappear(f, obj, 1*time.Minute)

		// Do create and verify.
		CreatePolarDBXClusterAndWaitUntilRunningOrFail(f, obj, 10*time.Minute)

		// Update object.
		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: obj.Name, Namespace: f.Namespace,
		}, obj))

		// Expect all ok in running.
		pxcframework.NewExpectation(f, obj).ExpectAllOk(true)
	})

	ginkgo.It("should polardbx cluster be in running in ten minutes after creation and sub-resources set", func() {
		obj := pxcframework.NewPolarDBXCluster(
			"e2e-test-quick-start",
			f.Namespace,
			pxcframework.ProtocolVersion(5),
			pxcframework.TopologyModeGuide("quick-start"),
		)

		// Always run clean up to make sure objects are cleaned.
		defer DeletePolarDBXClusterAndWaitUntilItDisappear(f, obj, 1*time.Minute)

		// Do create and verify.
		CreatePolarDBXClusterAndWaitUntilRunningOrFail(f, obj, 10*time.Minute)

		// Update object.
		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: obj.Name, Namespace: f.Namespace,
		}, obj))

		// Expect all ok in running.
		pxcframework.NewExpectation(f, obj).ExpectAllOk(false)
	})

	ginkgo.It("should service names and type be as expected", func() {
		serviceName := "e2e-test-service-s"
		serviceType := corev1.ServiceTypeNodePort

		obj := pxcframework.NewPolarDBXCluster(
			"e2e-test-service-rel",
			f.Namespace,
			pxcframework.ProtocolVersion(5),
			pxcframework.TopologyModeGuide("quick-start"),
			pxcframework.Service(serviceName, serviceType),
		)

		// Always run clean up to make sure objects are cleaned.
		defer DeletePolarDBXClusterAndWaitUntilItDisappear(f, obj, 1*time.Minute)

		// Do create and verify.
		CreatePolarDBXClusterAndWaitUntilRunningOrFail(f, obj, 10*time.Minute)

		// Update object.
		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: obj.Name, Namespace: f.Namespace,
		}, obj))

		// Expect services ok.
		pxcframework.NewExpectation(f, obj).ExpectServicesOk()
	})

	ginkgo.It("should pod replicas and specs of polardbx cluster be as expected", func() {
		resources := corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("2Gi"),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("100Mi"),
			},
		}
		obj := pxcframework.NewPolarDBXCluster(
			"e2e-test-topology-nodes",
			f.Namespace,
			pxcframework.ProtocolVersion(5),
			pxcframework.TopologyNode("cn", 2, "", "", false, resources),
			pxcframework.TopologyNode("dn", 2, "", "", false, resources),
		)

		// Always run clean up to make sure objects are cleaned.
		defer DeletePolarDBXClusterAndWaitUntilItDisappear(f, obj, 1*time.Minute)

		// Do create and verify.
		CreatePolarDBXClusterAndWaitUntilRunningOrFail(f, obj, 10*time.Minute)

		// Update object.
		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: obj.Name, Namespace: f.Namespace,
		}, obj))

		// Expect sub-resources (especially deployments and xstores ok)
		exp := pxcframework.NewExpectation(f, obj)
		exp.ExpectDeploymentsOk()
		exp.ExpectXStoresOk()
	})

	ginkgo.It("should node selectors of polardbx cluster be as expected", func() {
		resources := corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("2Gi"),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("100Mi"),
			},
		}
		obj := pxcframework.NewPolarDBXCluster(
			"e2e-test-tr-cn",
			f.Namespace,
			pxcframework.ProtocolVersion(5),
			pxcframework.TopologyNode("cn", 1, "", "", false, resources),
			pxcframework.TopologyNode("dn", 1, "", "", false, resources),
		)

		// Set a default node selector.
		obj.Spec.Topology.Rules.Components.CN = []polardbxv1polardbx.StatelessTopologyRuleItem{
			{
				Name: "default",
				NodeSelector: &polardbxv1polardbx.NodeSelectorReference{
					NodeSelector: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "kkk",
										Operator: corev1.NodeSelectorOpDoesNotExist,
									},
								},
							},
						},
					},
				},
			},
		}

		// Always run clean up to make sure objects are cleaned.
		defer DeletePolarDBXClusterAndWaitUntilItDisappear(f, obj, 1*time.Minute)

		// Do create and verify.
		CreatePolarDBXClusterAndWaitUntilRunningOrFail(f, obj, 10*time.Minute)

		// Update object.
		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: obj.Name, Namespace: f.Namespace,
		}, obj))

		// Expect deployments ok.
		exp := pxcframework.NewExpectation(f, obj)
		exp.ExpectDeploymentsOk()
	})

	ginkgo.It("should tls be disabled as expected", func() {
		obj := pxcframework.NewPolarDBXCluster(
			"e2e-test-no-ssl",
			f.Namespace,
			pxcframework.ProtocolVersion(5),
			pxcframework.TopologyModeGuide("quick-start"),
		)

		// Always run clean up to make sure objects are cleaned.
		defer DeletePolarDBXClusterAndWaitUntilItDisappear(f, obj, 1*time.Minute)

		// Do create and verify.
		CreatePolarDBXClusterAndWaitUntilRunningOrFail(f, obj, 10*time.Minute)

		// Update object.
		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: obj.Name, Namespace: f.Namespace,
		}, obj))

		// Expect TLS disabled.
		pxcframework.NewExpectation(f, obj).ExpectSecurityTLSNotOk()
	})

	ginkgo.It("should tls be enabled as expected", func() {
		obj := pxcframework.NewPolarDBXCluster(
			"e2e-test-ssl-self-signed",
			f.Namespace,
			pxcframework.ProtocolVersion(5),
			pxcframework.TopologyModeGuide("quick-start"),
			pxcframework.EnableTLS("", true),
		)

		// Always run clean up to make sure objects are cleaned.
		defer DeletePolarDBXClusterAndWaitUntilItDisappear(f, obj, 1*time.Minute)

		// Do create and verify.
		CreatePolarDBXClusterAndWaitUntilRunningOrFail(f, obj, 10*time.Minute)

		// Update object.
		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: obj.Name, Namespace: f.Namespace,
		}, obj))

		// Expect TLS enabled.
		pxcframework.NewExpectation(f, obj).ExpectSecurityTLSOk()
	})

	ginkgo.It("readonly pxc should be created as expected", func() {
		readOnlyName := "ro-test"
		obj := pxcframework.NewPolarDBXCluster(
			"e2e-test-readonly",
			f.Namespace,
			pxcframework.ProtocolVersion(5),
			pxcframework.InitReadonly(1, readOnlyName, true),
			pxcframework.TopologyModeGuide("quick-start-paxos"),
		)

		// Always run clean up to make sure objects are cleaned.
		defer DeletePolarDBXClusterAndWaitUntilItDisappear(f, obj, 1*time.Minute)

		// Do create and verify.
		CreatePolarDBXClusterAndWaitUntilRunningOrFail(f, obj, 10*time.Minute)

		// Fetch the readonly object
		readonlyObj := &polardbxv1.PolarDBXCluster{}
		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name:      obj.Name + "-" + readOnlyName,
			Namespace: f.Namespace,
		}, readonlyObj))

		WaitPolarDBXClusterRunningOrFail(f, readonlyObj, 20*time.Minute)

		// Update object.
		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: obj.Name, Namespace: f.Namespace,
		}, obj))

		// Update readonly object
		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name:      obj.Name + "-" + readOnlyName,
			Namespace: f.Namespace,
		}, readonlyObj))

		// Expect all ok in running.
		pxcframework.NewExpectation(f, obj).ExpectAllOk(true)
		pxcframework.NewExpectation(f, readonlyObj).ExpectAllOk(true)
	})

	ginkgo.It("cdc group pxc should be created as expected", func() {
		resources := corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("2Gi"),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("100Mi"),
			},
		}
		obj := pxcframework.NewPolarDBXCluster(
			"e2e-test-create-cdc-group-1",
			f.Namespace,
			pxcframework.ProtocolVersion(5),
			pxcframework.TopologyNode("cn", 1, "", "", false, resources),
			pxcframework.TopologyNode("dn", 1, "", "", false, resources),
			pxcframework.TopologyNode("cdc", 1, "", "", false, resources),
		)
		cdcGroup := pxcframework.CdcGroup("cdcgroup1", 1, "testenvk", "testenvv")
		obj.Spec.Topology.Nodes.CDC.Groups = []*polardbxv1polardbx.CdcGroup{
			&cdcGroup,
		}
		// Always run clean up to make sure objects are cleaned.
		defer DeletePolarDBXClusterAndWaitUntilItDisappear(f, obj, 1*time.Minute)

		// Do create and verify.
		CreatePolarDBXClusterAndWaitUntilRunningOrFail(f, obj, 10*time.Minute)

		var depoyList v1.DeploymentList
		framework.ExpectNoError(f.Client.List(f.Ctx, &depoyList, client.MatchingLabels{
			"polardbx/name":  "e2e-test-create-cdc-group-1",
			"polardbx/group": "cdcgroup1",
		}), obj)
		gomega.Expect(len(depoyList.Items)).Should(gomega.BeEquivalentTo(1))
		deploy := depoyList.Items[0]
		var hasTargetEnv bool
		for _, container := range deploy.Spec.Template.Spec.Containers {
			if container.Name == convention.ContainerEngine {
				for _, envKv := range container.Env {
					if envKv.Name == "testenvk" && envKv.Value == "testenvv" {
						hasTargetEnv = true
						break
					}
				}
				break
			}
		}
		gomega.Expect(hasTargetEnv).Should(gomega.BeEquivalentTo(true))
	})

})
