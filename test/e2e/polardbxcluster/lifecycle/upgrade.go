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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/onsi/ginkgo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/group"

	"github.com/alibaba/polardbx-operator/test/framework"
	pxcframework "github.com/alibaba/polardbx-operator/test/framework/polardbxcluster"
)

var _ = ginkgo.Describe("[PolarDBXCluster] [Lifecycle:Upgrade]", func() {
	f := framework.NewDefaultFramework(framework.TestContext)

	ginkgo.It("should cn numbers be as expected", func() {
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
			"e2e-test-upgrade-cn-numbers",
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

		*obj.Spec.Topology.Nodes.CN.Replicas = 1
		err := f.Client.Update(f.Ctx, obj)
		framework.ExpectNoError(err)

		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: obj.Name, Namespace: f.Namespace,
		}, obj))

		obj, err = pxcframework.WaitUntilPolarDBXClusterUpgradeCompleteOrFail(f.Client, obj.Name, obj.Namespace, 10*time.Minute)
		framework.ExpectNoError(err)
		pxcframework.ExpectBeInPhase(obj, polardbxv1polardbx.PhaseRunning)

		pxcframework.NewExpectation(f, obj).ExpectCNDeploymentsOk()
	})

	ginkgo.It("should cn numbers reduce  be as expected", func() {
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
			"e2e-test-upgrade-cn-numbers-reduce",
			f.Namespace,
			pxcframework.ProtocolVersion(5),
			pxcframework.TopologyNode("cn", 4, "", "", false, resources),
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

		*obj.Spec.Topology.Nodes.CN.Replicas = 2
		err := f.Client.Update(f.Ctx, obj)
		framework.ExpectNoError(err)

		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: obj.Name, Namespace: f.Namespace,
		}, obj))

		obj, err = pxcframework.WaitUntilPolarDBXClusterUpgradeCompleteOrFail(f.Client, obj.Name, obj.Namespace, 10*time.Minute)
		framework.ExpectNoError(err)
		pxcframework.ExpectBeInPhase(obj, polardbxv1polardbx.PhaseRunning)

		pxcframework.NewExpectation(f, obj).ExpectCNDeploymentsOk()
	})

	ginkgo.It("should cdc numbers 1 to 2 be as expected", func() {
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
			"e2e-test-upgrade-cdc-numbers-1-2",
			f.Namespace,
			pxcframework.ProtocolVersion(5),
			pxcframework.TopologyNode("cn", 2, "", "", false, resources),
			pxcframework.TopologyNode("dn", 2, "", "", false, resources),
			pxcframework.TopologyNode("cdc", 1, "", "", false, resources),
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

		obj.Spec.Topology.Nodes.CDC.Replicas = 2
		err := f.Client.Update(f.Ctx, obj)
		framework.ExpectNoError(err)

		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: obj.Name, Namespace: f.Namespace,
		}, obj))

		obj, err = pxcframework.WaitUntilPolarDBXClusterUpgradeCompleteOrFail(f.Client, obj.Name, obj.Namespace, 10*time.Minute)
		framework.ExpectNoError(err)
		pxcframework.ExpectBeInPhase(obj, polardbxv1polardbx.PhaseRunning)

		pxcframework.NewExpectation(f, obj).ExpectCNDeploymentsOk()
	})

	ginkgo.It("should cdc numbers 0 to 1 be as expected", func() {
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
			"e2e-test-upgrade-cdc-number-0-1",
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

		obj.Spec.Topology.Nodes.CDC = &polardbxv1polardbx.TopologyNodeCDC{
			Replicas: 1,
		}
		obj.Spec.Topology.Nodes.CDC.Template.Resources = resources
		err := f.Client.Update(f.Ctx, obj)
		framework.ExpectNoError(err)

		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: obj.Name, Namespace: f.Namespace,
		}, obj))

		obj, err = pxcframework.WaitUntilPolarDBXClusterUpgradeCompleteOrFail(f.Client, obj.Name, obj.Namespace, 10*time.Minute)
		framework.ExpectNoError(err)
		pxcframework.ExpectBeInPhase(obj, polardbxv1polardbx.PhaseRunning)

		pxcframework.NewExpectation(f, obj).ExpectCDCDeploymentsOk()
	})

	ginkgo.It("should cn resource upgrade as expected", func() {
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
			"e2e-test-upgrade-cn-resource",
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

		obj.Spec.Topology.Nodes.CN.Template.Resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("200m"),
			corev1.ResourceMemory: resource.MustParse("200Mi"),
		}

		err := f.Client.Update(f.Ctx, obj)
		framework.ExpectNoError(err)

		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: obj.Name, Namespace: f.Namespace,
		}, obj))

		obj, err = pxcframework.WaitUntilPolarDBXClusterUpgradeCompleteOrFail(f.Client, obj.Name, obj.Namespace, 10*time.Minute)
		framework.ExpectNoError(err)
		pxcframework.ExpectBeInPhase(obj, polardbxv1polardbx.PhaseRunning)

		pxcframework.NewExpectation(f, obj).ExpectCNDeploymentsOk()
	})

	ginkgo.It("should cn resource downgrade as expected", func() {
		resources := corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("2Gi"),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("200m"),
				corev1.ResourceMemory: resource.MustParse("200Mi"),
			},
		}
		obj := pxcframework.NewPolarDBXCluster(
			"e2e-test-downgrade-cn-resource",
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

		obj.Spec.Topology.Nodes.CN.Template.Resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("100Mi"),
		}

		err := f.Client.Update(f.Ctx, obj)
		framework.ExpectNoError(err)

		framework.ExpectNoError(f.Client.Get(f.Ctx, types.NamespacedName{
			Name: obj.Name, Namespace: f.Namespace,
		}, obj))

		obj, err = pxcframework.WaitUntilPolarDBXClusterUpgradeCompleteOrFail(f.Client, obj.Name, obj.Namespace, 10*time.Minute)
		framework.ExpectNoError(err)
		pxcframework.ExpectBeInPhase(obj, polardbxv1polardbx.PhaseRunning)

		pxcframework.NewExpectation(f, obj).ExpectCNDeploymentsOk()
	})

	doScaleAndExpect := func(name string, scale func(obj *polardbxv1.PolarDBXCluster) *polardbxv1.PolarDBXCluster, withCdc bool) {
		resources := corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("4Gi"),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("200m"),
				corev1.ResourceMemory: resource.MustParse("200Mi"),
			},
		}

		options := []pxcframework.FactoryOption{
			pxcframework.ProtocolVersion(5),
			pxcframework.TopologyNode("cn", 2, "", "", false, resources),
			pxcframework.TopologyNode("dn", 2, "", "", false, resources),
		}
		if withCdc {
			options = append(options, pxcframework.TopologyNode("cdc", 2, "", "", false, resources))
		}
		obj := pxcframework.NewPolarDBXCluster(
			name,
			f.Namespace,
			options...,
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

		// Create test database and table, and insert some data.
		exp.ExpectQueriesOk(func(ctx context.Context, db *sql.DB) error {
			_, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS tt")
			if err != nil {
				return err
			}
			conn, err := db.Conn(ctx)
			if err != nil {
				return err
			}
			defer conn.Close()
			_, err = conn.ExecContext(ctx, "USE tt")
			if err != nil {
				return err
			}
			_, err = conn.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test (id int primary key) dbpartition by hash(id)")
			if err != nil {
				return err
			}
			_, err = conn.ExecContext(ctx, "INSERT INTO test VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16)")
			if err != nil {
				return err
			}
			return nil
		}, "sql should be executed as expected")

		// Do scale
		obj = scale(obj)
		dnReplicas := obj.Spec.Topology.Nodes.DN.Replicas

		// Refresh expectation.
		exp = pxcframework.NewExpectation(f, obj)

		// Do expectations.
		exp.ExpectQueriesOk(func(ctx context.Context, db *sql.DB) error {
			// Check storages.
			mgr := group.NewGroupManagerWithDB(ctx, db, false)
			storageCnt, err := mgr.CountStorages()
			if err != nil {
				return err
			}

			if storageCnt != int(dnReplicas) {
				return fmt.Errorf("storage records should matches, expect %d, but is %d", dnReplicas, storageCnt)
			}

			conn, err := db.Conn(ctx)
			if err != nil {
				return err
			}
			defer conn.Close()

			_, err = conn.ExecContext(ctx, "USE tt")
			if err != nil {
				return err
			}

			rs, err := conn.QueryContext(ctx, "SELECT * FROM test")
			if err != nil {
				return err
			}
			defer rs.Close()

			cnt := 0
			var val int
			for rs.Next() {
				err = rs.Scan(&val)
				if err != nil {
					return err
				}
				cnt++
			}

			if cnt != 16 {
				return errors.New("should be 16 records")
			}
			return nil
		})
	}

	ginkgo.It("should dn scale out as expected", func() {
		doScaleAndExpect("e2e-test-scale-out", func(obj *polardbxv1.PolarDBXCluster) *polardbxv1.PolarDBXCluster {
			nodeDn := &obj.Spec.Topology.Nodes.DN
			// Simply plus 1 and update
			nodeDn.Replicas++
			err := f.Client.Update(f.Ctx, obj)
			framework.ExpectNoError(err, "update failed")

			obj, err = pxcframework.WaitUntilPolarDBXClusterUpgradeCompleteOrFail(f.Client, obj.Name, obj.Namespace, 10*time.Minute)
			framework.ExpectNoError(err)
			return obj
		}, true)
	})

	ginkgo.It("should dn scale in as expected", func() {
		doScaleAndExpect("e2e-test-scale-in", func(obj *polardbxv1.PolarDBXCluster) *polardbxv1.PolarDBXCluster {
			nodeDn := &obj.Spec.Topology.Nodes.DN
			// Simply minus 1 and update
			nodeDn.Replicas--
			err := f.Client.Update(f.Ctx, obj)
			framework.ExpectNoError(err, "update failed")

			obj, err = pxcframework.WaitUntilPolarDBXClusterUpgradeCompleteOrFail(f.Client, obj.Name, obj.Namespace, 10*time.Minute)
			framework.ExpectNoError(err)
			return obj
		}, true)
	})

	ginkgo.It("should dn scale out without cdc as expected", func() {
		doScaleAndExpect("e2e-test-scale-out-no-cdc", func(obj *polardbxv1.PolarDBXCluster) *polardbxv1.PolarDBXCluster {
			nodeDn := &obj.Spec.Topology.Nodes.DN
			// Simply plus 1 and update
			nodeDn.Replicas++
			err := f.Client.Update(f.Ctx, obj)
			framework.ExpectNoError(err, "update failed")

			obj, err = pxcframework.WaitUntilPolarDBXClusterUpgradeCompleteOrFail(f.Client, obj.Name, obj.Namespace, 10*time.Minute)
			framework.ExpectNoError(err)
			return obj
		}, false)
	})

	ginkgo.It("should dn scale in without cdc as expected", func() {
		doScaleAndExpect("e2e-test-scale-in-no-cdc", func(obj *polardbxv1.PolarDBXCluster) *polardbxv1.PolarDBXCluster {
			nodeDn := &obj.Spec.Topology.Nodes.DN
			// Simply minus 1 and update
			nodeDn.Replicas--
			err := f.Client.Update(f.Ctx, obj)
			framework.ExpectNoError(err, "update failed")

			obj, err = pxcframework.WaitUntilPolarDBXClusterUpgradeCompleteOrFail(f.Client, obj.Name, obj.Namespace, 10*time.Minute)
			framework.ExpectNoError(err)
			return obj
		}, false)
	})

	ginkgo.It("change log data separation config", func() {
		doScaleAndExpect("e2e-test-logdataseparation", func(obj *polardbxv1.PolarDBXCluster) *polardbxv1.PolarDBXCluster {
			nodeDn := &obj.Spec.Config.DN
			// Simply minus 1 and update
			nodeDn.LogDataSeparation = true
			err := f.Client.Update(f.Ctx, obj)
			framework.ExpectNoError(err, "update failed")

			obj, err = pxcframework.WaitUntilPolarDBXClusterUpgradeCompleteOrFail(f.Client, obj.Name, obj.Namespace, 60*time.Minute)
			framework.ExpectNoError(err)
			return obj
		}, false)
	})

})
