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

package instance

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	_ "github.com/go-sql-driver/mysql"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/command"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

func checkTopologySpec(xstore *polardbxv1.XStore) error {
	// Check the topology
	topology := &xstore.Spec.Topology

	// Empty means single node.
	if len(topology.NodeSets) == 0 {
		return nil
	}

	nodeSetNames := make(map[string]interface{})
	candidates := make([]*polardbxv1xstore.NodeSet, 0, 1)
	voter := make([]*polardbxv1xstore.NodeSet, 0, 1)
	learner := make([]*polardbxv1xstore.NodeSet, 0, 1)
	candidateCnt, voterCnt, learnerCnt := 0, 0, 0
	for _, ns := range topology.NodeSets {
		if _, ok := nodeSetNames[ns.Name]; ok {
			return errors.New("invalid topology: duplicate node set names: " + ns.Name)
		}
		nodeSetNames[ns.Name] = nil

		switch ns.Role {
		case polardbxv1xstore.RoleCandidate:
			candidates = append(candidates, &ns)
			candidateCnt += int(ns.Replicas)
		case polardbxv1xstore.RoleVoter:
			voter = append(voter, &ns)
			voterCnt += int(ns.Replicas)
		case polardbxv1xstore.RoleLearner:
			learner = append(learner, &ns)
			learnerCnt += int(ns.Replicas)
		default:
			return errors.New(string("invalid topology: unrecognized node set role: " + ns.Role))
		}
	}

	// It must meet the following requirements:
	//   1. The total number of candidates and voters must be odd.
	//   2. There must be at least one candidate.
	if candidateCnt == 0 {
		return errors.New("invalid topology: no candidates found")
	}
	if (candidateCnt+voterCnt)&1 == 0 {
		return errors.New("invalid topology: sum(candidate_size, voter_size) is even")
	}

	return nil
}

var CheckTopologySpec = xstorev1reconcile.NewStepBinder("CheckTopologySpec",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		err := checkTopologySpec(xstore)
		if err != nil {
			xstore.Status.Phase = polardbxv1xstore.PhaseFailed
			return flow.Error(err, "Check topology failed. Transfer phase into Failed.")
		}
		return flow.Pass()
	},
)

var UpdateObservedTopologyAndConfig = xstorev1reconcile.NewStepBinder("UpdateObservedTopologyAndConfig",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		xstore.Status.ObservedTopology = xstore.Spec.Topology.DeepCopy()
		xstore.Status.ObservedConfig = xstore.Spec.Config.DeepCopy()
		return flow.Continue("Update observed topology and config.", "current-generation", xstore.Generation)
	},
)

var UpdateObservedGeneration = xstorev1reconcile.NewStepBinder("UpdateObservedGeneration",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		prevGen := xstore.Status.ObservedGeneration
		xstore.Status.ObservedGeneration = xstore.Generation
		return flow.Continue("Update observed generation.", "previous-generation", prevGen,
			"current-generation", xstore.Generation)
	},
)

var QueryAndUpdateEngineVersion = xstorev1reconcile.NewStepBinder("QueryAndUpdateEngineVersion",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		leaderPod, err := rc.TryGetXStoreLeaderPod()
		if err != nil {
			return flow.Error(err, "Unable to get leader pod.")
		}
		if leaderPod == nil {
			return flow.Wait("Leader pod not found, wait.")
		}

		cmd := command.NewCanonicalCommandBuilder().Engine().Version().Build()
		buf := &bytes.Buffer{}
		err = rc.ExecuteCommandOn(leaderPod, convention.ContainerEngine, cmd, control.ExecOptions{
			Logger:  flow.Logger(),
			Stdout:  buf,
			Timeout: 2 * time.Second,
		})
		if err != nil {
			return flow.Error(err, "Failed to query version on leader pod.", "pod", leaderPod.Name)
		}

		engineVersion := strings.TrimSpace(buf.String())
		if engineVersion == "" {
			return flow.Error(errors.New("empty engine version"), "Engine version is empty.")
		}

		// Update the engine version in status.
		xstore := rc.MustGetXStore()
		xstore.Status.EngineVersion = engineVersion

		return flow.Pass()
	},
)

var CheckConnectivityAndSetEngineVersion = xstorev1reconcile.NewStepBinder("CheckConnectivityFromController",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		passwd, err := rc.GetXStoreAccountPassword(convention.SuperAccount)
		if err != nil {
			return flow.Error(err, "Unable to get password for super account.")
		}

		clusterAddr, err := rc.GetXStoreClusterAddr(convention.ServiceTypeReadWrite, convention.PortAccess)
		if err != nil {
			return flow.Error(err, "Unable to get cluster address.")
		}

		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/information_schema?timeout=1s",
			convention.SuperAccount, passwd, clusterAddr))
		if err != nil {
			return flow.Error(err, "Unable to open connection to cluster address.")
		}
		defer db.Close()

		if err := db.PingContext(rc.Context()); err != nil {
			// Wait 10 seconds for any error.
			flow.Logger().Error(err, "Ping failed.")
			return flow.RetryAfter(10*time.Second, "Failed to ping, wait for 10 seconds and retry...")
		}

		// Get version via SQL and update status.

		//goland:noinspection SqlNoDataSourceInspection,SqlDialectInspection
		row := db.QueryRowContext(rc.Context(), "SELECT VERSION()")
		var version string
		err = row.Scan(&version)
		if err != nil {
			return flow.Error(err, "Unable to read version")
		}
		xstore := rc.MustGetXStore()
		xstore.Status.EngineVersion = version

		return flow.Continue("Succeed.")
	},
)

var InjectFinalizerOnXStore = xstorev1reconcile.NewStepBinder("InjectFinalizerOnXStore",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		if controllerutil.ContainsFinalizer(xstore, xstoremeta.Finalizer) {
			return flow.Pass()
		}

		controllerutil.AddFinalizer(xstore, xstoremeta.Finalizer)
		rc.MarkXStoreChanged()

		return flow.Continue("Inject finalizer.")
	},
)

var RemoveFinalizerFromXStore = xstorev1reconcile.NewStepBinder("RemoveFinalizerFromXStore",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		if !controllerutil.ContainsFinalizer(xstore, xstoremeta.Finalizer) {
			return flow.Pass()
		}

		controllerutil.RemoveFinalizer(xstore, xstoremeta.Finalizer)
		rc.MarkXStoreChanged()

		return flow.Continue("Remove finalizer.")
	},
)

var GenerateRandInStatus = xstorev1reconcile.NewStepBinder("GenerateRandInStatus",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		if len(xstore.Status.Rand) == 0 {
			if val, ok := xstore.Annotations[xstoremeta.AnnotationGuideRand]; ok {
				xstore.Status.Rand = val
			} else {
				xstore.Status.Rand = rand.String(4)
			}
		}

		return flow.Pass()
	},
)

var DeleteAllPods = xstorev1reconcile.NewStepBinder("DeleteAllPods",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}

		for _, pod := range pods {
			if pod.DeletionTimestamp.IsZero() {
				if err := rc.Client().Delete(rc.Context(), &pod); err != nil {
					if apierrors.IsNotFound(err) {
						continue
					}
					return flow.Error(err, "Unable to delete pod.", "pod", pod.Name)
				}
			}
		}

		return flow.Continue("All pods deleted.")
	},
)

var AbortReconcileIfHintFound = xstorev1reconcile.NewStepBinder("AbortReconcileIfHintFound",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.ContainsControllerHint(xstoremeta.HintForbidden) {
			return flow.Wait("Found hint, abort reconcile.")
		}
		return flow.Pass()
	},
)

var FillServiceNameIfNotProvided = xstorev1reconcile.NewStepBinder("FillServiceNameIfNotProvided",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		if len(xstore.Spec.ServiceName) == 0 {
			xstore.Spec.ServiceName = xstore.Name
			rc.MarkXStoreChanged()
		}
		return flow.Pass()
	},
)

var BindPodPorts = xstorev1reconcile.NewStepBinder("BindPodPorts",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}

		podPorts := make(map[string]polardbxv1xstore.PodPorts)
		for _, pod := range pods {
			ports := polardbxv1xstore.PodPorts{}
			for _, container := range pod.Spec.Containers {
				for _, port := range container.Ports {
					ports[port.Name] = port.ContainerPort
				}
			}
			podPorts[pod.Name] = ports
		}

		xstore.Status.PodPorts = podPorts
		return flow.Continue("Pod ports updated!")
	},
)
