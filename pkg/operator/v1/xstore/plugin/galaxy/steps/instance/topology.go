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
	"errors"

	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/galaxy/galaxy"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

func checkTopologySpec(xstore *polardbxv1.XStore) error {
	topology := &xstore.Spec.Topology

	// Empty is allowed.
	if len(topology.NodeSets) == 0 {
		return errors.New("node sets are empty")
	}

	// Currently, galaxy engine only supports with 1 replica.
	if len(topology.NodeSets) != 1 {
		return errors.New("must specify only 1 node set with replica 1")
	}

	nodeSet := &topology.NodeSets[0]
	if nodeSet.Replicas != 1 {
		return errors.New("must specify only 1 node set with replica 1")
	}

	// Role must be candidate.
	if nodeSet.Role != polardbxv1xstore.RoleCandidate {
		return errors.New("role of node set must be candidate")
	}

	return nil
}

// Deprecated
var CheckTopologySpec = plugin.NewStepBinder(galaxy.Engine, "CheckTopologySpec",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		err := checkTopologySpec(rc.MustGetXStore())
		if err != nil {
			xstore.Status.Phase = polardbxv1xstore.PhaseFailed
			return flow.Error(err, "Check topology failed. Transfer phase into Failed.")
		}
		return flow.Pass()
	},
)
