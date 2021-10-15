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

package common

import (
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/helper"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
)

var PersistentStatus = polardbxv1reconcile.NewStepBinder("PersistentStatus",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.IsPolarDBXStatusChanged() {
			err := rc.UpdatePolarDBXStatus()
			if err != nil {
				return flow.Error(err, "Unable to persistent status.")
			}
		}
		return flow.Pass()
	},
)

var PersistentPolarDBXCluster = polardbxv1reconcile.NewStepBinder("PersistentPolarDBXCluster",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.IsPolarDBXChanged() {
			err := rc.UpdatePolarDBX()
			if err != nil {
				return flow.Error(err, "Unable to persistent polardbx.")
			}
		}
		return flow.Pass()
	},
)

var InitializeServiceName = polardbxv1reconcile.NewStepBinder("InitializeServiceName",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		if len(polardbx.Spec.ServiceName) == 0 {
			polardbx.Spec.ServiceName = polardbx.Name
			rc.MarkPolarDBXChanged()
		}
		return flow.Pass()
	},
)

var CheckDNs = polardbxv1reconcile.NewStepBinder("CheckDNs",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		topology := polardbx.Status.SpecSnapshot.Topology
		replicas := int(topology.Nodes.DN.Replicas)

		dnStores, err := rc.GetDNMap()
		if err != nil {
			return flow.Error(err, "Unable to get xstores of DN.")
		}

		// Ensure DNs are sorted and their indexes are incremental
		// when phase is not in creating or restoring.
		lastIndex := 0
		for ; lastIndex < replicas; lastIndex++ {
			if _, ok := dnStores[lastIndex]; !ok {
				break
			}
		}
		if lastIndex != replicas && lastIndex != len(dnStores) {
			helper.TransferPhase(polardbx, polardbxv1polardbx.PhaseFailed)
			return flow.Requeue("Found broken DN, transfer into failed.")
		}

		return flow.Pass()
	},
)
