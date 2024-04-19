package tde

import (
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/helper"
	polardbxreconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var UpdateDNsTdeOpen = polardbxreconcile.NewStepBinder("UpdateDNsTdeOpen",
	func(rc *polardbxreconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		topology := polardbx.Status.SpecSnapshot.Topology
		replicas := int(topology.Nodes.DN.Replicas)

		dnStores, err := rc.GetDNMap()
		if err != nil {
			return flow.Error(err, "Unable to get xstores of DN.")
		}
		// Ensure DNs are sorted and their indexes are incremental
		// when phase is not in creating or restoring.
		if !helper.IsPhaseIn(polardbx, polardbxv1polardbx.PhaseCreating, polardbxv1polardbx.PhaseRestoring) {
			lastIndex := 0
			for ; lastIndex < replicas; lastIndex++ {
				if _, ok := dnStores[lastIndex]; !ok {
					break
				}
			}

			if lastIndex != replicas && lastIndex != len(dnStores) {
				helper.TransferPhase(polardbx, polardbxv1polardbx.PhaseFailed)
				return flow.Retry("Found broken DN, transfer into failed.")
			}
		}
		for i := 0; i < replicas; i++ {
			dnXStore, _ := dnStores[i]
			dnXStore.Spec.TDE.Enable = polardbx.Spec.TDE.Enable
			dnXStore.Spec.TDE.KeyringPath = polardbx.Spec.TDE.KeyringPath
			err = rc.Client().Update(rc.Context(), dnXStore)
			if err != nil {
				return flow.Error(err, "Update xstore to open tde!", "xstore", dnXStore.Name)
			}
		}

		return flow.Pass()
	},
)
