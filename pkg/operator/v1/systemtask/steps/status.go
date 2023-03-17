package steps

import (
	"github.com/alibaba/polardbx-operator/api/v1/systemtask"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/systemtask/common"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var PersistentSystemTask = common.NewStepBinder("PersistentSystemTask",
	func(rc *common.Context, flow control.Flow) (reconcile.Result, error) {
		if rc.IsSystemTaskChanged() {
			if err := rc.UpdateSystemTask(); err != nil {
				return flow.Error(err, "Unable to persistent system task.")
			}
			return flow.Continue("Succeeds to persistent system task.")
		}
		return flow.Continue("Object not changed.")
	})

func TransferPhaseTo(phase systemtask.Phase, requeue bool) control.BindFunc {
	return common.NewStepBinder("TransferPhaseTo"+string(phase),
		func(rc *common.Context, flow control.Flow) (reconcile.Result, error) {
			systemTask := rc.MustGetSystemTask()
			systemTask.Status.Phase = phase
			rc.MarkSystemTaskChanged()
			if requeue {
				return flow.Retry("Retry immediately.")
			}
			return flow.Pass()
		},
	)
}
