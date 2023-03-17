package common

import "github.com/alibaba/polardbx-operator/api/v1/systemtask"

//  reconciler name

var (
	registeredMap = make(map[systemtask.Type]Reconciler, 0)
)

func Register(taskType systemtask.Type, reconciler Reconciler) {
	registeredMap[taskType] = reconciler
}

func MusterFindReconciler(taskType systemtask.Type) Reconciler {
	reconciler, ok := registeredMap[taskType]
	if !ok {
		panic("Failed to find system task reconciler, task type :  " + taskType)
	}
	return reconciler
}
