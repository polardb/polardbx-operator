package restart

import (
	"strings"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"

	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxreconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var GetRestartingPods = polardbxreconcile.NewStepBinder("GetRestartingPods",
	func(rc *polardbxreconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		pods, err := rc.GetPods(polardbxmeta.RoleCN)
		if err != nil {
			return flow.Error(err, "Unable to get pods fo CN.")
		}
		for _, pod := range pods {
			polardbx.Status.RestartingPods.ToDeletePod = append(polardbx.Status.RestartingPods.ToDeletePod, pod.Name)
		}
		if len(pods) == 1 {
			polardbx.Status.RestartingType = polardbxv1polardbx.Restart
		}
		return flow.Pass()
	},
)

var RollingRestartPods = polardbxreconcile.NewStepBinder("RollingRestartPods",
	func(rc *polardbxreconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()

		lastDeletedPod := polardbx.Status.RestartingPods.LastDeletedPod
		if lastDeletedPod != "" {
			s := strings.Split(lastDeletedPod, "-")
			podNum := s[len(s)-3]
			podDel := corev1.Pod{}
			var podList corev1.PodList
			err := rc.Client().List(
				rc.Context(),
				&podList,
				client.InNamespace(rc.Namespace()),
				client.MatchingLabels(convention.ConstLabelsWithRole(polardbx, polardbxmeta.RoleCN)),
			)
			if err != nil {
				return flow.Error(err, "Error getting pods")
			}
			for _, podTemp := range podList.Items {
				ss := strings.Split(podTemp.Name, "-")
				num := s[len(ss)-3]
				if num == podNum {
					podDel = podTemp
					break
				}
			}
			if !helper.IsPodReady(&podDel) || podDel.Name == "" || !podDel.DeletionTimestamp.IsZero() {
				return flow.Retry("Pod hasn't been deleted")
			}
		}

		polardbx.Status.RestartingPods.LastDeletedPod = ""

		if len(polardbx.Status.RestartingPods.ToDeletePod) == 0 {
			return flow.Pass()
		}

		var deletePodName string
		deletePodName, polardbx.Status.RestartingPods.ToDeletePod =
			polardbx.Status.RestartingPods.ToDeletePod[0], polardbx.Status.RestartingPods.ToDeletePod[1:]

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: rc.Namespace(),
				Name:      deletePodName,
			},
		}

		err := rc.Client().Delete(rc.Context(), pod)
		if err != nil {
			return flow.Error(err, "Unable to delete pod", "pod name", pod.Name)
		}

		polardbx.Status.RestartingPods.LastDeletedPod = deletePodName

		return flow.Retry("Rolling Restart...")
	},
)

var RestartingPods = polardbxreconcile.NewStepBinder("RestartingPods",
	func(rc *polardbxreconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetPods(polardbxmeta.RoleCN)
		if err != nil {
			return flow.Error(err, "Unable to get pods fo CN.")
		}

		for _, pod := range pods {
			err := rc.Client().Delete(rc.Context(), &pod)
			if err != nil {
				return flow.Error(err, "Unable to delete pod", "pod name", pod.Name)
			}
		}

		return flow.Pass()
	},
)

var ClosePolarDBXRestartPhase = polardbxreconcile.NewStepBinder("ClosePolarDBXRestartPhase",
	func(rc *polardbxreconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		polardbx.Status.Restarting = false

		err := rc.UpdatePolarDBXStatus()
		if err != nil {
			return flow.Error(err, "Unable to Update PolarDBX Status")
		}

		return flow.Pass()
	},
)

func IsRollingRestart(polardbx *polardbxv1.PolarDBXCluster) bool {
	return polardbx.Status.RestartingType == polardbxv1polardbx.RolingRestart
}
