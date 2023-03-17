package backupbinlog

import (
	"fmt"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/backupbinlog"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var ReconcileHeartbeatJob = polardbxv1reconcile.NewStepBinder("ReconcileHeartbeatJob", func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
	backupBinlog := rc.MustGetPolarDBXBackupBinlog()
	if backupBinlog.Status.ObservedGeneration < backupBinlog.Generation {
		polardbx := rc.MustGetPolarDBX()
		deployment, err := createHeartbeatJob(rc, backupBinlog, polardbx)
		if err != nil {
			flow.Logger().Error(err, "failed to create heartbeat job")
			return flow.Continue("ReconcileHeartbeatJob")
		}

		exist := true
		findDeployment := &appsv1.Deployment{}
		err = rc.Client().Get(rc.Context(), types.NamespacedName{
			Namespace: deployment.Namespace,
			Name:      deployment.Name,
		}, findDeployment)
		if err != nil {
			if apierrors.IsNotFound(err) {
				exist = false
			} else {
				return flow.RetryErr(err, "failed to find heartbeat job", "name", deployment.Name)
			}
		}
		if backupBinlog.Spec.PointInTimeRecover && !exist {
			err := rc.SetControllerRefAndCreate(deployment)
			if err != nil {
				return flow.RetryErr(err, "failed to create heartbeat job")
			}
		} else if !backupBinlog.Spec.PointInTimeRecover && exist {
			err := rc.Client().Delete(rc.Context(), deployment)
			if err != nil {
				return flow.RetryErr(err, "failed to delete heartbeat job")
			}
		}
	}
	return flow.Continue("ReconcileHeartbeatJob")
})

var TryDeleteHeartbeatJob = polardbxv1reconcile.NewStepBinder("TryDeleteHeartbeatJob", func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
	backupBinlog := rc.MustGetPolarDBXBackupBinlog()
	polardbx := rc.MustGetPolarDBX()
	deployment, err := createHeartbeatJob(rc, backupBinlog, polardbx)
	if err != nil {
		return flow.RetryErr(err, "ReconcileHeartbeatJob")
	}
	err = rc.Client().Delete(rc.Context(), deployment)
	if err != nil && !apierrors.IsNotFound(err) {
		return flow.RetryErr(err, "failed to delete heartbeat job", "name", deployment.Name)
	}
	return flow.Continue("DeleteHeartbeatJob")
})

func createHeartbeatJob(rc *polardbxv1reconcile.Context, backupBinlog *polardbxv1.PolarDBXBackupBinlog, polardbx *polardbxv1.PolarDBXCluster) (*appsv1.Deployment, error) {
	labels := map[string]string{
		meta.LabelJobType:      string(meta.HeartbeatJobType),
		meta.LabelName:         polardbx.Name,
		meta.LabelBackupBinlog: backupBinlog.Name,
	}
	heartbeatInterval, err := rc.Config().Backup().GetHeartbeatInterval()
	if err != nil {
		return nil, err
	}
	pod, err := rc.GetCNPod(polardbx)
	if err != nil {
		return nil, err
	}
	container := corev1.Container{
		Name:  "job",
		Image: rc.Config().Images().DefaultJobImage(),
		Env: []corev1.EnvVar{
			{
				Name:  backupbinlog.EnvHeartbeatHost,
				Value: polardbx.Name,
			},
			{
				Name:  backupbinlog.EnvHeartbeatPort,
				Value: "3306",
			},
			{
				Name:  backupbinlog.EnvHeartbeatUser,
				Value: "polardbx_root",
			},
			{
				Name:  backupbinlog.EnvHeartbeatInterval,
				Value: heartbeatInterval.String(),
			},
			{
				Name:  backupbinlog.EnvMaxRetryCount,
				Value: "10",
			},
			{
				Name: backupbinlog.EnvHeartbeatPassword,
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: polardbx.Name,
						},
						Key: "polardbx_root",
					},
				},
			},
		},
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("50m"),
				corev1.ResourceMemory: resource.MustParse("30Mi"),
			},
		},
	}
	var replicas int32 = 1
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s%s", rc.Config().Backup().GetHeartbeatJobNamePrefix(), polardbx.Name),
			Namespace: polardbx.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					ImagePullSecrets: pod.Spec.ImagePullSecrets,
					Containers:       []corev1.Container{container},
				},
			},
		},
	}
	return deployment, nil
}
