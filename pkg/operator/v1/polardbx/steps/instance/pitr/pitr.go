package pitr

import (
	"encoding/json"
	"errors"
	"fmt"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polarxv1polarx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/helper"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"net/http"
	"time"

	//"github.com/alibaba/polardbx-operator/pkg/pitr"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func IsPitrRestore(polardbx *polardbxv1.PolarDBXCluster) bool {
	if helper.IsPhaseIn(polardbx, polarxv1polarx.PhaseRestoring, polarxv1polarx.PhasePending) && polardbx.Spec.Restore != nil && polardbx.Spec.Restore.Time != "" {
		return true
	}
	return false
}

var LoadLatestBackupSetByTime = polardbxv1reconcile.NewStepBinder("LoadLatestBackupSetByTime",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		if polardbx.Spec.Restore.BackupSet == "" || len(polardbx.Spec.Restore.BackupSet) == 0 {
			backup, err := rc.GetLastCompletedPXCBackup(map[string]string{polardbxmeta.LabelName: polardbx.Spec.Restore.From.PolarBDXName}, rc.MustParseRestoreTime())
			if err != nil {
				return flow.Error(err, "failed to get last completed pxc backup")
			}
			polardbx.Spec.Restore.BackupSet = backup.Name
			rc.MarkPolarDBXChanged()
		}
		return flow.Continue("LoadLatestBackupSetByTime continue")
	},
)

var PreparePitrBinlogs = polardbxv1reconcile.NewStepBinder("PreparePitrBinlogs",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		backup, err := rc.GetPXCBackupByName(polardbx.Spec.Restore.BackupSet)
		if err != nil {
			return flow.Error(err, "failed to get pxc backup by name", "pxc backup name", polardbx.Spec.Restore.BackupSet, "", backup)
		}
		var job batchv1.Job
		jobName := NewJobName(polardbx.Name)
		err = rc.Client().Get(rc.Context(), types.NamespacedName{Namespace: rc.Namespace(), Name: jobName}, &job)
		if err != nil {
			if apierrors.IsNotFound(err) {
				flow.Logger().Info("to create pitr job")
				taskConfig, err := CreateTaskConfig(rc, backup)
				bytes, err := json.Marshal(taskConfig)
				if err != nil {
					return flow.Error(err, "failed to json marshal task config")
				}
				flow.Logger().Info(fmt.Sprintf("pitr task config %s", string(bytes)))
				if err != nil {
					return flow.RetryErr(err, "failed to task config")
				}
				job := CreatePrepareBinlogJob(rc, taskConfig)
				err = rc.SetControllerRefAndCreate(job)
				if err != nil {
					flow.RetryErr(err, "failed to job", "jobName", jobName)
				}
			} else {
				return flow.Error(err, "failed to job", "jobName", jobName)
			}
		}
		return flow.Continue("PreparePitrBinlogs continue")
	},
)

var WaitPreparePitrBinlogs = polardbxv1reconcile.NewStepBinder("WaitPreparePitrBinlogs",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		job, pod, err := GetJobAndPod(rc)
		if job != nil && job.Status.Failed > 0 {
			//change it to failed phase
			polardbxObj := rc.MustGetPolarDBX()
			polardbxObj.Status.Phase = polarxv1polarx.PhaseFailed
			rc.UpdatePolarDBXStatus()
			return flow.Error(errors.New("changed to failed phase"), "")
		}
		if err != nil {
			return flow.RetryErr(err, "failed to job and pod")
		}
		if len(pod.Status.ContainerStatuses) > 0 {
			ready := true
			for _, containerStatus := range pod.Status.ContainerStatuses {
				ready = ready && containerStatus.Ready
			}
			if ready {
				polardbxObj := rc.MustGetPolarDBX()
				port := k8shelper.MustGetPortFromContainer(
					k8shelper.MustGetContainerFromPod(pod, ContainerName),
					PortName,
				).ContainerPort
				polardbxObj.Status.PitrStatus = &polarxv1polarx.PitrStatus{
					PrepareJobEndpoint: fmt.Sprintf("http://%s:%d", pod.Status.PodIP, port),
					Job:                job.Name,
				}
				return flow.Continue("The container is ready")
			}
		}
		return flow.Retry("The container is not ready")
	},
)

var CleanPreparePitrBinlogJob = polardbxv1reconcile.NewStepBinder("CleanPreparePitrBinlogJob",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()
		if polardbx.Status.PitrStatus != nil {
			pitrEndpoint := polardbx.Status.PitrStatus.PrepareJobEndpoint
			if pitrEndpoint != "" {
				exitUrl := pitrEndpoint + "/exit"
				httpClient := http.Client{
					Timeout: 2 * time.Second,
				}
				_, err := httpClient.Get(pitrEndpoint + "/exit")
				if err != nil {
					flow.Logger().Error(err, fmt.Sprintf("fail send exit url = %s", exitUrl))
					go func() {
						time.Sleep(3 * time.Second)
						http.Get(exitUrl)
					}()
				}
			}
		}
		return flow.Continue("CleanPreparePitrBinlogJob continue")
	},
)
