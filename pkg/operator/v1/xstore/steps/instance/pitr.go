package instance

import (
	"encoding/json"
	"errors"
	"fmt"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"io"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"net/http"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"
)

var PreparePitrBinlogs = xstorev1reconcile.NewStepBinder("PreparePitrBinlogs",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		backup, err := rc.GetXStoreBackupByName(xstore.Spec.Restore.BackupSet)
		if err != nil {
			return flow.Error(err, "failed to get xstore backup by name", "xstore backup name", xstore.Spec.Restore.BackupSet, "", backup)
		}
		var job batchv1.Job
		jobName := NewJobName(xstore.Name)
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
					return flow.RetryErr(err, "failed to job", "jobName", jobName)
				}
			} else {
				return flow.Error(err, "failed to job", "jobName", jobName)
			}
		}
		return flow.Continue("PreparePitrBinlogs continue")
	},
)

var WaitPreparePitrBinlogs = xstorev1reconcile.NewStepBinder("WaitPreparePitrBinlogs",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		job, pod, err := GetJobAndPod(rc)
		if job != nil && job.Status.Failed > 0 {
			//change it to failed phase
			xstore := rc.MustGetXStore()
			xstore.Status.Phase = polardbxv1xstore.PhaseFailed
			rc.UpdateXStoreStatus()
			return flow.Error(errors.New("changed to failed phase"), "")
		}
		if err != nil {
			return flow.RetryErr(err, "failed to job and pod")
		}
		if pod.Status.Phase != corev1.PodRunning {
			return flow.Retry("wait for the pitr pod to be running")
		}
		port := k8shelper.MustGetPortFromContainer(
			k8shelper.MustGetContainerFromPod(pod, ContainerName),
			PortName,
		).ContainerPort
		endpoint := fmt.Sprintf("http://%s:%d", pod.Status.PodIP, port)
		lastErrUrl := endpoint + "/lastErr"
		client := http.Client{
			Timeout: 1 * time.Second,
		}
		rep, err := client.Get(lastErrUrl)
		if err != nil {
			return flow.RetryErr(err, "failed to request pitr  service ", "lastErrUrl", lastErrUrl)
		}
		if rep.StatusCode != http.StatusOK {
			return flow.RetryErr(err, "failed to request pitr  service ", "lastErrUrl", lastErrUrl, "response", rep)
		}
		defer rep.Body.Close()
		if rep.ContentLength != 0 {
			bodyContent, err := io.ReadAll(rep.Body)
			if err != nil {
				return flow.RetryErr(err, "failed to read body")
			}
			flow.Logger().Error(fmt.Errorf("lastErr %s", string(bodyContent)), "get lastErr from pitr service")
			xstore := rc.MustGetXStore()
			xstore.Status.Phase = polardbxv1xstore.PhaseFailed
			rc.UpdateXStoreStatus()
			return flow.Error(errors.New("changed to failed phase"), "")
		}

		if len(pod.Status.ContainerStatuses) > 0 {
			ready := true
			for _, containerStatus := range pod.Status.ContainerStatuses {
				ready = ready && containerStatus.Ready
			}
			if ready {
				xstore := rc.MustGetXStore()
				xstore.Status.PitrStatus = &polardbxv1xstore.PitrStatus{
					PrepareJobEndpoint: endpoint,
					Job:                job.Name,
				}
				xstore.Spec.Restore.PitrEndpoint = endpoint
				rc.MarkXStoreChanged()
				rc.UpdateXStoreStatus()
				return flow.Continue("The container is ready")
			}
		}
		return flow.Retry("The container is not ready")
	},
)

var CleanPreparePitrBinlogJob = xstorev1reconcile.NewStepBinder("CleanPreparePitrBinlogJob",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		if xstore.Status.PitrStatus != nil {
			pitrEndpoint := xstore.Status.PitrStatus.PrepareJobEndpoint
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
