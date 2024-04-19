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

package reconcile

import (
	"encoding/json"
	"errors"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/group"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoreconvention "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	dbutil "github.com/alibaba/polardbx-operator/pkg/util/database"
	"github.com/alibaba/polardbx-operator/pkg/util/name"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type BackupContext struct {
	*control.BaseReconcileContext
	xStoreContext              *Context
	xstore                     *polardbxv1.XStore
	xstoreBackup               *polardbxv1.XStoreBackup
	xstoreBackupChanged        bool
	xstoreBackupStatusSnapshot *polardbxv1.XStoreBackupStatus
	xstorePods                 []corev1.Pod
	xstoreTargetPod            *corev1.Pod
	xstoreBackupJob            *batchv1.Job
	xstoreCollectJob           *batchv1.Job
	xstoreBinlogBackupJob      *batchv1.Job
	polardbxBackup             *polardbxv1.PolarDBXBackup
	taskConfigMap              *corev1.ConfigMap
}

func (rc *BackupContext) SetControllerRef(obj metav1.Object) error {
	if obj == nil {
		return nil
	}
	backup := rc.MustGetXStoreBackup()
	return ctrl.SetControllerReference(backup, obj, rc.Scheme())
}

func (rc *BackupContext) SetControllerRefAndCreate(obj client.Object) error {
	if err := rc.SetControllerRef(obj); err != nil {
		return err
	}
	return rc.Client().Create(rc.Context(), obj)
}

func (rc *BackupContext) SetXStoreContext(xstoreContext *Context) {
	rc.xStoreContext = xstoreContext
}

func (rc *BackupContext) XStoreContext() *Context {
	return rc.xStoreContext
}

func (rc *BackupContext) MustGetXStoreBackup() *polardbxv1.XStoreBackup {
	xstoreBackup, err := rc.GetXStoreBackup()
	if err != nil {
		panic(err)
	}
	return xstoreBackup
}

func (rc *BackupContext) GetXStoreBackup() (*polardbxv1.XStoreBackup, error) {
	if rc.xstoreBackup == nil {
		var xstoreBackup polardbxv1.XStoreBackup
		err := rc.Client().Get(rc.Context(), rc.Request().NamespacedName, &xstoreBackup)
		if err != nil {
			return nil, err
		}
		rc.xstoreBackup = &xstoreBackup
		rc.xstoreBackupStatusSnapshot = rc.xstoreBackup.Status.DeepCopy()
	}
	return rc.xstoreBackup, nil
}

func (rc *BackupContext) GetPolarDBXBackup() (*polardbxv1.PolarDBXBackup, error) {
	if rc.polardbxBackup == nil {
		xstoreBackup, err := rc.GetXStoreBackup()
		if err != nil {
			return nil, err
		}
		var polardbxBackup polardbxv1.PolarDBXBackup
		pxcBackupKey := types.NamespacedName{
			Namespace: xstoreBackup.Namespace,
			Name:      xstoreBackup.Labels[meta.LabelTopBackup],
		}
		err = rc.Client().Get(rc.Context(), pxcBackupKey, &polardbxBackup)
		if err != nil {
			return nil, err
		}
		rc.polardbxBackup = &polardbxBackup
	}
	return rc.polardbxBackup, nil
}

func (rc *BackupContext) GetXStoreBackupJob() (*batchv1.Job, error) {
	if rc.xstoreBackupJob == nil {
		xstoreBackup := rc.MustGetXStoreBackup()

		var jobList batchv1.JobList
		err := rc.Client().List(rc.Context(), &jobList, client.InNamespace(rc.Request().Namespace),
			client.MatchingLabels{
				xstoremeta.LabelXStoreBackupName: xstoreBackup.Name,
			})
		if err != nil {
			return nil, err
		}

		if len(jobList.Items) == 0 {
			return nil, nil
		}

		ownedJobs := make([]*batchv1.Job, 0)
		for i := range jobList.Items {
			job := &jobList.Items[i]
			if err = k8shelper.CheckControllerReference(job, xstoreBackup); err == nil {
				ownedJobs = append(ownedJobs, job)
			}
		}

		if len(ownedJobs) == 0 {
			return nil, nil
		}

		if len(ownedJobs) > 1 {
			panic("multiple owned jobs found, must not happen")
		}

		rc.xstoreBackupJob = ownedJobs[0]
	}
	return rc.xstoreBackupJob, nil
}

func (rc *BackupContext) GetXStore() (*polardbxv1.XStore, error) {
	if rc.xstore == nil {
		backup := rc.MustGetXStoreBackup()
		var xstore polardbxv1.XStore
		xstoreSpec := types.NamespacedName{Namespace: rc.Request().Namespace, Name: backup.Spec.XStore.Name}
		err := rc.Client().Get(rc.Context(), xstoreSpec, &xstore)
		if err != nil {
			return nil, err
		}
		rc.xstore = &xstore
	}
	return rc.xstore, nil
}

func (rc *BackupContext) MarkXstoreBackupChanged() {
	rc.xstoreBackupChanged = true
}

func (rc *BackupContext) IsXstoreBackupChanged() bool {
	return rc.xstoreBackupChanged
}

// UpdateXStoreBackup only updates spec and replaces the status with the value from server
func (rc *BackupContext) UpdateXStoreBackup() error {
	if rc.xstoreBackup == nil {
		return nil
	}

	// Deep copy status before updating because client.update will update
	// the status of object.
	status := rc.xstoreBackup.Status.DeepCopy()
	err := rc.Client().Update(rc.Context(), rc.xstoreBackup)
	if err != nil {
		return err
	}

	// Restore the status (shallow copy is enough)
	rc.xstoreBackup.Status = *status

	return nil
}

func (rc *BackupContext) UpdateXStoreBackupStatus() error {
	if rc.xstoreBackupStatusSnapshot == nil {
		return nil
	}
	err := rc.Client().Status().Update(rc.Context(), rc.xstoreBackup)
	if err != nil {
		return err
	}
	rc.xstoreBackupStatusSnapshot = rc.xstoreBackup.Status.DeepCopy()
	return nil
}

func (rc *BackupContext) IsXStoreBackupStatusChanged() bool {
	if rc.xstoreBackupStatusSnapshot == nil {
		return false
	}
	return !equality.Semantic.DeepEqual(rc.xstoreBackup.Status, *rc.xstoreBackupStatusSnapshot)
}

func (rc *BackupContext) GetXStorePods() ([]corev1.Pod, error) {
	xstore, err := rc.GetXStore()
	if err != nil {
		return nil, err
	}

	matchingLabels := client.MatchingLabels{
		xstoremeta.LabelName: xstore.Name,
	}
	if len(xstore.Status.Rand) > 0 {
		matchingLabels[xstoremeta.LabelRand] = xstore.Status.Rand
	}

	if rc.xstorePods == nil {
		var xstorePods corev1.PodList
		err := rc.Client().List(rc.Context(), &xstorePods,
			client.InNamespace(rc.Namespace()), matchingLabels)
		if err != nil {
			return nil, err
		}

		pods := make([]corev1.Pod, 0, len(xstorePods.Items))
		for _, pod := range xstorePods.Items {
			if err = k8shelper.CheckControllerReference(&pod, xstore); err != nil {
				continue
			} else {
				pods = append(pods, pod)
			}
		}
		rc.xstorePods = pods
	}
	return rc.xstorePods, nil
}

func (rc *BackupContext) GetXStoreTargetPod() (*corev1.Pod, error) {
	if rc.xstoreTargetPod == nil {
		xstoreBackup := rc.MustGetXStoreBackup()

		// Find from status or job label
		targetPod := xstoreBackup.Status.TargetPod
		if len(targetPod) == 0 {
			job, err := rc.GetXStoreBackupJob()
			if err != nil {
				return nil, err
			}
			if job != nil {
				targetPod = job.Labels[xstoremeta.JobLabelTargetPod]
			}
		}

		if len(targetPod) > 0 {
			var pod corev1.Pod
			targetPodName := types.NamespacedName{Namespace: rc.Namespace(), Name: targetPod}
			err := rc.Client().Get(rc.Context(), targetPodName, &pod)
			if err != nil {
				return nil, err
			}
			rc.xstoreTargetPod = &pod
			return rc.xstoreTargetPod, nil
		}

		// TODO: Take health info and delay into consideration
		// set target pod for XStoreBackup on which backup will be performed
		// priority of the target pod: choice made by user > follower > leader
		pods, err := rc.GetXStorePods()
		if err != nil {
			return nil, err
		}

		rolePodMap := make(map[string]*corev1.Pod)
		for i := range pods {
			pod := &pods[i]
			rolePodMap[pod.Labels[xstoremeta.LabelRole]] = pod
		}

		if xstoreBackup.Spec.PreferredBackupRole == xstoremeta.RoleLeader { // preferred backup node is leader, just set it
			pod, ok := rolePodMap[xstoremeta.RoleLeader]
			if !ok {
				return nil, errors.New("target pod is leader, but leader not found")
			}
			rc.xstoreTargetPod = pod
			return pod, nil
		}

		// if `PreferredBackupRole` has not been set or set to something other than leader, then we pick follower as backup pod
		pod, ok := rolePodMap[xstoremeta.RoleFollower]
		if !ok {
			return nil, errors.New("target pod is follower, but follower not found")
		}
		manager, err := rc.GetXstoreGroupManagerByPod(pod)
		if err != nil {
			return nil, err
		}
		if manager == nil {
			return nil, errors.New("fail to connect to follower")
		}
		defer manager.Close()

		status, err := manager.ShowSlaveStatus()
		if err != nil {
			return nil, err
		}
		if status.SlaveSQLRunning == "No" || status.LastError != "" {
			return nil, errors.New("follower status abnormal, SlaveSQLRunning: " + status.SlaveSQLRunning +
				", LastError: " + status.LastError)
		}
		rc.xstoreTargetPod = pod
	}
	return rc.xstoreTargetPod, nil
}

func (rc *BackupContext) GetCollectBinlogJob() (*batchv1.Job, error) {
	if rc.xstoreCollectJob == nil {
		xstoreBackup := rc.MustGetXStoreBackup()

		var jobList batchv1.JobList
		err := rc.Client().List(rc.Context(), &jobList, client.InNamespace(rc.Request().Namespace),
			client.MatchingLabels{
				xstoremeta.LabelXStoreCollectName: xstoreBackup.Name,
			})
		if err != nil {
			return nil, err
		}

		if len(jobList.Items) == 0 {
			return nil, nil
		}

		ownedJobs := make([]*batchv1.Job, 0)
		for i := range jobList.Items {
			job := &jobList.Items[i]
			if err = k8shelper.CheckControllerReference(job, xstoreBackup); err == nil {
				ownedJobs = append(ownedJobs, job)
			}
		}

		if len(ownedJobs) == 0 {
			return nil, nil
		}

		if len(ownedJobs) > 1 {
			panic("multiple owned jobs found, must not happen")
		}

		rc.xstoreCollectJob = ownedJobs[0]
	}
	return rc.xstoreCollectJob, nil
}

func (rc *BackupContext) GetBackupBinlogJob() (*batchv1.Job, error) {
	if rc.xstoreBinlogBackupJob == nil {
		xstoreBackup := rc.MustGetXStoreBackup()

		var jobList batchv1.JobList
		err := rc.Client().List(rc.Context(), &jobList, client.InNamespace(rc.Request().Namespace),
			client.MatchingLabels{
				xstoremeta.LabelXStoreBinlogBackupName: xstoreBackup.Name,
			})
		if err != nil {
			return nil, err
		}

		if len(jobList.Items) == 0 {
			return nil, nil
		}

		ownedJobs := make([]*batchv1.Job, 0)
		for i := range jobList.Items {
			job := &jobList.Items[i]
			if err = k8shelper.CheckControllerReference(job, xstoreBackup); err == nil {
				ownedJobs = append(ownedJobs, job)
			}
		}

		if len(ownedJobs) == 0 {
			return nil, nil
		}

		if len(ownedJobs) > 1 {
			panic("multiple owned jobs found, must not happen")
		}

		rc.xstoreBinlogBackupJob = ownedJobs[0]
	}
	return rc.xstoreBinlogBackupJob, nil
}

func NewBackupContext(base *control.BaseReconcileContext) *BackupContext {
	return &BackupContext{
		BaseReconcileContext: base,
	}
}

func (rc *BackupContext) GetOrCreateXStoreBackupTaskConfigMap() (*corev1.ConfigMap, error) {
	if rc.taskConfigMap == nil {
		xstorebackup := rc.MustGetXStoreBackup()

		var cm corev1.ConfigMap
		err := rc.Client().Get(rc.Context(), types.NamespacedName{Namespace: rc.Namespace(), Name: name.XStoreBackupStableName(xstorebackup, "backup")}, &cm)
		if err != nil {
			if apierrors.IsNotFound(err) {
				rc.taskConfigMap = NewBackupTaskConfigMap(xstorebackup)
				err = rc.SetControllerRefAndCreate(rc.taskConfigMap)
				if err != nil {
					return nil, err
				}
				return rc.taskConfigMap, nil
			}
			return nil, err
		}

		rc.taskConfigMap = &cm
	}
	return rc.taskConfigMap, nil
}

func NewBackupTaskConfigMap(xstoreBackup *polardbxv1.XStoreBackup) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      convention.NewBackupConfigMapName(xstoreBackup, "backup"),
			Namespace: xstoreBackup.Namespace,
		},
		Immutable: pointer.Bool(false),
	}
}

func (rc *BackupContext) SaveTaskContext(key string, t interface{}) error {
	b, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err
	}

	cm, err := rc.GetOrCreateXStoreBackupTaskConfigMap()
	if err != nil {
		return err
	}

	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}

	if s, ok := cm.Data[key]; ok {
		if s == string(b) {
			return nil
		}
	}

	cm.Data[key] = string(b)
	return rc.Client().Update(rc.Context(), cm)
}

func (rc *BackupContext) IsTaskContextExists(key string) (bool, error) {
	cm, err := rc.GetOrCreateXStoreBackupTaskConfigMap()
	if err != nil {
		return false, err
	}
	_, ok := cm.Data[key]
	return ok, nil
}

func (rc *BackupContext) GetTaskContext(key string, t interface{}) error {
	cm, err := rc.GetOrCreateXStoreBackupTaskConfigMap()
	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(cm.Data[key]), t)
}

func (rc *BackupContext) GetSecret(name string) (*corev1.Secret, error) {
	secretKey := types.NamespacedName{
		Namespace: rc.Namespace(),
		Name:      name,
	}
	secret := &corev1.Secret{}
	err := rc.Client().Get(rc.Context(), secretKey, secret)
	if err != nil {
		return nil, err
	}
	return secret, nil
}

func (rc *BackupContext) NewSecretFromXStore(secret *corev1.Secret) (*corev1.Secret, error) {
	backup := rc.MustGetXStoreBackup()
	data := make(map[string][]byte)
	for user, passwd := range secret.Data {
		data[user] = passwd
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backup.Name,
			Namespace: backup.Namespace,
		},
		Data: data,
	}, nil
}

func (rc *BackupContext) GetXstoreGroupManagerByPod(pod *corev1.Pod) (group.GroupManager, error) {
	host := pod.Status.PodIP
	port := k8shelper.MustGetPortFromContainer(
		k8shelper.MustGetContainerFromPod(pod, convention.ContainerEngine),
		convention.PortAccess,
	).ContainerPort
	secret, err := rc.GetSecret(pod.Labels[xstoremeta.LabelName])
	if err != nil {
		return nil, err
	}
	user := xstoreconvention.SuperAccount
	passwd, err := rc.xStoreContext.GetXstoreAccountPasswordFromSecret(user, secret)
	if err != nil {
		return nil, err
	}
	return group.NewGroupManager(
		rc.Context(),
		dbutil.MySQLDataSource{
			Host:     host,
			Port:     int(port),
			Username: xstoreconvention.SuperAccount,
			Password: passwd,
		},
		true,
	), nil
}

func (rc *BackupContext) GetXStoreIsStandard() (bool, error) {
	xstore, err := rc.GetXStore()
	if err != nil {
		return false, err
	}
	isStandard := false
	if _, ok := xstore.Labels[polardbxmeta.LabelName]; !ok {
		isStandard = true
	}
	return isStandard, nil
}
