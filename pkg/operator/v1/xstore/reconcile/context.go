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
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms"
	"github.com/alibaba/polardbx-operator/pkg/util"
	"k8s.io/utils/pointer"
	"strings"
	"time"

	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"

	"google.golang.org/grpc"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	hpfs "github.com/alibaba/polardbx-operator/pkg/hpfs/proto"
	"github.com/alibaba/polardbx-operator/pkg/k8s/cache"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
)

type Context struct {
	*control.BaseReconcileContext

	// Caches
	xstoreKey     types.NamespacedName
	xstoreChanged bool
	xstore        *polardbxv1.XStore
	primaryXstore *polardbxv1.XStore
	xstoreStatus  *polardbxv1.XStoreStatus
	pods          []corev1.Pod
	podServices   map[string]corev1.Service
	nodes         []corev1.Node
	objectCache   cache.ObjectLoadingCache

	// Hint cache
	controllerHints []string

	// Hpfs
	hpfsConn   *grpc.ClientConn
	hpfsClient hpfs.HpfsServiceClient

	// Config
	configLoader func() config.Config

	taskConfigMap *corev1.ConfigMap

	// Parameter Template
	polardbxParameterTemplate *polardbxv1.PolarDBXParameterTemplate
	polardbxTemplateParams    map[string]map[string]polardbxv1.TemplateParams
	polardbxParamsRoleMap     map[string]map[string]polardbxv1.Params

	polardbxParameter       *polardbxv1.PolarDBXParameter
	polardbxParameterKey    types.NamespacedName
	polardbxParameterStatus *polardbxv1.PolarDBXParameterStatus
}

func (rc *Context) Debug() bool {
	if rc.BaseReconcileContext.Debug() {
		return true
	}
	r, _ := rc.containsControllerHint("debug")
	return r
}

func (rc *Context) GetNodes() ([]corev1.Node, error) {
	if rc.nodes == nil {
		var nodeList corev1.NodeList
		err := rc.Client().List(rc.Context(), &nodeList)
		if err != nil {
			return nil, err
		}
		rc.nodes = nodeList.Items
	}
	return rc.nodes, nil
}

func (rc *Context) SetXStoreKey(key types.NamespacedName) {
	rc.xstoreKey = key
}

func (rc *Context) CountCreatingLearners() (int, error) {
	xstore := rc.MustGetXStore()

	var readonlyXstoreList polardbxv1.XStoreList

	err := rc.Client().List(
		rc.Context(),
		&readonlyXstoreList,
		client.MatchingLabels(
			k8shelper.PatchLabels(
				map[string]string{
					xstoremeta.LabelPrimaryName: xstore.Name,
				},
			),
		),
	)

	if err != nil {
		return 0, fmt.Errorf("failed to fetch readonly xstore list: %w", err)
	}

	creatingCnt := 0

	for _, readonlyXstore := range readonlyXstoreList.Items {
		if readonlyXstore.Status.Phase == polardbxv1xstore.PhaseCreating {
			creatingCnt++
		}
	}

	return creatingCnt, nil
}

func (rc *Context) GetPrimaryXStore() (*polardbxv1.XStore, error) {
	xstore := rc.MustGetXStore()

	if !xstore.Spec.Readonly {
		return nil, errors.New("current xstore is not readonly")
	}

	if rc.primaryXstore == nil {
		primaryXStoreKey := types.NamespacedName{
			Namespace: rc.xstoreKey.Namespace,
			Name:      xstore.Spec.PrimaryXStore,
		}
		primaryXstore, err := rc.objectCache.GetObject(
			rc.Context(),
			primaryXStoreKey,
			&polardbxv1.XStore{})
		if err != nil {
			return nil, err
		}
		rc.primaryXstore = primaryXstore.(*polardbxv1.XStore)
		// TODO: maintain primaryXstoreStatus
	}
	return rc.primaryXstore, nil
}

func (rc *Context) MustGetPrimaryXStore() *polardbxv1.XStore {
	xstore, err := rc.GetPrimaryXStore()
	if err != nil {
		panic(err)
	}
	return xstore
}

func (rc *Context) GetXStore() (*polardbxv1.XStore, error) {
	if rc.xstore == nil {
		xstore, err := rc.objectCache.GetObject(
			rc.Context(),
			rc.xstoreKey,
			&polardbxv1.XStore{})
		if err != nil {
			return nil, err
		}
		rc.xstore = xstore.(*polardbxv1.XStore)
		rc.xstoreStatus = rc.xstore.Status.DeepCopy()
	}
	return rc.xstore, nil
}

func (rc *Context) MustGetXStore() *polardbxv1.XStore {
	xstore, err := rc.GetXStore()
	if err != nil {
		panic(err)
	}
	return xstore
}

func (rc *Context) containsControllerHint(hint string) (bool, error) {
	if rc.controllerHints == nil {
		xstore, err := rc.GetXStore()
		if err != nil {
			return false, err
		}

		val, ok := xstore.Annotations[xstoremeta.AnnotationControllerHints]
		rc.controllerHints = []string{}
		if ok {
			for _, v := range strings.Split(val, ",") {
				rc.controllerHints = append(rc.controllerHints, strings.TrimSpace(v))
			}
		}
	}

	for _, h := range rc.controllerHints {
		if h == hint {
			return true, nil
		}
	}
	return false, nil
}

func (rc *Context) ContainsControllerHint(hint string) bool {
	r, err := rc.containsControllerHint(hint)
	if err != nil {
		panic(err)
	}
	return r
}

func (rc *Context) SetControllerRef(obj client.Object) error {
	if obj == nil {
		return nil
	}
	xstore := rc.MustGetXStore()
	return ctrl.SetControllerReference(xstore, obj, rc.Scheme())
}

func (rc *Context) SetControllerRefAndCreate(obj client.Object) error {
	if err := rc.SetControllerRef(obj); err != nil {
		return err
	}
	return rc.Client().Create(rc.Context(), obj)
}

func (rc *Context) IsXStoreStatusChanged() bool {
	if rc.xstoreStatus == nil {
		return false
	}
	return !equality.Semantic.DeepEqual(&rc.xstore.Status, rc.xstoreStatus)
}

func (rc *Context) IsXStoreChanged() bool {
	return rc.xstoreChanged
}

func (rc *Context) MarkXStoreChanged() {
	rc.xstoreChanged = true
}

func (rc *Context) UpdateXStore() error {
	if rc.xstore == nil {
		return nil
	}

	// Deep copy status before updating because client.update will update
	// the status of object.
	status := rc.xstore.Status.DeepCopy()
	err := rc.Client().Update(rc.Context(), rc.xstore)
	if err != nil {
		return err
	}

	// Restore the status (shallow copy is enough)
	rc.xstore.Status = *status

	return nil
}

func (rc *Context) UpdateXStoreStatus() error {
	if rc.xstoreStatus == nil {
		return nil
	}

	err := rc.Client().Status().Update(rc.Context(), rc.xstore)
	if err != nil {
		return err
	}
	rc.xstoreStatus = rc.xstore.Status.DeepCopy()
	return nil
}

func (rc *Context) GetXStoreService(serviceType convention.ServiceType) (*corev1.Service, error) {
	xstore, err := rc.GetXStore()
	if err != nil {
		return nil, fmt.Errorf("failed to get xstore object: %w", err)
	}

	serviceKey := types.NamespacedName{
		Name:      convention.NewServiceName(xstore, serviceType),
		Namespace: rc.xstoreKey.Namespace,
	}

	service, err := rc.objectCache.GetObject(rc.Context(), serviceKey, &corev1.Service{})
	if err != nil {
		return nil, err
	}

	if err := k8shelper.CheckControllerReference(service, rc.MustGetXStore()); err != nil {
		return nil, err
	}

	return service.(*corev1.Service), nil
}

func (rc *Context) GetXStoreClusterAddr(serviceType convention.ServiceType, port string) (string, error) {
	svc, err := rc.GetXStoreService(serviceType)
	if err != nil {
		return "", err
	}
	return k8shelper.GetClusterAddrFromService(svc, port)
}

func (rc *Context) GetXStoreSecret() (*corev1.Secret, error) {
	xstore, err := rc.GetXStore()
	if err != nil {
		return nil, fmt.Errorf("failed to get xstore object: %w", err)
	}
	if xstore.Spec.Readonly {
		xstore, err = rc.GetPrimaryXStore()
		if err != nil {
			return nil, err
		}
	}

	secretKey := types.NamespacedName{Namespace: xstore.Namespace, Name: convention.NewSecretName(xstore)}
	secret, err := rc.objectCache.GetObject(rc.Context(), secretKey, &corev1.Secret{})
	if err != nil {
		return nil, err
	}
	if err := k8shelper.CheckControllerReference(secret, xstore); err != nil {
		return nil, err
	}
	return secret.(*corev1.Secret), nil
}

func (rc *Context) CreateSecretByXStore(xstore *polardbxv1.XStore) (*corev1.Secret, error) {
	secretKey := types.NamespacedName{}
	if xstore.Spec.Restore.BackupSet == "" || len(xstore.Spec.Restore.BackupSet) == 0 {
		backup, err := rc.GetLastCompletedXStoreBackup(map[string]string{
			xstoremeta.LabelName: xstore.Spec.Restore.From.XStoreName,
		}, rc.MustParseRestoreTime())
		if err != nil {
			return nil, err
		}
		secretKey = types.NamespacedName{Namespace: rc.Namespace(), Name: backup.Name}
	} else {
		secretKey = types.NamespacedName{Namespace: rc.Namespace(), Name: xstore.Spec.Restore.BackupSet}
	}
	secret, err := rc.objectCache.GetObject(rc.Context(), secretKey, &corev1.Secret{})
	if err != nil {
		return nil, err
	}
	data := make(map[string][]byte)
	for user, passwd := range secret.(*corev1.Secret).Data {
		data[user] = passwd
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      convention.NewSecretName(xstore),
			Namespace: xstore.Namespace,
			Labels:    convention.ConstLabels(xstore),
		},
		Immutable: pointer.Bool(true),
		Type:      corev1.SecretTypeOpaque,
		Data:      data,
	}, nil
}

func (rc *Context) GetXStoreAccountPassword(user string) (string, error) {
	secret, err := rc.GetXStoreSecret()
	if err != nil {
		return "", err
	}
	passwd, ok := secret.Data[user]
	if !ok {
		return "", errors.New("not found")
	}
	return string(passwd), nil
}

func (rc *Context) GetConfigMap(name string) (*corev1.ConfigMap, error) {
	cmKey := types.NamespacedName{
		Namespace: rc.xstoreKey.Namespace,
		Name:      name,
	}
	cm, err := rc.objectCache.GetObject(rc.Context(), cmKey, &corev1.ConfigMap{})
	if err != nil {
		return nil, err
	}
	return cm.(*corev1.ConfigMap), nil
}

func (rc *Context) GetXStoreConfigMap(cmType convention.ConfigMapType) (*corev1.ConfigMap, error) {
	xstore := rc.MustGetXStore()

	configMapKey := types.NamespacedName{
		Namespace: rc.xstoreKey.Namespace,
		Name:      convention.NewConfigMapName(xstore, cmType),
	}

	cm, err := rc.objectCache.GetObject(rc.Context(), configMapKey, &corev1.ConfigMap{})
	if err != nil {
		return nil, err
	}

	if err := k8shelper.CheckControllerReference(cm, rc.MustGetXStore()); err != nil {
		return nil, err
	}

	return cm.(*corev1.ConfigMap), nil
}

func (rc *Context) GetXStorePodByPrefix(name string) (*corev1.Pod, error) {
	xstore := rc.MustGetXStore()
	pods, err := rc.GetXStorePods()
	if err != nil {
		return nil, err
	}
	for _, pod := range pods {
		if strings.HasPrefix(pod.Name, convention.NewJobName(xstore, name)) {
			return &pod, nil
		}
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{}, "")
}

func (rc *Context) GetXStorePod(name string) (*corev1.Pod, error) {
	pods, err := rc.GetXStorePods()
	if err != nil {
		return nil, err
	}
	for _, pod := range pods {
		if pod.Name == name {
			return &pod, nil
		}
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{}, "")
}

func (rc *Context) getXStorePods(xstore *polardbxv1.XStore) ([]corev1.Pod, error) {
	podLabels := convention.ConstLabels(xstore)

	var podList corev1.PodList
	err := rc.Client().List(rc.Context(), &podList, client.InNamespace(rc.Namespace()),
		client.MatchingLabels(podLabels))
	if err != nil {
		return nil, err
	}

	// Branch pod isn't owned by this xstore, just ignore it.
	pods := make([]corev1.Pod, 0, len(podList.Items))
	for _, pod := range podList.Items {
		if err := k8shelper.CheckControllerReference(&pod, xstore); err != nil {
			continue
		}
		pods = append(pods, pod)
	}

	return pods, nil
}

func (rc *Context) GetXStorePods() ([]corev1.Pod, error) {
	if rc.pods == nil {
		xstore := rc.MustGetXStore()
		pods, err := rc.getXStorePods(xstore)
		if err != nil {
			return nil, err
		}

		rc.pods = pods
	}

	return rc.pods, nil
}

func (rc *Context) GetPrimaryXStorePods() ([]corev1.Pod, error) {
	xstore, err := rc.GetPrimaryXStore()
	if err != nil {
		return nil, err
	}
	pods, err := rc.getXStorePods(xstore)
	if err != nil {
		return nil, err
	}

	return pods, nil
}

func (rc *Context) GetXStoreServiceForPod(pod string) (*corev1.Service, error) {
	svcList, err := rc.GetXStorePodServices()
	if err != nil {
		return nil, err
	}
	svc, ok := svcList[pod]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{}, "")
	}
	return &svc, nil
}

func (rc *Context) GetXStorePodServices() (map[string]corev1.Service, error) {
	if rc.podServices == nil {
		svcLabels := convention.ConstLabels(rc.MustGetXStore())
		svcLabels = k8shelper.PatchLabels(svcLabels, map[string]string{
			xstoremeta.LabelServiceType: string(convention.ServiceTypeClusterIp),
		})

		var svcList corev1.ServiceList
		err := rc.Client().List(rc.Context(), &svcList,
			client.InNamespace(rc.Namespace()),
			client.MatchingLabels(svcLabels))
		if err != nil {
			return nil, err
		}

		podServices := make(map[string]corev1.Service)
		for _, svc := range svcList.Items {
			// Ignore not owned
			if err := k8shelper.CheckControllerReference(&svc, rc.MustGetXStore()); err != nil {
				continue
			}

			pod, ok := svc.Labels[xstoremeta.LabelPod]
			// Ignore without labels.
			if !ok {
				continue
			}
			podServices[pod] = svc
		}
		rc.podServices = podServices
	}
	return rc.podServices, nil
}

func (rc *Context) TryGetXStoreLeaderPod() (*corev1.Pod, error) {
	xstore := rc.MustGetXStore()
	readonly := xstore.Spec.Readonly
	var err error

	if readonly {
		xstore, err = rc.GetPrimaryXStore()
		if err != nil {
			return nil, err
		}
	}

	leaderPodName := xstore.Status.LeaderPod

	var pods []corev1.Pod

	if readonly {
		pods, err = rc.GetPrimaryXStorePods()
	} else {
		pods, err = rc.GetXStorePods()
	}
	if err != nil {
		return nil, err
	}

	if len(leaderPodName) > 0 {
		for i := range pods {
			if pods[i].Name == leaderPodName {
				return &pods[i], nil
			}
		}
		return nil, nil
	} else {
		for i := range pods {
			if role, ok := pods[i].Labels[xstoremeta.LabelRole]; ok && role == "leader" {
				return &pods[i], nil
			}
		}
		return nil, nil
	}
}

func (rc *Context) GetXStoreJob(jobName string) (*batchv1.Job, error) {
	xstore := rc.MustGetXStore()
	jobKey := types.NamespacedName{
		Namespace: xstore.Namespace,
		Name:      convention.NewJobName(xstore, jobName),
	}
	job, err := rc.objectCache.GetObject(rc.Context(), jobKey, &batchv1.Job{})
	if err != nil {
		return nil, err
	}
	if err := k8shelper.CheckControllerReference(job, rc.MustGetXStore()); err != nil {
		return nil, err
	}
	return job.(*batchv1.Job), nil
}

func (rc *Context) GetHpfsClient() (hpfs.HpfsServiceClient, error) {
	if rc.hpfsConn == nil {
		hpfsConn, err := grpc.Dial(rc.Config().Store().HostPathFileServiceEndpoint(), grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		rc.hpfsConn = hpfsConn
		rc.hpfsClient = hpfs.NewHpfsServiceClient(rc.hpfsConn)
	}

	return rc.hpfsClient, nil
}

func (rc *Context) UpdateXStoreCondition(cond *polardbxv1xstore.Condition) {
	if cond == nil {
		return
	}

	// Set condition's time
	now := metav1.Now()
	cond.LastProbeTime = nil
	cond.LastTransitionTime = now

	xstore := rc.MustGetXStore()
	if xstore.Status.Conditions == nil {
		xstore.Status.Conditions = []polardbxv1xstore.Condition{*cond}
		return
	}

	for i := range xstore.Status.Conditions {
		c := &xstore.Status.Conditions[i]
		// Branch same type found
		if c.Type == cond.Type {
			transition := c.Status != cond.Status
			if !transition {
				cond.LastTransitionTime = c.LastTransitionTime
				cond.Reason = c.Reason
				cond.Message = c.Message
			}
			cond.DeepCopyInto(c)
			return
		}
	}

	// Handle condition type not found
	xstore.Status.Conditions = append(xstore.Status.Conditions, *cond)
}

func (rc *Context) Config() config.Config {
	return rc.configLoader()
}

func (rc *Context) Close() error {
	errs := make([]error, 0)
	if rc.hpfsConn != nil {
		hpfsConn := rc.hpfsConn
		rc.hpfsConn = nil
		rc.hpfsClient = nil
		if err := hpfsConn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := rc.BaseReconcileContext.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (rc *Context) ParseRestoreTime() (time.Time, error) {
	xcluster := rc.MustGetXStore()
	if xcluster.Spec.Restore == nil {
		return time.Time{}, nil
	}

	location, err := time.LoadLocation(gms.StrOrDefault(xcluster.Spec.Restore.TimeZone, "Asia/Shanghai"))
	if err != nil {
		return time.Time{}, nil
	}

	return time.ParseInLocation("2006-01-02 15:04:05", xcluster.Spec.Restore.Time, location)
}

func (rc *Context) MustParseRestoreTime() time.Time {
	t, err := rc.ParseRestoreTime()
	if err != nil {
		panic(err)
	}
	return t
}

func (rc *Context) GetLastCompletedXStoreBackup(matchLabels map[string]string, beforeTime time.Time) (*polardbxv1.XStoreBackup, error) {
	xstoreBackupList := &polardbxv1.XStoreBackupList{}
	err := rc.Client().List(rc.Context(), xstoreBackupList, client.InNamespace(rc.Namespace()),
		client.MatchingLabels(matchLabels))
	if err != nil || len(xstoreBackupList.Items) == 0 {
		return nil, err
	}

	// Get the last backup, skip those not finished and before specified time
	var lastBackup *polardbxv1.XStoreBackup = nil
	var lastBackupStartTime *time.Time = nil
	for i := range xstoreBackupList.Items {
		backup := &xstoreBackupList.Items[i]
		if backup.Status.Phase != polardbxv1.XStoreBackupFinished {
			continue
		}
		if backup.Status.EndTime.After(beforeTime) {
			continue
		}
		if lastBackupStartTime == nil ||
			lastBackupStartTime.Before(backup.Status.StartTime.Time) {
			lastBackupStartTime = &backup.Status.StartTime.Time
			lastBackup = backup
		}
	}

	return lastBackup, nil
}

func (rc *Context) GetOrCreateXStoreTaskConfigMap() (*corev1.ConfigMap, error) {
	if rc.taskConfigMap == nil {
		xstore := rc.MustGetXStore()

		var cm corev1.ConfigMap
		err := rc.Client().Get(rc.Context(), types.NamespacedName{Namespace: rc.Namespace(), Name: util.StableName(xstore, "restore")}, &cm)
		if err != nil {
			if apierrors.IsNotFound(err) {
				rc.taskConfigMap = NewTaskConfigMap(xstore)
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

func (rc *Context) SaveTaskContext(key string, t interface{}) error {
	b, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err
	}

	cm, err := rc.GetOrCreateXStoreTaskConfigMap()
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

func (rc *Context) IsTaskContextExists(key string) (bool, error) {
	cm, err := rc.GetOrCreateXStoreTaskConfigMap()
	if err != nil {
		return false, err
	}
	_, ok := cm.Data[key]
	return ok, nil
}

func (rc *Context) GetTaskContext(key string, t interface{}) error {
	cm, err := rc.GetOrCreateXStoreTaskConfigMap()
	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(cm.Data[key]), t)
}

func (rc *Context) GetPolarDBXTemplateName() (parameterTemplateName string) {
	xStore := rc.MustGetXStore()
	return xStore.Spec.ParameterTemplate.Name
}

func (rc *Context) GetPolarDBXParameterTemplate(name string) (*polardbxv1.PolarDBXParameterTemplate, error) {
	tmKey := types.NamespacedName{
		Namespace: rc.xstoreKey.Namespace,
		Name:      name,
	}
	if rc.polardbxParameterTemplate == nil {
		polardbxParameterTemplate, err := rc.objectCache.GetObject(
			rc.Context(),
			tmKey,
			&polardbxv1.PolarDBXParameterTemplate{},
		)
		if err != nil {
			return nil, err
		}
		rc.polardbxParameterTemplate = polardbxParameterTemplate.(*polardbxv1.PolarDBXParameterTemplate)
	}
	return rc.polardbxParameterTemplate, nil
}

func (rc *Context) GetPolarDBXParameter() (*polardbxv1.PolarDBXParameter, error) {
	if rc.polardbxParameter == nil {
		polardbxParameter, err := rc.objectCache.GetObject(
			rc.Context(),
			rc.polardbxParameterKey,
			&polardbxv1.PolarDBXParameter{},
		)
		if err != nil {
			return nil, err
		}
		rc.polardbxParameter = polardbxParameter.(*polardbxv1.PolarDBXParameter)
		rc.polardbxParameterStatus = rc.polardbxParameter.Status.DeepCopy()
	}
	return rc.polardbxParameter, nil
}

func (rc *Context) MustGetPolarDBXParameter() *polardbxv1.PolarDBXParameter {
	polardbxparameter, err := rc.GetPolarDBXParameter()
	if err != nil {
		panic(err)
	}
	return polardbxparameter
}

func (rc *Context) GetXStoreRestarting() bool {
	xstore := rc.MustGetXStore()
	return xstore.Status.Restarting
}

func (rc *Context) GetXStoreUpdateConfingMap() bool {
	xstore := rc.MustGetXStore()
	return xstore.Status.UpdateConfigMap
}

func (rc *Context) SetPolarDBXParams(param map[string]map[string]polardbxv1.Params) {
	rc.polardbxParamsRoleMap = param
}

func (rc *Context) GetPolarDBXParams() (params map[string]map[string]polardbxv1.Params) {
	if rc.polardbxParamsRoleMap == nil {
		param := make(map[string]map[string]polardbxv1.Params)
		param[polardbxv1.DNReadOnly] = make(map[string]polardbxv1.Params)
		param[polardbxv1.DNReadWrite] = make(map[string]polardbxv1.Params)
		param[polardbxv1.DNRestart] = make(map[string]polardbxv1.Params)
		param[polardbxv1.GMSReadOnly] = make(map[string]polardbxv1.Params)
		param[polardbxv1.GMSReadWrite] = make(map[string]polardbxv1.Params)
		param[polardbxv1.GMSRestart] = make(map[string]polardbxv1.Params)
		rc.polardbxParamsRoleMap = param
	}
	return rc.polardbxParamsRoleMap
}

func (rc *Context) SetPolarDBXTemplateParams(templateParams map[string]map[string]polardbxv1.TemplateParams) {
	rc.polardbxTemplateParams = templateParams
}

func (rc *Context) GetPolarDBXTemplateParams() (templateParams map[string]map[string]polardbxv1.TemplateParams) {
	if rc.polardbxTemplateParams == nil {
		templateParam := make(map[string]map[string]polardbxv1.TemplateParams)
		templateParam[polardbxmeta.RoleDN] = make(map[string]polardbxv1.TemplateParams)
		templateParam[polardbxmeta.RoleGMS] = make(map[string]polardbxv1.TemplateParams)
		rc.polardbxTemplateParams = templateParam
	}
	return rc.polardbxTemplateParams
}

func NewContext(base *control.BaseReconcileContext, configLoader func() config.Config) *Context {
	return &Context{
		BaseReconcileContext: base,
		objectCache:          cache.NewObjectCache(base.Client(), base.Scheme()),
		configLoader:         configLoader,
	}
}

func NewTaskConfigMap(xstore *polardbxv1.XStore) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      convention.NewConfigMapName(xstore, "restore"),
			Namespace: xstore.Namespace,
			Labels:    k8shelper.PatchLabels(convention.ConstLabels(xstore), convention.LabelGeneration(xstore)),
		},
		Immutable: pointer.Bool(false),
	}
}
