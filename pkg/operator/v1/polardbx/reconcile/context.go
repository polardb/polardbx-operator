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
	"github.com/alibaba/polardbx-operator/pkg/hpfs/filestream"
	hpfs "github.com/alibaba/polardbx-operator/pkg/hpfs/proto"
	xstoreconvention "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	"github.com/alibaba/polardbx-operator/pkg/util/name"
	"google.golang.org/grpc"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sort"
	"strconv"
	"strings"
	"time"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/cache"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	k8sselectorutil "github.com/alibaba/polardbx-operator/pkg/k8s/helper/selector"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms/security"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/group"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	dbutil "github.com/alibaba/polardbx-operator/pkg/util/database"
)

type Context struct {
	*control.BaseReconcileContext

	// Caches
	objectCache         cache.ObjectLoadingCache
	polardbxKey         types.NamespacedName
	polardbxChanged     bool
	polardbx            *polardbxv1.PolarDBXCluster
	primaryPolardbx     *polardbxv1.PolarDBXCluster
	polardbxStatus      *polardbxv1.PolarDBXClusterStatus
	cnDeployments       map[string]*appsv1.Deployment
	cdcDeployments      map[string]*appsv1.Deployment
	columnarDeployments map[string]*appsv1.Deployment
	podsByRole          map[string][]corev1.Pod
	nodes               []corev1.Node
	gmsStore            *polardbxv1.XStore
	dnStores            map[int]*polardbxv1.XStore
	primaryDnStore      map[int]*polardbxv1.XStore

	polardbxMonitor    *polardbxv1.PolarDBXMonitor
	polardbxMonitorKey types.NamespacedName

	polardbxBackup               *polardbxv1.PolarDBXBackup
	polardbxBackupKey            types.NamespacedName
	polardbxBackupStatusSnapshot *polardbxv1.PolarDBXBackupStatus
	polardbxSeekCpJob            *batchv1.Job

	polardbxBackupScheduleKey    types.NamespacedName
	polardbxBackupSchedule       *polardbxv1.PolarDBXBackupSchedule
	polardbxBackupScheduleStatus *polardbxv1.PolarDBXBackupScheduleStatus

	polardbxParameterTemplate *polardbxv1.PolarDBXParameterTemplate
	polardbxTemplateParams    map[string]map[string]polardbxv1.TemplateParams
	polardbxParamsRoleMap     map[string]map[string]polardbxv1.Params
	roleToRestart             map[string]bool

	polardbxParameter       *polardbxv1.PolarDBXParameter
	polardbxParameterKey    types.NamespacedName
	polardbxParameterStatus *polardbxv1.PolarDBXParameterStatus

	// Hint cache
	controllerHints []string

	// Config
	configLoader func() config.Config

	// Managers
	gmsManager   gms.Manager
	groupManager group.GroupManager
	// xstoreManagerMap records xstore's pod name and its related group manager
	xstoreManagerMap map[string]group.GroupManager

	taskConfigMap *corev1.ConfigMap

	// Filestream client
	filestreamClient *filestream.FileClient

	//hpfs client
	hpfsConn   *grpc.ClientConn
	hpfsClient hpfs.HpfsServiceClient

	//backup binlog
	backupBinlog    *polardbxv1.PolarDBXBackupBinlog
	backupBinlogKey types.NamespacedName
}

func (rc *Context) Debug() bool {
	if rc.BaseReconcileContext.Debug() {
		return true
	}
	r, _ := rc.containsControllerHint("debug")
	return r
}

func (rc *Context) Config() config.Config {
	return rc.configLoader()
}

func (rc *Context) GetNodesSortedByName() ([]corev1.Node, error) {
	if rc.nodes == nil {
		var nodeList corev1.NodeList
		err := rc.Client().List(rc.Context(), &nodeList)
		if err != nil {
			return nil, err
		}
		rc.nodes = nodeList.Items
		sort.Slice(rc.nodes, func(i, j int) bool {
			return rc.nodes[i].Name < rc.nodes[j].Name
		})
	}
	return rc.nodes, nil
}

func (rc *Context) GetSortedSchedulableNodes(nodeSelector *corev1.NodeSelector) ([]corev1.Node, error) {
	nodes, err := rc.GetNodesSortedByName()
	if err != nil {
		return nil, err
	}

	filterNodes := func(nodes []corev1.Node, nodeSelector *corev1.NodeSelector) ([]corev1.Node, error) {
		r := make([]corev1.Node, 0)
		for _, n := range nodes {
			if n.Spec.Unschedulable {
				continue
			}
			if nodeSelector == nil {
				r = append(r, n)
			} else {
				ok, err := k8sselectorutil.IsNodeMatches(&n, nodeSelector)
				if err != nil {
					return nil, err
				}
				if ok {
					r = append(r, n)
				}
			}
		}
		return r, nil
	}

	return filterNodes(nodes, nodeSelector)
}

func (rc *Context) SetPolarDBXKey(key types.NamespacedName) {
	rc.polardbxKey = key
}

func (rc *Context) CheckPolarDBXExist(key types.NamespacedName) bool {
	polardbx, _ := rc.objectCache.GetObject(
		rc.Context(),
		key,
		&polardbxv1.PolarDBXCluster{},
	)
	return polardbx != nil
}

func (rc *Context) GetPolarDBX() (*polardbxv1.PolarDBXCluster, error) {
	if rc.polardbx == nil {
		polardbx, err := rc.objectCache.GetObject(
			rc.Context(),
			rc.polardbxKey,
			&polardbxv1.PolarDBXCluster{},
		)
		if err != nil {
			return nil, err
		}
		rc.polardbx = polardbx.(*polardbxv1.PolarDBXCluster)
		rc.polardbxStatus = rc.polardbx.Status.DeepCopy()
	}
	return rc.polardbx, nil
}

func (rc *Context) MustGetPolarDBX() *polardbxv1.PolarDBXCluster {
	polardbx, err := rc.GetPolarDBX()
	if err != nil {
		panic(err)
	}
	return polardbx
}

func (rc *Context) GetPrimaryPolarDBX() (*polardbxv1.PolarDBXCluster, error) {
	polardbx := rc.MustGetPolarDBX()

	if !polardbx.Spec.Readonly {
		return nil, errors.New("current pxc is not readonly")
	}

	if rc.primaryPolardbx == nil {
		primaryPolardbxKey := types.NamespacedName{
			Namespace: rc.polardbxKey.Namespace,
			Name:      polardbx.Spec.PrimaryCluster,
		}
		primaryPolardbx, err := rc.objectCache.GetObject(
			rc.Context(),
			primaryPolardbxKey,
			&polardbxv1.PolarDBXCluster{},
		)
		if err != nil {
			return nil, err
		}
		rc.primaryPolardbx = primaryPolardbx.(*polardbxv1.PolarDBXCluster)

	}

	return rc.primaryPolardbx, nil
}

func (rc *Context) MustGetPrimaryPolarDBX() *polardbxv1.PolarDBXCluster {
	polardbx, err := rc.GetPrimaryPolarDBX()
	if err != nil {
		panic(err)
	}
	return polardbx
}

func (rc *Context) GetReadonlyPolarDBXList() ([]*polardbxv1.PolarDBXCluster, error) {
	polardbx := rc.MustGetPolarDBX()

	if polardbx.Spec.Readonly {
		return nil, errors.New("current pxc is readonly")
	}

	var readonlyPolardbxClusterList polardbxv1.PolarDBXClusterList
	var readonlyPolardbxList []*polardbxv1.PolarDBXCluster

	err := rc.Client().List(
		rc.Context(),
		&readonlyPolardbxClusterList,
		client.MatchingLabels(
			k8shelper.PatchLabels(
				map[string]string{
					polardbxmeta.LabelType:        polardbxmeta.TypeReadonly,
					polardbxmeta.LabelPrimaryName: polardbx.Name,
				},
			),
		),
	)

	if err != nil {
		return nil, fmt.Errorf("unable to get readonly polardbx: %w", err)
	}

	for _, item := range readonlyPolardbxClusterList.Items {
		readonlyPolardbxList = append(readonlyPolardbxList, &item)
	}

	return readonlyPolardbxList, nil
}

func (rc *Context) containsControllerHint(hint string) (bool, error) {
	if rc.controllerHints == nil {
		polardbx, err := rc.GetPolarDBX()
		if err != nil {
			return false, err
		}

		val, ok := polardbx.Annotations[polardbxmeta.AnnotationControllerHints]
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

// TODO(siyun): consider SetControllerRef for readonly inst
func (rc *Context) SetControllerRef(obj client.Object) error {
	if obj == nil {
		return nil
	}
	polardbx := rc.MustGetPolarDBX()
	return ctrl.SetControllerReference(polardbx, obj, rc.Scheme())
}

func (rc *Context) SetControllerRefToBackup(obj client.Object) error {
	if obj == nil {
		return nil
	}
	polardbxBackup := rc.MustGetPolarDBXBackup()
	return ctrl.SetControllerReference(polardbxBackup, obj, rc.Scheme())
}

func (rc *Context) SetControllerRefAndCreate(obj client.Object) error {
	if err := rc.SetControllerRef(obj); err != nil {
		return err
	}
	return rc.Client().Create(rc.Context(), obj)
}

func (rc *Context) SetControllerRefAndCreateToBackup(obj client.Object) error {
	if err := rc.SetControllerRefToBackup(obj); err != nil {
		return err
	}
	return rc.Client().Create(rc.Context(), obj)
}

func (rc *Context) SetControllerRefAndUpdate(obj client.Object) error {
	if err := rc.SetControllerRef(obj); err != nil {
		return err
	}
	return rc.Client().Update(rc.Context(), obj)
}

func (rc *Context) SetControllerToOwnerAndCreate(owner, obj client.Object) error {
	if err := ctrl.SetControllerReference(owner, obj, rc.Scheme()); err != nil {
		return err
	}
	return rc.Client().Create(rc.Context(), obj)
}

func (rc *Context) IsPolarDBXStatusChanged() bool {
	if rc.polardbxStatus == nil {
		return false
	}
	return !equality.Semantic.DeepEqual(&rc.polardbx.Status, rc.polardbxStatus)
}

func (rc *Context) IsPolarDBXChanged() bool {
	return rc.polardbxChanged
}

// MarkPolarDBXChanged marks the change of spec for several crds
func (rc *Context) MarkPolarDBXChanged() {
	rc.polardbxChanged = true
}

func (rc *Context) UpdatePolarDBX() error {
	if rc.polardbx == nil {
		return nil
	}

	// Deep copy status before updating because client.update will update
	// the status of object.
	status := rc.polardbx.Status.DeepCopy()
	err := rc.Client().Update(rc.Context(), rc.polardbx)
	if err != nil {
		return err
	}

	// Restore the status (shallow copy is enough)
	rc.polardbx.Status = *status

	return nil
}

func (rc *Context) UpdatePolarDBXStatus() error {
	if rc.polardbxStatus == nil {
		return nil
	}

	err := rc.Client().Status().Update(rc.Context(), rc.polardbx)
	if err != nil {
		return err
	}
	rc.polardbxStatus = rc.polardbx.Status.DeepCopy()
	return nil
}

func (rc *Context) GetPolarDBXService(serviceType convention.ServiceType) (*corev1.Service, error) {
	polardbx, err := rc.GetPolarDBX()
	if err != nil {
		return nil, fmt.Errorf("unable to get polardbx object: %w", err)
	}

	serviceKey := types.NamespacedName{
		Namespace: rc.polardbxKey.Namespace,
		Name:      convention.NewServiceName(polardbx, serviceType),
	}

	service, err := rc.objectCache.GetObject(rc.Context(), serviceKey, &corev1.Service{})
	if err != nil {
		return nil, err
	}

	err = k8shelper.CheckControllerReference(service, polardbx)
	if err != nil {
		return nil, err
	}

	return service.(*corev1.Service), nil
}

func (rc *Context) GetPolarDBXClusterAddr(serviceType convention.ServiceType, port string) (string, error) {
	svc, err := rc.GetPolarDBXService(serviceType)
	if err != nil {
		return "", err
	}
	return k8shelper.GetClusterAddrFromService(svc, port)
}

func (rc *Context) getPolarDBXSecret(polardbx *polardbxv1.PolarDBXCluster, secretType convention.SecretType) (*corev1.Secret, error) {
	secretKey := types.NamespacedName{
		Namespace: rc.polardbxKey.Namespace,
		Name:      convention.NewSecretName(polardbx, secretType),
	}

	secret, err := rc.objectCache.GetObject(rc.Context(), secretKey, &corev1.Secret{})
	if err != nil {
		return nil, err
	}
	if err := k8shelper.CheckControllerReference(secret, polardbx); err != nil {
		return nil, err
	}

	return secret.(*corev1.Secret), nil
}

func (rc *Context) GetPolarDBXSecretForRestore() (*corev1.Secret, error) {
	polardbx, err := rc.GetPolarDBX()
	if err != nil {
		return nil, fmt.Errorf("unable to get polardbx object: %w", err)
	}
	secretKey := types.NamespacedName{}
	if polardbx.Spec.Restore.BackupSet == "" || len(polardbx.Spec.Restore.BackupSet) == 0 {
		backup, err := rc.GetLastCompletedPXCBackup(map[string]string{polardbxmeta.LabelName: polardbx.Spec.Restore.From.PolarBDXName}, rc.MustParseRestoreTime())
		if err != nil {
			return nil, err
		}
		secretKey = types.NamespacedName{
			Namespace: polardbx.Namespace,
			Name:      backup.Name,
		}
	} else {
		secretKey = types.NamespacedName{
			Namespace: polardbx.Namespace,
			Name:      polardbx.Spec.Restore.BackupSet,
		}
	}

	secret, err := rc.objectCache.GetObject(rc.Context(), secretKey, &corev1.Secret{})
	if err != nil {
		return nil, err
	}

	return secret.(*corev1.Secret), nil
}

func (rc *Context) ParseRestoreTime() (time.Time, error) {
	polarDBX := rc.MustGetPolarDBX()
	if polarDBX.Spec.Restore == nil {
		return time.Time{}, nil
	}

	location, err := time.LoadLocation(gms.StrOrDefault(polarDBX.Spec.Restore.TimeZone, "UTC"))
	if err != nil {
		return time.Time{}, nil
	}

	return time.ParseInLocation("2006-01-02T15:04:05Z", polarDBX.Spec.Restore.Time, location)
}

func (rc *Context) MustParseRestoreTime() time.Time {
	t, err := rc.ParseRestoreTime()
	if err != nil {
		panic(err)
	}
	return t
}

func (rc *Context) GetPolarDBXSecret(secretType convention.SecretType) (*corev1.Secret, error) {
	polardbx, err := rc.GetPolarDBX()
	if err != nil {
		return nil, fmt.Errorf("unable to get polardbx object: %w", err)
	}

	return rc.getPolarDBXSecret(polardbx, secretType)
}

func (rc *Context) GetPolarDBXEncodeKey() (string, error) {
	polardbx, err := rc.GetPolarDBX()
	if err != nil {
		return "", fmt.Errorf("unable to get polardbx object: %w", err)
	}

	if polardbx.Spec.Readonly {
		polardbx, err = rc.GetPrimaryPolarDBX()
		if err != nil {
			return "", err
		}
	}
	secret, err := rc.getPolarDBXSecret(polardbx, convention.SecretTypeSecurity)
	if err != nil {
		return "", err
	}
	encodeKey, ok := secret.Data[convention.SecretKeyEncodeKey]
	if !ok {
		return "", errors.New("not found")
	}
	return string(encodeKey), nil
}

func (rc *Context) GetPolarDBXTlsCerts() (map[string]string, error) {
	secret, err := rc.GetPolarDBXSecret(convention.SecretTypeSecurity)
	if err != nil {
		return nil, err
	}
	rootCrt, ok := secret.Data[convention.SecretKeyRootCrt]
	if !ok {
		return nil, errors.New("root.crt not found")
	}
	serverKey, ok := secret.Data[convention.SecretKeyServerKey]
	if !ok {
		return nil, errors.New("server.key not found")
	}
	serverCrt, ok := secret.Data[convention.SecretKeyServerCrt]
	if !ok {
		return nil, errors.New("server.crt not found")
	}
	return map[string]string{
		"root.crt":   string(rootCrt),
		"server.key": string(serverKey),
		"server.crt": string(serverCrt),
	}, nil
}

func (rc *Context) GetPolarDBXPasswordCipher() (security.PasswordCipher, error) {
	key, err := rc.GetPolarDBXEncodeKey()
	if err != nil {
		return nil, err
	}
	return security.NewPasswordCipher(key)
}

func (rc *Context) GetPolarDBXAccountPassword(user string) (string, error) {
	secret, err := rc.GetPolarDBXSecret(convention.SecretTypeAccount)
	if err != nil {
		return "", err
	}
	passwd, ok := secret.Data[user]
	if !ok {
		return "", errors.New("not found")
	}
	return string(passwd), nil
}

func (rc *Context) GetSecret(name string) (*corev1.Secret, error) {
	secretKey := types.NamespacedName{
		Namespace: rc.Namespace(),
		Name:      name,
	}
	cm, err := rc.objectCache.GetObject(rc.Context(), secretKey, &corev1.Secret{})
	if err != nil {
		return nil, err
	}
	return cm.(*corev1.Secret), nil
}

func (rc *Context) GetPolarDBXConfigMap(cmType convention.ConfigMapType) (*corev1.ConfigMap, error) {
	polardbx, err := rc.GetPolarDBX()
	if err != nil {
		return nil, fmt.Errorf("unable to get polardbx object: %w", err)
	}

	cm, err := rc.GetConfigMap(convention.NewConfigMapName(polardbx, cmType))
	if err != nil {
		return nil, err
	}

	if err := k8shelper.CheckControllerReference(cm, polardbx); err != nil {
		return nil, err
	}
	return cm, nil
}

func (rc *Context) GetConfigMap(name string) (*corev1.ConfigMap, error) {
	cmKey := types.NamespacedName{
		Namespace: rc.polardbxKey.Namespace,
		Name:      name,
	}
	cm, err := rc.objectCache.GetObject(rc.Context(), cmKey, &corev1.ConfigMap{})
	if err != nil {
		return nil, err
	}
	return cm.(*corev1.ConfigMap), nil
}

func (rc *Context) GetCNPod(polardbx *polardbxv1.PolarDBXCluster) (*corev1.Pod, error) {
	polardbx, err := rc.GetPolarDBX()
	if err != nil {
		return nil, fmt.Errorf("unable to get polardbx object: %w", err)
	}
	var cnPods corev1.PodList
	err = rc.Client().List(rc.Context(), &cnPods,
		client.InNamespace(rc.polardbxKey.Namespace),
		client.MatchingLabels(convention.ConstLabelsWithRole(polardbx, polardbxmeta.RoleCN)),
	)
	if err != nil {
		return nil, err
	}
	if len(cnPods.Items) <= 0 {
		return nil, errors.New("there is no cnpods")
	} else {
		return &cnPods.Items[0], nil
	}
}

func (rc *Context) getDNMap(polardbx *polardbxv1.PolarDBXCluster) (map[int]*polardbxv1.XStore, error) {
	var xstoreList polardbxv1.XStoreList
	err := rc.Client().List(rc.Context(), &xstoreList,
		client.InNamespace(rc.polardbxKey.Namespace),
		client.MatchingLabels(convention.ConstLabelsWithRole(polardbx, polardbxmeta.RoleDN)),
	)
	if err != nil {
		return nil, err
	}

	// Check & collect
	dnStores := make(map[int]*polardbxv1.XStore)
	for i, xstore := range xstoreList.Items {
		// Not owned, just ignore.
		if err := k8shelper.CheckControllerReference(&xstore, polardbx); err != nil {
			continue
		}

		// Parse index
		index := convention.MustParseIndexFromDN(&xstore)
		if index < 0 {
			continue
		}
		if _, found := dnStores[index]; found {
			return nil, errors.New("found xstore with duplicate index: " + strconv.Itoa(index))
		}
		dnStores[index] = &xstoreList.Items[i]
	}

	return dnStores, nil
}

func (rc *Context) GetDNMap() (map[int]*polardbxv1.XStore, error) {
	if rc.dnStores == nil {
		polardbx, err := rc.GetPolarDBX()
		if err != nil {
			return nil, fmt.Errorf("unable to get polardbx object: %w", err)
		}

		return rc.getDNMap(polardbx)
	}
	return rc.dnStores, nil
}

func (rc *Context) GetPrimaryDNMap() (map[int]*polardbxv1.XStore, error) {
	//if rc.primaryDnStore == nil {
	primaryPolardbx, err := rc.GetPrimaryPolarDBX()
	if err != nil {
		return nil, fmt.Errorf("unable to get primary polardbx object: %w", err)
	}

	return rc.getDNMap(primaryPolardbx)
	//}
	//// TODO: refresh cache
	//
	//return rc.primaryDnStore, nil
}

func (rc *Context) GetDNMapOf(polardbx *polardbxv1.PolarDBXCluster) (map[int]*polardbxv1.XStore, error) {
	return rc.getDNMap(polardbx)
}

func (rc *Context) getOrderedDNListFromMap(dnMap map[int]*polardbxv1.XStore) []*polardbxv1.XStore {
	type sortKey struct {
		xstore *polardbxv1.XStore
		index  int
	}

	keys := make([]sortKey, 0, len(dnMap))
	for i, v := range dnMap {
		keys = append(keys, sortKey{
			xstore: v,
			index:  i,
		})
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i].index < keys[j].index
	})

	dnList := make([]*polardbxv1.XStore, 0, len(dnMap))
	for _, v := range keys {
		dnList = append(dnList, v.xstore)
	}
	return dnList
}

func (rc *Context) GetOrderedDNList() ([]*polardbxv1.XStore, error) {
	dnMap, err := rc.GetDNMap()
	if err != nil {
		return nil, err
	}

	return rc.getOrderedDNListFromMap(dnMap), nil
}

func (rc *Context) GetOrderedDNListOf(polardbx *polardbxv1.PolarDBXCluster) ([]*polardbxv1.XStore, error) {
	dnMap, err := rc.GetDNMapOf(polardbx)
	if err != nil {
		return nil, err
	}

	return rc.getOrderedDNListFromMap(dnMap), nil
}

func (rc *Context) GetDN(i int) (*polardbxv1.XStore, error) {
	dnMap, err := rc.GetDNMap()
	if err != nil {
		return nil, err
	}
	xstore, ok := dnMap[i]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{}, "")
	}
	return xstore, nil
}

func (rc *Context) GetLeaderOfDN(xstore *polardbxv1.XStore) (*corev1.Pod, error) {
	leaderPodName := types.NamespacedName{Namespace: rc.Namespace(), Name: xstore.Status.LeaderPod}
	return rc.GetPodFromPodName(leaderPodName)
}

func (rc *Context) GetPodFromPodName(podName types.NamespacedName) (*corev1.Pod, error) {
	var pod corev1.Pod
	err := rc.Client().Get(rc.Context(), podName, &pod)
	if err != nil {
		return nil, err
	}
	return &pod, nil
}

func (rc *Context) GetXstoreByPod(pod *corev1.Pod) (*polardbxv1.XStore, error) {
	var xstore polardbxv1.XStore
	xstoreName := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Labels[xstoremeta.LabelName]}
	err := rc.Client().Get(rc.Context(), xstoreName, &xstore)
	if err != nil {
		return nil, err
	}
	return &xstore, nil
}

func (rc *Context) getDeploymentMap(polardbx *polardbxv1.PolarDBXCluster, role string) (map[string]*appsv1.Deployment, error) {
	var deploymentList appsv1.DeploymentList
	err := rc.Client().List(rc.Context(), &deploymentList,
		client.InNamespace(rc.polardbxKey.Namespace),
		client.MatchingLabels(convention.ConstLabelsWithRole(polardbx, role)),
	)
	if err != nil {
		return nil, err
	}

	deploymentMap := make(map[string]*appsv1.Deployment)
	for i, deploy := range deploymentList.Items {
		// Not owned, just ignore.
		if err := k8shelper.CheckControllerReference(&deploy, polardbx); err != nil {
			continue
		}

		deployGroup := convention.ParseGroupFromDeployment(&deploy)
		if _, found := deploymentMap[deployGroup]; found {
			return nil, errors.New("found deployment with duplicate group: " + deployGroup)
		}

		deploymentMap[deployGroup] = &deploymentList.Items[i]
	}

	return deploymentMap, nil
}

func (rc *Context) GetPrimaryDeploymentMap(role string) (map[string]*appsv1.Deployment, error) {
	polardbx, err := rc.GetPrimaryPolarDBX()
	if err != nil {
		return nil, fmt.Errorf("unable to get polardbx object: %w", err)
	}
	return rc.getDeploymentMap(polardbx, role)
}

func (rc *Context) GetDeploymentMap(role string) (map[string]*appsv1.Deployment, error) {
	var deploymentMapPtr *map[string]*appsv1.Deployment
	switch role {
	case polardbxmeta.RoleCN:
		deploymentMapPtr = &rc.cnDeployments
	case polardbxmeta.RoleCDC:
		deploymentMapPtr = &rc.cdcDeployments
	case polardbxmeta.RoleColumnar:
		deploymentMapPtr = &rc.columnarDeployments
	default:
		panic("required role to be cn, cdc or columnar, but found " + role)
	}

	if *deploymentMapPtr == nil {
		polardbx, err := rc.GetPolarDBX()
		if err != nil {
			return nil, fmt.Errorf("unable to get polardbx object: %w", err)
		}

		*deploymentMapPtr, err = rc.getDeploymentMap(polardbx, role)

		if err != nil {
			return nil, err
		}
	}

	return *deploymentMapPtr, nil
}

func (rc *Context) GetPods(role string) ([]corev1.Pod, error) {
	if role != polardbxmeta.RoleCN && role != polardbxmeta.RoleCDC && role != polardbxmeta.RoleColumnar && role != polardbxmeta.RoleDN && role != polardbxmeta.RoleGMS {
		panic("required role to be cn, cdc, dn or columnar, but found " + role)
	}
	pods := rc.podsByRole[role]
	if pods == nil {
		polardbx, err := rc.GetPolarDBX()
		if err != nil {
			return nil, fmt.Errorf("unable to get polardbx object: %w", err)
		}

		var podList corev1.PodList
		err = rc.Client().List(
			rc.Context(),
			&podList,
			client.InNamespace(rc.polardbxKey.Namespace),
			client.MatchingLabels(convention.ConstLabelsWithRole(polardbx, role)),
		)
		if err != nil {
			return nil, err
		}

		if rc.podsByRole == nil {
			rc.podsByRole = make(map[string][]corev1.Pod)
		}
		sort.Slice(podList.Items, func(i, j int) bool {
			return podList.Items[i].ObjectMeta.CreationTimestamp.Before(&podList.Items[j].ObjectMeta.CreationTimestamp)
		})
		rc.podsByRole[role] = podList.Items
	}

	return rc.podsByRole[role], nil
}

func (rc *Context) GetGMS() (*polardbxv1.XStore, error) {
	polardbx := rc.MustGetPolarDBX()
	var err error
	readonly := polardbx.Spec.Readonly

	if readonly {
		polardbx, err = rc.GetPrimaryPolarDBX()
		if err != nil {
			return nil, err
		}
	}

	if rc.gmsStore != nil {
		return rc.gmsStore, nil
	}

	gmsName := convention.NewGMSName(polardbx)
	if polardbx.Spec.ShareGMS {
		gmsName = convention.NewDNName(polardbx, 0)
	}

	gmsStore, err := rc.objectCache.GetObject(
		rc.Context(),
		types.NamespacedName{
			Namespace: rc.polardbxKey.Namespace,
			Name:      gmsName,
		},
		&polardbxv1.XStore{},
	)
	if err != nil {
		return nil, err
	}

	if err := k8shelper.CheckControllerReference(gmsStore, polardbx); err != nil {
		return nil, err
	}

	if !readonly {
		rc.gmsStore = gmsStore.(*polardbxv1.XStore)
	}

	return gmsStore.(*polardbxv1.XStore), nil
}

func (rc *Context) GetService(name string) (*corev1.Service, error) {
	svc, err := rc.objectCache.GetObject(
		rc.Context(),
		types.NamespacedName{
			Namespace: rc.polardbxKey.Namespace,
			Name:      name,
		},
		&corev1.Service{},
	)
	if err != nil {
		return nil, err
	}
	return svc.(*corev1.Service), nil
}

func (rc *Context) HasCNs() bool {
	polardbx := rc.MustGetPolarDBX()

	nodes := polardbx.Status.SpecSnapshot.Topology.Nodes

	if nodes.CN.Replicas == nil {
		return false
	}

	return *nodes.CN.Replicas > 0
}

func (rc *Context) GetDnVersion() (string, error) {
	xstore, err := rc.GetDN(0)

	if err != nil {
		return "", fmt.Errorf("unable to get xstore object: %w", err)
	}

	return xstore.Status.EngineVersion, nil
}

func (rc *Context) GetPolarDBXGMSManager() (gms.Manager, error) {
	if rc.gmsManager == nil {
		polardbx, err := rc.GetPolarDBX()
		if err != nil {
			return nil, fmt.Errorf("unable to get polardbx object: %w", err)
		}

		encodeKey, err := rc.GetPolarDBXEncodeKey()
		if err != nil {
			return nil, fmt.Errorf("unable to get polardbx encode key: %w", err)
		}
		passwordCipher, err := security.NewPasswordCipher(encodeKey)
		if err != nil {
			return nil, err
		}

		gmsStore, err := rc.GetGMS()
		if client.IgnoreNotFound(err) != nil {
			return nil, fmt.Errorf("unable to get gms object: %w", err)
		}

		if gmsStore == nil {
			return nil, nil
		}

		gmsService, err := rc.GetService(xstoreconvention.NewServiceName(gmsStore, xstoreconvention.ServiceTypeReadWrite))
		if err != nil {
			return nil, fmt.Errorf("unable to get gms service: %w", err)
		}

		gmsSecret, err := rc.GetSecret(xstoreconvention.NewSecretName(gmsStore))
		if err != nil {
			return nil, fmt.Errorf("unable to get gms secret: %w", err)
		}

		xPort := 0
		xContainerPort := k8shelper.GetPortFromService(gmsService, "polarx")
		if xContainerPort != nil {
			xPort = int(xContainerPort.Port)
		}

		gmsEngine := gmsStore.Spec.Engine
		annoStorageType, _ := polardbx.Annotations[polardbxmeta.AnnotationStorageType]
		storageType, err := gms.GetStorageType(gmsEngine, gmsStore.Status.EngineVersion, annoStorageType)
		if err != nil {
			return nil, err
		}

		gmsName := convention.NewGMSName(polardbx)
		if polardbx.Spec.Readonly {
			primaryPolardbx, err := rc.GetPrimaryPolarDBX()
			if err != nil {
				return nil, err
			}
			gmsName = convention.NewGMSName(primaryPolardbx)
		}
		// Mock GMS id if sharing GMS
		rc.gmsManager = gms.NewGmsManager(
			rc.Context(),
			polardbx.Name,
			&gms.MetaDB{
				Id: gmsName,
				// Host used to create metadata in GMS.
				Host: k8shelper.GetServiceDNSRecordWithSvc(gmsService, true),
				// Host used to create connections from operator.
				Host4Conn: k8shelper.GetServiceDNSRecordWithSvc(gmsService, false),
				Port:      int(k8shelper.MustGetPortFromService(gmsService, xstoreconvention.PortAccess).Port),
				XPort:     xPort,
				User:      xstoreconvention.SuperAccount,
				Passwd:    string(gmsSecret.Data[xstoreconvention.SuperAccount]),
				Type:      storageType,
			},
			passwordCipher,
		)
	}

	return rc.gmsManager, nil
}

func (rc *Context) GetPolarDBXGroupManager() (group.GroupManager, error) {
	if rc.groupManager == nil {
		polardbx, err := rc.GetPolarDBX()
		if err != nil {
			return nil, fmt.Errorf("unable to get polardbx object: %w", err)
		}

		serviceType := convention.ServiceTypeReadWrite

		if polardbx.Spec.Readonly {
			serviceType = convention.ServiceTypeReadOnly
			polardbx, err = rc.GetPrimaryPolarDBX()
			if err != nil {
				return nil, err
			}
		}

		service, err := rc.GetPolarDBXService(serviceType)
		if err != nil {
			return nil, err
		}

		// should get secret of primary pxc, since learner sync account and password from leader
		secret, err := rc.getPolarDBXSecret(polardbx, convention.SecretTypeAccount)
		if err != nil {
			return nil, err
		}

		caseInsensitive, _ := strconv.ParseBool(polardbx.Annotations[polardbxmeta.AnnotationSchemaCaseInsensitive])
		rc.groupManager = group.NewGroupManager(
			rc.Context(),
			dbutil.MySQLDataSource{
				Host:     k8shelper.GetServiceDNSRecordWithSvc(service, false),
				Port:     int(k8shelper.MustGetPortFromService(service, convention.PortAccess).Port),
				Username: convention.RootAccount,
				Password: string(secret.Data[convention.RootAccount]),
			},
			caseInsensitive,
		)
	}

	return rc.groupManager, nil
}

func (rc *Context) GetPolarDBXGroupManagerByBackup(backup *polardbxv1.PolarDBXBackup) (group.GroupManager, error) {
	serviceKey := types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.Cluster.Name}
	service := corev1.Service{}
	err := rc.Client().Get(rc.Context(), serviceKey, &service)
	if err != nil {
		return nil, err
	}

	secretKey := types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.Cluster.Name}
	secret := corev1.Secret{}
	err = rc.Client().Get(rc.Context(), secretKey, &secret)
	if err != nil {
		return nil, err
	}
	rc.groupManager = group.NewGroupManager(
		rc.Context(),
		dbutil.MySQLDataSource{
			Host:     k8shelper.GetServiceDNSRecordWithSvc(&service, false),
			Port:     int(k8shelper.MustGetPortFromService(&service, convention.PortAccess).Port),
			Username: convention.RootAccount,
			Password: string(secret.Data[convention.RootAccount]),
		},
		true,
	)
	return rc.groupManager, nil
}

func (rc *Context) GetXstoreGroupManagerByPod(pod *corev1.Pod) (group.GroupManager, error) {
	if rc.xstoreManagerMap != nil {
		if mgr, ok := rc.xstoreManagerMap[pod.Name]; ok {
			return mgr, nil
		}
	}
	host := pod.Status.PodIP
	port := k8shelper.MustGetPortFromContainer(
		k8shelper.MustGetContainerFromPod(pod, convention.ContainerEngine),
		convention.PortAccess,
	).ContainerPort
	xstore, err := rc.GetXstoreByPod(pod)
	if err != nil {
		return nil, err
	}
	passwd, err := rc.GetXStoreAccountPassword(xstoreconvention.SuperAccount, xstore)
	if err != nil {
		return nil, err
	}

	if rc.xstoreManagerMap == nil {
		rc.xstoreManagerMap = make(map[string]group.GroupManager)
	}
	rc.xstoreManagerMap[pod.Name] = group.NewGroupManager(
		rc.Context(),
		dbutil.MySQLDataSource{
			Host:     host,
			Port:     int(port),
			Username: xstoreconvention.SuperAccount,
			Password: passwd,
		},
		true,
	)
	return rc.xstoreManagerMap[pod.Name], nil
}

func (rc *Context) Close() error {
	errs := make([]error, 0)

	if rc.gmsManager != nil {
		err := rc.gmsManager.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}

	if rc.groupManager != nil {
		err := rc.groupManager.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}

	if rc.xstoreManagerMap != nil {
		for _, mgr := range rc.xstoreManagerMap {
			err := mgr.Close()
			if err != nil {
				errs = append(errs, err)
			}
		}
	}

	if rc.hpfsConn != nil {
		err := rc.hpfsConn.Close()
		rc.hpfsConn = nil
		rc.hpfsClient = nil
		if err != nil {
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

func (rc *Context) PolardbxMonitorKey() types.NamespacedName {
	return rc.polardbxMonitorKey
}

func (rc *Context) SetPolardbxMonitorKey(polardbxMonitorKey types.NamespacedName) {
	rc.polardbxMonitorKey = polardbxMonitorKey
}

func (rc *Context) GetPolarDBXMonitor() (*polardbxv1.PolarDBXMonitor, error) {
	if rc.polardbxMonitor == nil {
		polardbxMonitor, err := rc.objectCache.GetObject(
			rc.Context(),
			rc.polardbxMonitorKey,
			&polardbxv1.PolarDBXMonitor{},
		)
		if err != nil {
			return nil, err
		}
		rc.polardbxMonitor = polardbxMonitor.(*polardbxv1.PolarDBXMonitor)
	}
	return rc.polardbxMonitor, nil
}

func (rc *Context) MustGetPolarDBXMonitor() *polardbxv1.PolarDBXMonitor {
	polardbxMonitor, err := rc.GetPolarDBXMonitor()
	if err != nil {
		panic(err)
	}
	return polardbxMonitor
}

func (rc *Context) SetPolarDBXBackupKey(polardbxBackupKey types.NamespacedName) {
	rc.polardbxBackupKey = polardbxBackupKey
}

func (rc *Context) GetPolarDBXBackup() (*polardbxv1.PolarDBXBackup, error) {
	if rc.polardbxBackup == nil {
		var polardbxBackup polardbxv1.PolarDBXBackup
		err := rc.Client().Get(rc.Context(), rc.Request().NamespacedName, &polardbxBackup)
		if err != nil {
			return nil, err
		}
		rc.polardbxBackup = &polardbxBackup
		rc.polardbxBackupStatusSnapshot = rc.polardbxBackup.Status.DeepCopy()
	}
	return rc.polardbxBackup, nil
}

func (rc *Context) MustGetPolarDBXBackup() *polardbxv1.PolarDBXBackup {
	polardbxBackup, err := rc.GetPolarDBXBackup()
	if err != nil {
		panic(err)
	}
	return polardbxBackup
}

// UpdatePolarDBXBackup only updates spec and replaces the status with the value from server
func (rc *Context) UpdatePolarDBXBackup() error {
	if rc.polardbxBackup == nil {
		return nil
	}

	// Deep copy status before updating because client.update will update
	// the status of object.
	status := rc.polardbxBackup.Status.DeepCopy()
	err := rc.Client().Update(rc.Context(), rc.polardbxBackup)
	if err != nil {
		return err
	}

	// Restore the status (shallow copy is enough)
	rc.polardbxBackup.Status = *status

	return nil
}

func (rc *Context) UpdatePolarDBXBackupStatus() error {
	if rc.polardbxBackupStatusSnapshot == nil {
		return nil
	}
	err := rc.Client().Status().Update(rc.Context(), rc.polardbxBackup)
	if err != nil {
		return err
	}
	rc.polardbxBackupStatusSnapshot = rc.polardbxBackup.Status.DeepCopy()
	return nil
}

func (rc *Context) IsPXCBackupStatusChanged() bool {
	if rc.polardbxBackupStatusSnapshot == nil {
		return false
	}
	return !equality.Semantic.DeepEqual(rc.polardbxBackup.Status, *rc.polardbxBackupStatusSnapshot)
}

func (rc *Context) GetXStoreBackups() (*polardbxv1.XStoreBackupList, error) {
	backup := rc.MustGetPolarDBXBackup()

	var xstoreBackups polardbxv1.XStoreBackupList
	err := rc.Client().List(rc.Context(), &xstoreBackups, client.InNamespace(rc.Namespace()), client.MatchingLabels{
		polardbxmeta.LabelName:      backup.Spec.Cluster.Name,
		polardbxmeta.LabelTopBackup: backup.Name,
	})
	if err != nil {
		return nil, err
	}
	return &xstoreBackups, nil
}

func (rc *Context) GetXstoreBackupByName(xstoreBackupName string) (*polardbxv1.XStoreBackup, error) {
	var xstoreBackup polardbxv1.XStoreBackup
	xstoreBackupKey := types.NamespacedName{
		Namespace: rc.Namespace(),
		Name:      xstoreBackupName,
	}
	err := rc.Client().Get(rc.Context(), xstoreBackupKey, &xstoreBackup)
	if err != nil {
		return nil, err
	}
	return &xstoreBackup, nil
}

func (rc *Context) GetXStoreBackupPods() ([]corev1.Pod, error) {
	xstoreBackups, err := rc.GetXStoreBackups()
	if err != nil {
		return nil, err
	}
	pods := make([]corev1.Pod, 0)
	for _, xstoreBackup := range xstoreBackups.Items {
		var targetPod corev1.Pod
		targetPodSpec := types.NamespacedName{Namespace: xstoreBackup.Namespace, Name: xstoreBackup.Status.TargetPod}
		err = rc.Client().Get(rc.Context(), targetPodSpec, &targetPod)
		if err != nil {
			return nil, err
		}
		pods = append(pods, targetPod)
	}
	return pods, nil
}

func (rc *Context) GetXStoreAccountPassword(user string, xstore *polardbxv1.XStore) (string, error) {
	secret, err := rc.GetXStoreSecret(xstore)
	if err != nil {
		return "", err
	}
	passwd, ok := secret.Data[user]
	if !ok {
		return "", errors.New("not found")
	}
	return string(passwd), nil
}

func (rc *Context) GetXStoreSecret(xstore *polardbxv1.XStore) (*corev1.Secret, error) {
	secretKey := types.NamespacedName{Namespace: xstore.Namespace, Name: xstore.Name}
	secret, err := rc.objectCache.GetObject(rc.Context(), secretKey, &corev1.Secret{})
	if err != nil {
		return nil, err
	}
	if err := k8shelper.CheckControllerReference(secret, xstore); err != nil {
		return nil, err
	}
	return secret.(*corev1.Secret), nil
}

func (rc *Context) GetSeekCpJob() (*batchv1.Job, error) {
	if rc.polardbxSeekCpJob == nil {
		pxcBackup := rc.MustGetPolarDBXBackup()
		var jobList batchv1.JobList
		err := rc.Client().List(rc.Context(), &jobList, client.InNamespace(rc.Request().Namespace),
			client.MatchingLabels{
				polardbxmeta.SeekCpJobLabelBackupName: pxcBackup.Name,
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
			if err = k8shelper.CheckControllerReference(job, pxcBackup); err == nil {
				ownedJobs = append(ownedJobs, job)
			}
		}

		if len(ownedJobs) == 0 {
			return nil, nil
		}

		if len(ownedJobs) > 1 {
			panic("multiple owned jobs found, must not happen")
		}

		rc.polardbxSeekCpJob = ownedJobs[0]
	}
	return rc.polardbxSeekCpJob, nil
}

func (rc *Context) GetCompletedPXCBackup(matchLabels map[string]string) (*polardbxv1.PolarDBXBackup, error) {
	polardbxBackupList := &polardbxv1.PolarDBXBackupList{}
	err := rc.Client().List(rc.Context(), polardbxBackupList, client.InNamespace(rc.Namespace()),
		client.MatchingLabels(matchLabels))
	if err != nil || len(polardbxBackupList.Items) == 0 {
		return nil, err
	}

	return &polardbxBackupList.Items[0], nil
}

func (rc *Context) GetLastCompletedPXCBackup(matchLabels map[string]string, beforeTime time.Time) (*polardbxv1.PolarDBXBackup, error) {
	polardbxBackupList := &polardbxv1.PolarDBXBackupList{}
	err := rc.Client().List(rc.Context(), polardbxBackupList, client.InNamespace(rc.Namespace()),
		client.MatchingLabels(matchLabels))
	if err != nil || len(polardbxBackupList.Items) == 0 {
		return nil, err
	}
	var lastBackup *polardbxv1.PolarDBXBackup = nil
	var lastBackupRestoreTime *time.Time = nil
	for i := range polardbxBackupList.Items {
		backup := &polardbxBackupList.Items[i]
		if backup.Status.Phase != polardbxv1.BackupFinished {
			continue
		}
		if backup.Status.LatestRecoverableTimestamp.After(beforeTime) {
			continue
		}
		if lastBackupRestoreTime == nil ||
			lastBackupRestoreTime.Before(backup.Status.LatestRecoverableTimestamp.Time) {
			lastBackupRestoreTime = &backup.Status.LatestRecoverableTimestamp.Time
			lastBackup = backup
		}
	}

	return lastBackup, nil
}

func (rc *Context) GetPXCBackupByName(name string) (*polardbxv1.PolarDBXBackup, error) {
	polardbxBackup := &polardbxv1.PolarDBXBackup{}
	pxcBackupKey := types.NamespacedName{
		Namespace: rc.Namespace(),
		Name:      name,
	}
	err := rc.Client().Get(rc.Context(), pxcBackupKey, polardbxBackup)
	if err != nil {
		return nil, err
	}
	return polardbxBackup, nil
}

func (rc *Context) SaveTaskContext(key string, t interface{}) error {
	b, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err
	}

	cm, err := rc.GetOrCreatePolarDBXBackupTaskConfigMap()
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

func (rc *Context) GetOrCreatePolarDBXBackupTaskConfigMap() (*corev1.ConfigMap, error) {
	if rc.taskConfigMap == nil {
		backup := rc.MustGetPolarDBXBackup()

		var cm corev1.ConfigMap
		err := rc.Client().Get(rc.Context(), types.NamespacedName{Namespace: rc.Namespace(), Name: name.PolarDBXBackupStableName(backup, "seekcp")}, &cm)
		if err != nil {
			if apierrors.IsNotFound(err) {
				rc.taskConfigMap = NewTaskConfigMap(backup)
				err = rc.SetControllerRefAndCreateToBackup(rc.taskConfigMap)
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

func NewTaskConfigMap(polardbxBackup *polardbxv1.PolarDBXBackup) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      convention.NewConfigMapNameForBackup(polardbxBackup, "seekcp"),
			Namespace: polardbxBackup.Namespace,
		},
		Immutable: pointer.Bool(false),
	}
}

func (rc *Context) IsTaskContextExists(key string) (bool, error) {
	cm, err := rc.GetOrCreatePolarDBXBackupTaskConfigMap()
	if err != nil {
		return false, err
	}
	_, ok := cm.Data[key]
	return ok, nil
}

func (rc *Context) GetTaskContext(key string, t interface{}) error {
	cm, err := rc.GetOrCreatePolarDBXBackupTaskConfigMap()
	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(cm.Data[key]), t)
}

func (rc *Context) GetPolarDBXTemplateName() (parameterTemplateName string) {
	polardbx := rc.MustGetPolarDBX()
	return polardbx.Spec.ParameterTemplate.Name
}

func (rc *Context) GetPolarDBXTemplateNameSpace() (parameterTemplateName string) {
	polardbx := rc.MustGetPolarDBX()
	return polardbx.Spec.ParameterTemplate.Namespace
}

func (rc *Context) SetPolarDBXParams(param map[string]map[string]polardbxv1.Params) {
	rc.polardbxParamsRoleMap = param
}

func (rc *Context) GetPolarDBXParams() (params map[string]map[string]polardbxv1.Params) {
	if rc.polardbxParamsRoleMap == nil {
		param := make(map[string]map[string]polardbxv1.Params)
		param[polardbxv1.CNReadOnly] = make(map[string]polardbxv1.Params)
		param[polardbxv1.CNReadWrite] = make(map[string]polardbxv1.Params)
		param[polardbxv1.CNRestart] = make(map[string]polardbxv1.Params)
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
		templateParam[polardbxmeta.RoleCN] = make(map[string]polardbxv1.TemplateParams)
		templateParam[polardbxmeta.RoleDN] = make(map[string]polardbxv1.TemplateParams)
		templateParam[polardbxmeta.RoleGMS] = make(map[string]polardbxv1.TemplateParams)
		rc.polardbxTemplateParams = templateParam
	}
	return rc.polardbxTemplateParams
}

func (rc *Context) GetPolarDBXParameterTemplate(nameSpace, name string) (*polardbxv1.PolarDBXParameterTemplate, error) {
	if nameSpace == "" {
		nameSpace = rc.polardbxKey.Namespace
	}
	tmKey := types.NamespacedName{
		Namespace: nameSpace,
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

func (rc *Context) MustGetPolarDBXParameterTemplate(nameSpace, name string) *polardbxv1.PolarDBXParameterTemplate {
	polardbxParameterTemplate, err := rc.GetPolarDBXParameterTemplate(nameSpace, name)
	if err != nil {
		panic(err)
	}
	return polardbxParameterTemplate
}

func (rc *Context) PolardbxParameterKey() types.NamespacedName {
	return rc.polardbxParameterKey
}

func (rc *Context) SetPolardbxParameterKey(polardbxParameterKey types.NamespacedName) {
	rc.polardbxParameterKey = polardbxParameterKey
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

func (rc *Context) GetPolarDBXParameterTemplateName() (templateName map[string]string) {
	polardbxParameter := rc.MustGetPolarDBXParameter()
	templateName = make(map[string]string)
	if polardbxParameter.Spec.NodeType.CN.Name != "" {
		templateName[polardbxmeta.RoleCN] = polardbxParameter.Spec.NodeType.CN.Name
	}
	if polardbxParameter.Spec.NodeType.DN.Name != "" {
		templateName[polardbxmeta.RoleDN] = polardbxParameter.Spec.NodeType.DN.Name
	}
	if polardbxParameter.Status.ParameterSpecSnapshot.NodeType.GMS.Name != "" {
		templateName[polardbxmeta.RoleGMS] = polardbxParameter.Status.ParameterSpecSnapshot.NodeType.GMS.Name
	}
	return templateName
}

func (rc *Context) UpdatePolarDBXParameter() error {
	if rc.polardbxParameter == nil {
		return nil
	}

	// Deep copy status before updating because client.update will update
	// the status of object.
	status := rc.polardbxParameter.Status.DeepCopy()
	err := rc.Client().Update(rc.Context(), rc.polardbxParameter)
	if err != nil {
		return err
	}

	// Restore the status (shallow copy is enough)
	rc.polardbxParameter.Status = *status

	return nil
}

func (rc *Context) UpdatePolarDBXParameterStatus() error {
	if rc.polardbxParameterStatus == nil {
		return nil
	}

	err := rc.Client().Status().Update(rc.Context(), rc.polardbxParameter)
	if err != nil {
		return err
	}
	rc.polardbxParameterStatus = rc.polardbxParameter.Status.DeepCopy()
	return nil
}

func (rc *Context) IsPolarDBXParameterStatusChanged() bool {
	if rc.polardbxParameterStatus == nil {
		return false
	}
	return !equality.Semantic.DeepEqual(&rc.polardbxParameter.Status, rc.polardbxParameterStatus)
}

func (rc *Context) IsPolarDBXParameterChanged() bool {
	if rc.polardbxParameter == nil {
		return false
	}
	return !equality.Semantic.DeepEqual(&rc.polardbxParameter, rc.polardbxParameter)
}

func (rc *Context) GetPolarDBXRestarting() bool {
	polardbx := rc.MustGetPolarDBX()
	return polardbx.Status.Restarting
}

func (rc *Context) GetRoleToRestart() map[string]bool {
	if rc.roleToRestart == nil {
		rc.roleToRestart = make(map[string]bool)
	}
	return rc.roleToRestart
}

func (rc *Context) GetRoleToRestartByRole(role string) bool {
	return rc.roleToRestart[role]
}

func (rc *Context) SetRoleToRestart(roleToRestart map[string]bool) {
	rc.roleToRestart = roleToRestart
}

func NewContext(base *control.BaseReconcileContext, configLoader func() config.Config) *Context {
	return &Context{
		BaseReconcileContext: base,
		configLoader:         configLoader,
		objectCache:          cache.NewObjectCache(base.Client(), base.Scheme()),
	}
}

func (rc *Context) NewSecretFromPolarDBX(secret *corev1.Secret) (*corev1.Secret, error) {
	backup := rc.MustGetPolarDBXBackup()
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

func (rc *Context) GetFilestreamClient() (*filestream.FileClient, error) {
	if rc.filestreamClient == nil {
		hostPort := strings.SplitN(rc.Config().Store().FilestreamServiceEndpoint(), ":", 2)
		if len(hostPort) < 2 {
			return nil, errors.New("invalid filestream endpoint: " + rc.Config().Store().FilestreamServiceEndpoint())
		}
		port, err := strconv.Atoi(hostPort[1])
		if err != nil {
			return nil, errors.New("invalid filestream port: " + hostPort[1])
		}
		rc.filestreamClient = filestream.NewFileClient(hostPort[0], port, nil)
	}
	return rc.filestreamClient, nil
}

func (rc *Context) SetPolarDBXBackupScheduleKey(key types.NamespacedName) {
	rc.polardbxBackupScheduleKey = key
}

func (rc *Context) GetPolarDBXBackupSchedule() (*polardbxv1.PolarDBXBackupSchedule, error) {
	if rc.polardbxBackupSchedule == nil {
		schedule, err := rc.objectCache.GetObject(
			rc.Context(),
			rc.polardbxBackupScheduleKey,
			&polardbxv1.PolarDBXBackupSchedule{},
		)
		if err != nil {
			return nil, err
		}
		rc.polardbxBackupSchedule = schedule.(*polardbxv1.PolarDBXBackupSchedule)
		rc.polardbxBackupScheduleStatus = rc.polardbxBackupSchedule.Status.DeepCopy()
	}
	return rc.polardbxBackupSchedule, nil
}

func (rc *Context) MustGetPolarDBXBackupSchedule() *polardbxv1.PolarDBXBackupSchedule {
	schedule, err := rc.GetPolarDBXBackupSchedule()
	if err != nil {
		panic(err)
	}
	return schedule
}

func (rc *Context) IsPolarDBXBackupScheduleStatusChanged() bool {
	if rc.polardbxBackupScheduleStatus == nil {
		return false
	}
	return !equality.Semantic.DeepEqual(rc.polardbxBackupScheduleStatus, &rc.polardbxBackupSchedule.Status)
}

func (rc *Context) UpdatePolarDBXBackupScheduleStatus() error {
	if rc.polardbxBackupScheduleStatus == nil {
		return nil
	}
	err := rc.Client().Status().Update(rc.Context(), rc.polardbxBackupSchedule)
	if err != nil {
		return err
	}
	rc.polardbxBackupScheduleStatus = rc.polardbxBackupSchedule.Status.DeepCopy()
	return nil
}

func (rc *Context) GetPolarDBXBackupListByPolarDBXName(polardbxName string) (*polardbxv1.PolarDBXBackupList, error) {
	return rc.GetPolarDBXBackupListByLabels(map[string]string{polardbxmeta.LabelName: polardbxName})
}

func (rc *Context) GetPolarDBXBackupListByScheduleName(scheduleName string) (*polardbxv1.PolarDBXBackupList, error) {
	return rc.GetPolarDBXBackupListByLabels(map[string]string{polardbxmeta.LabelBackupSchedule: scheduleName})
}

func (rc *Context) GetPolarDBXBackupListByLabels(labels map[string]string) (*polardbxv1.PolarDBXBackupList, error) {
	var backupList polardbxv1.PolarDBXBackupList
	err := rc.Client().List(rc.Context(), &backupList, client.InNamespace(rc.Namespace()), client.MatchingLabels(labels))
	if err != nil {
		return nil, err
	}
	return &backupList, nil
}

func (rc *Context) SetBackupBinlogKey(key types.NamespacedName) {
	rc.backupBinlogKey = key
}

func (rc *Context) GetPolarDBXBackupBinlog() (*polardbxv1.PolarDBXBackupBinlog, error) {
	if rc.backupBinlog == nil {
		var backupBinlog polardbxv1.PolarDBXBackupBinlog
		err := rc.Client().Get(rc.Context(), rc.backupBinlogKey, &backupBinlog)
		if err != nil {
			return nil, err
		}
		rc.backupBinlog = &backupBinlog
	}
	return rc.backupBinlog, nil
}

func (rc *Context) MustGetPolarDBXBackupBinlog() *polardbxv1.PolarDBXBackupBinlog {
	backupBinlog, err := rc.GetPolarDBXBackupBinlog()
	if err != nil {
		panic(err)
	}
	return backupBinlog
}

func (rc *Context) UpdatePolarDbXBackupBinlog() error {
	backupBinlog := rc.MustGetPolarDBXBackupBinlog()
	err := rc.Client().Update(rc.Context(), backupBinlog)
	return err
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
