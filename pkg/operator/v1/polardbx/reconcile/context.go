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
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

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
	xstoreconvention "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	dbutil "github.com/alibaba/polardbx-operator/pkg/util/database"
)

type Context struct {
	*control.BaseReconcileContext

	// Caches
	objectCache     cache.ObjectLoadingCache
	polardbxKey     types.NamespacedName
	polardbxChanged bool
	polardbx        *polardbxv1.PolarDBXCluster
	polardbxStatus  *polardbxv1.PolarDBXClusterStatus
	cnDeployments   map[string]*appsv1.Deployment
	cdcDeployments  map[string]*appsv1.Deployment
	podsByRole      map[string][]corev1.Pod
	nodes           []corev1.Node
	gmsStore        *polardbxv1.XStore
	dnStores        map[int]*polardbxv1.XStore

	// Hint cache
	controllerHints []string

	// Config
	configLoader func() config.Config

	// Managers
	gmsManager   gms.Manager
	groupManager group.GroupManager
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

func (rc *Context) SetControllerRef(obj client.Object) error {
	if obj == nil {
		return nil
	}
	polardbx := rc.MustGetPolarDBX()
	return ctrl.SetControllerReference(polardbx, obj, rc.Scheme())
}

func (rc *Context) SetControllerRefAndCreate(obj client.Object) error {
	if err := rc.SetControllerRef(obj); err != nil {
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

func (rc *Context) IsPolarDBXStatusChanged() bool {
	if rc.polardbxStatus == nil {
		return false
	}
	return !equality.Semantic.DeepEqual(&rc.polardbx.Status, rc.polardbxStatus)
}

func (rc *Context) IsPolarDBXChanged() bool {
	return rc.polardbxChanged
}

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

func (rc *Context) GetPolarDBXSecret(secretType convention.SecretType) (*corev1.Secret, error) {
	polardbx, err := rc.GetPolarDBX()
	if err != nil {
		return nil, fmt.Errorf("unable to get polardbx object: %w", err)
	}

	secretKey := types.NamespacedName{
		Namespace: rc.polardbxKey.Namespace,
		Name:      convention.NewSecretName(polardbx, secretType),
	}

	secret, err := rc.objectCache.GetObject(rc.Context(), secretKey, &corev1.Secret{})
	if err != nil {
		return nil, err
	}
	if err := k8shelper.CheckControllerReference(secret, rc.MustGetPolarDBX()); err != nil {
		return nil, err
	}

	return secret.(*corev1.Secret), nil
}

func (rc *Context) GetPolarDBXEncodeKey() (string, error) {
	secret, err := rc.GetPolarDBXSecret(convention.SecretTypeSecurity)
	if err != nil {
		return "", err
	}
	encodeKey, ok := secret.Data[convention.SecretKeyEncodeKey]
	if !ok {
		return "", errors.New("not found")
	}
	return string(encodeKey), nil
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
		Namespace: rc.polardbxKey.Namespace,
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

func (rc *Context) GetDNMap() (map[int]*polardbxv1.XStore, error) {
	if rc.dnStores == nil {
		polardbx, err := rc.GetPolarDBX()
		if err != nil {
			return nil, fmt.Errorf("unable to get polardbx object: %w", err)
		}

		var xstoreList polardbxv1.XStoreList
		err = rc.Client().List(rc.Context(), &xstoreList,
			client.InNamespace(rc.polardbxKey.Namespace),
			client.MatchingLabels(convention.ConstLabels(polardbx, polardbxmeta.RoleDN)),
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

		rc.dnStores = dnStores
	}
	return rc.dnStores, nil
}

func (rc *Context) GetOrderedDNList() ([]*polardbxv1.XStore, error) {
	dnMap, err := rc.GetDNMap()
	if err != nil {
		return nil, err
	}

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
	return dnList, nil
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

func (rc *Context) GetDeploymentMap(role string) (map[string]*appsv1.Deployment, error) {
	var deploymentMapPtr *map[string]*appsv1.Deployment
	switch role {
	case polardbxmeta.RoleCN:
		deploymentMapPtr = &rc.cnDeployments
	case polardbxmeta.RoleCDC:
		deploymentMapPtr = &rc.cdcDeployments
	default:
		panic("required role to be cn or cdc, but found " + role)
	}

	if *deploymentMapPtr == nil {
		polardbx, err := rc.GetPolarDBX()
		if err != nil {
			return nil, fmt.Errorf("unable to get polardbx object: %w", err)
		}

		var deploymentList appsv1.DeploymentList
		err = rc.Client().List(rc.Context(), &deploymentList,
			client.InNamespace(rc.polardbxKey.Namespace),
			client.MatchingLabels(convention.ConstLabels(polardbx, role)),
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

		*deploymentMapPtr = deploymentMap
	}

	return *deploymentMapPtr, nil
}

func (rc *Context) GetPods(role string) ([]corev1.Pod, error) {
	if role != polardbxmeta.RoleCN && role != polardbxmeta.RoleCDC {
		panic("required role to be cn or cdc, but found " + role)
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
			client.MatchingLabels(convention.ConstLabels(polardbx, role)),
		)
		if err != nil {
			return nil, err
		}

		if rc.podsByRole == nil {
			rc.podsByRole = make(map[string][]corev1.Pod)
		}

		rc.podsByRole[role] = podList.Items
	}

	return rc.podsByRole[role], nil
}

func (rc *Context) GetGMS() (*polardbxv1.XStore, error) {
	polardbx := rc.MustGetPolarDBX()

	if polardbx.Spec.ShareGMS {
		return rc.GetDN(0)
	} else {
		if rc.gmsStore == nil {
			gmsStore, err := rc.objectCache.GetObject(
				rc.Context(),
				types.NamespacedName{
					Namespace: rc.polardbxKey.Namespace,
					Name:      convention.NewGMSName(polardbx),
				},
				&polardbxv1.XStore{},
			)
			if err != nil {
				return nil, err
			}

			if err := k8shelper.CheckControllerReference(gmsStore, polardbx); err != nil {
				return nil, err
			}
			rc.gmsStore = gmsStore.(*polardbxv1.XStore)
		}
		return rc.gmsStore, nil
	}
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
		storageType, err := gms.GetStorageType(gmsEngine, gmsStore.Status.EngineVersion)
		if err != nil {
			return nil, err
		}

		// Mock GMS id if sharing GMS
		rc.gmsManager = gms.NewGmsManager(
			rc.Context(),
			polardbx.Name,
			&gms.MetaDB{
				Id:     convention.NewGMSName(polardbx),
				Host:   gmsService.Spec.ClusterIP,
				Port:   int(k8shelper.MustGetPortFromService(gmsService, xstoreconvention.PortAccess).Port),
				XPort:  xPort,
				User:   xstoreconvention.SuperAccount,
				Passwd: string(gmsSecret.Data[xstoreconvention.SuperAccount]),
				Type:   storageType,
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

		service, err := rc.GetPolarDBXService(convention.ServiceTypeReadWrite)
		if err != nil {
			return nil, err
		}

		secret, err := rc.GetPolarDBXSecret(convention.SecretTypeAccount)
		if err != nil {
			return nil, err
		}

		caseInsensitive, _ := strconv.ParseBool(polardbx.Annotations[polardbxmeta.AnnotationSchemaCaseInsensitive])
		rc.groupManager = group.NewGroupManager(
			rc.Context(),
			dbutil.MySQLDataSource{
				Host:     service.Spec.ClusterIP,
				Port:     int(k8shelper.MustGetPortFromService(service, convention.PortAccess).Port),
				Username: convention.RootAccount,
				Password: string(secret.Data[convention.RootAccount]),
			},
			caseInsensitive,
		)
	}

	return rc.groupManager, nil
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

	if err := rc.BaseReconcileContext.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func NewContext(base *control.BaseReconcileContext, configLoader func() config.Config) *Context {
	return &Context{
		BaseReconcileContext: base,
		configLoader:         configLoader,
		objectCache:          cache.NewObjectCache(base.Client(), base.Scheme()),
	}
}
