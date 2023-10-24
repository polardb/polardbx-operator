package reconcile

import (
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/k8s/cache"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	. "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type FollowerContext struct {
	*control.BaseReconcileContext
	// Caches
	leanerKey      types.NamespacedName
	xStoreFollower *polardbxv1.XStoreFollower
	xStore         *polardbxv1.XStore
	xStoreContext  *Context
	objectCache    cache.ObjectLoadingCache
	// Config
	configLoader func() config.Config
	changed      bool
	commitIndex  int64
	killAllOnce  bool
}

func (rc *FollowerContext) KillAllOnce() bool {
	return rc.killAllOnce
}

func (rc *FollowerContext) SetKillAllOnce(killAllOnce bool) {
	rc.killAllOnce = killAllOnce
}

func (rc *FollowerContext) SetCommitIndex(commitIndex int64) {
	rc.commitIndex = commitIndex
}

func (rc *FollowerContext) GetCommitIndex() int64 {
	return rc.commitIndex
}

func (rc *FollowerContext) IsChanged() bool {
	return rc.changed
}

func (rc *FollowerContext) MarkChanged() {
	rc.changed = true
}

func (rc *FollowerContext) UpdateXStoreFollower() error {
	if rc.xStoreFollower == nil {
		return nil
	}
	err := rc.Client().Update(rc.Context(), rc.xStoreFollower)
	if err != nil {
		return err
	}
	return nil
}

func (rc *FollowerContext) SetControllerRef(obj client.Object) error {
	if obj == nil {
		return nil
	}
	xstoreFollower := rc.MustGetXStoreFollower()
	return ctrl.SetControllerReference(xstoreFollower, obj, rc.Scheme())
}

func (rc *FollowerContext) SetControllerRefAndCreate(obj client.Object) error {
	if err := rc.SetControllerRef(obj); err != nil {
		return err
	}
	return rc.Client().Create(rc.Context(), obj)
}

func (rc *FollowerContext) GetXStoreFollower() (*polardbxv1.XStoreFollower, error) {
	if rc.xStoreFollower == nil {
		xStoreLearner, err := rc.objectCache.GetObject(
			rc.Context(),
			rc.leanerKey,
			&polardbxv1.XStoreFollower{})
		if err != nil {
			return nil, err
		}
		rc.xStoreFollower = xStoreLearner.(*polardbxv1.XStoreFollower)
	}
	return rc.xStoreFollower, nil
}

func (rc *FollowerContext) MustGetXStoreFollower() *polardbxv1.XStoreFollower {
	xStoreFollower, err := rc.GetXStoreFollower()
	if err != nil {
		panic(err)
	}
	return xStoreFollower
}

func (rc *FollowerContext) GetXStore() (*polardbxv1.XStore, error) {
	xStoreFollower := rc.MustGetXStoreFollower()
	if rc.xStore == nil {
		xStoreKey := types.NamespacedName{Name: xStoreFollower.Spec.XStoreName, Namespace: rc.leanerKey.Namespace}
		xstore, err := rc.objectCache.GetObject(
			rc.Context(),
			xStoreKey,
			&polardbxv1.XStore{})
		if err != nil {
			return nil, err
		}
		rc.xStore = xstore.(*polardbxv1.XStore)
	}
	return rc.xStore, nil
}

func (rc *FollowerContext) CreateTmpPod(podTemplate *corev1.Pod, nodeName string) (*corev1.Pod, error) {
	pod, err := CreateTmpPod(podTemplate, nodeName, rc.GetTmpPodLabel())
	if err != nil {
		return nil, err
	}
	err = rc.SetControllerRefAndCreate(pod)
	if err != nil {
		return nil, err
	}
	return pod, nil
}

func (rc *FollowerContext) GetTmpPodLabel() map[string]string {
	return map[string]string{
		xstoremeta.LabelName:        "",
		xstoremeta.LabelRebuildTask: rc.MustGetXStoreFollower().Name,
		xstoremeta.LabelTmp:         "true",
	}
}

func (rc *FollowerContext) GetTmpPod() (*corev1.Pod, error) {
	podList := corev1.PodList{}
	err := rc.Client().List(rc.Context(), &podList, client.InNamespace(rc.Namespace()), client.MatchingLabels(rc.GetTmpPodLabel()))
	if err != nil {
		return nil, err
	}
	if len(podList.Items) > 0 {
		return &(podList.Items[0]), nil
	}
	return nil, nil
}

func (rc *FollowerContext) MustGetXStore() *polardbxv1.XStore {
	xstore, err := rc.GetXStore()
	if err != nil {
		panic(err)
	}
	return xstore
}

func (rc *FollowerContext) SetFollowerKey(key types.NamespacedName) {
	rc.leanerKey = key
}

func (rc *FollowerContext) SetXStoreContext(xstoreContext *Context) {
	rc.xStoreContext = xstoreContext
}

func (rc *FollowerContext) XStoreContext() *Context {
	return rc.xStoreContext
}

func (rc *FollowerContext) GetPodByName(podName string) (*corev1.Pod, error) {
	pod := corev1.Pod{}
	objKey := types.NamespacedName{
		Namespace: rc.Namespace(),
		Name:      podName,
	}
	err := rc.Client().Get(rc.Context(), objKey, &pod)
	if err != nil {
		return nil, err
	}
	return &pod, nil
}

func NewFollowerContext(base *control.BaseReconcileContext, configLoader func() config.Config) *FollowerContext {
	return &FollowerContext{
		BaseReconcileContext: base,
		objectCache:          cache.NewObjectCache(base.Client(), base.Scheme()),
		configLoader:         configLoader,
	}
}

func CreateTmpPod(podTemplate *corev1.Pod, nodeName string, label map[string]string) (*corev1.Pod, error) {
	pod := podTemplate.DeepCopy()
	pod.UID = ""
	pod.ResourceVersion = ""
	pod.OwnerReferences = []metav1.OwnerReference{}
	pod.Spec.NodeName = nodeName
	// add pod affinity
	pod.Status = corev1.PodStatus{}
	//modify label xstore name
	pod.SetLabels(k8shelper.PatchLabels(pod.Labels, label, map[string]string{
		xstoremeta.LabelOriginName: pod.Labels[xstoremeta.LabelName],
	}))
	pod.SetAnnotations(k8shelper.PatchAnnotations(pod.Annotations, map[string]string{
		"runmode": "debug",
	}))
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == "engine" {
			pod.Spec.Containers[i].Command = []string{"/bin/bash", "-c"}
			pod.Spec.Containers[i].Args = []string{`sleep 36000000`}
			break
		}
	}
	podAntiAffinity := pod.Spec.Affinity.PodAntiAffinity
	if podAntiAffinity != nil {
		if podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
			podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{}
		}
		podAntiAffinityTerm := corev1.PodAffinityTerm{
			TopologyKey: "kubernetes.io/hostname",
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					xstoremeta.LabelPod: podTemplate.Labels[xstoremeta.LabelName],
				},
			},
		}
		podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, podAntiAffinityTerm)
		pod.Spec.Affinity.PodAntiAffinity = podAntiAffinity
	}
	pod.Name = getTmpPodName(pod.Name)
	return pod, nil
}

func getTmpPodName(podName string) string {
	return podName + TempPodSuffix
}
