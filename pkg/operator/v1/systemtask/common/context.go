package common

import (
	"fmt"
	v1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/api/v1/systemtask"
	"github.com/alibaba/polardbx-operator/pkg/k8s/cache"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/config"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Context struct {
	*control.BaseReconcileContext

	// Caches
	taskKey     types.NamespacedName
	taskChanged bool
	systemTask  *v1.SystemTask
	objectCache cache.ObjectLoadingCache

	// Hint cache
	controllerHints []string
	// Config
	configLoader func() config.Config

	//balance resource
	brTargetPod      *corev1.Pod
	brTargetNodeName string
}

func (ctx *Context) ConfigLoader() func() config.Config {
	return ctx.configLoader
}

func (ctx *Context) BrTargetPod() *corev1.Pod {
	return ctx.brTargetPod
}

func (ctx *Context) SetBrTargetPod(brTargetPod *corev1.Pod) {
	ctx.brTargetPod = brTargetPod
}

func (ctx *Context) BrTargetNodeName() string {
	return ctx.brTargetNodeName
}

func (ctx *Context) SetBrTargetNodeName(brTargetNodeName string) {
	ctx.brTargetNodeName = brTargetNodeName
}

func NewContext(base *control.BaseReconcileContext, configLoader func() config.Config) *Context {
	return &Context{
		BaseReconcileContext: base,
		objectCache:          cache.NewObjectCache(base.Client(), base.Scheme()),
		configLoader:         configLoader,
	}
}

func (ctx *Context) SetKey(taskKey types.NamespacedName) {
	ctx.taskKey = taskKey
}

func (ctx *Context) MustGetSystemTask() *v1.SystemTask {
	if ctx.systemTask == nil {
		var systemTask v1.SystemTask
		err := ctx.Client().Get(ctx.Context(), ctx.taskKey, &systemTask)
		if err != nil {
			panic(err)
		}
		if systemTask.Status.StBalanceResourceStatus == nil {
			systemTask.Status.StBalanceResourceStatus = &systemtask.StBalanceResourceStatus{}
		}
		ctx.systemTask = &systemTask
	}
	return ctx.systemTask
}

func (ctx *Context) IsSystemTaskChanged() bool {
	return ctx.taskChanged
}

func (ctx *Context) MarkSystemTaskChanged() {
	ctx.taskChanged = true
	return
}

func (ctx *Context) UpdateSystemTask() error {
	err := ctx.Client().Update(ctx.Context(), ctx.systemTask)
	return err
}

func (ctx *Context) GetAllXStores() ([]v1.XStore, error) {
	var xstoreList v1.XStoreList
	err := ctx.Client().List(ctx.Context(), &xstoreList, client.InNamespace(ctx.Namespace()))
	return xstoreList.Items, err
}

func (ctx *Context) GetXStoreByName(name string) (*v1.XStore, error) {
	var xstore v1.XStore
	objKey := types.NamespacedName{
		Name:      name,
		Namespace: ctx.Namespace(),
	}
	err := ctx.Client().Get(ctx.Context(), objKey, &xstore)
	if err != nil {
		return nil, err
	}
	return &xstore, nil
}

func (ctx *Context) GetAllXStorePods() ([]corev1.Pod, error) {
	pods := make([]corev1.Pod, 0)
	for _, role := range []string{polardbxmeta.RoleDN} {
		var podList corev1.PodList
		err := ctx.Client().List(ctx.Context(), &podList, client.InNamespace(ctx.Namespace()), client.MatchingLabels(map[string]string{
			polardbxmeta.LabelRole: role,
		}))
		if err != nil {
			return nil, err
		}
		pods = append(pods, podList.Items...)
	}
	return pods, nil
}

func (ctx *Context) GetNodeXStorePodMap(separateRole bool, logger bool) (map[string][]corev1.Pod, error) {
	pods, err := ctx.GetAllXStorePods()
	if err != nil {
		return nil, err
	}
	newPods := make([]corev1.Pod, 0)
	if separateRole {
		for _, pod := range pods {
			if logger {
				if xstoremeta.IsPodRoleVoter(&pod) {
					newPods = append(newPods, pod)
				}
			} else {
				if !xstoremeta.IsPodRoleVoter(&pod) {
					newPods = append(newPods, pod)
				}
			}
		}
	} else {
		newPods = pods
	}

	result := make(map[string][]corev1.Pod)
	for _, pod := range newPods {
		nodeName := pod.Spec.NodeName
		if nodeName == "" {
			return nil, fmt.Errorf("node name not found, pod name: %s", pod.Name)
		}
		nodePods, ok := result[nodeName]
		if !ok {
			nodePods = make([]corev1.Pod, 0)
		}
		result[nodeName] = append(nodePods, pod)
	}
	nodes, err := ctx.GetAllNodes()
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
		_, ok := result[node.Name]
		if !ok {
			result[node.Name] = make([]corev1.Pod, 0)
		}
	}
	return result, nil
}

func (ctx *Context) GetAllNodes() ([]corev1.Node, error) {
	systemtask := ctx.MustGetSystemTask()
	var nodeList corev1.NodeList
	err := ctx.Client().List(ctx.Context(), &nodeList)
	result := make([]corev1.Node, 0)
	if len(systemtask.Spec.Nodes) > 0 {
		nodeMap := make(map[string]bool, len(systemtask.Spec.Nodes))
		for _, nodeName := range systemtask.Spec.Nodes {
			nodeMap[nodeName] = true
		}
		for _, item := range nodeList.Items {
			if _, ok := nodeMap[item.Name]; ok {
				result = append(result, item)
			}
		}
	}

	return result, err
}

func (rc *Context) SetControllerRef(obj client.Object) error {
	if obj == nil {
		return nil
	}
	systemTask := rc.MustGetSystemTask()
	return ctrl.SetControllerReference(systemTask, obj, rc.Scheme())
}

func (rc *Context) SetControllerRefAndCreate(obj client.Object) error {
	if err := rc.SetControllerRef(obj); err != nil {
		return err
	}
	return rc.Client().Create(rc.Context(), obj)
}
