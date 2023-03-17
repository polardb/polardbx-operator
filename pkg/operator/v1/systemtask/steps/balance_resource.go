package steps

import (
	"bytes"
	"errors"
	v1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/api/v1/systemtask"
	"github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/systemtask/common"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/command"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	xstoreinstance "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/steps/instance"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"math"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"
)

const RebuildTaskName = "res-balance-rebuild"

func getMinCountNodePods(nodePodMap map[string][]corev1.Pod) (string, []corev1.Pod) {
	var nodeName string
	var pods []corev1.Pod
	podLen := math.MaxInt
	for k, v := range nodePodMap {
		if len(v) < podLen {
			nodeName = k
			pods = v
			podLen = len(pods)
		}
	}
	return nodeName, pods
}

func getMaxCountNodePods(nodePodMap map[string][]corev1.Pod) (string, []corev1.Pod) {
	var nodeName string
	var pods []corev1.Pod
	podLen := math.MinInt
	for k, v := range nodePodMap {
		if len(v) > podLen {
			nodeName = k
			pods = v
			podLen = len(pods)
		}
	}
	return nodeName, pods
}

func getTargetXStorePod(rc *common.Context, logger bool) (*corev1.Pod, string, error) {
	nodePodMap, err := rc.GetNodeXStorePodMap(true, logger)
	if err != nil {
		return nil, "", err
	}
	//not logger
	_, maxCountPods := getMaxCountNodePods(nodePodMap)
	minCountNodeName, minCountPods := getMinCountNodePods(nodePodMap)
	var targetPod *corev1.Pod
	if len(maxCountPods)-len(minCountPods) > 1 {
		xstoreNameMap := make(map[string]bool)
		for _, pod := range minCountPods {
			xstoreNameMap[pod.Labels[xstoremeta.LabelName]] = true
		}
		for _, pod := range maxCountPods {
			_, ok := xstoreNameMap[pod.Labels[xstoremeta.LabelName]]
			if !ok {
				targetPod = &pod
				break
			}
		}
	}
	return targetPod, minCountNodeName, nil
}

func getTargetXStorePodAllRole(rc *common.Context) (*corev1.Pod, string, error) {
	nodePodMap, err := rc.GetNodeXStorePodMap(false, false)
	if err != nil {
		return nil, "", err
	}
	_, maxCountPods := getMaxCountNodePods(nodePodMap)
	minCountNodeName, minCountPods := getMinCountNodePods(nodePodMap)
	var targetPod *corev1.Pod
	if len(maxCountPods)-len(minCountPods) > 1 {
		xstoreNameMap := make(map[string]bool)
		for _, pod := range minCountPods {
			xstoreNameMap[pod.Labels[xstoremeta.LabelName]] = true
		}
		for _, pod := range maxCountPods {
			_, ok := xstoreNameMap[pod.Labels[xstoremeta.LabelName]]
			if !ok {
				// check if logger
				if xstoremeta.IsPodRoleVoter(&pod) {
					targetPod = &pod
					break
				}
			}
		}
	}
	return targetPod, minCountNodeName, nil
}

func newXStoreFollower(rc *common.Context, targetPod *corev1.Pod, targetNodeName string) (*v1.XStoreFollower, error) {
	xstoreName := targetPod.Labels[xstoremeta.LabelName]
	xstore, err := rc.GetXStoreByName(xstoreName)
	if err != nil {
		return nil, err
	}
	if xstore.Spec.PrimaryXStore != "" {
		xstoreName = xstore.Spec.PrimaryXStore
	}
	rebuildTask := v1.XStoreFollower{
		ObjectMeta: metav1.ObjectMeta{
			Name:      RebuildTaskName,
			Namespace: rc.Namespace(),
			Labels: map[string]string{
				common.LabelBalanceResource: "true",
			},
		},
		Spec: v1.XStoreFollowerSpec{
			Local:         false,
			NodeName:      targetNodeName,
			TargetPodName: targetPod.Name,
			XStoreName:    xstoreName,
		},
	}
	return &rebuildTask, nil
}

var CreateBalanceTaskIfNeed = common.NewStepBinder("CreateBalanceTaskIfNeed",
	func(rc *common.Context, flow control.Flow) (reconcile.Result, error) {
		systemTask := rc.MustGetSystemTask()

		var xstoreFollowerList v1.XStoreFollowerList
		err := rc.Client().List(rc.Context(), &xstoreFollowerList, client.InNamespace(rc.Namespace()), client.MatchingLabels(map[string]string{
			common.LabelBalanceResource: "true",
		}))
		if err != nil {
			return flow.RetryErr(err, "failed to get xstore follower list")
		}
		if len(xstoreFollowerList.Items) > 0 {
			for _, xf := range xstoreFollowerList.Items {
				if xf.Status.Phase == xstore.FollowerPhaseSuccess {
					rc.Client().Delete(rc.Context(), &xf)
				}
			}
			return flow.RetryAfter(time.Second, "try for xstore follower status")
		}
		if systemTask.Status.StBalanceResourceStatus.RebuildFinish {
			return flow.Pass()
		}
		var targetPod *corev1.Pod
		var targetNode string
		targetPod, targetNode, err = getTargetXStorePod(rc, false)
		if err != nil {
			return flow.RetryErr(err, "failed to get xstore pod", "logger", false)
		}
		if targetPod == nil {
			targetPod, targetNode, err = getTargetXStorePod(rc, true)
			if err != nil {
				return flow.RetryErr(err, "failed to get xstore pod", "logger", false)
			}
		}

		if targetPod == nil {
			targetPod, targetNode, err = getTargetXStorePodAllRole(rc)
			if err != nil {
				return flow.RetryErr(err, "failed to get xstore pod", "all role", true)
			}
		}

		rc.MarkSystemTaskChanged()
		if targetPod == nil {
			systemTask.Status.StBalanceResourceStatus.RebuildFinish = true
			return flow.Pass()
		}
		systemTask.Status.StBalanceResourceStatus.RebuildTaskName = RebuildTaskName
		//kill mysqld in the target pod, ensure not leader
		cmd := command.NewCanonicalCommandBuilder().Process().KillAllMyProcess().Build()
		buf := &bytes.Buffer{}
		err = rc.ExecuteCommandOn(targetPod, convention.ContainerEngine, cmd, control.ExecOptions{
			Logger:  flow.Logger(),
			Stdout:  buf,
			Timeout: 8 * time.Second,
		})
		rebuildTask, err := newXStoreFollower(rc, targetPod, targetNode)
		if err != nil {
			return flow.RetryErr(err, "Failed to create xstore follower task")
		}
		err = rc.SetControllerRefAndCreate(rebuildTask)
		if err != nil {
			return flow.RetryErr(err, "Failed to Create rebuild task", "task name", RebuildTaskName)
		}
		return flow.Retry("CreateBalanceTaskIfNeed Success")
	})

func IsRebuildFinish(rc *common.Context) bool {
	systemTask := rc.MustGetSystemTask()
	return systemTask.Status.StBalanceResourceStatus.RebuildFinish
}

var CheckAllXStoreHealth = common.NewStepBinder("CheckAllXStoreHealth",
	func(rc *common.Context, flow control.Flow) (reconcile.Result, error) {
		//check if xstore has leader follower logger
		var xstores v1.XStoreList
		err := rc.Client().List(rc.Context(), &xstores, client.InNamespace(rc.Namespace()))
		if err != nil {
			return flow.RetryErr(err, "Failed to")
		}
		systemTask := rc.MustGetSystemTask()
		if len(xstores.Items) == 0 {
			systemTask.Status.Phase = systemtask.SuccessPhase
			rc.MarkSystemTaskChanged()
			return flow.Pass()
		}
		for _, xstore := range xstores.Items {
			//fetch pods of this xstore
			var podList corev1.PodList
			err := rc.Client().List(rc.Context(), &podList, client.InNamespace(rc.Namespace()), client.MatchingLabels{
				xstoremeta.LabelName: xstore.Name,
			})
			if err != nil {
				return flow.RetryErr(err, "failed to list pods", "xstore", xstore.Name)
			}
			markMap := make(map[string]bool)
			for _, pod := range podList.Items {
				markMap[pod.Name] = true
			}
			if len(markMap) < 3 {
				return flow.RetryErr(errors.New("UnhealthXstore"), "unhealth xstore", "xstore name", xstore.Name)
			}
		}
		return flow.Pass()
	})

type MyNode struct {
	Name      string
	CandNum   int
	LeaderNum int
	Neighbors map[string]*MyNode
	Pods      []corev1.Pod
}

func isLeaderPod(rc *common.Context, pod corev1.Pod, logger logr.Logger) (bool, error) {
	xstoreRequest := rc.Request()
	xstoreRequest.Name = pod.Labels[xstoremeta.LabelName]
	xstoreContext := xstorev1reconcile.NewContext(
		control.NewBaseReconcileContextFrom(rc.BaseReconcileContext, rc.Context(), xstoreRequest),
		rc.ConfigLoader(),
	)
	role, _, err := xstoreinstance.ReportRoleAndCurrentLeader(xstoreContext, &pod, logger)
	if err != nil {
		return false, err
	}
	return role == xstoremeta.RoleLeader, nil
}

func BuildMyNodeFromPods(rc *common.Context, pods []corev1.Pod, logger logr.Logger) (map[string]*MyNode, error) {
	xstoreToPods := make(map[string][]corev1.Pod)
	nodes := make(map[string]*MyNode)
	for _, pod := range pods {
		if !xstoremeta.IsPodRoleCandidate(&pod) {
			continue
		}
		xstoreName := pod.Labels[xstoremeta.LabelName]
		xstorePods, ok := xstoreToPods[xstoreName]
		if !ok {
			xstorePods = make([]corev1.Pod, 0)
		}
		xstorePods = append(xstorePods, pod)
		xstoreToPods[xstoreName] = xstorePods
		nodeName := pod.Spec.NodeName
		_, ok = nodes[nodeName]
		if !ok {
			nodes[nodeName] = &MyNode{
				Name:      nodeName,
				CandNum:   0,
				LeaderNum: 0,
				Neighbors: make(map[string]*MyNode),
				Pods:      make([]corev1.Pod, 0),
			}
		}
		nodes[nodeName].Pods = append(nodes[nodeName].Pods, pod)
		nodes[nodeName].CandNum = nodes[nodeName].CandNum + 1
		isLeader, err := isLeaderPod(rc, pod, logger)
		if err != nil {
			return nil, err
		}
		if isLeader {
			nodes[nodeName].LeaderNum = nodes[nodeName].LeaderNum + 1
		}
	}
	for _, v := range xstoreToPods {
		neighborNodes := make([]*MyNode, 0)
		for _, pod := range v {
			nodeName := pod.Spec.NodeName
			node := nodes[nodeName]
			neighborNodes = append(neighborNodes, node)
		}
		for i := 0; i < len(neighborNodes)-1; i++ {
			node := neighborNodes[i]
			for j := i + 1; j < len(neighborNodes); j++ {
				nodeInner := neighborNodes[j]
				node.Neighbors[nodeInner.Name] = nodeInner
				nodeInner.Neighbors[node.Name] = node
			}
		}
	}
	return nodes, nil
}

func getMinCountNode(nodes map[string]*MyNode, cntFunc func(node *MyNode) int) (map[string]*MyNode, int) {
	var cnt int = math.MaxInt
	for _, v := range nodes {
		if cntFunc(v) < cnt {
			cnt = cntFunc(v)
		}
	}
	myNodes := map[string]*MyNode{}
	for _, v := range nodes {
		if cntFunc(v) == cnt {
			myNodes[v.Name] = v
		}
	}
	return myNodes, cnt
}

func VisitNode(nodes map[string]*MyNode, minLeaderCount int, visitedNodes map[string]bool, leader bool, minCandPodCount int) (*MyNode, *MyNode, bool) {
	for _, node := range nodes {
		_, ok := visitedNodes[node.Name]
		if ok {
			continue
		}
		visitedNodes[node.Name] = true
		if leader {
			if node.LeaderNum-minLeaderCount >= 2 {
				return node, nil, true
			}
		} else {
			if minLeaderCount == -1 {
				minLeaderCount = node.LeaderNum
			}
			if minCandPodCount == -1 {
				minCandPodCount = node.CandNum
			}
			if (node.CandNum > minCandPodCount && node.LeaderNum > minLeaderCount) || node.LeaderNum-minLeaderCount >= 2 {
				return node, nil, true
			}
		}

		fromNode, toNode, found := VisitNode(node.Neighbors, minLeaderCount, visitedNodes, leader, minCandPodCount)
		if found {
			if toNode == nil {
				toNode = node
			}
			return fromNode, toNode, true
		}
	}
	return nil, nil, false
}

func changeLeader(rc *common.Context, leaderPod corev1.Pod, targetPod corev1.Pod, logger logr.Logger) {
	cmd := command.NewCanonicalCommandBuilder().Consensus().SetLeader(targetPod.Name).Build()
	rc.ExecuteCommandOn(&leaderPod, convention.ContainerEngine, cmd, control.ExecOptions{
		Logger:  logger,
		Timeout: 8 * time.Second,
	})
}

var BalanceRole = common.NewStepBinder("BalanceRole",
	func(rc *common.Context, flow control.Flow) (reconcile.Result, error) {
		systemTask := rc.MustGetSystemTask()
		xstorePods, err := rc.GetAllXStorePods()
		if err != nil {
			return flow.RetryErr(err, "Failed to get xstore pods")
		}
		myNodes, err := BuildMyNodeFromPods(rc, xstorePods, flow.Logger())
		if err != nil {
			return flow.RetryErr(err, "Failed to BuildMyNodeFromPods")
		}
		minLeaderCountNodes, minLeaderCount := getMinCountNode(myNodes, func(node *MyNode) int {
			return node.LeaderNum
		})
		visitedNodes := map[string]bool{}
		fromNode, toNode, found := VisitNode(minLeaderCountNodes, minLeaderCount, visitedNodes, true, -1)
		if systemTask.Status.StBalanceResourceStatus.BalanceLeaderFinish || !found {
			systemTask.Status.StBalanceResourceStatus.BalanceLeaderFinish = true
			rc.MarkSystemTaskChanged()
			minCandCountNodes, _ := getMinCountNode(myNodes, func(node *MyNode) int {
				return node.CandNum
			})
			allMinCountNodes := map[string]*MyNode{}
			for k, v := range minCandCountNodes {
				_, ok := minLeaderCountNodes[k]
				if ok {
					allMinCountNodes[k] = v
				}
			}
			visitedNodes = map[string]bool{}
			fromNode, toNode, found = VisitNode(allMinCountNodes, -1, visitedNodes, false, -1)
		}
		if !found {
			systemTask.Status.Phase = systemtask.SuccessPhase
			rc.MarkSystemTaskChanged()
			return flow.Retry("BalanceRole Finishes")
		}
		// select xstore pod
		xstoreLeaderPodMap := map[string]corev1.Pod{}
		for _, fromNodePod := range fromNode.Pods {
			leader, err := isLeaderPod(rc, fromNodePod, flow.Logger())
			if err != nil {
				return flow.RetryErr(err, "Fail check if leader")
			}
			if leader {
				xstoreLeaderPodMap[fromNodePod.Labels[xstoremeta.LabelName]] = fromNodePod
			}
		}
		var leaderPod corev1.Pod
		var targetNodePod corev1.Pod
		var ok bool
		for _, toNodePod := range toNode.Pods {
			xstoreName := toNodePod.Labels[xstoremeta.LabelName]
			leaderPod, ok = xstoreLeaderPodMap[xstoreName]
			if ok {
				targetNodePod = toNodePod
				break
			}
		}
		if targetNodePod.Name != "" {
			changeLeader(rc, leaderPod, targetNodePod, flow.Logger())
		}
		return flow.Retry("Retry BalanceRole")
	})
