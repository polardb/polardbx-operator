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

package instance

import (
	"bytes"
	"encoding/json"
	"fmt"
	polarxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/api/v1/common"
	"github.com/alibaba/polardbx-operator/pkg/featuregate"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/common/channel"
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	xstoreexec "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/command"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	commonsteps "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/common/steps"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

const (
	SlaveSqlRunningYes      string  = "Yes"
	ReplicationDelaySeconds float64 = 1800 //half an hour
)

type ConsensusGlobalInfoItem struct {
	ServerId       int64  `json:"serverId,omitempty"`
	Addr           string `json:"addr,omitempty"`
	Role           string `json:"role,omitempty"`
	MatchIndex     int64  `json:"matchIndex,omitempty"`
	NextIndex      int64  `json:"nextIndex,omitempty"`
	AppliedIndex   int64  `json:"appliedIndex,omitempty"`
	ElectionWeight int64  `json:"electionWeight,omitempty"`
	ForceSync      bool   `json:"forceSync,omitempty"`
	LearnerSource  int64  `json:"learnerSource,omitempty"`
	IsLogger       *bool  `json:"isLogger,omitempty"`
	Local          bool   `json:"local,omitempty"`
	Pod            string `json:"pod,omitempty"`
}

func ShowGlobalInfo(rc *xstorev1reconcile.Context, pod *corev1.Pod, logger logr.Logger) ([]ConsensusGlobalInfoItem, error) {
	cmd := xstoreexec.NewCanonicalCommandBuilder().Consensus().List(true).Build()
	stdout := &bytes.Buffer{}
	err := rc.ExecuteCommandOn(pod, "engine", cmd, control.ExecOptions{
		Logger:  logger,
		Stdout:  stdout,
		Timeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	commandResult := stdout.String()
	logger.Info(fmt.Sprintf("show global info result = %s", commandResult))
	parsedResult, err := xstoreexec.ParseCommandResultGenerally(commandResult)
	if err != nil {
		return nil, err
	}
	if len(parsedResult) == 0 {
		return nil, fmt.Errorf("failed to get global info")
	}
	result := make([]ConsensusGlobalInfoItem, 0, len(parsedResult))
	for _, oneParsedResult := range parsedResult {
		item := ConsensusGlobalInfoItem{}
		item.ServerId, err = strconv.ParseInt(strings.TrimSpace(oneParsedResult["server_id"].(string)), 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse server_id")
		}
		item.Addr = strings.TrimSpace(oneParsedResult["addr"].(string))
		item.Role = strings.TrimSpace(oneParsedResult["role"].(string))
		item.MatchIndex, err = strconv.ParseInt(strings.TrimSpace(oneParsedResult["match_index"].(string)), 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse match_index")
		}
		item.NextIndex, err = strconv.ParseInt(strings.TrimSpace(oneParsedResult["next_index"].(string)), 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse next_index")
		}
		item.AppliedIndex, err = strconv.ParseInt(strings.TrimSpace(oneParsedResult["applied_index"].(string)), 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse applied_index")
		}
		item.ElectionWeight, err = strconv.ParseInt(strings.TrimSpace(oneParsedResult["election_weight"].(string)), 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse election_weight")
		}
		item.ForceSync = "Yes" == strings.TrimSpace(oneParsedResult["force_sync"].(string))
		item.LearnerSource, err = strconv.ParseInt(strings.TrimSpace(oneParsedResult["learner_source"].(string)), 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse learner_source")
		}
		result = append(result, item)
	}
	return result, nil
}

type ShowSlaveStatusResult struct {
	RelayLogFile         string
	RelayLogPos          int64
	SlaveIORunning       string
	SlaveSQLRunning      string
	SalveSqlRunningState string
	LastSqlError         string
	SecondsBehindMaster  float64
}

func ShowSlaveStatus(rc *xstorev1reconcile.Context, pod *corev1.Pod, logger logr.Logger) (*ShowSlaveStatusResult, error) {
	// Setup buffer and start a report role command.
	stdout := &bytes.Buffer{}
	cmd := xstoreexec.NewCanonicalCommandBuilder().Consensus().ShowSlaveStatus().Build()
	err := rc.ExecuteCommandOn(pod, "engine", cmd, control.ExecOptions{
		Logger:  logger,
		Stdout:  stdout,
		Timeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	parsedResult, err := xstoreexec.ParseCommandResultGenerally(stdout.String())
	if err != nil {
		return nil, err
	}
	if len(parsedResult) == 0 {
		return nil, fmt.Errorf("failed to get show slave status")
	}
	oneParsedResult := parsedResult[0]
	relayLogPos, err := strconv.ParseInt(strings.TrimSpace(oneParsedResult["relay_log_pos"].(string)), 10, 64)
	if err != nil {
		return nil, err
	}
	var secondsBehindMaster float64
	secondsBehindMaster, err = strconv.ParseFloat(strings.TrimSpace(oneParsedResult["seconds_behind_master"].(string)), 64)
	if err != nil {
		secondsBehindMaster = -1
	}
	showSlaveStatusResult := ShowSlaveStatusResult{
		RelayLogFile:         strings.TrimSpace(oneParsedResult["relay_log_file"].(string)),
		RelayLogPos:          relayLogPos,
		SlaveIORunning:       strings.TrimSpace(oneParsedResult["slave_io_running"].(string)),
		SlaveSQLRunning:      strings.TrimSpace(oneParsedResult["slave_sql_running"].(string)),
		SalveSqlRunningState: strings.TrimSpace(oneParsedResult["slave_sql_running_state"].(string)),
		LastSqlError:         strings.TrimSpace(oneParsedResult["last_sql_error"].(string)),
		SecondsBehindMaster:  secondsBehindMaster,
	}
	return &showSlaveStatusResult, nil
}

type ConsensusLocalInfo struct {
	Pod          string
	Addr         string
	ServerId     string
	Role         string
	LeaderPod    string
	LeaderAddr   string
	CurrentTerm  string
	LastLogIndex string
	AppliedIndex string
	CommitIndex  string
}

func ShowThis(rc *xstorev1reconcile.Context, pod *corev1.Pod, logger logr.Logger, full bool) (*ConsensusLocalInfo, error) {
	// Setup buffer and start a report role command.
	stdout := &bytes.Buffer{}
	cmd := xstoreexec.NewCanonicalCommandBuilder().Consensus().This(full).Build()
	err := rc.ExecuteCommandOn(pod, "engine", cmd, control.ExecOptions{
		Logger:  logger,
		Stdout:  stdout,
		Timeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	parsedResult, err := xstoreexec.ParseCommandResultGenerally(stdout.String())
	if err != nil {
		return nil, err
	}
	if len(parsedResult) == 0 {
		return nil, fmt.Errorf("failed to get show slave status")
	}
	oneParsedResult := parsedResult[0]
	var getStringValue = func(m map[string]interface{}, key string) string {
		val, ok := m[key]
		if ok {
			return val.(string)
		}
		return ""
	}
	if full {
		return &ConsensusLocalInfo{
			Pod:          getStringValue(oneParsedResult, "pod"),
			Addr:         getStringValue(oneParsedResult, "addr"),
			ServerId:     getStringValue(oneParsedResult, "server_id"),
			Role:         getStringValue(oneParsedResult, "role"),
			LeaderPod:    getStringValue(oneParsedResult, "leader_pod"),
			LeaderAddr:   getStringValue(oneParsedResult, "leader_addr"),
			CurrentTerm:  getStringValue(oneParsedResult, "current_term"),
			LastLogIndex: getStringValue(oneParsedResult, "last_log_index"),
			AppliedIndex: getStringValue(oneParsedResult, "applied_index"),
			CommitIndex:  getStringValue(oneParsedResult, "commit_index"),
		}, nil
	}
	return &ConsensusLocalInfo{
		Pod:        getStringValue(oneParsedResult, "pod"),
		Addr:       getStringValue(oneParsedResult, "addr"),
		Role:       getStringValue(oneParsedResult, "role"),
		LeaderPod:  getStringValue(oneParsedResult, "leader_pod"),
		LeaderAddr: getStringValue(oneParsedResult, "leader_addr"),
	}, nil
}

func parseConsensusReportRoleResult(s string) (string, string) {
	lines := strings.Split(s, "\n")
	if len(lines) == 1 {
		return strings.TrimSpace(lines[0]), ""
	} else {
		return strings.TrimSpace(lines[0]), strings.TrimSpace(lines[1])
	}
}

func ReportRoleAndCurrentLeader(rc *xstorev1reconcile.Context, pod *corev1.Pod, logger logr.Logger) (string, string, error) {
	// Setup buffer and start a report role command.
	stdout := &bytes.Buffer{}
	cmd := xstoreexec.NewCanonicalCommandBuilder().Consensus().ReportRole(true).Build()
	err := rc.ExecuteCommandOn(pod, "engine", cmd, control.ExecOptions{
		Logger:  logger,
		Stdout:  stdout,
		Timeout: 10 * time.Second,
	})
	if err != nil {
		return "", "", err
	}

	// Parse role and leader pod from stdout.
	role, leaderPod := parseConsensusReportRoleResult(stdout.String())
	logger.Info("Be aware of pod's role and current leader.", "pod", pod.Name, "role", role, "leader-pod", leaderPod)
	return role, leaderPod, nil
}

func SetPodElectionWeight(rc *xstorev1reconcile.Context, leaderPod *corev1.Pod, logger logr.Logger, weight int, pods []string) ([]int, error) {
	cmd := xstoreexec.NewCanonicalCommandBuilder().
		Consensus().
		ConfigureElectionWeight(weight, pods...).
		Build()
	stdout := &bytes.Buffer{}
	err := rc.ExecuteCommandOn(leaderPod, "engine", cmd, control.ExecOptions{
		Logger:  logger,
		Stdout:  stdout,
		Timeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	result := make([]int, 0)
	for _, val := range strings.Split(stdout.String(), ",") {
		parsedWeighted, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
		if err != nil {
			return nil, err
		}
		result = append(result, int(parsedWeighted))
	}
	return result, nil
}

func TryReconcileLabels(rc *xstorev1reconcile.Context, pods []corev1.Pod, leaderPod string, logger logr.Logger) {
	for i := range pods {
		pod := &pods[i]
		if !xstoremeta.IsPodRoleCandidate(pod) {
			continue
		}

		expectRole := xstoremeta.RoleFollower
		if pod.Name == leaderPod {
			expectRole = xstoremeta.RoleLeader
		}

		// update label if not match
		if pod.Labels[xstoremeta.LabelRole] != expectRole {
			pod.Labels[xstoremeta.LabelRole] = expectRole
			err := rc.Client().Update(rc.Context(), pod)
			if err != nil {
				logger.Error(err, "Unable to reconcile label of node role", "pod", pod.Name, "role", expectRole)
			}
		}
	}
}

func TryDetectLeaderChange(rc *xstorev1reconcile.Context, pods []corev1.Pod, logger logr.Logger) (string, bool) {
	// Get previous leader
	previousLeader := ""
	var previousLeaderPod *corev1.Pod
	for i := range pods {
		pod := &pods[i]
		if !xstoremeta.IsPodRoleCandidate(pod) {
			continue
		}

		if pod.Labels[xstoremeta.LabelRole] == xstoremeta.RoleLeader {
			previousLeader, previousLeaderPod = pod.Name, pod
			break
		}
	}

	podsVisited := make(map[string]struct{})

	// Detect current leader, first let's see if leader isn't changed.
	if previousLeader != "" {
		role, _, err := ReportRoleAndCurrentLeader(rc, previousLeaderPod, logger)
		podsVisited[previousLeader] = struct{}{}
		if err != nil {
			logger.Error(err, "Unable to report role and current leader on previous leader.", "pod", previousLeader)
		} else {
			if role == xstoremeta.RoleLeader {
				return previousLeader, false
			}
		}
	}

	// Build pod map
	podMap := make(map[string]*corev1.Pod)
	for i := range pods {
		pod := pods[i]
		podMap[pod.Name] = &pod
	}

	// Scan the pods if we can't tell leader from previous leader
	for i := range pods {
		pod := &pods[i]

		if !xstoremeta.IsPodRoleCandidate(pod) || k8shelper.IsPodFailed(pod) || !k8shelper.IsPodReady(pod) {
			continue
		}

		// Skip previous visited.
		if _, visited := podsVisited[pod.Name]; visited {
			continue
		}

		role, _, err := ReportRoleAndCurrentLeader(rc, pod, logger)
		if err != nil {
			logger.Error(err, "Unable to report role and current leader on pod.", "pod", pod.Name)
		} else {
			if role == xstoremeta.RoleLeader {
				return pod.Name, pod.Name != previousLeader
			}
		}

		podsVisited[pod.Name] = struct{}{}
	}

	return "", true
}

func TryDetectLeaderAndTryReconcileLabels(rc *xstorev1reconcile.Context, pods []corev1.Pod, logger logr.Logger) (string, bool) {
	currentLeader, leaderChanged := TryDetectLeaderChange(rc, pods, logger)
	if leaderChanged {
		TryReconcileLabels(rc, pods, currentLeader, logger)
	}
	return currentLeader, leaderChanged
}

var ReconcileConsensusRoleLabels = xstorev1reconcile.NewStepBinder("ReconcileConsensusRoleLabels",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()

		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}

		flow.Logger().Info("Try detecting leader and reconciling the labels...")

		currentLeader, leaderSwitched := TryDetectLeaderAndTryReconcileLabels(rc, pods, flow.Logger())

		if len(currentLeader) == 0 {
			xstore.Status.LeaderPod = ""

			rc.UpdateXStoreCondition(&polardbxv1xstore.Condition{
				Type:    polardbxv1xstore.LeaderReady,
				Status:  corev1.ConditionFalse,
				Reason:  "LeaderNotFound",
				Message: "Leader not found",
			})

			return flow.Continue("Leader not found!")
		} else if leaderSwitched {
			xstore.Status.LeaderPod = currentLeader

			rc.UpdateXStoreCondition(&polardbxv1xstore.Condition{
				Type:    polardbxv1xstore.LeaderReady,
				Status:  corev1.ConditionTrue,
				Reason:  "LeaderFound",
				Message: "Leader found: " + currentLeader,
			})

			return flow.Continue("Leader changed!", "leader-pod", currentLeader)
		} else {
			xstore.Status.LeaderPod = currentLeader

			rc.UpdateXStoreCondition(&polardbxv1xstore.Condition{
				Type:    polardbxv1xstore.LeaderReady,
				Status:  corev1.ConditionTrue,
				Reason:  "LeaderFound",
				Message: "Leader found: " + currentLeader,
			})

			return flow.Continue("Leader not changed.", "leader-pod", currentLeader)
		}
	},
)

var WaitUntilLeaderElected = xstorev1reconcile.NewStepBinder("WaitUntilLeaderElected",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		leaderPod, err := rc.TryGetXStoreLeaderPod()
		if err != nil {
			return flow.RetryErr(err, "Unable to get leader pod.")
		}

		if leaderPod == nil {
			return flow.RetryAfter(10*time.Second, "Leader not found, keep waiting...")
		}

		return flow.Continue("Leader found.", "leader-pod", leaderPod.Name)
	},
)

var AddLearnerNodesToClusterOnLeader = xstorev1reconcile.NewStepBinder("AddLearnerNodesToClusterOnLeader",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get readonly xstore pods")
		}

		learnerPods := k8shelper.FilterPodsBy(pods, xstoremeta.IsPodRoleLearner)

		// No learner pods.
		if len(learnerPods) == 0 {
			return flow.Pass()
		}

		learnerNodes := commonsteps.TransformPodsIntoNodes(rc.Namespace(), learnerPods)

		leaderPod, err := rc.TryGetXStoreLeaderPod()
		if err != nil {
			return flow.RetryErr(err, "Unable to get leader pod.")
		}

		if leaderPod == nil {
			return flow.RetryAfter(10*time.Second, "Leader not found, keep waiting...")
		}
		for _, learnerNode := range learnerNodes {
			cmd := xstoreexec.NewCanonicalCommandBuilder().Consensus().AddLearner(fmt.Sprintf("%s:%d", learnerNode.Host, learnerNode.Port)).Build()
			err := rc.ExecuteCommandOn(leaderPod, convention.ContainerEngine, cmd, control.ExecOptions{
				Logger:  flow.Logger(),
				Timeout: 2 * time.Second,
			})
			if err != nil {
				return flow.RetryErr(err, "Unable to add learner node.", "pod", learnerNode.Pod, "leader", leaderPod.Name)
			}
		}

		return flow.Continue("Learner nodes added.")
	},
)

func newXStoreFollowerName(xStoreName string) string {
	return xStoreName + "-xf"
}

var RestoreToLearner = xstorev1reconcile.NewStepBinder("RestoreToLearner",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		if featuregate.EnableLabMode.Enabled() {
			if rc.MustGetXStore().Spec.Engine != "xcluster" {
				return flow.Pass()
			}
		}
		//check xStore follower task exists
		xfName := newXStoreFollowerName(rc.Name())
		objKey := types.NamespacedName{
			Namespace: rc.Namespace(),
			Name:      xfName,
		}
		xf := polarxv1.XStoreFollower{}
		err := rc.Client().Get(rc.Context(), objKey, &xf)
		if err != nil && !strings.Contains(err.Error(), "not found") {
			return flow.RetryErr(err, "failed to get xf", "xfName", xfName)
		}
		if err != nil && strings.Contains(err.Error(), "not found") {
			pods, err := rc.GetXStorePods()
			if err != nil {
				return flow.RetryErr(err, "Failed to get pods")
			}
			xf := &polarxv1.XStoreFollower{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: rc.Namespace(),
					Name:      xfName,
				},
				Spec: polarxv1.XStoreFollowerSpec{
					Local:         true,
					XStoreName:    rc.MustGetPrimaryXStore().Name,
					TargetPodName: pods[0].Name,
				},
			}
			err = rc.SetControllerRefAndCreate(xf)
			if err != nil {
				return flow.RetryErr(err, "failed to create xstore follower")
			}
			return flow.Retry("just retry")
		}
		if xf.Status.Phase != polardbxv1xstore.FollowerPhaseSuccess {
			return flow.Retry("wait for the xf to be success phase", "xfName", xf.Name)
		}
		err = rc.Client().Delete(rc.Context(), &xf)
		if err != nil {
			return flow.RetryErr(err, "failed to delete xf", "xfName", xf.Name)
		}
		return flow.Continue("RestoreToLearner success.")
	},
)

var DropLearnerOnLeader = xstorev1reconcile.NewStepBinder("DropLearnerOnLeader",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Continue("Unable to get readonly xstore pods")
		}

		learnerPods := k8shelper.FilterPodsBy(pods, xstoremeta.IsPodRoleLearner)

		// No learner pods.
		if len(learnerPods) == 0 {
			return flow.Pass()
		}

		leaderPod, err := rc.TryGetXStoreLeaderPod()
		if err != nil {
			return flow.Continue("Unable to get leader pod.")
		}

		if leaderPod == nil {
			return flow.Continue("Leader not found")
		}

		sharedCm, err := rc.GetXStoreConfigMap(convention.ConfigMapTypeShared)
		if err != nil {
			return flow.Error(err, "Unable to get shared config map.")
		}

		sharedChannel, err := commonsteps.ParseChannelFromConfigMap(sharedCm)
		if err != nil {
			return flow.Error(err, "Unable to parse shared channel from config map.")
		}
		nodeMap := map[string]channel.Node{}
		for _, node := range sharedChannel.Nodes {
			nodeMap[node.Pod] = node
		}
		for _, learnerPod := range learnerPods {
			node, ok := nodeMap[learnerPod.Name]
			if !ok {
				return flow.RetryErr(fmt.Errorf("%s", "failed to get node in the shared channel"), "PodName", learnerPod.Name)
			}
			cmd := xstoreexec.NewCanonicalCommandBuilder().Consensus().DropLearner(fmt.Sprintf("%s:%d", node.Host, node.Port)).Build()
			err := rc.ExecuteCommandOn(leaderPod, convention.ContainerEngine, cmd, control.ExecOptions{
				Logger:  flow.Logger(),
				Timeout: 2 * time.Second,
			})
			if err != nil {
				continue
			}
		}

		return flow.Continue("Learner nodes deleted.")
	},
)

func DisableElectionByPod(rc *xstorev1reconcile.Context, pod *corev1.Pod, logger logr.Logger) error {
	cmd := xstoreexec.NewCanonicalCommandBuilder().Consensus().DisableElection().Build()
	err := rc.ExecuteCommandOn(pod, "engine", cmd, control.ExecOptions{
		Logger: logger,
	})
	if err != nil {
		return err
	}
	return nil
}

func EnableElectionByPod(rc *xstorev1reconcile.Context, pod *corev1.Pod, logger logr.Logger) error {
	cmd := xstoreexec.NewCanonicalCommandBuilder().Consensus().EnableElection().Build()
	err := rc.ExecuteCommandOn(pod, "engine", cmd, control.ExecOptions{
		Logger: logger,
	})
	if err != nil {
		return err
	}
	return nil
}

var DisableElection = xstorev1reconcile.NewStepBinder("DisableElection",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {

		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.RetryErr(err, "Failed to get pods")
		}
		leaderPod, err := GetLeaderPod(rc, flow.Logger(), true)
		if err != nil || leaderPod == nil {
			return flow.RetryErr(err, "Failed to get leader pod")
		}
		var execErr error
		var errPodName string
		for _, pod := range pods {
			if pod.Name == leaderPod.Name {
				continue
			}
			currentExecErr := DisableElectionByPod(rc, &pod, flow.Logger())
			if currentExecErr != nil {
				execErr = currentExecErr
				errPodName = pod.Name
			}
		}
		if execErr != nil {
			return flow.RetryErr(execErr, "Failed to disable election", "pod", errPodName)
		}

		return flow.Continue("Disable Election Success.")
	},
)

var EnableElection = xstorev1reconcile.NewStepBinder("EnableElection",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {

		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.RetryErr(err, "Failed to get pods")
		}
		var execErr error
		var errPodName string
		for _, pod := range pods {
			currentExecErr := EnableElectionByPod(rc, &pod, flow.Logger())
			if currentExecErr != nil {
				execErr = currentExecErr
				errPodName = pod.Name
			}
		}
		if execErr != nil {
			return flow.RetryErr(execErr, "Failed to enable election", "pod", errPodName)
		}

		return flow.Continue("Enable Election Success.")
	},
)

var CleanFlushLocalAnnotation = xstorev1reconcile.NewStepBinder("CleanFlushLocalAnnotation",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.RetryErr(err, "failed to get xstore pods")
		}
		for _, pod := range pods {
			podAnnotations := k8shelper.PatchAnnotations(pod.Annotations, map[string]string{
				xstoremeta.AnnotationFlushLocal: "false",
			})
			pod.SetAnnotations(podAnnotations)
			err := rc.Client().Update(rc.Context(), &pod)
			if err != nil {
				return flow.RetryErr(err, "failed to update pod annotation")
			}
		}
		return flow.Continue("CleanFlushLocalAnnotation Success.")
	},
)

var MarkFlushIpInAnnotation = xstorev1reconcile.NewStepBinder("MarkFlushIpInAnnotation",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.RetryErr(err, "failed to get xstore pods")
		}
		for _, pod := range pods {
			if pod.Status.PodIP != "" {
				pod.SetAnnotations(k8shelper.PatchAnnotations(pod.Annotations, map[string]string{
					xstoremeta.AnnotationFlushIp: pod.Status.PodIP,
				}))
				err := rc.Client().Update(rc.Context(), &pod)
				if err != nil {
					return flow.RetryErr(err, "failed to update pod", "pod", pod.Name)
				}
			}
		}
		return flow.Continue("CleanFlushLocalAnnotation Success.")
	},
)

var SyncPaxosMeta = xstorev1reconcile.NewStepBinder("SyncPaxosMeta",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		leaderPod, err := rc.TryGetXStoreLeaderPod()
		if err != nil || leaderPod == nil {
			return flow.RetryErr(err, "Failed to get xstore leader pod")
		}
		globalInfo, err := ShowGlobalInfo(rc, leaderPod, flow.Logger())
		if err != nil {
			return flow.RetryErr(err, "failed to show global info")
		}
		globalInfoMap := map[string]*ConsensusGlobalInfoItem{}
		for i, oneGlobalInfo := range globalInfo {
			globalInfoMap[oneGlobalInfo.Addr] = &globalInfo[i]
		}
		globalInfoItems, svcMetaMap, err := GenerateExpectedGlobalInfo(rc)
		if err != nil {
			return flow.RetryErr(err, "failed to generate expected global info")
		}
		if bytes, err := json.Marshal(globalInfoItems); err == nil {
			flow.Logger().Info(fmt.Sprintf("globalInfoItems = %s", string(bytes)))
		}
		for _, item := range globalInfoItems {
			svcMetaMap[item.Pod] = !item.Local
		}
		rc.SetSvcMetaMap(svcMetaMap)

		var leaderMatchIndex int64 = -1
		for _, globalInfoItem := range globalInfo {
			if strings.EqualFold(globalInfoItem.Role, string(polardbxv1xstore.RoleLeader)) {
				leaderMatchIndex = globalInfoItem.MatchIndex
				break
			}
		}
		if leaderMatchIndex == -1 {
			return flow.Retry("failed to find match index of leader")
		}

		var retry bool
		accessMap := map[string]bool{}
		for _, globalInfoItem := range globalInfoItems {
			accessMap[globalInfoItem.Addr] = true
			if actualItem, ok := globalInfoMap[globalInfoItem.Addr]; ok {

				if strings.EqualFold(actualItem.Role, string(polardbxv1xstore.RoleFollower)) && !(globalInfoItem.IsLogger != nil && *globalInfoItem.IsLogger) {
					if rc.GetMetaFollowerAddr() == "" {
						rc.SetMetaFollowerAddr(globalInfoItem.Addr)
					}
				}

				if strings.EqualFold(actualItem.Role, string(polardbxv1xstore.RoleLearner)) {
					if leaderMatchIndex-actualItem.AppliedIndex > 2000 || actualItem.AppliedIndex == 0 {
						retry = true
						flow.Logger().Info(fmt.Sprintf("addr = %s , leaderMatchIndex-actualItem.AppliedIndex = %d", actualItem.Addr, leaderMatchIndex-actualItem.AppliedIndex))
						continue
					}
					if strings.EqualFold(globalInfoItem.Role, string(polardbxv1xstore.RoleFollower)) {
						cmd := xstoreexec.NewCanonicalCommandBuilder().Consensus().ChangeLearnerToFollower(actualItem.Addr).Build()
						err := rc.ExecuteCommandOn(leaderPod, "engine", cmd, control.ExecOptions{
							Logger: flow.Logger(),
						})
						if err != nil {
							return flow.RetryErr(err, fmt.Sprintf("failed to change learner to follower addr: %s", actualItem.Addr))
						}
						if globalInfoItem.IsLogger != nil && *globalInfoItem.IsLogger {
							// change weight to 1
							_, err := SetPodElectionWeight(rc, leaderPod, flow.Logger(), 1, []string{globalInfoItem.Addr})
							if err != nil {
								return flow.RetryErr(err, fmt.Sprintf("failed to set weight of %s to 1", globalInfoItem.Addr))
							}
						}
					}
				}
			} else {
				cmd := xstoreexec.NewCanonicalCommandBuilder().Consensus().AddLearner(globalInfoItem.Addr).Build()
				err := rc.ExecuteCommandOn(leaderPod, "engine", cmd, control.ExecOptions{
					Logger: flow.Logger(),
				})
				if err != nil {
					return flow.RetryErr(err, fmt.Sprintf("failed to add learner addr: %s", actualItem.Addr))
				}
				retry = true
			}
		}
		for _, globalInfoItem := range globalInfo {
			if !accessMap[globalInfoItem.Addr] {
				serverId := globalInfoItem.Addr
				host := strings.Split(globalInfoItem.Addr, ":")[0]
				if _, err := strconv.ParseInt(strings.Split(host, ".")[0], 10, 64); err != nil {
					serverId = strconv.FormatInt(globalInfoItem.ServerId, 10)
				}
				if strings.EqualFold(globalInfoItem.Role, string(polardbxv1xstore.RoleFollower)) {
					cmd := xstoreexec.NewCanonicalCommandBuilder().Consensus().ChangeFollowerToLearner(serverId).Build()
					err := rc.ExecuteCommandOn(leaderPod, "engine", cmd, control.ExecOptions{
						Logger: flow.Logger(),
					})
					if err != nil {
						return flow.RetryErr(err, fmt.Sprintf("failed to change follower to learner: %s", globalInfoItem.Addr))
					}
					globalInfoItem.Role = string(polardbxv1xstore.RoleLearner)
				}
				if strings.EqualFold(globalInfoItem.Role, string(polardbxv1xstore.RoleLearner)) {
					cmd := xstoreexec.NewCanonicalCommandBuilder().Consensus().DropLearner(serverId).Build()
					err := rc.ExecuteCommandOn(leaderPod, "engine", cmd, control.ExecOptions{
						Logger: flow.Logger(),
					})
					if err != nil {
						return flow.RetryErr(err, fmt.Sprintf("failed to drop learner: %s", globalInfoItem.Addr))
					}
				}
			}
		}

		if retry {
			return flow.Retry("to retry")
		}

		return flow.Continue("SyncPaxosMeta Success.")
	},
)

func GenerateExpectedGlobalInfo(rc *xstorev1reconcile.Context) ([]ConsensusGlobalInfoItem, map[string]bool, error) {
	pods, err := rc.GetPrimaryXStorePods()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to get xstore pods¬")
	}
	readOnlyPods, err := rc.GetReadonlyPods()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to get readonly pods")
	}
	if len(readOnlyPods) > 0 {
		pods = append(pods, readOnlyPods...)
	}
	podMap := make(map[string]corev1.Pod, 0)
	for _, pod := range pods {
		podMap[pod.Name] = pod
	}

	podServices, err := rc.GetPrimaryXStorePodServices()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to get xstore pod")
	}
	readonlyPodServices, err := rc.GetReadonlyPodServices()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to get readonly pod services")
	}
	if len(readonlyPodServices) > 0 {
		for k, v := range readonlyPodServices {
			podServices[k] = v
		}
	}
	carePodService := featuregate.EnableXStoreWithPodService.Enabled()
	xstore := rc.MustGetXStore()
	if xstore.Annotations[common.AnnotationOperatorCreateVersion] == "" {
		carePodService = true
	}
	if xstore.Spec.Readonly {
		primaryXStore := rc.MustGetPrimaryXStore()
		if primaryXStore.Annotations[common.AnnotationOperatorCreateVersion] == "" {
			carePodService = true
		}
	}
	globalInfo := make([]ConsensusGlobalInfoItem, 0)
	markMap := map[string]bool{}
	svcMetaMap := map[string]bool{}
	if carePodService {
		for _, podService := range podServices {
			podName := podService.Labels[xstoremeta.LabelPod]
			svcMetaMap[podName] = true
			pod, ok := podMap[podName]
			if !ok {
				return nil, nil, fmt.Errorf("failed to get pod by pod name %s podService %s", podName, podService.Name)
			}
			nodeRole := pod.Labels[xstoremeta.LabelNodeRole]
			role := polardbxv1xstore.RoleFollower
			if nodeRole == xstoremeta.RoleLearner {
				role = polardbxv1xstore.RoleLearner
			}
			isLogger := xstoremeta.IsPodRoleVoter(&pod)
			host := podService.Spec.ClusterIP
			var addr string
			if strings.EqualFold(host, "None") {
				host = podService.Name
				paxosPort := k8shelper.MustGetPortFromContainer(
					k8shelper.MustGetContainerFromPod(&pod, convention.ContainerEngine),
					"paxos",
				).ContainerPort
				addr = fmt.Sprintf("%s:%d", host, paxosPort)
			} else {
				for _, svcPort := range podService.Spec.Ports {
					if svcPort.Name == "paxos" {
						addr = fmt.Sprintf("%s:%d", host, svcPort.Port)
						break
					}
				}
			}
			if addr != "" {
				globalInfo = append(globalInfo, ConsensusGlobalInfoItem{
					Addr:     addr,
					Role:     string(role),
					IsLogger: &isLogger,
					Pod:      podName,
				})
				markMap[podName] = true
			}
		}
	}
	for podName, pod := range podMap {
		if !markMap[podName] {
			nodeRole := pod.Labels[xstoremeta.LabelNodeRole]
			role := polardbxv1xstore.RoleFollower
			if nodeRole == xstoremeta.RoleLearner {
				role = polardbxv1xstore.RoleLearner
			}
			if pod.Status.PodIP != "" {
				paxosPort := k8shelper.MustGetPortFromContainer(
					k8shelper.MustGetContainerFromPod(&pod, convention.ContainerEngine),
					"paxos",
				).ContainerPort
				addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, paxosPort)
				isLogger := xstoremeta.IsPodRoleVoter(&pod)
				globalInfo = append(globalInfo, ConsensusGlobalInfoItem{
					Addr:     addr,
					Role:     string(role),
					IsLogger: &isLogger,
					Local:    true,
					Pod:      pod.Name,
				})
				markMap[podName] = true
			}
		}
	}
	return globalInfo, svcMetaMap, nil
}
