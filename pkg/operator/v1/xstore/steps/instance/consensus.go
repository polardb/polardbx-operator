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
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

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
		Timeout: 2 * time.Second,
	})
	if err != nil {
		return "", "", err
	}

	// Parse role and leader pod from stdout.
	role, leaderPod := parseConsensusReportRoleResult(stdout.String())
	logger.Info("Be aware of pod's role and current leader.", "pod", pod.Name, "role", role, "leader-pod", leaderPod)
	return role, leaderPod, nil
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
		role, leader, err := ReportRoleAndCurrentLeader(rc, previousLeaderPod, logger)
		podsVisited[previousLeader] = struct{}{}
		if err != nil {
			logger.Error(err, "Unable to report role and current leader on previous leader.", "pod", previousLeader)
		} else {
			if role == xstoremeta.RoleLeader {
				return previousLeader, false
			} else if leader != "" {
				return leader, true
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

		role, leader, err := ReportRoleAndCurrentLeader(rc, pod, logger)
		if err != nil {
			logger.Error(err, "Unable to report role and current leader on pod.", "pod", pod.Name)
		} else {
			if role == xstoremeta.RoleLeader {
				return pod.Name, pod.Name != previousLeader
			} else if leader != "" {
				// Do not trust followers, as they can cache the status. Just make sure again.
				if _, visited := podsVisited[leader]; !visited {
					role, _, err := ReportRoleAndCurrentLeader(rc, podMap[leader], logger)
					if err == nil && role == xstoremeta.RoleLeader {
						return leader, true
					}
					podsVisited[leader] = struct{}{}
				}
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
			return flow.Error(err, "Unable to get leader pod.")
		}

		if leaderPod == nil {
			return flow.Wait("Leader not found, keep waiting...")
		}

		return flow.Continue("Leader found.", "leader-pod", leaderPod.Name)
	},
)

var AddLearnerNodesToClusterOnLeader = xstorev1reconcile.NewStepBinder("AddLearnerNodesToClusterOnLeader",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get xstore pods.")
		}

		learnerPods := k8shelper.FilterPodsBy(pods, xstoremeta.IsPodRoleLearner)

		// No learner pods.
		if len(learnerPods) == 0 {
			return flow.Pass()
		}

		leaderPod, err := rc.TryGetXStoreLeaderPod()
		if err != nil {
			return flow.Error(err, "Unable to get leader pod.")
		}

		if leaderPod == nil {
			return flow.Wait("Leader not found, keep waiting...")
		}

		for _, learnerPod := range learnerPods {
			cmd := xstoreexec.NewCanonicalCommandBuilder().
				Consensus().
				AddNode(learnerPod.Name, xstoremeta.RoleLearner).
				Build()

			err := rc.ExecuteCommandOn(leaderPod, convention.ContainerEngine, cmd, control.ExecOptions{
				Logger:  flow.Logger(),
				Timeout: 2 * time.Second,
			})

			if err != nil {
				return flow.Error(err, "Unable to add learner node.", "pod", learnerPod.Name)
			}
		}

		return flow.Continue("Learner nodes added.")
	},
)
