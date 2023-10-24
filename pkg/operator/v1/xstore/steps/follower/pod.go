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

package follower

import (
	"bytes"
	"context"
	"fmt"
	polarxv1 "github.com/alibaba/polardbx-operator/api/v1"
	xstorev1 "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/command"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	xstoreinstance "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/steps/instance"
	"github.com/alibaba/polardbx-operator/pkg/util/json"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strconv"
	"strings"
	"time"
)

func IsFromPodChosen(xstoreLeader *polarxv1.XStoreFollower) bool {
	return xstoreLeader.Spec.FromPodName != ""
}

// Try to choose a pod from the source xstore.
// if the from-pod is set, this step will skip

var TryChooseFromPod = NewStepBinder("TryChooseFromPod",
	func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
		if IsFromPodChosen(rc.MustGetXStoreFollower()) {
			flow.Logger().Info("FromPod has been chosen before")
			return flow.Pass()
		}
		//begin choose from pod. first choose follower then leader
		xstoreContext := rc.XStoreContext()
		pods, err := xstoreContext.GetXStorePods()
		if err != nil {
			return flow.RetryErr(ErrorGetXStorePod, "Failed to get xstore pod", "xstore name", xstoreContext.Name())
		}
		var leaderPod *corev1.Pod
		var followerPod *corev1.Pod
		var fromPod *corev1.Pod
		for _, pod := range pods {
			role, _, err := xstoreinstance.ReportRoleAndCurrentLeader(xstoreContext, &pod, flow.Logger())
			if err != nil {
				continue
			}
			flow.Logger().Info("pod node role " + pod.Labels[xstoremeta.LabelNodeRole])
			if !xstoremeta.IsPodRoleCandidate(&pod) || pod.Name == rc.MustGetXStoreFollower().Spec.TargetPodName {
				continue
			}
			flow.Logger().Info("pod node role " + pod.Labels[xstoremeta.LabelNodeRole])
			if role == xstoremeta.RoleLeader {
				leaderPod = pod.DeepCopy()
			} else if role == xstoremeta.RoleFollower {
				followerPod = pod.DeepCopy()
			}
		}
		fromPod = leaderPod
		if followerPod != nil {
			//check if  the follower pod is ready to be backup
			//check the slave status
			showSlaveStatusResult, err := xstoreinstance.ShowSlaveStatus(xstoreContext, followerPod, flow.Logger())
			if err == nil {
				if showSlaveStatusResult.SlaveSQLRunning == xstoreinstance.SlaveSqlRunningYes && showSlaveStatusResult.SecondsBehindMaster < xstoreinstance.ReplicationDelaySeconds {
					fromPod = followerPod
				}
			}
			if err != nil {
				flow.Logger().Error(err, "failed to show slave status")
			}
		}
		if fromPod == nil {
			return flow.RetryErr(ErrorFromPodNotFound, "")
		}
		rc.MustGetXStoreFollower().Spec.FromPodName = fromPod.Name
		rc.MarkChanged()
		return flow.Continue("TryChooseFromPod  success.", "FromPodName", fromPod.Name)
	})

// Check if the from-pod exists

var TryLoadFromPod = NewStepBinder("TryLoadFromPod",
	func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
		xstoreLearner := rc.MustGetXStoreFollower()
		fromPodName := xstoreLearner.Spec.FromPodName
		podKey := types.NamespacedName{Name: fromPodName, Namespace: rc.Namespace()}
		pod := corev1.Pod{}
		err := rc.Client().Get(rc.BaseReconcileContext.Context(), podKey, &pod)
		if err != nil {
			//changed status to failed
			xstoreLearner := rc.MustGetXStoreFollower()
			xstoreLearner.Status.Phase = xstorev1.FollowerPhaseFailed
			xstoreLearner.Status.Message = "The from-pod is not found."
			rc.MarkChanged()
			return flow.Error(ErrorFromPodNotFound, "", "FromPodName", podKey.String())
		}
		return flow.Continue("TryLoadFromPod Success.")
	})

var TryLoadTargetPod = NewStepBinder("TryLoadTargetPod", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	xstoreFollower := rc.MustGetXStoreFollower()
	targetPodName := xstoreFollower.Spec.TargetPodName
	podKey := types.NamespacedName{Name: targetPodName, Namespace: rc.Namespace()}
	pod := corev1.Pod{}
	err := rc.Client().Get(rc.BaseReconcileContext.Context(), podKey, &pod)
	if err != nil {
		//changed status to failed
		xstoreFollower.Status.Phase = xstorev1.FollowerPhaseFailed
		xstoreFollower.Status.Message = "The target-pod is not found."
		rc.MarkChanged()
		return flow.Error(ErrorFromPodNotFound, "", "TargetPodName", podKey.String())
	}
	nodeRole, ok := pod.Labels[xstoremeta.LabelNodeRole]
	if !ok {
		return flow.Error(ErrorLabelNotFound, fmt.Sprintf("Failed get label %s", xstoremeta.LabelNodeRole))
	}
	switch strings.ToLower(nodeRole) {
	case strings.ToLower(string(xstorev1.RoleVoter)):
		xstoreFollower.Spec.Role = xstorev1.FollowerRoleLogger
	case strings.ToLower(string(xstorev1.RoleCandidate)):
		xstoreFollower.Spec.Role = xstorev1.FollowerRoleFollower
	case strings.ToLower(string(xstorev1.RoleLearner)):
		xstoreFollower.Spec.Role = xstorev1.FollowerRoleLearner
	default:
		return flow.Error(ErrorInvalidNodeRole, "")
	}
	//check node name
	if !xstoreFollower.Spec.Local {
		// check if the node name is the same as the target pod's
		if xstoreFollower.Spec.NodeName == pod.Spec.NodeName {
			xstoreFollower.Status.Phase = xstorev1.FollowerPhaseFailed
			xstoreFollower.Status.Message = "remote rebuild. The node name is the same as the current pod's"
			rc.MarkChanged()
			return flow.Error(ErrorInvalidNodeName, "", "TargetPodName", podKey.String())
		}
	}
	xstoreFollower.Status.TargetPodName = targetPodName
	xstoreFollower.Status.RebuildPodName = targetPodName
	xstoreFollower.Status.RebuildNodeName = pod.Spec.NodeName
	xstoreFollower.SetLabels(k8shelper.PatchLabels(xstoreFollower.GetLabels(), map[string]string{
		xstoremeta.LabelTargetXStore: pod.Labels[xstoremeta.LabelName],
		xstoremeta.LabelPod:          pod.Name,
	}))
	rc.MarkChanged()
	return flow.Continue("TryLoadTargetPod Success.")
})

var CheckIfTargetPodNotLeader = NewStepBinder("CheckIfTargetPodNotLeader", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	xstoreContext := rc.XStoreContext()
	xStoreFollower := rc.MustGetXStoreFollower()
	targetPodName := xStoreFollower.Spec.TargetPodName
	leaderPod, err := getLeaderPod(xstoreContext, flow.Logger())
	if err != nil {
		return flow.RetryErr(err, "")
	}
	if leaderPod.Name == targetPodName {
		xstoreLearner := rc.MustGetXStoreFollower()
		xstoreLearner.Status.Phase = xstorev1.FollowerPhaseFailed
		xstoreLearner.Status.Message = "The from-pod is not found."
		rc.MarkChanged()
		return flow.Error(ErrorTargetPodShouldNotBeLeader, "")
	}
	return flow.Continue("CheckIfTargetPodNotLeader success")
})

var DisableFromPodPurgeLog = NewStepBinder("DisableFromPodPurgeLog", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	xstoreContext := rc.XStoreContext()
	fromPod, err := xstoreContext.GetXStorePod(rc.MustGetXStoreFollower().Spec.FromPodName)
	if err != nil {
		return flow.RetryErr(err, "DisableFromPodPurgeLog Failed")
	}
	if val, ok := fromPod.Annotations[xstoremeta.AnnotationRebuildFromPod]; ok && val == "on" {
		flow.Pass()
	}
	fromPod.SetAnnotations(k8shelper.PatchAnnotations(fromPod.Annotations, map[string]string{
		xstoremeta.AnnotationRebuildFromPod: "on",
	}))
	err = rc.Client().Update(rc.Context(), fromPod)
	if err != nil {
		return flow.RetryErr(err, "Failed to Update from pod")
	}
	return flow.Continue("DisableFromPodPurgeLog Success.")
})

var EnableFromPodPurgeLog = NewStepBinder("EnableFromPodPurgeLog", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	xstoreContext := rc.XStoreContext()
	fromPod, err := xstoreContext.GetXStorePod(rc.MustGetXStoreFollower().Spec.FromPodName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return flow.Pass()
		}
		return flow.RetryErr(err, "EnableFromPodPurgeLog Failed")
	}
	annotation := fromPod.Annotations
	delete(annotation, xstoremeta.AnnotationRebuildFromPod)
	fromPod.SetAnnotations(annotation)
	err = rc.Client().Update(rc.Context(), fromPod)
	if err != nil {
		return flow.RetryErr(err, "Failed to Update from pod")
	}
	return flow.Continue("EnableFromPodPurgeLog Success.")
})

var SetKillAllOnce = NewStepBinder("SetKillAllOnce", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	rc.SetKillAllOnce(true)
	return flow.Continue("SetKillAllOnce Success.")
})

var KillAllProcessOnRebuildPod = NewStepBinder("KillAllProcessOnRebuildPod", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	if rc.KillAllOnce() {
		if HasFlowFlags(rc, "KillAllProcessOnRebuildPodOnce") {
			return flow.Pass()
		}
	}
	targetPod, err := rc.GetPodByName(rc.MustGetXStoreFollower().Status.RebuildPodName)
	if err != nil {
		return flow.RetryErr(err, "DisableFromPodPurgeLog Failed")
	}
	var engineContainerStarted bool = false
	if targetPod.Status.ContainerStatuses != nil {
		for _, containerStatus := range targetPod.Status.ContainerStatuses {
			if containerStatus.Name == convention.ContainerEngine {
				if containerStatus.State.Running != nil {
					engineContainerStarted = true
				}
			}
		}
	}
	if engineContainerStarted {
		cmd := command.NewCanonicalCommandBuilder().Process().KillAllMyProcess().Build()
		buf := &bytes.Buffer{}
		err = rc.ExecuteCommandOn(targetPod, convention.ContainerEngine, cmd, control.ExecOptions{
			Logger:  flow.Logger(),
			Stdout:  buf,
			Timeout: 8 * time.Second,
		})
		if err != nil && err != context.DeadlineExceeded {
			return flow.RetryErr(err, "Failed to kill all on target pod.", "pod", targetPod.Name)
		}
	}
	if rc.KillAllOnce() {
		SetFlowFlags(rc, "KillAllProcessOnRebuildPodOnce", true)
	}
	return flow.Continue("KillAllProcessOnRebuildPod Success.")
})

var LoadLeaderLogPosition = NewStepBinder("LoadLeaderLogPosition", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	xstoreContext := rc.XStoreContext()
	pods, err := xstoreContext.GetXStorePods()
	if err != nil {
		return flow.RetryErr(ErrorGetXStorePod, "Failed to get xstore pod", "xstore name", xstoreContext.Name())
	}
	var leaderPod *corev1.Pod
	for _, pod := range pods {
		role, _, err := xstoreinstance.ReportRoleAndCurrentLeader(xstoreContext, &pod, flow.Logger())
		if err != nil {
			continue
		}
		if role == xstoremeta.RoleLeader {
			leaderPod = &pod
		}
	}
	if leaderPod == nil {
		return flow.RetryErr(ErrorLeaderPodNotFound, "")
	}
	localInfo, err := xstoreinstance.ShowThis(xstoreContext, leaderPod, flow.Logger(), true)
	if err != nil {
		return flow.RetryErr(err, "Failed to query local info on leader pod.", "pod", leaderPod.Name)
	}
	commitIndex, err := strconv.ParseInt(localInfo.CommitIndex, 10, 64)
	if err != nil {
		return flow.RetryErr(ErrorInvalidCommitIndex, "")
	}
	rc.SetCommitIndex(commitIndex)
	return flow.Continue("LoadLeaderLogPosition Success.")
})

func getLeaderPod(xStoreContext *xstorev1reconcile.Context, logger logr.Logger) (*corev1.Pod, error) {
	pods, err := xStoreContext.GetXStorePods()
	if err != nil {
		return nil, err
	}
	for _, pod := range pods {
		role, _, err := xstoreinstance.ReportRoleAndCurrentLeader(xStoreContext, &pod, logger)
		if err != nil {
			continue
		}
		if role == xstoremeta.RoleLeader {
			return pod.DeepCopy(), nil
		}
	}
	return nil, ErrorLeaderPodNotFound
}

var TryCreateTmpPodIfRemote = NewStepBinder("TryCreateTmpPodIfRemote", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	xStoreFollower := rc.MustGetXStoreFollower()
	if xStoreFollower.Spec.Local {
		return flow.Pass()
	}
	targetPodName := xStoreFollower.Spec.TargetPodName
	targetPod, err := rc.GetPodByName(targetPodName)
	if err != nil {
		return flow.RetryErr(err, "pod not found", "podName", targetPodName)
	}
	pod, err := rc.GetTmpPod()
	if err != nil {
		return flow.RetryErr(ErrorFailGetPod, "failed to get tmp pod")
	}
	if pod != nil {
		xStoreFollower.Status.RebuildPodName = pod.Name
		rc.MarkChanged()
		return flow.Pass()
	}
	tmpPod, err := rc.CreateTmpPod(targetPod, xStoreFollower.Spec.NodeName)
	if err != nil {
		return flow.RetryErr(err, "failed to create tmp pod")
	}
	xStoreFollower.Status.RebuildPodName = tmpPod.Name
	rc.MarkChanged()
	return flow.Continue("TryCreateTmpPodIfRemote Success.")
})

var TryWaitForTmpPodScheduledIfRemote = NewStepBinder("TryWaitForTmpPodScheduledIfRemote", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	xStoreFollower := rc.MustGetXStoreFollower()
	if xStoreFollower.Spec.Local {
		return flow.Pass()
	}
	tmpPod, err := rc.GetTmpPod()
	if err != nil {
		return flow.Error(err, "failed to get tmp pod")
	}
	if tmpPod == nil || k8shelper.IsPodScheduled(tmpPod) {
		xStoreFollower.Status.RebuildNodeName = tmpPod.Spec.NodeName
		xStoreFollower.Status.RebuildPodName = tmpPod.Name
		rc.MarkChanged()
		return flow.Continue("TryCreateTmpPodIfRemote Success.")
	}
	return flow.Retry("TryCreateTmpPodIfRemote Retry.")
})

var TryExchangeTargetPod = NewStepBinder("TryExchangeTargetPod", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	xStoreFollower := rc.MustGetXStoreFollower()
	if xStoreFollower.Spec.Local {
		return flow.Pass()
	}
	if xStoreFollower.Status.ToCleanHostPathVolume != nil {
		return flow.Pass()
	}
	tmpPod, err := rc.GetTmpPod()
	if err != nil {
		return flow.Error(err, "failed to get tmp pod")
	}
	//edit xstore
	targetPod, err := rc.GetPodByName(xStoreFollower.Spec.TargetPodName)
	if err != nil {
		return flow.RetryErr(err, "failed to get target pod")
	}
	xStoreName := targetPod.Labels[xstoremeta.LabelName]
	var xStore polarxv1.XStore
	objKey := types.NamespacedName{
		Name:      xStoreName,
		Namespace: rc.Namespace(),
	}
	err = rc.Client().Get(rc.Context(), objKey, &xStore)
	if err != nil {
		return flow.RetryErr(err, "Failed to get xstore", "xstore", objKey.String())
	}
	hostPathVolume, ok := xStore.Status.BoundVolumes[targetPod.Name]
	if !ok {
		return flow.RetryErr(ErrorNotFoundPodVolume, "Failed to find HostPath volume in xStore")
	}
	toCleanHostPathVolume := hostPathVolume.DeepCopy()
	hostPathVolume.Host = tmpPod.Spec.NodeName
	err = rc.Client().Status().Update(rc.Context(), &xStore)
	if err != nil {
		return flow.RetryErr(ErrorFailUpdateXStore, "failed to update xStore")
	}
	xStoreFollower.Status.ToCleanHostPathVolume = toCleanHostPathVolume
	rc.MarkChanged()
	return flow.Continue("TryExchangeTargetPod Retry.")
})

var TryDeleteTmpPodIfRemote = NewStepBinder("TryDeleteTmpPodIfRemote", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	xStoreFollower := rc.MustGetXStoreFollower()
	if xStoreFollower.Spec.Local {
		return flow.Pass()
	}
	tmpPod, err := rc.GetTmpPod()
	if err != nil {
		flow.RetryErr(ErrorFailGetPod, "fail to get tmp pod")
	}
	if tmpPod != nil {
		err = rc.Client().Delete(rc.Context(), tmpPod)
		if err != nil {
			return flow.RetryErr(ErrorFailDeletePod, "fail to delete pod")
		}
	}
	return flow.Continue("TryDeleteTmpPodIfRemote Retry.")
})

var TryCleanHostPathVolumeIfRemote = NewStepBinder("TryCleanHostPathVolumeIfRemote", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	xStoreFollower := rc.MustGetXStoreFollower()
	if xStoreFollower.Spec.Local {
		return flow.Pass()
	}
	hostPathVolume := xStoreFollower.Status.ToCleanHostPathVolume
	if hostPathVolume != nil {
		xStoreContext := rc.XStoreContext()
		hpfsClient, err := xStoreContext.GetHpfsClient()
		if err != nil {
			return flow.RetryErr(err, "Failed to get hpfs client")
		}
		err = xstoreinstance.DeleteHostPathVolume(rc.Context(), hpfsClient, hostPathVolume)
		if err != nil {
			return flow.RetryErr(err, "failed to delete host path volume", "HostPathVolume", json.Convert2JsonString(hostPathVolume))
		}
		xStoreFollower.Status.ToCleanHostPathVolume = nil
		rc.MarkChanged()
	}
	return flow.Continue("TryCleanHostPathVolumeIfRemote Retry.")
})

var WaitForTargetPodReady = NewStepBinder("WaitForTargetPodReady", func(rc *xstorev1reconcile.FollowerContext, flow control.Flow) (reconcile.Result, error) {
	xStoreFollower := rc.MustGetXStoreFollower()
	rebuildPod, err := rc.GetPodByName(xStoreFollower.Spec.TargetPodName)
	if err != nil {
		return flow.RetryErr(err, "Failed to ge the pod", "podName", xStoreFollower.Spec.TargetPodName)
	}

	if rebuildPod.Spec.NodeName != xStoreFollower.Status.RebuildNodeName {
		err = rc.Client().Delete(rc.Context(), rebuildPod)
		if err != nil {
			return flow.RetryErr(ErrorFailDeletePod, "failed to delete pod", "pod name", rebuildPod.Name)
		}
		return flow.Retry("delete old pod", "podName", rebuildPod.Name, "nodeName", rebuildPod.Spec.NodeName)
	}

	if !k8shelper.IsPodReady(rebuildPod) {
		return flow.Retry("Retry. The pod has not been ready", "pod name", rebuildPod.Name)
	}
	return flow.Continue("WaitForTargetPodReady Retry.")
})
