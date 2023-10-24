package instance

import (
	"bytes"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/featuregate"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstoreexec "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/command"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/common/channel"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strconv"
	"strings"
	"time"
)

func WhenNeedAdapt(binders ...control.BindFunc) control.BindFunc {
	return xstorev1reconcile.NewStepIfBinder("WhenNeedAdapt",
		func(rc *xstorev1reconcile.Context, log logr.Logger) (bool, error) {
			if featuregate.EnforceClusterIpXStorePod.Enabled() {
				annotations := rc.MustGetXStore().GetAnnotations()
				if val, ok := annotations[xstoremeta.AnnotationAdapting]; ok && xstoremeta.IsAdaptingTrue(val) {
					podServcies, err := rc.GetXStorePodServices()
					if podServcies == nil || len(podServcies) == 0 {
						return true, nil
					}
					return false, err
				}
			}
			return false, nil
		},
		binders...,
	)
}

var FlushClusterMetadata = xstorev1reconcile.NewStepBinder("FlushClusterMetadata",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		leaderPod, err := GetLeaderPod(rc, flow.Logger(), true)
		if err != nil || leaderPod == nil {
			return flow.RetryErr(err, "Failed to get leader pod")
		}
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.RetryErr(err, "Failed to get pods")
		}
		for _, pod := range pods {
			action := "reset-cluster-info-to-learner"
			if pod.Name == leaderPod.Name {
				action = "reset-cluster-info-to-local"
			}
			cmd := xstoreexec.NewCanonicalCommandBuilder().Consensus().PrepareHandleIndicate(action).Build()
			rc.ExecuteCommandOn(&pod, "engine", cmd, control.ExecOptions{
				Logger: flow.Logger(),
			})
		}
		for _, pod := range pods {
			cmd := xstoreexec.NewCanonicalCommandBuilder().Consensus().SetReadonly().Build()
			rc.ExecuteCommandOn(&pod, "engine", cmd, control.ExecOptions{
				Logger:  flow.Logger(),
				Timeout: time.Minute * 1,
			})
		}
		var shutdownFunc = func(pod *corev1.Pod) {
			cmd := xstoreexec.NewCanonicalCommandBuilder().Engine().Shutdown().Build()
			rc.ExecuteCommandOn(pod, "engine", cmd, control.ExecOptions{
				Logger: flow.Logger(),
			})
			cmd = xstoreexec.NewCanonicalCommandBuilder().Process().KillAllMyProcess().Build()
			rc.ExecuteCommandOn(pod, "engine", cmd, control.ExecOptions{
				Logger: flow.Logger(),
			})
		}
		for _, pod := range pods {
			if pod.Name != leaderPod.Name {
				shutdownFunc(&pod)
			}
		}
		shutdownFunc(leaderPod)
		return flow.Continue("FlushClusterMetadata Success.")
	},
)

func GetLeaderPod(rc *xstorev1reconcile.Context, logger logr.Logger, force bool) (*corev1.Pod, error) {
	pods, err := rc.GetXStorePods()
	if err != nil {
		return nil, err
	}
	var leaderPod *corev1.Pod
	for _, pod := range pods {
		role, _, err := ReportRoleAndCurrentLeader(rc, &pod, logger)
		if err != nil {
			continue
		}
		if role == xstoremeta.RoleLeader {
			leaderPod = pod.DeepCopy()
			break
		}
	}
	if leaderPod == nil && force {
		var maxAppliedIndex int64 = -1
		for _, pod := range pods {
			if xstoremeta.IsPodRoleVoter(&pod) {
				continue
			}
			localInfo, err := ShowThis(rc, &pod, logger, true)
			if err != nil {
				return nil, err
			}
			appliedIndex, err := strconv.ParseInt(localInfo.AppliedIndex, 10, 64)
			if appliedIndex > maxAppliedIndex {
				maxAppliedIndex = appliedIndex
				leaderPod = &pod
			}
		}
	}
	return leaderPod, nil
}

var UpdateSharedConfigMap = xstorev1reconcile.NewStepBinder("UpdateSharedConfigMap",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		cm, err := rc.GetXStoreConfigMap(convention.ConfigMapTypeShared)
		if err != nil {
			return flow.RetryErr(err, "failed to get shared configmap")
		}
		sharedChannel, err := parseChannelFromConfigMap(cm)
		if err != nil {
			return flow.Error(err, "Unable to parse shared channel from config map.")
		}
		for i, sharedChannelNode := range sharedChannel.Nodes {
			podService, err := rc.GetXStoreServiceForPod(sharedChannelNode.Pod)
			if err != nil || podService.Spec.ClusterIP == "None" {
				return flow.RetryErr(err, "Failed to get pod service", "pod", sharedChannelNode.Pod)
			}
			newNode := sharedChannelNode
			newNode.Host = podService.Spec.ClusterIP
			sharedChannel.Nodes[i] = newNode
		}
		cm.Data[channel.SharedChannelKey] = sharedChannel.String()
		err = rc.Client().Update(rc.Context(), cm)
		if err != nil {
			return flow.Error(err, "Unable to update shared config map.")
		}
		return flow.Pass()
	},
)

var ReAddFollower = xstorev1reconcile.NewStepBinder("ReAddFollower",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		leaderPod, err := GetLeaderPod(rc, flow.Logger(), false)
		if err != nil {
			return flow.RetryErr(err, "Failed to get leader pod")
		}
		cm, err := rc.GetXStoreConfigMap(convention.ConfigMapTypeShared)
		if err != nil {
			return flow.RetryErr(err, "failed to get shared configmap")
		}
		sharedChannel, err := parseChannelFromConfigMap(cm)
		if err != nil {
			return flow.Error(err, "Unable to parse shared channel from config map.")
		}
		for _, sharedChannelNode := range sharedChannel.Nodes {
			if sharedChannelNode.Pod == leaderPod.Name {
				continue
			}
			nodeAddress := fmt.Sprintf("%s:%d", sharedChannelNode.Host, sharedChannelNode.Port)
			cmd := xstoreexec.NewCanonicalCommandBuilder().Consensus().AddLearner(nodeAddress).Build()
			var buffer bytes.Buffer
			err = rc.ExecuteCommandOn(leaderPod, "engine", cmd, control.ExecOptions{
				Logger: flow.Logger(),
				Stdout: &buffer,
			})
			flow.Logger().Error(err, "Failed to UpdateClusterInfo", "pod", leaderPod.Name)
			response := buffer.String()
			if strings.Contains(response, "ERROR") && !strings.Contains(response, "Target node already exists") {
				flow.Logger().Error(err, "Failed to UpdateClusterInfo", "pod", leaderPod.Name)
			}
			buffer.Reset()
			cmd = xstoreexec.NewCanonicalCommandBuilder().Consensus().ChangeLearnerToFollower(nodeAddress).Build()
			err = rc.ExecuteCommandOn(leaderPod, "engine", cmd, control.ExecOptions{
				Logger: flow.Logger(),
				Stdout: &buffer,
			})
			flow.Logger().Info("change learner to follower", "response", buffer.String(), "pod", leaderPod.Name)
			flow.Logger().Error(err, "Failed to ChangeLearnerToFollower", "pod", leaderPod.Name)
		}
		return flow.Pass()
	},
)
