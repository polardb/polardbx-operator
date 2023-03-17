package instance

import (
	"fmt"
	polarxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strconv"
	"strings"
	"time"
)

var CleanRebuildJob = xstorev1reconcile.NewStepBinder("CleanRebuildJob",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		var err error
		xstoreFollowerList := &polarxv1.XStoreFollowerList{}
		listOpts := []client.ListOption{
			client.InNamespace(rc.Namespace()),
			client.MatchingLabels{xstoremeta.LabelName: rc.Name()},
		}
		err = rc.Client().List(rc.Context(), xstoreFollowerList, listOpts...)
		if err != nil {
			return flow.RetryErr(err, "failed to list xstore followers")
		}
		if len(xstoreFollowerList.Items) == 0 {
			return flow.Continue("CleanRebuildJob success.")
		}
		xstoreFollower := &polarxv1.XStoreFollower{}
		opts := []client.DeleteAllOfOption{
			client.InNamespace(rc.Namespace()),
			client.MatchingLabels{xstoremeta.LabelName: rc.Name()},
		}
		err = rc.Client().DeleteAllOf(rc.Context(), xstoreFollower, opts...)
		if err != nil {
			return flow.RetryErr(err, "failed to delete all xstore followers")
		}
		return flow.Retry("CleanRebuildJob retry.")
	})

func newXStoreFollower(rc *xstorev1reconcile.Context, targetPod *corev1.Pod) *polarxv1.XStoreFollower {
	xstore := rc.MustGetXStore()
	xstoreName := xstore.Name
	if xstore.Spec.PrimaryXStore != "" {
		xstoreName = xstore.Spec.PrimaryXStore
	}
	time.Now().UnixMilli()
	rebuildTask := polarxv1.XStoreFollower{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("auto-ts%d", time.Now().UnixMilli()),
			Namespace: xstore.Namespace,
			Labels: map[string]string{
				xstoremeta.LabelAutoRebuild: "true",
			},
		},
		Spec: polarxv1.XStoreFollowerSpec{
			Local:         false,
			TargetPodName: targetPod.Name,
			XStoreName:    xstoreName,
		},
	}
	return &rebuildTask
}

func isHaOff(val string) bool {
	lowerCaseHaVal := strings.ToLower(val)
	if lowerCaseHaVal == "off" || lowerCaseHaVal == "0" || lowerCaseHaVal == "false" {
		return true
	}
	return false
}

func isPodInDebugRunmode(pod *corev1.Pod) bool {
	if val, ok := pod.Annotations["runmode"]; ok {
		val = strings.ToLower(val)
		if val == "debug" {
			return true
		}
	}
	return false
}

func TryGetAutoRebuildToken(rc *xstorev1reconcile.Context, rebuildTaskName string) error {
	var configMap corev1.ConfigMap
	err := rc.Client().Get(rc.Context(), types.NamespacedName{
		Namespace: rc.Namespace(),
		Name:      convention.AutoRebuildConfigMapName,
	}, &configMap)
	if err != nil {
		if errors.IsNotFound(err) {
			//create autorebuild config map
			configMap = corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      convention.AutoRebuildConfigMapName,
					Namespace: rc.Namespace(),
				},
				Data: map[string]string{},
			}
			err := rc.Client().Create(rc.Context(), &configMap)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	data := configMap.Data
	if data == nil {
		data = map[string]string{}
	}
	newMap := map[string]string{}
	if len(data) >= rc.Config().Store().GetMaxAutoRebuildingCount() {
		// try release
		for k, v := range data {
			ts, _ := strconv.ParseInt(v, 10, 63)
			// check if the token has been hold more than 10 seconds
			// < 10, hold the token
			// >= 10, check the status of rebuild task
			if time.Now().Unix()-ts < 10 {
				newMap[k] = v
			} else {
				var xf polarxv1.XStoreFollower
				err := rc.Client().Get(rc.Context(), types.NamespacedName{
					Namespace: rc.Namespace(),
					Name:      k,
				}, &xf)
				release := false
				if err != nil {
					if errors.IsNotFound(err) {
						release = true
					}
				} else {
					if polardbxv1xstore.IsEndPhase(xf.Status.Phase) {
						release = true
					}
				}
				if !release {
					newMap[k] = v
				}
			}
		}
		if len(newMap) >= rc.Config().Store().GetMaxAutoRebuildingCount() {
			return fmt.Errorf("no rebuid task token could be released")
		}
		data = newMap
	}
	data[rebuildTaskName] = strconv.FormatInt(time.Now().Unix(), 10)
	configMap.Data = data
	err = rc.Client().Update(rc.Context(), &configMap)
	if err != nil {
		return err
	}
	return nil
}

var CheckFollowerStatus = xstorev1reconcile.NewStepBinder("CheckFollowerStatus",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.MustGetXStore()
		if annoVal, ok := xstore.Annotations[xstoremeta.AnnotationAutoRebuild]; ok && isHaOff(annoVal) {
			return flow.Pass()
		}
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.RetryErr(err, "Failed to get xstore pods")
		}
		for _, pod := range pods {
			if xstoremeta.IsPodRoleVoter(&pod) || xstoremeta.IsRoleLeader(&pod) || isPodInDebugRunmode(&pod) {
				continue
			}
			showSlaveStatusResult, err := ShowSlaveStatus(rc, &pod, flow.Logger())
			if err != nil {
				// ignore now
				flow.Logger().Error(err, "failed to show slave status", "podname", pod.Name)
				continue
			}
			if showSlaveStatusResult.LastSqlError != "" && strings.ToLower(showSlaveStatusResult.SlaveSQLRunning) == "no" {
				// create rebuild task. check if a rebuild task exists
				var xstoreFollowerList polarxv1.XStoreFollowerList
				err := rc.Client().List(rc.Context(), &xstoreFollowerList, client.InNamespace(rc.Namespace()), client.MatchingLabels(map[string]string{
					xstoremeta.LabelTargetXStore: pod.Labels[xstoremeta.LabelName],
					xstoremeta.LabelPod:          pod.Name,
				}))
				if err != nil {
					flow.Logger().Error(err, "failed to list rebuild task", "podname", pod.Name)
					continue
				}
				var exist bool
				if len(xstoreFollowerList.Items) > 0 {
					for _, xstoreFollower := range xstoreFollowerList.Items {
						if !polardbxv1xstore.IsEndPhase(xstoreFollower.Status.Phase) {
							exist = true
							break
						}
					}
				}
				if !exist {
					xstoreFollower := newXStoreFollower(rc, &pod)
					err := TryGetAutoRebuildToken(rc, xstoreFollower.Name)
					if err != nil {
						flow.Logger().Error(err, "Failed to TryGetAutoRebuildToken")
					}
					if err == nil {
						err := rc.Client().Create(rc.Context(), xstoreFollower)
						if err != nil {
							flow.Logger().Error(err, "Failed to create xstore follower")
						}
					}
				}
			}
		}
		return flow.Pass()
	})
