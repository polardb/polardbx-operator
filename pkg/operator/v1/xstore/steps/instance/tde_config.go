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
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	xstoreconvention "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/galaxy/factory"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	k8sreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func WhenTdeOpen(binders ...control.BindFunc) control.BindFunc {
	return xstorev1reconcile.NewStepIfBinder("WhenTdeOpen",
		func(rc *xstorev1reconcile.Context, log logr.Logger) (bool, error) {
			if rc.MustGetXStore().Spec.TDE.Enable && !rc.MustGetXStore().Status.TdeStatus {
				return true, nil
			}
			return false, nil
		},
		binders...,
	)
}

var UpdateTdeConfig = xstorev1reconcile.NewStepBinder("UpdateTdeConfig",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (k8sreconcile.Result, error) {
		if !rc.IsTdeEnable() {
			return flow.Pass()
		}
		xstore := rc.MustGetXStore()
		configMap, err := rc.GetConfigMap(xstoreconvention.NewConfigMapName(xstore, xstoreconvention.ConfigMapTypeTde))
		if err != nil {
			return flow.Error(err, "can not find tde configMap")
		}
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "can not find pod")
		}
		leaderPod, followerPod := &corev1.Pod{}, &corev1.Pod{}
		for i := range pods {
			if role, ok := pods[i].Labels[xstoremeta.LabelRole]; ok && role == xstoremeta.RoleLeader {
				leaderPod = &pods[i]
			}
			if role, ok := pods[i].Labels[xstoremeta.LabelRole]; ok && role == xstoremeta.RoleFollower {
				followerPod = &pods[i]
			}
		}
		path := configMap.Data[xstoreconvention.KeyringPath]
		leaderKeyring, err := getPodTDEKeyring(rc, leaderPod, path, flow)
		if err != nil {
			return flow.Error(err, "can not find leaderKeyring")
		}
		followerKeyring, err := getPodTDEKeyring(rc, followerPod, path, flow)
		if err != nil {
			return flow.Error(err, "can not find followerKeyring")
		}
		if configMap.BinaryData == nil {
			configMap.BinaryData = make(map[string][]byte)
		}
		configMap.BinaryData[xstore.Namespace+"-"+leaderPod.Name+"-"+xstoreconvention.Keyring] = []byte(*leaderKeyring)
		configMap.BinaryData[xstore.Namespace+"-"+followerPod.Name+"-"+xstoreconvention.Keyring] = []byte(*followerKeyring)
		if err := rc.Client().Update(rc.Context(), configMap); err != nil {
			return flow.Error(err, "Unable to update tde configmap.")
		}

		return flow.Continue("Tde config is updated.")
	})

func getPodTDEKeyring(rc *xstorev1reconcile.Context, targetPod *corev1.Pod, path string, flow control.Flow) (*string, error) {
	command := []string{"cat", path}
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	err := rc.ExecuteCommandOn(targetPod, "engine", command, control.ExecOptions{
		Logger: flow.Logger(),
		Stdin:  nil,
		Stdout: stdout,
		Stderr: stderr,
	})
	if err != nil {
		return nil, err
	}
	result := stdout.String()
	return &result, nil
}

var CreateTdeConfigMap = xstorev1reconcile.NewStepBinder("CreateTdeConfigMap",
	func(rc *xstorev1reconcile.Context, flow control.Flow) (k8sreconcile.Result, error) {
		xstore := rc.MustGetXStore()
		cmType := xstoreconvention.ConfigMapTypeTde
		cm, err := rc.GetXStoreConfigMap(cmType)
		// 1. Branch not found, create a new one.
		// 2. Branch found, but outdated (by comparing the generation label), update the configmap.
		if cm == nil {
			cm, err = factory.NewConfigMap(rc, xstore, cmType)
			if err != nil {
				return flow.Error(err, "Unable to construct configmap.")
			}

			if err := rc.SetControllerRefAndCreate(cm); err != nil {
				return flow.Error(err, "Unable to create configmap.")
			}
		} else {
			outdated, err := xstoreconvention.IsGenerationOutdated(xstore, cm)
			if err != nil {
				return flow.Error(err, "Unable to resolve generation.")
			}

			if outdated {
				newCm, err := factory.NewConfigMap(rc, xstore, cmType)
				if err != nil {
					return flow.Error(err, "Unable to construct configmap.")
				}
				err = rc.SetControllerRef(newCm)
				if err != nil {
					return flow.Error(err, "Unable to set controller reference.")
				}
				if err := rc.Client().Update(rc.Context(), newCm); err != nil {
					return flow.Error(err, "Unable to update configmap.")
				}
			}
		}
		return flow.Continue("Tde ConfigMap is Created")
	},
)
