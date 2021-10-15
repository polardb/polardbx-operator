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
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/command"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/plugin/galaxy/galaxy"
	xstorereconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
)

func setSuperReadOnly(rc *xstorereconcile.Context, log logr.Logger, pod *corev1.Pod) error {
	return rc.ExecuteCommandOn(pod, convention.ContainerEngine,
		command.NewCanonicalCommandBuilder().Config().Set(map[string]string{
			"super_read_only": "ON",
		}).Build(),
		control.ExecOptions{
			Logger:  log,
			Timeout: 2 * time.Second,
		},
	)
}

var SetSuperReadOnlyOnAllCandidates = plugin.NewStepBinder(galaxy.Engine, "SetSuperReadOnlyOnAllCandidates",
	func(rc *xstorereconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}

		candidatePods := k8shelper.FilterPodsBy(pods, func(pod *corev1.Pod) bool {
			return xstoremeta.IsPodRoleCandidate(pod)
		})

		for _, pod := range candidatePods {
			err := setSuperReadOnly(rc, flow.Logger(), &pod)
			if err != nil {
				return flow.Error(err, "Unable to set super-read-only on pod.", "pod", pod.Name)
			}
		}

		return flow.Pass()
	},
)

func unsetSuperReadOnly(rc *xstorereconcile.Context, log logr.Logger, pod *corev1.Pod) error {
	return rc.ExecuteCommandOn(pod, convention.ContainerEngine,
		command.NewCanonicalCommandBuilder().Config().Set(map[string]string{
			"super_read_only": "OFF",
		}).Build(),
		control.ExecOptions{
			Logger:  log,
			Timeout: 2 * time.Second,
		},
	)
}

var UnsetSuperReadOnlyOnAllCandidates = plugin.NewStepBinder(galaxy.Engine, "UnsetSuperReadOnlyOnAllCandidates",
	func(rc *xstorereconcile.Context, flow control.Flow) (reconcile.Result, error) {
		pods, err := rc.GetXStorePods()
		if err != nil {
			return flow.Error(err, "Unable to get pods.")
		}

		candidatePods := k8shelper.FilterPodsBy(pods, func(pod *corev1.Pod) bool {
			return xstoremeta.IsPodRoleCandidate(pod)
		})

		for _, pod := range candidatePods {
			err := unsetSuperReadOnly(rc, flow.Logger(), &pod)
			if err != nil {
				return flow.Error(err, "Unable to unset super-read-only on pod.", "pod", pod.Name)
			}
		}

		return flow.Pass()
	},
)
