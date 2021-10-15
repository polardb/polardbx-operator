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

package helper

import appsv1 "k8s.io/api/apps/v1"

func IsDeploymentRolledOut(deploy *appsv1.Deployment) bool {
	if deploy == nil {
		return false
	}
	if deploy.Status.ObservedGeneration < deploy.Generation {
		return false
	}
	for _, cond := range deploy.Status.Conditions {
		if cond.Type == appsv1.DeploymentProgressing {
			if cond.Reason == "ProgressDeadlineExceeded" {
				return false
			}
		}
	}
	if deploy.Spec.Replicas != nil && deploy.Status.UpdatedReplicas < *deploy.Spec.Replicas {
		return false
	}
	if deploy.Status.Replicas > deploy.Status.UpdatedReplicas {
		return false
	}
	if deploy.Status.AvailableReplicas < deploy.Status.UpdatedReplicas {
		return false
	}
	return true
}
