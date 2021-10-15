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

import (
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
)

func IsPhaseIn(polardbx *polardbxv1.PolarDBXCluster, phases ...polardbxv1polardbx.Phase) bool {
	if polardbx == nil {
		return false
	}
	for _, phase := range phases {
		if polardbx.Status.Phase == phase {
			return true
		}
	}
	return false
}

func TransferPhase(polardbx *polardbxv1.PolarDBXCluster, phase polardbxv1polardbx.Phase) {
	polardbx.Status.Stage = polardbxv1polardbx.StageEmpty
	polardbx.Status.Phase = phase
}
