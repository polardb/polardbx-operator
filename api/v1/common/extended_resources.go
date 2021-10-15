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

package common

import (
	"bytes"

	corev1 "k8s.io/api/core/v1"
)

const (
	ResourceIOPS      corev1.ResourceName = "iops"
	ResourceBPS       corev1.ResourceName = "bps"
	ResourceReadIOPS  corev1.ResourceName = "iops.read"
	ResourceWriteIOPS corev1.ResourceName = "iops.write"
	ResourceReadBPS   corev1.ResourceName = "bps.read"
	ResourceWriteBPS  corev1.ResourceName = "bps.write"
)

const Unlimited int64 = -1

var ExtendedResource = []corev1.ResourceName{
	ResourceIOPS,
	ResourceBPS,
	ResourceReadIOPS,
	ResourceWriteIOPS,
	ResourceReadBPS,
	ResourceWriteBPS,
}

func ResourceBlkioValueStr(limit corev1.ResourceList) (string, bool) {
	buf := bytes.Buffer{}
	for _, r := range ExtendedResource {
		v, ok := limit[r]
		if ok {
			buf.WriteString(string(r))
			buf.WriteRune('=')
			buf.WriteString(v.String())
			buf.WriteRune(',')
		}
	}
	if buf.Len() > 0 {
		s := buf.String()
		return s[:len(s)-1], true
	}
	return "", false
}

func getResourceBlkioValue(limit corev1.ResourceList, key corev1.ResourceName) int64 {
	if limit == nil {
		return Unlimited
	}
	v, ok := limit[key]
	if !ok {
		return Unlimited
	}
	return v.Value()
}

func blkioValueOverwrite(total, sub int64) int64 {
	if total == Unlimited {
		return sub
	}
	if sub == Unlimited {
		return total
	}
	if sub < total {
		return sub
	}
	return total
}

func GetResourceReadIOPSValue(limit corev1.ResourceList) int64 {
	return blkioValueOverwrite(
		getResourceBlkioValue(limit, ResourceIOPS),
		getResourceBlkioValue(limit, ResourceReadIOPS),
	)
}

func GetResourceWriteIOPSValue(limit corev1.ResourceList) int64 {
	return blkioValueOverwrite(
		getResourceBlkioValue(limit, ResourceIOPS),
		getResourceBlkioValue(limit, ResourceWriteIOPS),
	)
}

func GetResourceReadBPSValue(limit corev1.ResourceList) int64 {
	return blkioValueOverwrite(
		getResourceBlkioValue(limit, ResourceBPS),
		getResourceBlkioValue(limit, ResourceReadBPS),
	)
}

func GetResourceWriteBPSValue(limit corev1.ResourceList) int64 {
	return blkioValueOverwrite(
		getResourceBlkioValue(limit, ResourceBPS),
		getResourceBlkioValue(limit, ResourceWriteBPS),
	)
}

type ExtendedResourceRequirements struct {
	corev1.ResourceRequirements `json:",inline"`
	LimitsIO                    corev1.ResourceList `json:"limits.io,omitempty"`
}
