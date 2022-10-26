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

package factory

import (
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strconv"
)

func (f *objectFactory) newReadonlySpec(readonlyParam *polardbxv1polardbx.ReadonlyParam) polardbxv1.PolarDBXClusterSpec {
	polardbx := f.rc.MustGetPolarDBX()

	specCopy := polardbx.Spec

	specCopy.Readonly = true
	specCopy.PrimaryCluster = polardbx.Name

	// Must clear these
	specCopy.ServiceName = ""
	specCopy.InitReadonly = nil
	specCopy.Topology.Nodes.CDC = nil

	*specCopy.Topology.Nodes.CN.Replicas = int32(readonlyParam.CnReplicas)

	for key, value := range readonlyParam.ExtraParams {
		switch key {
		case "AttendHtap":
			if attendHtap, _ := strconv.ParseBool(value.String()); attendHtap {
				if specCopy.Config.CN.Static != nil {
					specCopy.Config.CN.Static.AttendHtap = true
				} else {
					specCopy.Config.CN.Static = &polardbxv1polardbx.CNStaticConfig{
						AttendHtap: true,
					}
				}
			}
		}
	}

	return specCopy
}

func (f *objectFactory) NewReadonlyPolardbx(readonlyParam *polardbxv1polardbx.ReadonlyParam) (*polardbxv1.PolarDBXCluster, error) {
	polardbx, err := f.rc.GetPolarDBX()
	if err != nil {
		return nil, err
	}

	readonlyPolardbx := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:        polardbx.Name + "-" + readonlyParam.Name,
			Namespace:   polardbx.Namespace,
			Annotations: polardbx.Annotations, // Might be risky, readonly instances were not considered while designing annotation
			Finalizers:  []string{polardbxmeta.Finalizer},
		},
		Spec: f.newReadonlySpec(readonlyParam),
	}

	return readonlyPolardbx, nil
}
