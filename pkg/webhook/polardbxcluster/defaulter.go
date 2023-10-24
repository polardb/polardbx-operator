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

package polardbxcluster

import (
	"context"
	"github.com/alibaba/polardbx-operator/api/v1/common"
	"k8s.io/apimachinery/pkg/util/rand"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/webhook/extension"
)

type PolarDBXClusterV1Defaulter struct {
	configLoader func() *DefaulterConfig
}

func (d *PolarDBXClusterV1Defaulter) Default(ctx context.Context, obj runtime.Object) error {
	polardbx := obj.(*polardbxv1.PolarDBXCluster)

	// Protocol version.
	// Note: Default of IntOrString is IntOrString{Type: 0, IntVal: 0, StrVal: ""}.
	if polardbx.Spec.ProtocolVersion.Type == intstr.Int &&
		polardbx.Spec.ProtocolVersion.IntVal == 0 {
		polardbx.Spec.ProtocolVersion = intstr.FromString(d.configLoader().ProtocolVersion)
	}
	if polardbx.Spec.ProtocolVersion.String() == "5" {
		polardbx.Spec.ProtocolVersion = intstr.FromString("5.7")
	}
	if polardbx.Spec.ProtocolVersion.String() == "8" {
		polardbx.Spec.ProtocolVersion = intstr.FromString("8.0")
	}
	if polardbx.Annotations == nil {
		polardbx.Annotations = map[string]string{}
	}
	if _, ok := polardbx.Annotations[common.AnnotationOperatorCreateVersion]; !ok {
		polardbx.Annotations[common.AnnotationOperatorCreateVersion] = d.configLoader().OperatorVersion
	}

	if polardbx.Spec.Config.CN.Static != nil {
		if polardbx.Spec.Config.CN.Static.RPCProtocolVersion.String() == "" {
			polardbx.Spec.Config.CN.Static.RPCProtocolVersion = intstr.FromString("1")
		}
	}

	// Service name, default to the object name.
	if polardbx.Spec.ServiceName == "" {
		polardbx.Spec.ServiceName = polardbx.Name
	}

	// Service type.
	if polardbx.Spec.ServiceType == "" {
		polardbx.Spec.ServiceType = corev1.ServiceType(d.configLoader().ServiceType)
	}

	// Upgrade strategy.
	if polardbx.Spec.UpgradeStrategy == "" {
		polardbx.Spec.UpgradeStrategy = polardbxv1polardbx.UpgradeStrategyType(d.configLoader().UpgradeStrategy)
	}

	// Storage engine.
	topology := &polardbx.Spec.Topology
	nodes := &topology.Nodes
	if nodes.GMS.Template != nil && nodes.GMS.Template.Engine == "" {
		nodes.GMS.Template.Engine = d.configLoader().StorageEngine
	}
	if nodes.DN.Template.Engine == "" {
		nodes.DN.Template.Engine = d.configLoader().StorageEngine
	}
	if nodes.CDC != nil {
		for _, group := range nodes.CDC.Groups {
			if group.Template == nil {
				group.Template = &polardbx.Spec.Topology.Nodes.CDC.Template
			}
		}
	}

	// Readonly cluster initialization list
	if polardbx.Spec.InitReadonly != nil {
		for i, readonlyParam := range polardbx.Spec.InitReadonly {
			if readonlyParam.CnReplicas < 0 {
				polardbx.Spec.InitReadonly[i].CnReplicas = 0
			}
			if readonlyParam.Name == "" {
				polardbx.Spec.InitReadonly[i].Name = "ro-" + rand.String(4)
			}
		}
	}

	return nil
}

func NewPolarDBXClusterV1Defaulter(configLoader func() *DefaulterConfig) extension.CustomDefaulter {
	return &PolarDBXClusterV1Defaulter{
		configLoader: configLoader,
	}
}
