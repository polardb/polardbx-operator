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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1common "github.com/alibaba/polardbx-operator/api/v1/common"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
)

type FactoryOption func(polardbxcluster *polardbxv1.PolarDBXCluster)

func ProtocolVersion(ver int) FactoryOption {
	return func(polardbxcluster *polardbxv1.PolarDBXCluster) {
		polardbxcluster.Spec.ProtocolVersion = intstr.FromInt(ver)
	}
}

func ShareGMS(polardbxcluster *polardbxv1.PolarDBXCluster) {
	polardbxcluster.Spec.ShareGMS = true
}

func Service(name string, serviceType corev1.ServiceType) FactoryOption {
	return func(polardbxcluster *polardbxv1.PolarDBXCluster) {
		polardbxcluster.Spec.ServiceName = name
		polardbxcluster.Spec.ServiceType = serviceType
	}
}

func TopologyModeGuide(guide string) FactoryOption {
	return func(polardbxcluster *polardbxv1.PolarDBXCluster) {
		polardbxcluster.Annotations = k8shelper.PatchAnnotations(
			polardbxcluster.Annotations,
			map[string]string{
				polardbxmeta.AnnotationTopologyModeGuide: guide,
			},
		)
	}
}

func EncodeKeySecret(name, key string) FactoryOption {
	return func(polardbxcluster *polardbxv1.PolarDBXCluster) {
		security := polardbxcluster.Spec.Security
		if security == nil {
			security = &polardbxv1polardbx.Security{}
		}
		security.EncodeKey = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: name,
			},
			Key: key,
		}
		polardbxcluster.Spec.Security = security
	}
}

func TopologyNode(role string, replicas int, engine, image string, hostNetwork bool, resources corev1.ResourceRequirements) FactoryOption {
	switch role {
	case polardbxmeta.RoleCN:
		return func(polardbxcluster *polardbxv1.PolarDBXCluster) {
			int32Replicas := int32(replicas)
			polardbxcluster.Spec.Topology.Nodes.CN = polardbxv1polardbx.TopologyNodeCN{
				Replicas: &int32Replicas,
				Template: polardbxv1polardbx.CNTemplate{
					Image:       image,
					HostNetwork: hostNetwork,
					Resources:   *resources.DeepCopy(),
				},
			}
		}
	case polardbxmeta.RoleDN:
		return func(polardbxcluster *polardbxv1.PolarDBXCluster) {
			polardbxcluster.Spec.Topology.Nodes.DN = polardbxv1polardbx.TopologyNodeDN{
				Replicas: int32(replicas),
				Template: polardbxv1polardbx.XStoreTemplate{
					Image:       image,
					Engine:      engine,
					HostNetwork: pointer.Bool(hostNetwork),
					Resources: polardbxv1common.ExtendedResourceRequirements{
						ResourceRequirements: *resources.DeepCopy(),
					},
				},
			}
		}
	case polardbxmeta.RoleCDC:
		return func(polardbxcluster *polardbxv1.PolarDBXCluster) {
			polardbxcluster.Spec.Topology.Nodes.CDC = &polardbxv1polardbx.TopologyNodeCDC{
				Replicas: int32(replicas),
				Template: polardbxv1polardbx.CDCTemplate{
					Image:       image,
					HostNetwork: hostNetwork,
					Resources:   *resources.DeepCopy(),
				},
			}
		}
	case polardbxmeta.RoleColumnar:
		return func(polardbxcluster *polardbxv1.PolarDBXCluster) {
			polardbxcluster.Spec.Topology.Nodes.Columnar = &polardbxv1polardbx.TopologyNodeColumnar{
				Replicas: int32(replicas),
				Template: polardbxv1polardbx.ColumnarTemplate{
					Image:       image,
					HostNetwork: hostNetwork,
					Resources:   *resources.DeepCopy(),
				},
			}
		}
	case polardbxmeta.RoleGMS:
		return func(polardbxcluster *polardbxv1.PolarDBXCluster) {
			polardbxcluster.Spec.Topology.Nodes.GMS.Template = &polardbxv1polardbx.XStoreTemplate{
				Image:       image,
				Engine:      engine,
				HostNetwork: pointer.Bool(hostNetwork),
				Resources: polardbxv1common.ExtendedResourceRequirements{
					ResourceRequirements: *resources.DeepCopy(),
				},
			}
		}
	default:
		panic("unrecognized role: " + role)
	}
}

func EnableTLS(secretName string, selfSigned bool) FactoryOption {
	return func(polardbxcluster *polardbxv1.PolarDBXCluster) {
		if polardbxcluster.Spec.Security == nil {
			polardbxcluster.Spec.Security = &polardbxv1polardbx.Security{}
		}
		polardbxcluster.Spec.Security.TLS = &polardbxv1polardbx.TLS{
			SecretName:         secretName,
			GenerateSelfSigned: selfSigned,
		}
	}
}

func ParameterTemplate(templateName string) FactoryOption {
	return func(polardbxcluster *polardbxv1.PolarDBXCluster) {
		polardbxcluster.Spec.ParameterTemplate.Name = templateName
	}
}

func InitReadonly(cnReplicas int, name string, attendHtap bool) FactoryOption {
	return func(polardbxcluster *polardbxv1.PolarDBXCluster) {
		var extraParams = make(map[string]intstr.IntOrString)
		if attendHtap {
			extraParams["AttendHtap"] = intstr.FromString("true")
		}
		polardbxcluster.Spec.InitReadonly = []*polardbxv1polardbx.ReadonlyParam{
			{
				CnReplicas:  cnReplicas,
				Name:        name,
				ExtraParams: extraParams,
			},
		}
	}
}

func NewPolarDBXCluster(name, namespace string, opts ...FactoryOption) *polardbxv1.PolarDBXCluster {
	obj := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	for _, opt := range opts {
		opt(obj)
	}

	return obj
}
