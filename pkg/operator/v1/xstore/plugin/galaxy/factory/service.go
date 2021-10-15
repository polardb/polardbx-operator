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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/convention"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
)

func newReadWriteService(xstore *polardbxv1.XStore) *corev1.Service {
	serviceName := convention.NewServiceName(xstore, convention.ServiceTypeReadWrite)
	serviceType := xstore.Spec.ServiceType
	serviceLabels := k8shelper.DeepCopyStrMap(xstore.Spec.ServiceLabels)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: xstore.Namespace,
			Labels: k8shelper.PatchLabels(
				serviceLabels,
				convention.ConstLabels(xstore),
				map[string]string{
					xstoremeta.LabelServiceType: xstoremeta.ServiceTypeReadWrite,
				},
			),
			Annotations: map[string]string{},
		},
		Spec: corev1.ServiceSpec{
			Selector: k8shelper.PatchLabels(
				convention.ConstLabels(xstore),
				map[string]string{
					xstoremeta.LabelRole: xstoremeta.RoleLeader,
				},
			),
			Type: serviceType,
			Ports: []corev1.ServicePort{
				{
					Name:       convention.PortAccess,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromString(convention.PortAccess),
					Port:       3306,
				},
				{
					Name:       "polarx",
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromString("polarx"),
					Port:       31306,
				},
			},
		},
	}
}

func newReadOnlyService(xstore *polardbxv1.XStore) *corev1.Service {
	serviceName := convention.NewServiceName(xstore, convention.ServiceTypeReadOnly)
	serviceType := xstore.Spec.ServiceType
	serviceLabels := k8shelper.DeepCopyStrMap(xstore.Spec.ServiceLabels)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: xstore.Namespace,
			Labels: k8shelper.PatchLabels(
				serviceLabels,
				convention.ConstLabels(xstore),
				map[string]string{
					xstoremeta.LabelServiceType: xstoremeta.ServiceTypeReadOnly,
				},
			),
			Annotations: map[string]string{},
		},
		Spec: corev1.ServiceSpec{
			Selector: k8shelper.PatchLabels(
				convention.ConstLabels(xstore),
				map[string]string{
					xstoremeta.LabelRole: xstoremeta.RoleLearner,
				},
			),
			Type: serviceType,
			Ports: []corev1.ServicePort{
				{
					Name:       convention.PortAccess,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromString(convention.PortAccess),
					Port:       3306,
				},
				{
					Name:       "polarx",
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromString("polarx"),
					Port:       31306,
				},
			},
		},
	}
}

func newMetricsService(xstore *polardbxv1.XStore) *corev1.Service {
	serviceName := convention.NewServiceName(xstore, convention.ServiceTypeMetrics)
	serviceLabels := k8shelper.DeepCopyStrMap(xstore.Spec.ServiceLabels)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: xstore.Namespace,
			Labels: k8shelper.PatchLabels(
				serviceLabels,
				convention.ConstLabels(xstore),
				map[string]string{
					xstoremeta.LabelServiceType: xstoremeta.ServiceTypeMetrics,
				},
			),
			Annotations: map[string]string{},
		},
		Spec: corev1.ServiceSpec{
			Selector: convention.ConstLabels(xstore),
			Type:     corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:       convention.PortMetrics,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromString(convention.PortMetrics),
					Port:       8081,
				},
			},
		},
	}
}

func NewService(xstore *polardbxv1.XStore, serviceType convention.ServiceType) *corev1.Service {
	switch serviceType {
	case convention.ServiceTypeReadWrite:
		return newReadWriteService(xstore)
	case convention.ServiceTypeReadOnly:
		return newReadOnlyService(xstore)
	case convention.ServiceTypeMetrics:
		return newMetricsService(xstore)
	default:
		panic("invalid service type: " + serviceType)
	}
}
