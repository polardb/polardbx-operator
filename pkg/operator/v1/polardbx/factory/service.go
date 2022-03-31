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
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
)

func (f *objectFactory) newServicePorts(polardbx *polardbxv1.PolarDBXCluster) []corev1.ServicePort {
	return []corev1.ServicePort{
		{
			Name:       "mysql",
			Protocol:   corev1.ProtocolTCP,
			Port:       3306,
			TargetPort: intstr.FromString("mysql"),
		},
		{
			Name:       "metrics",
			Protocol:   corev1.ProtocolTCP,
			Port:       8081,
			TargetPort: intstr.FromString("metrics"),
		},
	}
}

func (f *objectFactory) NewService() (*corev1.Service, error) {
	polardbx, err := f.rc.GetPolarDBX()
	if err != nil {
		return nil, err
	}

	serviceName := polardbx.Spec.ServiceName
	if len(serviceName) == 0 {
		serviceName = polardbx.Name
	}

	serviceType := polardbx.Spec.ServiceType
	if len(serviceType) == 0 {
		serviceType = corev1.ServiceTypeNodePort
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: f.rc.Namespace(),
			Labels:    convention.ConstLabelsForCN(polardbx, polardbxmeta.CNTypeRW),
		},
		Spec: corev1.ServiceSpec{
			Type:     serviceType,
			Selector: convention.ConstLabelsForCN(polardbx, polardbxmeta.CNTypeRW),
			Ports:    f.newServicePorts(polardbx),
		},
	}, nil
}

func (f *objectFactory) NewReadOnlyService() (*corev1.Service, error) {
	polardbx, err := f.rc.GetPolarDBX()
	if err != nil {
		return nil, err
	}

	serviceName := polardbx.Spec.ServiceName
	if len(serviceName) == 0 {
		serviceName = polardbx.Name
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName + "-ro",
			Namespace: f.rc.Namespace(),
			Labels:    convention.ConstLabelsForCN(polardbx, polardbxmeta.CNTypeRO),
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Selector: convention.ConstLabelsForCN(polardbx, polardbxmeta.CNTypeRO),
			Ports:    f.newServicePorts(polardbx),
		},
	}, nil
}

func (f *objectFactory) NewCDCMetricsService() (*corev1.Service, error) {
	polardbx, err := f.rc.GetPolarDBX()
	if err != nil {
		return nil, err
	}

	serviceName := polardbx.Spec.ServiceName
	if len(serviceName) == 0 {
		serviceName = polardbx.Name
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName + "-cdc-metrics",
			Namespace: f.rc.Namespace(),
			Labels:    convention.ConstLabelsForCDC(polardbx),
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Selector: convention.ConstLabelsForCDC(polardbx),
			Ports: []corev1.ServicePort{
				{
					Name:       "metrics",
					Protocol:   corev1.ProtocolTCP,
					Port:       8081,
					TargetPort: intstr.FromString("metrics"),
				},
			},
		},
	}, nil
}
