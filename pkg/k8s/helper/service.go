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
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

func GetPortFromService(svc *corev1.Service, port string) *corev1.ServicePort {
	for _, servicePort := range svc.Spec.Ports {
		if servicePort.Name == port {
			return &servicePort
		}
	}
	return nil
}

func MustGetPortFromService(svc *corev1.Service, port string) *corev1.ServicePort {
	servicePort := GetPortFromService(svc, port)
	if servicePort == nil {
		panic("port not found: " + port)
	}
	return servicePort
}

func GetClusterAddrFromService(svc *corev1.Service, port string) (string, error) {
	svcPort := GetPortFromService(svc, port)
	if svcPort == nil {
		return "", errors.New("port not found: " + port)
	}
	return fmt.Sprintf("%s:%d", svc.Spec.ClusterIP, svcPort.Port), nil
}

func GetClusterIpPortFromService(svc *corev1.Service, port string) (string, int32, error) {
	svcPort := GetPortFromService(svc, port)
	if svcPort == nil {
		return "", -1, errors.New("port not found: " + port)
	}
	return svc.Spec.ClusterIP, svcPort.Port, nil
}

func GetServiceDNSRecord(svcName, namespace string, withinNamespace bool) string {
	if withinNamespace {
		return svcName
	} else {
		return svcName + "." + namespace
	}
}

func GetServiceDNSRecordWithSvc(svc *corev1.Service, withinNamespace bool) string {
	return GetServiceDNSRecord(svc.Name, svc.Namespace, withinNamespace)
}
