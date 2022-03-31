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

package polardbxmonitor

import (
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type MonitorFactoryOption func(monitor *polardbxv1.PolarDBXMonitor)

func MonitorInterval(interval metav1.Duration) MonitorFactoryOption {
	return func(monitor *polardbxv1.PolarDBXMonitor) {
		monitor.Spec.MonitorInterval = interval
	}
}

func ScrapeTimeout(timeout metav1.Duration) MonitorFactoryOption {
	return func(monitor *polardbxv1.PolarDBXMonitor) {
		monitor.Spec.ScrapeTimeout = timeout
	}
}

func NewPolarDBXMonitor(clusterName, namespace string, opts ...MonitorFactoryOption) *polardbxv1.PolarDBXMonitor {
	obj := &polardbxv1.PolarDBXMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName + "-monitor",
			Namespace: namespace,
		},

		Spec: polardbxv1.PolarDBXMonitorSpec{
			ClusterName: clusterName,
		},
	}

	for _, opt := range opts {
		opt(obj)
	}

	return obj
}
