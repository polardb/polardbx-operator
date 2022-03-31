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
	"context"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/test/framework/common"
	v1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

var (
	poll = 2 * time.Second
)

func WaitPolarDBXMonitorInStatus(c client.Client, name, namespace string,
	statuslist []polardbx.MonitorStatus, timeout time.Duration) (*polardbxv1.PolarDBXMonitor, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var polardbxmonitor polardbxv1.PolarDBXMonitor
	err := wait.PollImmediate(poll, timeout, func() (bool, error) {
		err := c.Get(ctx, types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		}, &polardbxmonitor)
		if err != nil {
			return true, err // stop wait with error
		}
		for _, s := range statuslist {
			if polardbxmonitor.Status.MonitorStatus == s {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return &polardbxmonitor, nil
}

func WaitForPolarDBXMonitorToDisappear(c client.Client, name, namespace string, timeout time.Duration) error {
	return common.WaitForObjectToDisappear(c, name, namespace, poll, timeout, &polardbxv1.PolarDBXMonitor{})
}

func WaitForPolarDBXMonitorWithLabelsToDisappear(c client.Client, ns string, labels map[string]string, timeout time.Duration) error {
	return common.WaitForObjectsWithLabelsToDisappear(c, labels, ns, poll, timeout, &polardbxv1.PolarDBXMonitorList{})
}

func WaitForServiceMonitorWithLabelsToDisappear(c client.Client, ns string, labels map[string]string, timeout time.Duration) error {
	return common.WaitForObjectsWithLabelsToDisappear(c, labels, ns, poll, timeout, &v1.ServiceMonitorList{})
}
