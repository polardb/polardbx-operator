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

package monitor

import (
	"errors"
	"fmt"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/factory"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	promv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var CheckServiceMonitorExists = polardbxv1reconcile.NewStepBinder("CreateServiceMonitorIfNeeded",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbx := rc.MustGetPolarDBX()

		labels := convention.ConstLabels(polardbx)

		var serviceMonitors promv1.ServiceMonitorList
		err := rc.Client().List(rc.Context(), &serviceMonitors, client.InNamespace(rc.Namespace()),
			client.MatchingLabels(labels))

		if err != nil {
			return flow.Continue("Unable to get servicemonitor for polardbx, skip!")
		}

		if len(serviceMonitors.Items) != 0 {
			return flow.Error(errors.New("servicemonitor already exists for polardbx"),
				"Unable to create service monitor for polardbx", "polardbx", polardbx.Name)
		}

		return flow.Pass()
	},
)

var CreateServiceMonitorIfNeeded = polardbxv1reconcile.NewStepBinder("CreateServiceMonitorIfNeeded",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		objectFactory := factory.NewObjectFactory(rc)
		serviceMonitors, err := objectFactory.NewServiceMonitors()
		if err != nil {
			return flow.Error(err, "Unable to generate service monitors")
		}

		polardbx := rc.MustGetPolarDBX()
		polardbxmonitor := rc.MustGetPolarDBXMonitor()

		polardbxmonitor.Labels = convention.ConstLabels(polardbx)
		rc.SetControllerRefAndUpdate(polardbxmonitor)

		for role, serviceMonitor := range serviceMonitors {
			ctrl.SetControllerReference(polardbxmonitor, &serviceMonitor, rc.Scheme())
			err = rc.Client().Create(rc.Context(), &serviceMonitor)
			if err != nil {
				flow.Logger().Error(err, "Unable to create service monitors for role: "+role)
				return flow.Error(err, "Unable to create service monitors for role: "+role)
			}
		}

		return flow.Pass()
	},
)

var SyncPolarDBXMonitorSpecToStatus = polardbxv1reconcile.NewStepBinder("SyncPolarDBXMonitorSpecToStatus",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbxMonitor := rc.MustGetPolarDBXMonitor()
		polardbxMonitor.Status.MonitorSpecSnapshot = polardbxMonitor.Spec.DeepCopy()
		return flow.Pass()
	},
)

var UpdateServiceMonitorIfNeeded = polardbxv1reconcile.NewStepBinder("UpdateServiceMonitorIfNeeded",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		monitor := rc.MustGetPolarDBXMonitor()
		polardbx := rc.MustGetPolarDBX()

		labels := convention.ConstLabels(polardbx)

		var serviceMonitors promv1.ServiceMonitorList
		err := rc.Client().List(rc.Context(), &serviceMonitors, client.InNamespace(rc.Namespace()),
			client.MatchingLabels(labels))

		if err != nil {
			return flow.Error(err, "Unable to get servicemonitor for polardbx", "polardbx", polardbx.Name)
		}

		for _, serviceMonitor := range serviceMonitors.Items {
			//endpoint := serviceMonitor.Spec.Endpoints[0]
			serviceMonitor.Spec.Endpoints[0].Interval = fmt.Sprintf("%.0fs",
				monitor.Spec.MonitorInterval.Seconds())
			serviceMonitor.Spec.Endpoints[0].ScrapeTimeout = fmt.Sprintf("%.0fs",
				monitor.Spec.ScrapeTimeout.Seconds())
			err := rc.Client().Update(rc.Context(), serviceMonitor)
			if err != nil {
				return flow.Error(err, "Unable to update service monitor",
					"servicemonitor", serviceMonitor.Name)
			}
		}

		return flow.Pass()
	},
)

func TransferMonitorStatusTo(status polardbxv1polardbx.MonitorStatus, requeue bool) control.BindFunc {
	return polardbxv1reconcile.NewStepBinder("TransferMonitorStatusTo"+string(status),
		func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
			monitor := rc.MustGetPolarDBXMonitor()
			monitor.Status.MonitorStatus = status
			if requeue {
				return flow.Retry("Retry immediately.")
			}
			return flow.Pass()
		},
	)
}

var PersistPolarDBXMonitor = polardbxv1reconcile.NewStepBinder("PersistPolarDBXMonitor",
	func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
		polardbxmonitor := rc.MustGetPolarDBXMonitor()
		err := rc.Client().Update(rc.Context(), polardbxmonitor)
		if err != nil {
			return flow.Error(err, "Unable to persist polardbx monitor status")
		}

		return flow.Pass()
	},
)
