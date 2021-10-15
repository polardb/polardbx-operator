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
	"strings"

	promv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxmeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
)

func toPrometheusLabel(label string) string {
	return strings.NewReplacer(".", "_", "/", "_").Replace(label)
}

func relabelConfig4GMS(rc *polardbxv1reconcile.Context, polardbx *polardbxv1.PolarDBXCluster) []*promv1.RelabelConfig {
	if polardbx.Spec.ShareGMS {
		return []*promv1.RelabelConfig{
			{
				SourceLabels: []string{
					toPrometheusLabel(polardbxmeta.LabelRole),
				},
				Regex:       "(.*)",
				Separator:   ";",
				Action:      "replace",
				Replacement: polardbxmeta.RoleGMS,
				TargetLabel: toPrometheusLabel(polardbxmeta.LabelRole),
			},
		}
	}
	return nil
}

func suffixPatcher(suffix string) func(s string) string {
	return func(s string) string {
		return s + suffix
	}
}

func (f *objectFactory) NewServiceMonitors() (map[string]promv1.ServiceMonitor, error) {
	polardbx, err := f.rc.GetPolarDBX()
	if err != nil {
		return nil, err
	}

	return map[string]promv1.ServiceMonitor{
		polardbxmeta.RoleGMS: {
			ObjectMeta: metav1.ObjectMeta{
				Name:      f.rc.NameInto(suffixPatcher("-gms")),
				Namespace: f.rc.Namespace(),
			},
			Spec: promv1.ServiceMonitorSpec{
				JobLabel: f.rc.NameInto(suffixPatcher("-gms")),
				TargetLabels: []string{
					polardbxmeta.LabelName,
					polardbxmeta.LabelRole,
				},
				PodTargetLabels: []string{
					xstoremeta.LabelName,
					xstoremeta.LabelRole,
					xstoremeta.LabelNodeRole,
					xstoremeta.LabelNodeSet,
				},
				Endpoints: []promv1.Endpoint{
					{
						Port:           "metrics",
						Interval:       "5s",
						ScrapeTimeout:  "4s",
						RelabelConfigs: relabelConfig4GMS(f.rc, polardbx),
					},
				},
				NamespaceSelector: promv1.NamespaceSelector{
					MatchNames: []string{f.rc.Namespace()},
				},
				Selector: metav1.LabelSelector{
					MatchLabels: map[string]string{
						polardbxmeta.LabelName: polardbx.Name,
						polardbxmeta.LabelRole: polardbxmeta.RoleGMS,
					},
				},
			},
		},
		polardbxmeta.RoleCN: {
			ObjectMeta: metav1.ObjectMeta{
				Name:      f.rc.NameInto(suffixPatcher("-gms")),
				Namespace: f.rc.Namespace(),
			},
			Spec: promv1.ServiceMonitorSpec{
				JobLabel: f.rc.NameInto(suffixPatcher("-gms")),
				TargetLabels: []string{
					polardbxmeta.LabelName,
					polardbxmeta.LabelRole,
					polardbxmeta.LabelCNType,
				},
				PodTargetLabels: []string{},
				Endpoints: []promv1.Endpoint{
					{
						Port:          "metrics",
						Interval:      "5s",
						ScrapeTimeout: "4s",
					},
				},
				NamespaceSelector: promv1.NamespaceSelector{
					MatchNames: []string{f.rc.Namespace()},
				},
				Selector: metav1.LabelSelector{
					MatchLabels: map[string]string{
						polardbxmeta.LabelName: polardbx.Name,
						polardbxmeta.LabelRole: polardbxmeta.RoleCN,
					},
				},
			},
		},
		polardbxmeta.RoleDN: {
			ObjectMeta: metav1.ObjectMeta{
				Name:      f.rc.NameInto(suffixPatcher("-dn")),
				Namespace: f.rc.Namespace(),
			},
			Spec: promv1.ServiceMonitorSpec{
				JobLabel: f.rc.NameInto(suffixPatcher("-dn")),
				TargetLabels: []string{
					polardbxmeta.LabelName,
					polardbxmeta.LabelRole,
					polardbxmeta.LabelDNIndex,
				},
				PodTargetLabels: []string{
					xstoremeta.LabelName,
					xstoremeta.LabelRole,
					xstoremeta.LabelNodeRole,
					xstoremeta.LabelNodeSet,
				},
				Endpoints: []promv1.Endpoint{
					{
						Port:          "metrics",
						Interval:      "5s",
						ScrapeTimeout: "4s",
					},
				},
				NamespaceSelector: promv1.NamespaceSelector{
					MatchNames: []string{f.rc.Namespace()},
				},
				Selector: metav1.LabelSelector{
					MatchLabels: map[string]string{
						polardbxmeta.LabelName: polardbx.Name,
						polardbxmeta.LabelRole: polardbxmeta.RoleDN,
					},
				},
			},
		},
		polardbxmeta.RoleCDC: {
			ObjectMeta: metav1.ObjectMeta{
				Name:      f.rc.NameInto(suffixPatcher("-cdc")),
				Namespace: f.rc.Namespace(),
			},
			Spec: promv1.ServiceMonitorSpec{
				JobLabel: f.rc.NameInto(suffixPatcher("-cdc")),
				TargetLabels: []string{
					polardbxmeta.LabelName,
					polardbxmeta.LabelRole,
				},
				PodTargetLabels: []string{},
				Endpoints: []promv1.Endpoint{
					{
						Port:          "metrics",
						Interval:      "5s",
						ScrapeTimeout: "4s",
					},
				},
				NamespaceSelector: promv1.NamespaceSelector{
					MatchNames: []string{f.rc.Namespace()},
				},
				Selector: metav1.LabelSelector{
					MatchLabels: map[string]string{
						polardbxmeta.LabelName: polardbx.Name,
						polardbxmeta.LabelRole: polardbxmeta.RoleCDC,
					},
				},
			},
		},
	}, nil
}
