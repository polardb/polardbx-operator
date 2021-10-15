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

package metric

import "github.com/prometheus/client_golang/prometheus"

type Metric struct {
	Desc *prometheus.Desc
	Type prometheus.ValueType
}

func NewMetric(name, namespace, subsystem, doc string, t prometheus.ValueType, variableLabels []string, constLabels prometheus.Labels) Metric {
	return Metric{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, name),
			doc,
			variableLabels,
			constLabels,
		),
		Type: t,
	}
}
