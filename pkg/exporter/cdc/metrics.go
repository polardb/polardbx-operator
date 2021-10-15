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

package cdc

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/alibaba/polardbx-operator/pkg/exporter/metric"
)

const (
	namespace = "polardbx_cdc"
)

func newPrometheusMetricsForCdc(valType prometheus.ValueType, name, subsystem, doc string, varLabels []string, constLabels prometheus.Labels) metric.Metric {
	return metric.NewMetric(name, namespace, subsystem, doc, valType, varLabels, constLabels)
}

const (
	subsystemDumper = "dumper"
	subsystemTask   = "task"
)

var (
	metrics = map[string]metric.Metric{
		// Dumper master
		"polardbx_cdc_dumper_m_delay":    newPrometheusMetricsForCdc(prometheus.GaugeValue, "delay_in_millisecond", subsystemDumper, "Delay time in milliseconds", nil, prometheus.Labels{"role": "master"}),
		"polardbx_cdc_dumper_m_eps":      newPrometheusMetricsForCdc(prometheus.GaugeValue, "events_processed_per_second", subsystemDumper, "Average events processed per second.", nil, prometheus.Labels{"role": "master", "event": "all"}),
		"polardbx_cdc_dumper_m_dml_eps":  newPrometheusMetricsForCdc(prometheus.GaugeValue, "events_processed_per_second", subsystemDumper, "Average events processed per second.", nil, prometheus.Labels{"role": "master", "event": "dml"}),
		"polardbx_cdc_dumper_m_tps":      newPrometheusMetricsForCdc(prometheus.GaugeValue, "transactions_processed_per_second", subsystemDumper, "Average transaction processed per second.", nil, prometheus.Labels{"role": "master"}),
		"polardbx_cdc_dumper_m_bps":      newPrometheusMetricsForCdc(prometheus.GaugeValue, "bytes_processed_per_second", subsystemDumper, "Average bytes processed per second.", nil, prometheus.Labels{"role": "master"}),
		"polardbx_cdc_dumper_m_event_rt": newPrometheusMetricsForCdc(prometheus.GaugeValue, "time_used_per_event", subsystemDumper, "Average process time used per event.", nil, prometheus.Labels{"role": "master"}),
		"polardbx_cdc_dumper_m_txn_rt":   newPrometheusMetricsForCdc(prometheus.GaugeValue, "time_used_per_transaction", subsystemDumper, "Average process time used per transaction.", nil, prometheus.Labels{"role": "master"}),

		// Dumper slave
		"polardbx_cdc_dumper_s_delay":    newPrometheusMetricsForCdc(prometheus.GaugeValue, "delay_in_millisecond", subsystemDumper, "Delay time in milliseconds", nil, prometheus.Labels{"role": "slave"}),
		"polardbx_cdc_dumper_s_eps":      newPrometheusMetricsForCdc(prometheus.GaugeValue, "events_processed_per_second", subsystemDumper, "Average events processed per second.", nil, prometheus.Labels{"role": "slave", "event": "all"}),
		"polardbx_cdc_dumper_s_dml_eps":  newPrometheusMetricsForCdc(prometheus.GaugeValue, "events_processed_per_second", subsystemDumper, "Average events processed per second.", nil, prometheus.Labels{"role": "slave", "event": "dml"}),
		"polardbx_cdc_dumper_s_tps":      newPrometheusMetricsForCdc(prometheus.GaugeValue, "transactions_processed_per_second", subsystemDumper, "Average transaction processed per second.", nil, prometheus.Labels{"role": "slave"}),
		"polardbx_cdc_dumper_s_bps":      newPrometheusMetricsForCdc(prometheus.GaugeValue, "bytes_processed_per_second", subsystemDumper, "Average bytes processed per second.", nil, prometheus.Labels{"role": "slave"}),
		"polardbx_cdc_dumper_s_event_rt": newPrometheusMetricsForCdc(prometheus.GaugeValue, "time_used_per_event", subsystemDumper, "Average process time used per event.", nil, prometheus.Labels{"role": "slave"}),
		"polardbx_cdc_dumper_s_txn_rt":   newPrometheusMetricsForCdc(prometheus.GaugeValue, "time_used_per_transaction", subsystemDumper, "Average process time used per transaction.", nil, prometheus.Labels{"role": "slave"}),

		// Task (merger)
		"polardbx_cdc_task_merge_txn_count":     newPrometheusMetricsForCdc(prometheus.GaugeValue, "total_merged_transactions", subsystemTask, "Total merged transactions.", nil, prometheus.Labels{"role": "merger", "transaction_type": "all"}),
		"polardbx_cdc_task_merge_txn_2pc_count": newPrometheusMetricsForCdc(prometheus.GaugeValue, "total_merged_transactions", subsystemTask, "Total merged transactions.", nil, prometheus.Labels{"role": "merger", "transaction_type": "2pc"}),
		"polardbx_cdc_task_merge_buf_size":      newPrometheusMetricsForCdc(prometheus.GaugeValue, "merge_queue_size", subsystemTask, "Current size of merge queue.", nil, prometheus.Labels{"role": "merger"}),
		"polardbx_cdc_task_transmit_buf_size":   newPrometheusMetricsForCdc(prometheus.GaugeValue, "transmit_queue_size", subsystemTask, "Current size of transmit queue.", nil, prometheus.Labels{"role": "merger"}),

		// Task (extractor)
		"polardbx_task_e_network_in_bytes_total_naive": newPrometheusMetricsForCdc(prometheus.GaugeValue, "total_network_in_bytes", subsystemTask, "Total network input bytes.", nil, prometheus.Labels{"role": "extractor"}),
		"polardbx_task_e_in_tps":                       newPrometheusMetricsForCdc(prometheus.GaugeValue, "transactions_per_second", subsystemTask, "Transactions per second.", nil, prometheus.Labels{"role": "extractor"}),
		"polardbx_task_e_in_eps":                       newPrometheusMetricsForCdc(prometheus.GaugeValue, "events_per_second", subsystemTask, "Events per second.", nil, prometheus.Labels{"role": "extractor"}),
		"polardbx_task_e_in_max_delay":                 newPrometheusMetricsForCdc(prometheus.GaugeValue, "delay_in_millisecond", subsystemTask, "Delay time in milliseconds.", nil, prometheus.Labels{"role": "extractor"}),

		// JVM
		"polardbx_cdc_dumper_m_youngUsed":            newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_generation_used", subsystemDumper, "Current generation used.", nil, prometheus.Labels{"generation": "young", "role": "master"}),
		"polardbx_cdc_dumper_m_youngMax":             newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_generation_max_capacity", subsystemDumper, "Current generation max capacity.", nil, prometheus.Labels{"generation": "young", "role": "master"}),
		"polardbx_cdc_dumper_m_oldUsed":              newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_generation_used", subsystemDumper, "Current generation used.", nil, prometheus.Labels{"generation": "old", "role": "master"}),
		"polardbx_cdc_dumper_m_oldMax":               newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_generation_max_capacity", subsystemDumper, "Current generation max capacity.", nil, prometheus.Labels{"generation": "old", "role": "master"}),
		"polardbx_cdc_dumper_m_heapUsage":            newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_heap_usage", subsystemDumper, "Current heap usage.", nil, prometheus.Labels{"role": "master"}),
		"polardbx_cdc_dumper_m_youngCollectionCount": newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_collector_invocation_count", subsystemDumper, "Total GC collector invocation count.", nil, prometheus.Labels{"generation": "young", "role": "master"}),
		"polardbx_cdc_dumper_m_oldCollectionCount":   newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_collector_invocation_count", subsystemDumper, "Total GC collector invocation count.", nil, prometheus.Labels{"generation": "old", "role": "master"}),
		"polardbx_cdc_dumper_m_youngCollectionTime":  newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_collector_time_total", subsystemDumper, "Total GC collector invocation time.", nil, prometheus.Labels{"generation": "young", "role": "master"}),
		"polardbx_cdc_dumper_m_oldCollectionTime":    newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_collector_time_total", subsystemDumper, "Total GC collector invocation time.", nil, prometheus.Labels{"generation": "old", "role": "master"}),
		"polardbx_cdc_dumper_m_currentThreadCount":   newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_threads_live_count", subsystemDumper, "Current alive threads.", nil, prometheus.Labels{"role": "master"}),

		"polardbx_cdc_dumper_s_youngUsed":            newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_generation_used", subsystemDumper, "Current generation used.", nil, prometheus.Labels{"generation": "young", "role": "slave"}),
		"polardbx_cdc_dumper_s_youngMax":             newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_generation_max_capacity", subsystemDumper, "Current generation max capacity.", nil, prometheus.Labels{"generation": "young", "role": "slave"}),
		"polardbx_cdc_dumper_s_oldUsed":              newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_generation_used", subsystemDumper, "Current generation used.", nil, prometheus.Labels{"generation": "old", "role": "slave"}),
		"polardbx_cdc_dumper_s_oldMax":               newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_generation_max_capacity", subsystemDumper, "Current generation max capacity.", nil, prometheus.Labels{"generation": "old", "role": "slave"}),
		"polardbx_cdc_dumper_s_heapUsage":            newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_heap_usage", subsystemDumper, "Current heap usage.", nil, prometheus.Labels{"role": "slave"}),
		"polardbx_cdc_dumper_s_youngCollectionCount": newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_collector_invocation_count", subsystemDumper, "Total GC collector invocation count.", nil, prometheus.Labels{"generation": "young", "role": "slave"}),
		"polardbx_cdc_dumper_s_oldCollectionCount":   newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_collector_invocation_count", subsystemDumper, "Total GC collector invocation count.", nil, prometheus.Labels{"generation": "old", "role": "slave"}),
		"polardbx_cdc_dumper_s_youngCollectionTime":  newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_collector_time_total", subsystemDumper, "Total GC collector invocation time.", nil, prometheus.Labels{"generation": "young", "role": "slave"}),
		"polardbx_cdc_dumper_s_oldCollectionTime":    newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_collector_time_total", subsystemDumper, "Total GC collector invocation time.", nil, prometheus.Labels{"generation": "old", "role": "slave"}),
		"polardbx_cdc_dumper_s_currentThreadCount":   newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_threads_live_count", subsystemDumper, "Current alive threads.", nil, prometheus.Labels{"role": "slave"}),

		"polardbx_cdc_task_youngUsed":            newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_generation_used", subsystemTask, "Current generation used.", nil, prometheus.Labels{"generation": "young"}),
		"polardbx_cdc_task_youngMax":             newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_generation_max_capacity", subsystemTask, "Current generation max capacity.", nil, prometheus.Labels{"generation": "young"}),
		"polardbx_cdc_task_oldUsed":              newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_generation_used", subsystemTask, "Current generation used.", nil, prometheus.Labels{"generation": "old"}),
		"polardbx_cdc_task_oldMax":               newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_generation_max_capacity", subsystemTask, "Current generation max capacity.", nil, prometheus.Labels{"generation": "old"}),
		"polardbx_cdc_task_heapUsage":            newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_heap_usage", subsystemTask, "Current heap usage.", nil, nil),
		"polardbx_cdc_task_youngCollectionCount": newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_collector_invocation_count", subsystemTask, "Total GC collector invocation count.", nil, prometheus.Labels{"generation": "young"}),
		"polardbx_cdc_task_oldCollectionCount":   newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_collector_invocation_count", subsystemTask, "Total GC collector invocation count.", nil, prometheus.Labels{"generation": "old"}),
		"polardbx_cdc_task_youngCollectionTime":  newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_collector_time_total", subsystemTask, "Total GC collector invocation time.", nil, prometheus.Labels{"generation": "young"}),
		"polardbx_cdc_task_oldCollectionTime":    newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_gc_collector_time_total", subsystemTask, "Total GC collector invocation time.", nil, prometheus.Labels{"generation": "old"}),
		"polardbx_cdc_task_currentThreadCount":   newPrometheusMetricsForCdc(prometheus.GaugeValue, "jvm_threads_live_count", subsystemTask, "Current alive threads.", nil, nil),
	}

	up = prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "up"), "Was the last scrape of PolarDB-X CDC successful.", nil, nil)
)
