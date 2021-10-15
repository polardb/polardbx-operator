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

package polardbx

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/alibaba/polardbx-operator/pkg/exporter/metric"
)

const (
	// For prometheus statsMetrics
	namespace = "polardbx"

	polardbxRoot = "polardbx_root"
)

var newMetric = func(name, subsystem, doc string, t prometheus.ValueType, variableLabels []string, constLabels prometheus.Labels) metric.Metric {
	return metric.NewMetric(name, namespace, subsystem, doc, t, variableLabels, constLabels)
}

// Keep the metrics stable. Otherwise, please refer to haproxy-exporter to
// specify different metrics along with different versions.
var (
	commonVariableLabels = []string{"schema"}
	statsSubsystem       = "stats"

	// Columns from "show @@stats" from manager port
	statsMetrics = map[string]metric.Metric{
		"net_in":                   newMetric("network_in_bytes_total", statsSubsystem, "Total network input bytes.", prometheus.GaugeValue, commonVariableLabels, nil),
		"net_out":                  newMetric("network_out_bytes_total", statsSubsystem, "Total network output bytes.", prometheus.GaugeValue, commonVariableLabels, nil),
		"active_connection":        newMetric("active_connections", statsSubsystem, "Current active connections.", prometheus.GaugeValue, commonVariableLabels, nil),
		"connection_count":         newMetric("connection_count", statsSubsystem, "Current connection count.", prometheus.GaugeValue, commonVariableLabels, nil),
		"time_cost":                newMetric("request_time_cost_total", statsSubsystem, "Total response time since the service starts, unit is microsecond.", prometheus.GaugeValue, commonVariableLabels, nil),
		"request_count":            newMetric("request_count_total", statsSubsystem, "Total request count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"query_count":              newMetric("query_count_total", statsSubsystem, "Total query request count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"insert_count":             newMetric("insert_count_total", statsSubsystem, "Total insert request count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"delete_count":             newMetric("delete_count_total", statsSubsystem, "Total delete request count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"update_count":             newMetric("update_count_total", statsSubsystem, "Total update request count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"replace_count":            newMetric("replace_count_total", statsSubsystem, "Total replace request count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"running_count":            newMetric("running_count", statsSubsystem, "Current running request count.", prometheus.GaugeValue, commonVariableLabels, nil),
		"physical_request_count":   newMetric("physical_request_count_total", statsSubsystem, "Total physical request count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"physical_time_cost":       newMetric("physical_request_time_cost_total", statsSubsystem, "Total physical response time since the service starts, unit is microsecond.", prometheus.GaugeValue, commonVariableLabels, nil),
		"error_count":              newMetric("error_count_total", statsSubsystem, "Total error count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"violation_error_count":    newMetric("violation_error_count_total", statsSubsystem, "Total violation error count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"multi_db_count":           newMetric("multi_database_request_count_total", statsSubsystem, "Total multi-database request count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"temp_table_count":         newMetric("temp_table_count", statsSubsystem, "Current temporary table count.", prometheus.GaugeValue, commonVariableLabels, nil),
		"join_multi_db_count":      newMetric("multi_database_join_request_count_total", statsSubsystem, "Total multi-database join request count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"aggregate_multi_db_count": newMetric("multi_database_aggregation_request_count_total", statsSubsystem, "Total multi-database aggregation request count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"hint_count":               newMetric("hint_count_total", statsSubsystem, "Total requests with hint count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"slow_request":             newMetric("slow_request_count_total", statsSubsystem, "Total slow request count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"physical_slow_request":    newMetric("physical_slow_request_count_total", statsSubsystem, "Total physical slow request count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"trans_count_xa":           newMetric("xa_transaction_count_total", statsSubsystem, "Total XA transactions count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"trans_count_best_effort":  newMetric("best_effort_transaction_count_total", statsSubsystem, "Total best-effort transactions count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"trans_count_tso":          newMetric("tso_transaction_count_total", statsSubsystem, "Total TSO transactions count since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"ccl_kill":                 newMetric("ccl_kill_count_total", statsSubsystem, "Total killed queries by CCL system since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"ccl_run":                  newMetric("ccl_run_count_total", statsSubsystem, "Total passed queries by CCL system since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"ccl_wait":                 newMetric("ccl_wait_count_total", statsSubsystem, "Total waited queries detected by CCL system since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
		"ccl_wait_kill":            newMetric("ccl_wait_kill_count_total", statsSubsystem, "Total waited and killed queries detected by CCL system since the service starts.", prometheus.GaugeValue, commonVariableLabels, nil),
	}

	commonStcVariableLabels = append(commonVariableLabels, "mysql_addr", "appname", "group_name", "atom_name")
	stcSubsystem            = "stc"

	// Columns from "show @@stc" from manager port
	stcMetrics = map[string]metric.Metric{
		"readcount":               newMetric("read_count_total", stcSubsystem, "Total read count on the atom since the service starts.", prometheus.GaugeValue, commonStcVariableLabels, nil),
		"writecount":              newMetric("write_count_total", stcSubsystem, "Total write count on the atom since the service starts.", prometheus.GaugeValue, commonStcVariableLabels, nil),
		"totalcount":              newMetric("request_count_total", stcSubsystem, "Total request count on the atom since the service starts.", prometheus.GaugeValue, commonStcVariableLabels, nil),
		"readtimecost":            newMetric("read_time_cost_total", stcSubsystem, "Total time cost on processing the read requests on the atom since the service starts, unit is millisecond.", prometheus.GaugeValue, commonStcVariableLabels, nil),
		"writetimecost":           newMetric("write_time_cost_total", stcSubsystem, "Total time cost on processing the write requests on the atom since the service starts, unit is millisecond.", prometheus.GaugeValue, commonStcVariableLabels, nil),
		"totaltimecost":           newMetric("request_time_cost_total", stcSubsystem, "Total time cost on processing all requests on the atom since the service starts, unit is milliseconds.", prometheus.GaugeValue, commonStcVariableLabels, nil),
		"connerrcount":            newMetric("connection_error_count_total", stcSubsystem, "Total connection error count on the atom since the service starts.", prometheus.GaugeValue, commonStcVariableLabels, nil),
		"sqlerrcount":             newMetric("sql_error_count_total", stcSubsystem, "Total sql error count on the atom since the service starts.", prometheus.GaugeValue, commonStcVariableLabels, nil),
		"sqllength":               newMetric("sql_length_total", stcSubsystem, "Sum of lengths of SQL processed on the atom since the service starts.", prometheus.GaugeValue, commonStcVariableLabels, nil),
		"rows":                    newMetric("returned_rows_total", stcSubsystem, "Number of total returned rows on the atom since the service starts.", prometheus.GaugeValue, commonStcVariableLabels, nil),
		"phy_active_connections":  newMetric("active_physical_connection_count", stcSubsystem, "Current active physical connection count on the atom.", prometheus.GaugeValue, commonStcVariableLabels, nil),
		"phy_pooling_connections": newMetric("pooling_physical_connection_count", stcSubsystem, "Current pooling physical connection count on the atom.", prometheus.GaugeValue, commonStcVariableLabels, nil),
	}

	htcSubsystem = "htc"

	// Columns from "show @@htc" from manager port
	htcMetrics = map[string]metric.Metric{
		"current_time":              newMetric("current_system_time", htcSubsystem, "Current system time.", prometheus.GaugeValue, nil, nil),
		"cpu":                       newMetric("cpu_percentage", htcSubsystem, "Current system CPU percentage.", prometheus.GaugeValue, nil, nil),
		"load":                      newMetric("load", htcSubsystem, "Current system load.", prometheus.GaugeValue, nil, nil),
		"freemem":                   newMetric("free_memory_bytes", htcSubsystem, "Current system free memory in bytes.", prometheus.GaugeValue, nil, nil),
		"netin":                     newMetric("network_in_bytes_total", htcSubsystem, "Total system network input bytes.", prometheus.GaugeValue, nil, nil),
		"netout":                    newMetric("network_out_bytes_total", htcSubsystem, "Total system network output bytes.", prometheus.GaugeValue, nil, nil),
		"netio":                     newMetric("network_bytes_total", htcSubsystem, "Total network transferred bytes.", prometheus.GaugeValue, nil, nil),
		"fullgccount":               newMetric("full_gc_count_total", htcSubsystem, "Total full GC count.", prometheus.GaugeValue, nil, nil),
		"fullgctime":                newMetric("full_gc_time_total", htcSubsystem, "Total full GC time.", prometheus.GaugeValue, nil, nil),
		"memory_pool_usage":         newMetric("memory_pool_usage", htcSubsystem, "Current usage of memory pool.", prometheus.GaugeValue, nil, nil),
		"ddl_job_count":             newMetric("ddl_job_count", htcSubsystem, "Number of DDL jobs.", prometheus.GaugeValue, nil, nil),
		"query_count":               newMetric("query_count_total", htcSubsystem, "Total query count.", prometheus.GaugeValue, nil, nil),
		"high_runner_process_count": newMetric("high_runner_process_count", htcSubsystem, "High runner process count.", prometheus.GaugeValue, nil, nil),
		"low_runner_process_count":  newMetric("low_runner_process_count", htcSubsystem, "Low runner process count.", prometheus.GaugeValue, nil, nil),
		"high_pending_split_size":   newMetric("high_pending_split_size", htcSubsystem, "High pending split size.", prometheus.GaugeValue, nil, nil),
		"low_pending_split_size":    newMetric("low_pending_split_size", htcSubsystem, "Low pending split size.", prometheus.GaugeValue, nil, nil),
		"blocked_split_size":        newMetric("blocked_split_size", htcSubsystem, "Blocked split size.", prometheus.GaugeValue, nil, nil),
	}

	polardbxUp = prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "up"), "Was the last scrape of PolarDB-X successful.", nil, nil)
)
