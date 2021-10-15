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

package gms

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	ConfigKeyEnableLocalMode                     = "ENABLE_LOCAL_MODE"
	ConfigKeyEnableBackgroundStatisticCollection = "ENABLE_BACKGROUND_STATISTIC_COLLECTION"
	ConfigKeyMemoryLimitPerQuery                 = "PER_QUERY_MEMORY_LIMIT"
	ConfigKeyPhysicalConnSocketTimeout           = "SOCKET_TIMEOUT"
	ConfigKeyInformationSchemaAggregateStats     = "INFO_SCHEMA_QUERY_WITH_STAT"
	ConfigKeyPhysicalConnIdleTimeout             = "CONN_POOL_IDLE_TIMEOUT"
	ConfigKeyPhysicalConnBlockingTimeout         = "CONN_POOL_BLOCK_TIMEOUT"
	ConfigKeyPhysicalConnMinPoolSize             = "CONN_POOL_MIN_POOL_SIZE"
	ConfigKeyPhysicalConnMaxPoolSize             = "CONN_POOL_MAX_POOL_SIZE"
	ConfigKeyTimeZone                            = "LOGICAL_DB_TIME_ZONE"
	ConfigKeyEnableComplexDmlCrossDB             = "ENABLE_COMPLEX_DML_CROSS_DB"
	ConfigKeyMaxAllowedPacketSize                = "MAX_ALLOWED_PACKET"
	ConfigKeyForbidDeleteWholeTable              = "FORBID_EXECUTE_DML_ALL"
	ConfigKeyLogicalConnIdleTimeout              = "LOGIC_IDLE_TIMEOUT"
	ConfigKeyTransactionLogsPurgeStartTime       = "PURGE_TRANS_START_TIME"
	ConfigKeySlowSqlRtThreshold                  = "SLOW_SQL_TIME"
	ConfigKeyMergeUnionSize                      = "MERGE_UNIONS_SIZE"
	ConfigKeyEnableBinlogRowsQueryLogEvents      = "ENABLE_SQL_FLASHBACK_EXACT_MATCH"
	ConfigKeyParallelism                         = "PARALLELISM"
	ConfigKeyEnableTableRecycleBin               = "ENABLE_RECYCLEBIN"
)

func strOnOrOff(x *bool) string {
	if x != nil && *x {
		return "ON"
	} else {
		return "OFF"
	}
}

func btoa(x *bool) string {
	if x != nil && *x {
		return "true"
	} else {
		return "false"
	}
}

func itoa32(x *int32, defaultVal int32) string {
	if x == nil {
		return fmt.Sprintf("%d", defaultVal)
	}
	return fmt.Sprintf("%d", *x)
}

func itoa64(x *int64, defaultVal int64) string {
	if x == nil {
		return fmt.Sprintf("%d", defaultVal)
	}
	return fmt.Sprintf("%d", *x)
}

func strOrDefault(s, defaultVal string) string {
	if len(s) == 0 {
		return defaultVal
	}
	return s
}

func GenerateDynamicConfigMap1(config map[string]intstr.IntOrString) map[string]string {
	strConfig := make(map[string]string)
	for k, v := range config {
		strConfig[k] = v.String()
	}
	return GenerateDynamicConfigMap(strConfig)
}

func GenerateDynamicConfigMap(config map[string]string) map[string]string {
	configMap := map[string]string{
		ConfigKeyEnableLocalMode:                     "true",
		ConfigKeyEnableBackgroundStatisticCollection: "true",
		ConfigKeyMemoryLimitPerQuery:                 "-1",
		ConfigKeyPhysicalConnBlockingTimeout:         "5000",
		ConfigKeyPhysicalConnSocketTimeout:           "900000",
		ConfigKeyInformationSchemaAggregateStats:     "false",
		ConfigKeyPhysicalConnIdleTimeout:             "30",
		ConfigKeyPhysicalConnMinPoolSize:             "20",
		ConfigKeyPhysicalConnMaxPoolSize:             "60",
		ConfigKeyTimeZone:                            "system",
		ConfigKeyEnableComplexDmlCrossDB:             "true",
		ConfigKeyMaxAllowedPacketSize:                "16777216",
		ConfigKeyForbidDeleteWholeTable:              "true",
		ConfigKeyLogicalConnIdleTimeout:              "28800000",
		ConfigKeyTransactionLogsPurgeStartTime:       "00:00-01:00",
		ConfigKeySlowSqlRtThreshold:                  "1000",
		ConfigKeyMergeUnionSize:                      "10",
		ConfigKeyEnableBinlogRowsQueryLogEvents:      "OFF",
		ConfigKeyParallelism:                         "-1",
		ConfigKeyEnableTableRecycleBin:               "false",
	}

	// Append key-value in config.Raw
	if config != nil {
		for k, v := range config {
			configMap[k] = v
		}
	}

	return configMap
}
