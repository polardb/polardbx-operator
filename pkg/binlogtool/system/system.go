/*
Copyright 2022 Alibaba Group Holding Limited.

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

package system

import "bytes"

// Transaction log table.
//
// CREATE TABLE `__drds_global_tx_log` (
//  `TXID` bigint(20) NOT NULL,
//  `START_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
//  `TYPE` enum('TCC','XA','BED','TSO','HLC') NOT NULL,
//  `STATE` enum('PREPARE','COMMIT','ROLLBACK','SUCCEED','ABORTED') NOT NULL,
//  `RETRIES` int(11) NOT NULL DEFAULT '0',
//  `COMMIT_TS` bigint(20) DEFAULT NULL,
//  `PARTICIPANTS` blob,
//  `TIMEOUT` timestamp NULL DEFAULT NULL,
//  `SERVER_ADDR` varchar(21) NOT NULL,
//  `CONTEXT` text NOT NULL,
//  `ERROR` text,
//  PRIMARY KEY (`TXID`)
// ) ENGINE=InnoDB DEFAULT CHARSET=utf8

// CDC heartbeat table.
//
// CREATE TABLE IF NOT EXISTS `__cdc_heartbeat__` (
//   `id` bigint(20) not null auto_increment primary key,
//   sname varchar(20) default null,
//   gmt_modified datetime(3) default null,
// ) broadcast;

const (
	CDCLogicalDB = "__cdc__"
)

const (
	GlobalTransactionLogTable = "__drds_global_tx_log"
	CDCHeartbeatTable         = "__cdc_heartbeat__"
)

const (
	SingleDBSuffix = "__single"
)

func IsTransactionLogTable(table []byte) bool {
	return bytes.Equal([]byte(GlobalTransactionLogTable), table)
}

func IsCDCHeartbeatTable(schema []byte, table []byte) bool {
	return bytes.HasPrefix(schema, []byte(CDCLogicalDB)) &&
		!bytes.HasSuffix(schema, []byte(SingleDBSuffix)) &&
		bytes.HasPrefix(table, []byte(CDCHeartbeatTable))
}

const (
	TXIDColumnIndex = 0
)

const (
	TransactionPrefix = "drds-"
)
