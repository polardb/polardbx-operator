/*
Copyright 2022 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the fic language governing permissions and
limitations under the License.
*/

package spec

import "encoding/json"

// References:
// 1. https://dev.mysql.com/doc/internals/en/event-content-writing-conventions.html
// 2. https://dev.mysql.com/doc/internals/en/event-header-fields.html
// 3. https://dev.mysql.com/doc/internals/en/event-classes-and-types.html
// 4. https://dev.mysql.com/doc/internals/en/event-meanings.html
// 5. https://dev.mysql.com/doc/internals/en/event-data-for-fic-event-types.html
// 6. https://dev.mysql.com/doc/internals/en/event-flags.html

// BINLOG_MAGIC is the magic number at the header of each binlog file.
var BINLOG_MAGIC = [...]byte{0xfe, 0x62, 0x69, 0x6e}

// Binlog format versions.
const (
	V1 uint32 = 1
	V3 uint32 = 3
	V4 uint32 = 4
)

// Event types.
const (
	UNKNOWN_EVENT             uint8 = 0
	START_EVENT_V3                  = 1
	QUERY_EVENT                     = 2
	STOP_EVENT                      = 3
	ROTATE_EVENT                    = 4
	INTVAR_EVENT                    = 5
	LOAD_EVENT                      = 6
	SLAVE_EVENT                     = 7
	CREATE_FILE_EVENT               = 8
	APPEND_BLOCK_EVENT              = 9
	EXEC_LOAD_EVENT                 = 10
	DELETE_FILE_EVENT               = 11
	NEW_LOAD_EVENT                  = 12
	RAND_EVENT                      = 13
	USER_VAR_EVENT                  = 14
	FORMAT_DESCRIPTION_EVENT        = 15
	XID_EVENT                       = 16
	BEGIN_LOAD_QUERY_EVENT          = 17
	EXECUTE_LOAD_QUERY_EVENT        = 18
	TABLE_MAP_EVENT                 = 19
	PRE_GA_WRITE_ROWS_EVENT         = 20 // WRITE_ROWS_EVENT_V0
	PRE_GA_UPDATE_ROWS_EVENT        = 21 // UPDATE_ROWS_EVENT_V0
	PRE_GA_DELETE_ROWS_EVENT        = 22 // DELETE_ROWS_EVENT_V0
	WRITE_ROWS_EVENT_V1             = 23
	UPDATE_ROWS_EVENT_V1            = 24
	DELETE_ROWS_EVENT_V1            = 25
	INCIDENT_EVENT                  = 26
	HEARTBEAT_LOG_EVENT             = 27
	IGNORABLE_LOG_EVENT             = 28
	ROWS_QUERY_LOG_EVENT            = 29
	WRITE_ROWS_EVENT_V2             = 30
	UPDATE_ROWS_EVENT_V2            = 31
	DELETE_ROWS_EVENT_V2            = 32
	GTID_LOG_EVENT                  = 33
	ANONYMOUS_GTID_LOG_EVENT        = 34
	PREVIOUS_GTIDS_LOG_EVENT        = 35
	TRANSACTION_CONTEXT_EVENT       = 36
	VIEW_CHANGE_EVENT               = 37
	XA_PREPARE_LOG_EVENT            = 38
	PARTIAL_UPDATE_ROWS_EVENT       = 39
	TRANSACTION_PAYLOAD_EVENT       = 40
	HEARTBEAT_LOG_EVENT_V2          = 41
)

func EventTypeName(e byte) string {
	switch e {
	case UNKNOWN_EVENT:
		return "Unknown"
	case START_EVENT_V3:
		return "Start"
	case QUERY_EVENT:
		return "Query"
	case STOP_EVENT:
		return "Stop"
	case ROTATE_EVENT:
		return "Rotate"
	case INTVAR_EVENT:
		return "Intvar"
	case LOAD_EVENT:
		return "Load"
	case SLAVE_EVENT:
		return "Slave"
	case CREATE_FILE_EVENT:
		return "CreateFile"
	case APPEND_BLOCK_EVENT:
		return "AppendBlock"
	case EXEC_LOAD_EVENT:
		return "ExecLoad"
	case DELETE_FILE_EVENT:
		return "DeleteFile"
	case NEW_LOAD_EVENT:
		return "NewLoad"
	case RAND_EVENT:
		return "Rand"
	case USER_VAR_EVENT:
		return "UserVar"
	case FORMAT_DESCRIPTION_EVENT:
		return "FormatDescription"
	case XID_EVENT:
		return "XID"
	case BEGIN_LOAD_QUERY_EVENT:
		return "BeginLoadQuery"
	case EXECUTE_LOAD_QUERY_EVENT:
		return "ExecuteLoadQuery"
	case TABLE_MAP_EVENT:
		return "TableMap"
	case PRE_GA_UPDATE_ROWS_EVENT:
		return "UpdateRows_v0"
	case PRE_GA_WRITE_ROWS_EVENT:
		return "WriteRows_v0"
	case PRE_GA_DELETE_ROWS_EVENT:
		return "DeleteRows_v0"
	case WRITE_ROWS_EVENT_V1:
		return "WriteRows_v1"
	case UPDATE_ROWS_EVENT_V1:
		return "UpdateRows_v1"
	case DELETE_ROWS_EVENT_V1:
		return "DeleteRows_v1"
	case INCIDENT_EVENT:
		return "Incident"
	case HEARTBEAT_LOG_EVENT:
		return "Heartbeat"
	case IGNORABLE_LOG_EVENT:
		return "Ignorable"
	case ROWS_QUERY_LOG_EVENT:
		return "RowsQuery"
	case WRITE_ROWS_EVENT_V2:
		return "WriteRows_v2"
	case UPDATE_ROWS_EVENT_V2:
		return "UpdateRows_v2"
	case DELETE_ROWS_EVENT_V2:
		return "DeleteRows_v2"
	case GTID_LOG_EVENT:
		return "GTID"
	case ANONYMOUS_GTID_LOG_EVENT:
		return "AnonymousGTID"
	case PREVIOUS_GTIDS_LOG_EVENT:
		return "PreviousGTIDs"
	case TRANSACTION_CONTEXT_EVENT:
		return "TransactionContext"
	case VIEW_CHANGE_EVENT:
		return "ViewChange"
	case XA_PREPARE_LOG_EVENT:
		return "XAPrepare"
	case PARTIAL_UPDATE_ROWS_EVENT:
		return "PartialUpdateRows"
	case TRANSACTION_PAYLOAD_EVENT:
		return "TransactionPayload"
	case HEARTBEAT_LOG_EVENT_V2:
		return "Heartbeat_v2"
	case GCN_LOG_EVENT:
		return "GCN"
	case SEQUENCE_EVENT:
		return "Sequence"
	case ANNOTATE_ROWS_EVENT:
		return "AnnotateRows_MariaDB"
	case BINLOG_CHECKPOINT_EVENT:
		return "BinlogCheckpoint_MariaDB"
	case GTID_EVENT:
		return "GTID_MariaDB"
	case GTID_LIST_EVENT:
		return "GTIDList_MariaDB"
	case START_ENCRYPTION_EVENT:
		return "StartEncryption_MariaDB"
	case CONSENSUS_LOG_EVENT:
		return "Consensus"
	case PREVIOUS_CONSENSUS_INDEX_LOG_EVENT:
		return "PreviousConsensusIndex"
	case CONSENSUS_CLUSTER_INFO_EVENT:
		return "ConsensusClusterInfo"
	case CONSENSUS_EMPTY_EVENT:
		return "ConsensusEmpty"
	case PREVIOUS_PREPARED_XIDS_EVENT:
		return "PreviousPreparedXIDs"
	case GROUP_UPDATE_ROWS_EVENT:
		return "GroupUpdateRows"
	default:
		return "Unrecognized"
	}
}

// Event flags.
const (
	LOG_EVENT_BINLOG_IN_USE_F            uint16 = 0x1
	LOG_EVENT_THREAD_SPECIFIC_F                 = 0x4
	LOG_EVENT_SUPPRESS_USE_F                    = 0x8
	LOG_EVENT_UPDATE_TABLE_MAP_VERSION_F        = 0x10
	LOG_EVENT_ARTIFICIAL_F                      = 0x20
	LOG_EVENT_RELAY_LOG_F                       = 0x40
	LOG_EVENT_IGNORABLE_F                       = 0x80
	LOG_EVENT_NO_FILTER_F                       = 0x100
	LOG_EVENT_MTS_ISOLATE_F                     = 0x200
)

// Status var codes.
const (
	Q_FLAGS2_CODE                     uint8 = 0
	Q_SQL_MODE_CODE                         = 1
	Q_CATALOG_CODE                          = 2
	Q_AUTO_INCREMENT                        = 3
	Q_CHARSET_CODE                          = 4
	Q_TIME_ZONE_CODE                        = 5
	Q_CATALOG_NZ_CODE                       = 6
	Q_LC_TIME_NAMES_CODE                    = 7
	Q_CHARSET_DATABASE_CODE                 = 8
	Q_TABLE_MAP_FOR_UPDATE_CODE             = 9
	Q_MASTER_DATA_WRITTEN_CODE              = 10
	Q_INVOKER                               = 11
	Q_UPDATED_DB_NAMES                      = 12
	Q_MICROSECONDS                          = 13
	Q_COMMIT_TS                             = 14
	Q_COMMIT_TS2                            = 15
	Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP       = 16
	Q_DDL_LOGGED_WITH_XID                   = 17
	Q_DEFAULT_COLLATION_FOR_UTF8MB4         = 18
	Q_SQL_REQUIRE_PRIMARY_KEY               = 19
	Q_DEFAULT_TABLE_ENCRYPTION              = 20
)

// Rows event flags.
const (
	STMT_END_F              uint16 = 0x1
	NO_FOREIGN_KEY_CHECKS_F        = 0x2
	RELAXED_UNIQUE_CHECKS_F        = 0x4
	COMPLETE_ROWS_F                = 0x8
)

// Load file keyword flags.
const (
	DUMPFILE_FLAG     uint8 = 0x1
	OPT_ENCLOSED_FLAG       = 0x2
	REPLACE_FLAG            = 0x4
	IGNORE_FLAG             = 0x8
)

// Load file field or line option flags.
const (
	FIELD_TERM_EMPTY = 0x1
	ENCLOSED_EMPTY   = 0x2
	LINE_TERM_EMPTY  = 0x4
	LINE_START_EMPTY = 0x8
	ESCAPED_EMPTY    = 0x10
)

// Field types.
const (
	MYSQL_TYPE_DECIMAL     uint8 = 0
	MYSQL_TYPE_TINY              = 1
	MYSQL_TYPE_SHORT             = 2
	MYSQL_TYPE_LONG              = 3
	MYSQL_TYPE_FLOAT             = 4
	MYSQL_TYPE_DOUBLE            = 5
	MYSQL_TYPE_NULL              = 6
	MYSQL_TYPE_TIMESTAMP         = 7
	MYSQL_TYPE_LONGLONG          = 8
	MYSQL_TYPE_INT24             = 9
	MYSQL_TYPE_DATE              = 10
	MYSQL_TYPE_TIME              = 11
	MYSQL_TYPE_DATETIME          = 12
	MYSQL_TYPE_YEAR              = 13
	MYSQL_TYPE_NEWDATE           = 14
	MYSQL_TYPE_VARCHAR           = 15
	MYSQL_TYPE_BIT               = 16
	MYSQL_TYPE_TIMESTAMP2        = 17
	MYSQL_TYPE_DATETIME2         = 18
	MYSQL_TYPE_TIME2             = 19
	MYSQL_TYPE_TYPED_ARRAY       = 20 // Used for replication only
	MYSQL_TYPE_INVALID           = 243
	MYSQL_TYPE_BOOL              = 244 // Currently just a placeholder
	MYSQL_TYPE_JSON              = 245
	MYSQL_TYPE_NEWDECIMAL        = 246
	MYSQL_TYPE_ENUM              = 247
	MYSQL_TYPE_SET               = 248
	MYSQL_TYPE_TINY_BLOB         = 249
	MYSQL_TYPE_MEDIUM_BLOB       = 250
	MYSQL_TYPE_LONG_BLOB         = 251
	MYSQL_TYPE_BLOB              = 252
	MYSQL_TYPE_VAR_STRING        = 253
	MYSQL_TYPE_STRING            = 254
	MYSQL_TYPE_GEOMETRY          = 255
)

func IsIntegerField(t uint8) bool {
	switch t {
	case MYSQL_TYPE_TINY,
		MYSQL_TYPE_SHORT,
		MYSQL_TYPE_INT24,
		MYSQL_TYPE_LONG,
		MYSQL_TYPE_LONGLONG,
		MYSQL_TYPE_YEAR:
		return true
	}
	return false
}

func IsNumericField(t uint8) bool {
	switch t {
	case MYSQL_TYPE_TINY,
		MYSQL_TYPE_SHORT,
		MYSQL_TYPE_INT24,
		MYSQL_TYPE_LONG,
		MYSQL_TYPE_LONGLONG,
		MYSQL_TYPE_YEAR,
		MYSQL_TYPE_FLOAT,
		MYSQL_TYPE_DOUBLE,
		MYSQL_TYPE_DECIMAL,
		MYSQL_TYPE_NEWDECIMAL:
		return true
	}
	return false
}

func FieldTypeName(t uint8) string {
	switch t {
	case MYSQL_TYPE_DECIMAL:
		return "DECIMAL"
	case MYSQL_TYPE_TINY:
		return "TINYINT"
	case MYSQL_TYPE_SHORT:
		return "SHORTINT"
	case MYSQL_TYPE_LONG:
		return "INT"
	case MYSQL_TYPE_FLOAT:
		return "FLOAT"
	case MYSQL_TYPE_DOUBLE:
		return "DOUBLE"
	case MYSQL_TYPE_NULL:
		return "NULL"
	case MYSQL_TYPE_TIMESTAMP:
		return "TIMESTAMP"
	case MYSQL_TYPE_LONGLONG:
		return "BIGINT"
	case MYSQL_TYPE_INT24:
		return "MEDIUMINT"
	case MYSQL_TYPE_DATE:
		return "DATE"
	case MYSQL_TYPE_TIME:
		return "TIME"
	case MYSQL_TYPE_DATETIME:
		return "DATETIME"
	case MYSQL_TYPE_YEAR:
		return "YEAR"
	case MYSQL_TYPE_NEWDATE:
		return "NEWDATE"
	case MYSQL_TYPE_VARCHAR:
		return "VARCHAR"
	case MYSQL_TYPE_BIT:
		return "BIT"
	case MYSQL_TYPE_TIMESTAMP2:
		return "TIMESTAMP2"
	case MYSQL_TYPE_DATETIME2:
		return "DATETIME2"
	case MYSQL_TYPE_TIME2:
		return "TIME2"
	case MYSQL_TYPE_TYPED_ARRAY:
		return "TYPED_ARRAY"
	case MYSQL_TYPE_INVALID:
		return "INVALID"
	case MYSQL_TYPE_BOOL:
		return "BOOL"
	case MYSQL_TYPE_JSON:
		return "JSON"
	case MYSQL_TYPE_NEWDECIMAL:
		return "NEWDECIMAL"
	case MYSQL_TYPE_ENUM:
		return "ENUM"
	case MYSQL_TYPE_SET:
		return "SET"
	case MYSQL_TYPE_TINY_BLOB:
		return "TINYBLOB"
	case MYSQL_TYPE_MEDIUM_BLOB:
		return "MEDIUMBLOB"
	case MYSQL_TYPE_LONG_BLOB:
		return "LONGBLOB"
	case MYSQL_TYPE_BLOB:
		return "BLOB"
	case MYSQL_TYPE_VAR_STRING:
		return "VARSTRING"
	case MYSQL_TYPE_STRING:
		return "STRING"
	case MYSQL_TYPE_GEOMETRY:
		return "GEOMETRY"
	default:
		return "UNKNOWN"
	}
}

// FieldMetaLength is the field type meta length array (aligned to 16).
var FieldMetaLength = [256]byte{
	2, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, // 0 - 15
	2, 1, 1, 1, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 16 - 31
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 32 - 47
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 48 - 63
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 64 - 79
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 80 - 95
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 96 - 111
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 112 - 127
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 128 - 143
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 144 - 159
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 160 - 175
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 176 - 191
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 192 - 207
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 208 - 223
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 224 - 239
	0, 0, 0, 0, 1, 1, 2, 2, 2, 1, 1, 1, 1, 0, 2, 1, // 240 - 255
}

// Optional metadata field type.
const (
	TABLE_MAP_OPT_META_SIGNEDNESS uint8 = iota + 1
	TABLE_MAP_OPT_META_DEFAULT_CHARSET
	TABLE_MAP_OPT_META_COLUMN_CHARSET
	TABLE_MAP_OPT_META_COLUMN_NAME
	TABLE_MAP_OPT_META_SET_STR_VALUE
	TABLE_MAP_OPT_META_ENUM_STR_VALUE
	TABLE_MAP_OPT_META_GEOMETRY_TYPE
	TABLE_MAP_OPT_META_SIMPLE_PRIMARY_KEY
	TABLE_MAP_OPT_META_PRIMARY_KEY_WITH_PREFIX
	TABLE_MAP_OPT_META_ENUM_AND_SET_DEFAULT_CHARSET
	TABLE_MAP_OPT_META_ENUM_AND_SET_COLUMN_CHARSET
)

// User var type.
const (
	STRING_RESULT  uint8 = 0
	REAL_RESULT          = 1
	INT_RESULT           = 2
	ROW_RESULT           = 3
	DECIMAL_RESULT       = 4
)

// Handle duplicates.
const (
	LOAD_DUP_ERROR   uint8 = 0
	LOAD_DUP_IGNORE        = 1
	LOAD_DUP_REPLACE       = 2
)

// Rows event extra data type.
const (
	RW_V_EXTRAINFO_TAG uint8 = 0x00
)

// Rows event extra info formats.
const (
	NDB   uint8 = 0x00
	OPEN1       = 0x40
	OPEN2       = 0x41
	MULTI       = 0xff
)

// TS type of GTID event.
const (
	LOGICAL_TIMESTAMP_TYPECODE uint8 = 2
)

// Checksum related.
const (
	CHECKSUM_VERSION_PRODUCT          = 5<<16 + 6<<8 + 1 // 5.6.1
	CHECKSUM_CRC32_SIGNATURE_LEN      = 4
	BINLOG_CHECKSUM_LEN               = CHECKSUM_CRC32_SIGNATURE_LEN
	BINLOG_CHECKSUM_ALG_DESC_LEN      = 1
	BINLOG_CHECKSUM_ALG_OFF      byte = 0
	BINLOG_CHECKSUM_ALG_CRC32    byte = 1
	BINLOG_CHECKSUM_ALG_UNDEF    byte = 255
)

type BinlogChecksumAlgorithm byte

const (
	BinlogChecksumAlgorithmOff       = BinlogChecksumAlgorithm(BINLOG_CHECKSUM_ALG_OFF)
	BinlogChecksumAlgorithmCrc32     = BinlogChecksumAlgorithm(BINLOG_CHECKSUM_ALG_CRC32)
	BinlogChecksumAlgorithmUndefined = BinlogChecksumAlgorithm(BINLOG_CHECKSUM_ALG_UNDEF)
)

func (alg BinlogChecksumAlgorithm) String() string {
	switch alg {
	case BinlogChecksumAlgorithmOff:
		return "none"
	case BinlogChecksumAlgorithmCrc32:
		return "crc32"
	case BinlogChecksumAlgorithmUndefined:
		return "undefined"
	default:
		panic("never reach here")
	}
}

func (alg BinlogChecksumAlgorithm) MarshalJSON() ([]byte, error) {
	return json.Marshal(alg.String())
}
