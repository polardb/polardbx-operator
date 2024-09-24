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

package event

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/str"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

// Query_log_event/QUERY_EVENT
//
// Fixed data part:
//
// 4 bytes. The ID of the thread that issued this statement. Needed for temporary tables. This is also useful for a DBA for knowing who did what on the master.
// 4 bytes. The time in seconds that the statement took to execute. Only useful for inspection by the DBA.
// 1 byte. The length of the name of the database which was the default database when the statement was executed. This name appears later, in the variable data part. It is necessary for statements such as INSERT INTO t VALUES(1) that don't specify the database and rely on the default database previously selected by USE.
// 2 bytes. The error code resulting from execution of the statement on the master. Error codes are defined in include/mysqld_error.h. 0 means no error. How come statements with a nonzero error code can exist in the binary log? This is mainly due to the use of nontransactional tables within transactions. For example, if an INSERT ... SELECT fails after inserting 1000 rows into a MyISAM table (for example, with a duplicate-key violation), we have to write this statement to the binary log, because it truly modified the MyISAM table. For transactional tables, there should be no event with a nonzero error code (though it can happen, for example if the connection was interrupted (Control-C)). The slave checks the error code: After executing the statement itself, it compares the error code it got with the error code in the event, and if they are different it stops replicating (unless --slave-skip-errors was used to ignore the error).
// 2 bytes (not present in v1, v3). The length of the status variable block.
//
// Variable part:
//
// Zero or more status variables (not present in v1, v3). Each status variable consists of one byte code identifying the variable stored, followed by the value of the variable. The format of the value is variable-specific, as described later.
// The default database name (null-terminated).
// The SQL statement. The slave knows the size of the other fields in the variable part (the sizes are given in the fixed data part), so by subtraction it can know the size of the statement.

var StrictParseMode = true

const (
	MAX_DBS_IN_EVENT_MTS      uint8 = 16
	OVER_MAX_DBS_IN_EVENT_MTS       = 254
)

type StatusVariables struct {
	Flags2                 uint32  `json:"flags_2,omitempty"`
	SqlMode                uint64  `json:"sql_mode,omitempty"`
	Catalog                str.Str `json:"catalog,omitempty"`
	AutoIncrementIncrement uint16  `json:"auto_increment_increment,omitempty"`
	AutoIncrementOffset    uint16  `json:"auto_increment_offset,omitempty"`
	ClientCharset          uint16  `json:"client_charset,omitempty"`
	ClientCollation        uint16  `json:"client_collation,omitempty"`
	ServerCollation        uint16  `json:"server_collation,omitempty"`
	TimeZone               str.Str `json:"time_zone,omitempty"`
	User                   str.Str `json:"user,omitempty"`
	Host                   str.Str `json:"host,omitempty"`
	CommitGCN              uint64  `json:"commit_gcn,omitempty"`
	PrepareGCN             uint64  `json:"prepare_gcn,omitempty"`
}

func (d *StatusVariables) parseStatusVarsBlock(block []byte) error {
	off := 0
Loop:
	for off < len(block) {
		code := block[off]
		off++

		var err error
		var length int
		switch code {
		case spec.Q_FLAGS2_CODE:
			length = 4
			if len(block) < off+length {
				return layout.ErrNotEnoughBytes
			}
			utils.ReadNumberLittleEndianHack(&d.Flags2, block[off:])
		case spec.Q_SQL_MODE_CODE:
			length = 8
			if len(block) < off+length {
				return layout.ErrNotEnoughBytes
			}
			utils.ReadNumberLittleEndianHack(&d.SqlMode, block[off:])
		case spec.Q_CATALOG_NZ_CODE:
			var bytesLen uint8
			if len(block) < off+1 {
				return layout.ErrNotEnoughBytes
			}
			utils.ReadNumberLittleEndianHack(&bytesLen, block[off:])
			length = 1 + int(bytesLen)
			if len(block) < off+length {
				return layout.ErrNotEnoughBytes
			}
			d.Catalog = make([]byte, bytesLen)
			copy(d.Catalog, block[off+1:off+length])
		case spec.Q_AUTO_INCREMENT:
			length = 4
			if len(block) < off+length {
				return layout.ErrNotEnoughBytes
			}
			utils.ReadNumberLittleEndianHack(&d.AutoIncrementIncrement, block[off:])
			utils.ReadNumberLittleEndianHack(&d.AutoIncrementOffset, block[off+2:])
		case spec.Q_CHARSET_CODE:
			length = 6
			if len(block) < off+length {
				return layout.ErrNotEnoughBytes
			}
			utils.ReadNumberLittleEndianHack(&d.ClientCharset, block[off:])
			utils.ReadNumberLittleEndianHack(&d.ClientCollation, block[off+2:])
			utils.ReadNumberLittleEndianHack(&d.ServerCollation, block[off+4:])
		case spec.Q_TIME_ZONE_CODE:
			var bytesLen uint8
			if len(block) < off+1 {
				return layout.ErrNotEnoughBytes
			}
			utils.ReadNumberLittleEndianHack(&bytesLen, block[off:])
			length = 1 + int(bytesLen)
			if len(block) < off+length {
				return layout.ErrNotEnoughBytes
			}
			d.TimeZone = make([]byte, bytesLen)
			copy(d.TimeZone, block[off+1:off+length])
		case spec.Q_CATALOG_CODE:
			var bytesLen uint8
			if len(block) < off+1 {
				return layout.ErrNotEnoughBytes
			}
			utils.ReadNumberLittleEndianHack(&bytesLen, block[off:])
			length = 1 + int(bytesLen)
			if len(block) < off+length {
				return layout.ErrNotEnoughBytes
			}
			d.TimeZone = make([]byte, bytesLen-1)
			copy(d.TimeZone, block[off+1:off+length-1])
			if block[off+length-1] != 0 {
				return errors.New("not end with null")
			}
		case spec.Q_LC_TIME_NAMES_CODE:
			length = 2
		case spec.Q_CHARSET_DATABASE_CODE:
			length = 2
		case spec.Q_TABLE_MAP_FOR_UPDATE_CODE:
			length = 8
		case spec.Q_MASTER_DATA_WRITTEN_CODE:
			length = 4
		case spec.Q_INVOKER:
			var userLen, hostLen uint8
			length, err = layout.Decl(
				layout.Number(&userLen), layout.Bytes(&userLen, &d.User),
				layout.Number(&hostLen), layout.Bytes(&hostLen, &d.Host),
			).FromBlock(block[off:])
		case spec.Q_MICROSECONDS:
			length = 3
		case spec.Q_UPDATED_DB_NAMES:
			var mtsAccessedDbs uint8
			if len(block) < off+1 {
				return layout.ErrNotEnoughBytes
			}
			utils.ReadNumberLittleEndianHack(&mtsAccessedDbs, block[off:])
			length = 1
			mtsAccessedDbNames := make([][]byte, mtsAccessedDbs)
			for i := range mtsAccessedDbNames {
				bs := block[off+length:]
				strLen := bytes.IndexByte(bs, 0)
				if strLen < 0 {
					return errors.New("no null byte")
				}
				mtsAccessedDbNames[i] = make([]byte, strLen)
				copy(mtsAccessedDbNames[i], bs[:strLen])
				length += strLen + 1
			}
		case spec.Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP:
			length = 1
		case spec.Q_DDL_LOGGED_WITH_XID:
			length = 8
		case spec.Q_DEFAULT_COLLATION_FOR_UTF8MB4:
			length = 2
		case spec.Q_DEFAULT_TABLE_ENCRYPTION:
			length = 1
		case spec.Q_SQL_REQUIRE_PRIMARY_KEY:
			length = 1
		case spec.Q_HRNOW:
			length = 2
		case spec.Q_LIZARD_COMMIT_GCN:
			length = 8
			if len(block) < off+length {
				return layout.ErrNotEnoughBytes
			}
			utils.ReadNumberLittleEndianHack(&d.CommitGCN, block[off:])
		case spec.Q_LIZARD_PREPARE_GCN:
			length = 8
			if len(block) < off+length {
				return layout.ErrNotEnoughBytes
			}
			utils.ReadNumberLittleEndianHack(&d.PrepareGCN, block[off:])
		case spec.Q_OPT_FLASHBACK_AREA:
			length = 1
		case spec.Q_OPT_INDEX_FORMAT_GPP_ENABLED:
			length = 1
		default:
			// Unrecognized, skip.
			if StrictParseMode {
				panic(fmt.Sprintf("unrecognized status var code: %d", code))
			}
			break Loop
		}

		if err != nil {
			return nil
		}

		off += length
	}

	return nil
}

type QueryEvent struct {
	ThreadID   uint32          `json:"thread_id,omitempty"`
	ExecTime   uint32          `json:"exec_time"`
	ErrorCode  uint16          `json:"error_code"`
	Schema     str.Str         `json:"schema"`
	Query      str.Str         `json:"query"`
	StatusVars StatusVariables `json:"status_vars,omitempty"`
}

func (d *QueryEvent) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	var schemaLength uint8
	var statusVarsLength uint16
	return layout.Decl(
		// Post header
		layout.Number(&d.ThreadID),
		layout.Number(&d.ExecTime),
		layout.Number(&schemaLength),
		layout.Number(&d.ErrorCode),
		layout.If(version == spec.V4, layout.Number(&statusVarsLength)),

		// Payload
		layout.If(version == spec.V4, layout.Area(&statusVarsLength, func(data []byte) (int, error) {
			return int(statusVarsLength), d.StatusVars.parseStatusVarsBlock(data)
		})),
		layout.Bytes(&schemaLength, &d.Schema),
		layout.Null(),
		layout.Bytes(layout.Infinite(), &d.Query),
	)
}
