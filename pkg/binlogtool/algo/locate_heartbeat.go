//go:build polardbx

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

package algo

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog"
	"time"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/rows"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/str"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/bitmap"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/tx"
)

type HeartbeatLocatePolicy uint8

const (
	UniqueWritePolicy HeartbeatLocatePolicy = iota + 1
	ExactCommitTS
	AroundCommitTS
	LargerCommitTS
)

type LocateHeartbeat struct {
	parser tx.TransactionEventParser

	policy   HeartbeatLocatePolicy
	sname    string
	commitTs uint64
}

type HeartbeatRow struct {
	Id        uint64    `json:"id,omitempty"`
	Sname     str.Str   `json:"sname,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
}

func ExtractHeartbeatFieldsFromRowsEvent(rowsEvent event.RowsEventVariants) (HeartbeatRow, error) {
	fakeTableMap := &event.TableMapEvent{
		ColumnCount: 3,
		Columns: []event.Column{
			{Type: spec.MYSQL_TYPE_LONGLONG},           // bigint(20) not null
			{Type: spec.MYSQL_TYPE_VARCHAR, Meta: 20},  // varchar(20) null
			{Type: spec.MYSQL_TYPE_DATETIME2, Meta: 3}, // datetime(3) null
		},
		NullBitmap: bitmap.NewBitmap([]byte{0x60}, 3), // 011
	}
	var firstRow rows.Row
	switch v := rowsEvent.(type) {
	case *event.WriteRowsEvent:
		if r, err := rows.DecodeWriteRowsEvent(v, fakeTableMap); err != nil {
			return HeartbeatRow{}, err
		} else {
			firstRow = r.DecodedRows[0]
		}
	case *event.DeleteRowsEvent:
		if r, err := rows.DecodeDeleteRowsEvent(v, fakeTableMap); err != nil {
			return HeartbeatRow{}, err
		} else {
			firstRow = r.DecodedRows[0]
		}
	case *event.UpdateRowsEvent:
		if r, err := rows.DecodeUpdateRowsEvent(v, fakeTableMap); err != nil {
			return HeartbeatRow{}, err
		} else {
			firstRow = r.DecodedRows[0]
		}
	default:
		return HeartbeatRow{}, errors.New("not supported")
	}
	return HeartbeatRow{
		Id:        firstRow[0].Value.(uint64),
		Sname:     firstRow[1].Value.(str.Str),
		Timestamp: firstRow[2].Value.(time.Time),
	}, nil
}

func normalDist(x, y uint64) uint64 {
	if x >= y {
		return x - y
	}
	return y - x
}

var ErrTargetHeartbeatTransactionNotFound = errors.New("target heartbeat transaction not found")

func (lh *LocateHeartbeat) Perform() (uint64, uint64, *binlog.EventOffset, *HeartbeatRow, error) {
	var heartbeatTxid, heartbeatCommitTs uint64
	var heartbeatEventOffset *binlog.EventOffset
	var heartbeatRow *HeartbeatRow

	var eventHandler tx.EventHandler
	heartbeatTxSet := make(map[uint64]int)
	heartbeatRowsMap := make(map[uint64]*HeartbeatRow)

	if lh.policy == UniqueWritePolicy {
		eventHandler = func(txEvent *tx.Event) error {
			if txEvent.Type == tx.Prepare && txEvent.HeartbeatRowsLogEvents != nil {
				for _, rowsEv := range txEvent.HeartbeatRowsLogEvents {
					if writeRowsEv, ok := rowsEv.(*event.WriteRowsEvent); ok {
						hrow, err := ExtractHeartbeatFieldsFromRowsEvent(writeRowsEv)
						if err != nil {
							return fmt.Errorf("unable to extract heartbeat rows event: %w", err)
						}

						if bytes.Equal([]byte(lh.sname), []byte(hrow.Sname)) {
							heartbeatTxid = txEvent.XID
							heartbeatRow = &hrow
							heartbeatEventOffset = &binlog.EventOffset{File: txEvent.File, Offset: txEvent.EndPos}
							return tx.StopParse
						}
					}
				}
			}
			return nil
		}
	} else if lh.policy == ExactCommitTS {
		eventHandler = func(txEvent *tx.Event) error {
			if txEvent.Type == tx.Commit && txEvent.Ts == lh.commitTs {
				heartbeatTxid = txEvent.XID
				heartbeatCommitTs = txEvent.Ts
				heartbeatEventOffset = &binlog.EventOffset{File: txEvent.File, Offset: txEvent.EndPos}
				return tx.StopParse
			}
			return nil
		}
	} else if lh.policy == AroundCommitTS || lh.policy == LargerCommitTS {
		eventHandler = func(txEvent *tx.Event) error {
			if txEvent.Type == tx.Prepare && len(txEvent.HeartbeatRowsLogEvents) > 0 {
				heartbeatTxSet[txEvent.XID] = 1
				rowsEv := txEvent.HeartbeatRowsLogEvents[0].(event.RowsEventVariants)
				hr, err := ExtractHeartbeatFieldsFromRowsEvent(rowsEv)
				if err != nil {
					return fmt.Errorf("unable to extract heartbeat rows event: %w", err)
				}
				heartbeatRowsMap[txEvent.XID] = &hr
			} else if txEvent.Type == tx.Commit {
				if _, ok := heartbeatTxSet[txEvent.XID]; ok {
					if lh.policy == LargerCommitTS && txEvent.Ts <= lh.commitTs {
						return nil
					}
					// the nearest ts
					if normalDist(heartbeatCommitTs, lh.commitTs) > normalDist(txEvent.Ts, lh.commitTs) {
						heartbeatCommitTs, heartbeatTxid = txEvent.Ts, txEvent.XID
						heartbeatEventOffset = &binlog.EventOffset{File: txEvent.File, Offset: txEvent.EndPos}
						heartbeatRow = heartbeatRowsMap[txEvent.XID]
					}
				}
			}

			return nil
		}
	}

	err := lh.parser.Parse(eventHandler)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	if heartbeatTxid == 0 {
		return 0, 0, nil, nil, ErrTargetHeartbeatTransactionNotFound
	}

	return heartbeatTxid, heartbeatCommitTs, heartbeatEventOffset, heartbeatRow, nil
}

type LocateHeartbeatOption func(*LocateHeartbeat)

func WithUniqueWritePolicy(uniqueSname string) LocateHeartbeatOption {
	return func(lh *LocateHeartbeat) {
		lh.policy = UniqueWritePolicy
		lh.sname = uniqueSname
	}
}

func WithCommitTSPolicy(commitTs uint64, policy HeartbeatLocatePolicy) LocateHeartbeatOption {
	if policy != LargerCommitTS &&
		policy != AroundCommitTS &&
		policy != ExactCommitTS {
		panic("invalid commit ts policy")
	}
	return func(lh *LocateHeartbeat) {
		lh.commitTs = commitTs
		lh.policy = policy
	}
}

func NewLocateHeartbeat(parser tx.TransactionEventParser, opts ...LocateHeartbeatOption) *LocateHeartbeat {
	lh := &LocateHeartbeat{parser: parser}
	for _, opt := range opts {
		opt(lh)
	}
	if lh.policy == 0 {
		panic("must provide the locate policy")
	}
	return lh
}
