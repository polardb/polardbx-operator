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

package tx

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/system"
)

type EventHandler func(*Event) error
type ContentHandler func([]event.LogEvent, *Event) error

type TransactionEventParser interface {
	Parse(h EventHandler) error
}

type transactionEventParser struct {
	binlogScanner binlog.LogEventScanner
}

var ErrNotXAEvent = errors.New("not a xa event")

func firstPartBefore(b []byte, split byte) ([]byte, error) {
	commaIdx := bytes.IndexByte(b, split)
	if commaIdx < 0 {
		return nil, errors.New("comma not found")
	}
	return b[:commaIdx], nil
}

func unwrapHex(s []byte) []byte {
	return s[2 : len(s)-1]
}

func parseXIDFromQueryArgs(args []byte) (uint64, error) {
	// The first part.
	hexGtridAndBqual, err := firstPartBefore(args, ',')
	if err != nil {
		return 0, err
	}
	hexGtridAndBqual = bytes.TrimSpace(hexGtridAndBqual)
	hexGtridAndBqual = unwrapHex(hexGtridAndBqual)

	// Hex value.
	gtrid, err := hex.DecodeString(string(hexGtridAndBqual))
	if err != nil {
		return 0, err
	}

	return ParseXIDFromGtrid(gtrid)
}

func ParseXIDFromGtrid(gtrid []byte) (uint64, error) {
	gtrid, err := firstPartBefore(gtrid, '@')
	if err != nil {
		return 0, err
	}

	// Leading prefix.
	gtrid = gtrid[len(system.TransactionPrefix):]

	return strconv.ParseUint(string(gtrid), 16, 64)
}

func ParseXIDAndType(query []byte) (uint64, EventType, error) {
	if bytes.HasPrefix(query, []byte(XACommit)) {
		xid, err := parseXIDFromQueryArgs(query[len(XACommit):])
		if err != nil {
			return 0, 0, fmt.Errorf("unable to parse xid: %w", err)
		}
		return xid, Commit, nil
	} else if bytes.HasPrefix(query, []byte(XARollback)) {
		xid, err := parseXIDFromQueryArgs(query[len(XARollback):])
		if err != nil {
			return 0, 0, fmt.Errorf("unable to parse xid: %w", err)
		}
		return xid, Rollback, nil
	} else if bytes.HasPrefix(query, []byte(XAStart)) {
		xid, err := parseXIDFromQueryArgs(query[len(XAStart):])
		if err != nil {
			return 0, 0, fmt.Errorf("unable to parse xid: %w", err)
		}
		return xid, Begin, nil
	} else {
		return 0, 0, ErrNotXAEvent
	}
}

func TransactionEventParserInterestedEvents() []byte {
	return []byte{
		spec.TABLE_MAP_EVENT,
		spec.QUERY_EVENT,
		spec.DELETE_ROWS_EVENT_V1,
		spec.DELETE_ROWS_EVENT_V2,
		spec.UPDATE_ROWS_EVENT_V1,
		spec.UPDATE_ROWS_EVENT_V2,
		spec.WRITE_ROWS_EVENT_V1,
		spec.WRITE_ROWS_EVENT_V2,
		spec.XID_EVENT,
		spec.XA_PREPARE_LOG_EVENT,
		spec.GCN_LOG_EVENT,
		spec.SEQUENCE_EVENT,
	}
}

func TransactionEventParserHeaderFilter() binlog.LogEventHeaderFilter {
	events := TransactionEventParserInterestedEvents()
	return func(header event.LogEventHeader) bool {
		return bytes.IndexByte(events, header.EventTypeCode()) >= 0
	}
}

var StopParse = errors.New("stop parse")

func (s *transactionEventParser) processOne(h EventHandler) error {
	var beginQueryEvent event.LogEvent
	var beginQueryEventOffset binlog.EventOffset
	var commitTs uint64
	var xid uint64
	var txLogTableWriteRowsEvent event.LogEvent
	schemaAndTables := make(map[uint64][2][]byte)

	var heartbeatRowsEvents []any

	for {
		offset, logEvent, err := s.binlogScanner.Next()
		if err != nil {
			return err
		}

		switch logEvent.EventHeader().EventTypeCode() {
		case spec.GCN_LOG_EVENT:
			gcnEvent := logEvent.EventData().(*event.GCNEvent)
			commitTs = gcnEvent.GCN
		case spec.SEQUENCE_EVENT:
			sequenceEvent := logEvent.EventData().(*event.SequenceEvent)
			if sequenceEvent.SequenceType == spec.COMMIT_SEQUENCE {
				commitTs = sequenceEvent.SequenceNum
			}
		case spec.TABLE_MAP_EVENT:
			// Record table name.
			tableMapEvent := logEvent.EventData().(*event.TableMapEvent)
			schemaAndTables[tableMapEvent.TableID] = [2][]byte{
				tableMapEvent.Schema,
				tableMapEvent.Table,
			}

		case spec.WRITE_ROWS_EVENT_V1, spec.WRITE_ROWS_EVENT_V2:
			// If it's a WRITE_ROWS_EVENT, it can be part of the distributed transaction
			// due to the 2PC optimization. It represents the last prepare and the first commit.
			// Thus, we emit two events.
			rowsEvent := logEvent.EventData().(*event.WriteRowsEvent)
			schemaAndTable, ok := schemaAndTables[rowsEvent.TableID]

			if !ok {
				return fmt.Errorf("table not found, expect a TABLE_MAP_EVENT before")
			}

			// Find the system transaction table.
			if system.IsTransactionLogTable(schemaAndTable[1]) {
				// Must be the first 8 bytes after null bit map.
				bitmapLen := (rowsEvent.Columns.NumBitsSet() + 7) / 8
				if _, err := layout.Number(&xid).FromBlock(rowsEvent.Rows[bitmapLen:]); err != nil {
					return fmt.Errorf("unable to parse xid from system table: %w", err)
				}
				txLogTableWriteRowsEvent = logEvent
			}

			if system.IsCDCHeartbeatTable(schemaAndTable[0], schemaAndTable[1]) {
				heartbeatRowsEvents = append(heartbeatRowsEvents, rowsEvent)
			}

		case spec.UPDATE_ROWS_EVENT_V1, spec.UPDATE_ROWS_EVENT_V2:
			rowsEvent := logEvent.EventData().(*event.UpdateRowsEvent)
			schemaAndTable, ok := schemaAndTables[rowsEvent.TableID]

			if !ok {
				return fmt.Errorf("table not found, expect a TABLE_MAP_EVENT before")
			}

			if system.IsCDCHeartbeatTable(schemaAndTable[0], schemaAndTable[1]) {
				heartbeatRowsEvents = append(heartbeatRowsEvents, rowsEvent)
			}

		case spec.DELETE_ROWS_EVENT_V1, spec.DELETE_ROWS_EVENT_V2:
			rowsEvent := logEvent.EventData().(*event.DeleteRowsEvent)
			schemaAndTable, ok := schemaAndTables[rowsEvent.TableID]

			if !ok {
				return fmt.Errorf("table not found, expect a TABLE_MAP_EVENT before")
			}

			if system.IsCDCHeartbeatTable(schemaAndTable[0], schemaAndTable[1]) {
				heartbeatRowsEvents = append(heartbeatRowsEvents, rowsEvent)
			}

		case spec.XID_EVENT:
			beginEv, prepareEv, commitEv := &Event{
				Raw:    beginQueryEvent,
				File:   beginQueryEventOffset.File,
				EndPos: beginQueryEventOffset.Offset,
				Type:   Begin,
				XID:    xid,
			}, &Event{
				HeartbeatRowsLogEvents: heartbeatRowsEvents,
				Raw:                    txLogTableWriteRowsEvent,
				File:                   offset.File,
				EndPos:                 offset.Offset,
				Type:                   Prepare,
				XID:                    xid,
			}, &Event{
				Raw:    logEvent,
				Ts:     0, // FIXME (commit ts of the special 2PC event should be extracted from the write rows event)
				File:   offset.File,
				EndPos: offset.Offset,
				Type:   Commit,
				XID:    xid,
			}

			// Find xid before in WRITE_ROWS_EVENT.
			// Emit prepare and commit once.
			if xid != 0 {
				if err := h(beginEv); err != nil {
					return err
				}

				if err := h(prepareEv); err != nil {
					return err
				}

				if err := h(commitEv); err != nil {
					return err
				}
				return nil
			}
		case spec.QUERY_EVENT:
			queryEvent := logEvent.EventData().(*event.QueryEvent)
			if bytes.Equal(queryEvent.Query, []byte("BEGIN")) {
				beginQueryEvent = logEvent
				beginQueryEventOffset = offset
			} else {
				xid, t, err := ParseXIDAndType(queryEvent.Query)
				if err != nil {
					if err == ErrNotXAEvent {
						continue
					}
					return err
				}

				ev := &Event{
					Ts:     commitTs,
					Raw:    logEvent,
					File:   offset.File,
					EndPos: offset.Offset,
					Type:   t,
					XID:    xid,
				}

				// Emit event.
				if err := h(ev); err != nil {
					return err
				}
				return nil
			}
		case spec.XA_PREPARE_LOG_EVENT:
			xaPrepareEvent := logEvent.EventData().(*event.XAPrepareEvent)
			// Skip one-phase prepare.
			if xaPrepareEvent.OnePhase {
				continue
			}
			xid, err := ParseXIDFromGtrid(xaPrepareEvent.Gtrid)
			if err != nil {
				return fmt.Errorf("unable to parse xid: %w", err)
			}

			prepareEv := &Event{
				HeartbeatRowsLogEvents: heartbeatRowsEvents,
				Raw:                    logEvent,
				File:                   offset.File,
				EndPos:                 offset.Offset,
				Type:                   Prepare,
				XID:                    xid,
			}

			// Emit event.
			if err := h(prepareEv); err != nil {
				return err
			}
			return nil
		default:
		}
	}
}

func (s *transactionEventParser) tryClose() error {
	if closer, ok := s.binlogScanner.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (s *transactionEventParser) Parse(h EventHandler) error {
	// Always try close.
	defer s.tryClose()

	for {
		if err := s.processOne(h); err != nil {
			if err == io.EOF {
				return nil
			}
			if err == StopParse {
				return nil
			}
			return err
		}
	}
}

type TransactionEventParserOption func(s *transactionEventParser)

func NewTransactionEventParser(s binlog.LogEventScanner, opts ...TransactionEventParserOption) TransactionEventParser {
	parser := &transactionEventParser{
		binlogScanner: s,
	}

	for _, opt := range opts {
		opt(parser)
	}

	return parser
}
