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
	"encoding/binary"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog"
	"sync"
	"sync/atomic"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/tx"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

type SeekConsistentPoint struct {
	txEventParsers map[string]tx.TransactionEventParser
	heartbeatTxid  uint64
}

// Stage 1. Locate the heartbeat transaction and emit the recoverable transactions' IDs.
func (sct *SeekConsistentPoint) stageOne(streamIndexes map[string]int) (
	recoverableTxs map[uint64]int,
	heartbeatPrepareAndCommitOffsets [][2]*binlog.EventOffset,
	txsBetweenHeartbeat [][]tx.Event,
	err error,
) {
	var mu sync.Mutex
	recoverableTxs = make(map[uint64]int)

	heartbeatPrepareAndCommitOffsets = make([][2]*binlog.EventOffset, len(sct.txEventParsers))
	txsBetweenHeartbeat = make([][]tx.Event, len(sct.txEventParsers))
	errOccurred := int32(0)
	scanErrs := make([]error, len(sct.txEventParsers))
	var wg sync.WaitGroup
	for streamName := range sct.txEventParsers {
		txParser := sct.txEventParsers[streamName]
		streamIndex := streamIndexes[streamName]
		pcOffset := &heartbeatPrepareAndCommitOffsets[streamIndex]
		wg.Add(1)
		go func() {
			defer wg.Done()

			curRecoverableTxs := make(map[uint64]int)
			eventsBetween := make([]tx.Event, 0)

			if err := txParser.Parse(func(event *tx.Event) error {
				if errOccurred > 0 {
					return tx.StopParse
				}

				// If is heartbeat transaction. We use P_h and C_h in the following comments
				// to denote the prepared and commit event.
				if event.XID == sct.heartbeatTxid {
					if event.Type == tx.Prepare {
						// Move the commits in event recorded to recoverable txs if P_h moves ahead.
						if pcOffset[0] != nil {
							for _, event := range eventsBetween {
								curRecoverableTxs[event.XID] = 1
							}
							eventsBetween = make([]tx.Event, 0)
						}
						pcOffset[0] = &binlog.EventOffset{File: event.File, Offset: event.EndPos}
					}
					if event.Type == tx.Commit {
						pcOffset[1] = &binlog.EventOffset{File: event.File, Offset: event.EndPos}
						return tx.StopParse
					}
					return nil
				} else {
					// Before the first P_h.
					if pcOffset[0] == nil {
						if event.Type == tx.Commit {
							curRecoverableTxs[event.XID] = 1
						}
					} else {
						eventsBetween = append(eventsBetween, *event)
					}
				}

				return nil
			}); err != nil {
				atomic.AddInt32(&errOccurred, 1)
				scanErrs[streamIndex] = err
				return
			}

			// Write to global set.
			mu.Lock()
			defer mu.Unlock()

			for xid := range curRecoverableTxs {
				recoverableTxs[xid] = 1
			}

			txsBetweenHeartbeat[streamIndex] = eventsBetween
		}()
	}

	// Memory barrier here, so don't worry about non-synced arrays.
	wg.Wait()

	if errOccurred > 0 {
		multiErr := utils.MultiError()
		for streamName := range sct.txEventParsers {
			streamIndex := streamIndexes[streamName]
			if err := scanErrs[streamIndex]; err != nil {
				multiErr.Add(fmt.Errorf("error occurs when parsing stream %s: %w", streamName, err))
			}
		}
		err = multiErr
		return
	}

	// Check if P_h and C_h are found in all streams.
	notFoundErrs := utils.MultiError()
	for streamName := range sct.txEventParsers {
		streamIndex := streamIndexes[streamName]
		pcOffset := &heartbeatPrepareAndCommitOffsets[streamIndex]
		if pcOffset[0] == nil {
			notFoundErrs.Add(fmt.Errorf("heartbeat prepare not found in stream %s", streamName))
		} else if pcOffset[1] == nil {
			notFoundErrs.Add(fmt.Errorf("heartbeat commit not found in stream %s", streamName))
		}
	}
	if notFoundErrs.Size() > 0 {
		err = notFoundErrs
		return
	}

	return
}

// Last prepare of recoverable transactions in [l, r].
func scanEventBorder(l, r int, recoverableTxs map[uint64]int, txEvents []tx.Event) int {
	for i := r; i >= l; i-- {
		ev := &txEvents[i]
		if ev.Type != tx.Prepare {
			continue
		}
		if _, ok := recoverableTxs[ev.XID]; ok {
			return i
		}
	}
	return l - 1
}

// Stage 2. Scan the binary logs events between heartbeat prepare and commit
// with an iteration and calculate the total recoverable transactions.
func (sct *SeekConsistentPoint) stageTwo(
	streamIndexes map[string]int,
	recoverableTxs map[uint64]int,
	heartbeatPrepareAndCommitOffsets [][2]*binlog.EventOffset,
	txsBetweenHeartbeat [][]tx.Event) ([]uint64, map[string]binlog.EventOffset, error) {
	eventBorders := make([]int, len(streamIndexes))

	// Scan the borders (reverse scan), i.e index of the last recoverable prepare.
	for _, streamIndex := range streamIndexes {
		txEvents := txsBetweenHeartbeat[streamIndex]
		eventBorders[streamIndex] = scanEventBorder(0, len(txEvents)-1, recoverableTxs, txEvents)
	}

	// Iterate.
	eventOffsets := make([]int, len(streamIndexes))
	for {
		// Scan committed but not in recoverable.
		foundNewTxs := false
		for _, streamIndex := range streamIndexes {
			txEvents := txsBetweenHeartbeat[streamIndex]
			border := eventBorders[streamIndex]
			for i := border; i >= eventOffsets[streamIndex]; i-- {
				ev := txEvents[i]
				if ev.Type == tx.Commit {
					if _, ok := recoverableTxs[ev.XID]; !ok {
						recoverableTxs[ev.XID] = 1
						foundNewTxs = true
					}
				}
			}
		}
		if !foundNewTxs {
			break
		}

		// Update the offset and rescan the border.
		borderChanged := false
		for _, streamIndex := range streamIndexes {
			eventOffsets[streamIndex] = eventBorders[streamIndex] + 1
			txEvents := txsBetweenHeartbeat[streamIndex]
			eventBorders[streamIndex] = scanEventBorder(eventOffsets[streamIndex], len(txEvents)-1, recoverableTxs, txEvents)
			if eventBorders[streamIndex] != eventOffsets[streamIndex] {
				borderChanged = true
			}
		}

		if !borderChanged {
			break
		}
	}

	// Return.
	streamBorders := make(map[string]binlog.EventOffset)
	for streamName, streamIndex := range streamIndexes {
		txEvents := txsBetweenHeartbeat[streamIndex]
		borderIndex := eventBorders[streamIndex]
		if borderIndex < 0 { // Use P_h
			streamBorders[streamName] = *heartbeatPrepareAndCommitOffsets[streamIndex][0]
		} else { // Use last P
			ev := txEvents[borderIndex]
			streamBorders[streamName] = binlog.EventOffset{File: ev.File, Offset: ev.EndPos}
		}
	}

	recoverableTxs[sct.heartbeatTxid] = 1

	return utils.MapKeys(recoverableTxs), streamBorders, nil
}

func (sct *SeekConsistentPoint) buildStreamIndexes() map[string]int {
	streamIndexes := make(map[string]int)
	off := 0
	for streamName := range sct.txEventParsers {
		streamIndexes[streamName] = off
		off++
	}
	return streamIndexes
}

func (sct *SeekConsistentPoint) Perform() ([]uint64, map[string]binlog.EventOffset, error) {
	streamIndexes := sct.buildStreamIndexes()

	recoverableTxs, heartbeatPrepareAndCommitOffsets, txsBetweenHeartbeat, err := sct.stageOne(streamIndexes)
	if err != nil {
		return nil, nil, err
	}

	return sct.stageTwo(streamIndexes, recoverableTxs, heartbeatPrepareAndCommitOffsets, txsBetweenHeartbeat)
}

func NewSeekConsistentPoint(txEventParsers map[string]tx.TransactionEventParser, heartbeatTxid uint64) *SeekConsistentPoint {
	return &SeekConsistentPoint{
		txEventParsers: txEventParsers,
		heartbeatTxid:  heartbeatTxid,
	}
}

func SerializeCpResult(recoverableTxs []uint64, borders map[string]binlog.EventOffset) ([]byte, error) {
	byteBuf := &bytes.Buffer{}
	if err := binary.Write(byteBuf, binary.LittleEndian, uint32(len(recoverableTxs))); err != nil {
		return nil, err
	}
	for _, txid := range recoverableTxs {
		if err := binary.Write(byteBuf, binary.LittleEndian, txid); err != nil {
			return nil, err
		}
	}
	if err := binary.Write(byteBuf, binary.LittleEndian, uint16(len(borders))); err != nil {
		return nil, err
	}
	for streamName, offset := range borders {
		if err := binary.Write(byteBuf, binary.LittleEndian, uint8(len(streamName))); err != nil {
			return nil, err
		}
		if _, err := byteBuf.Write([]byte(streamName)); err != nil {
			return nil, err
		}
		if err := binary.Write(byteBuf, binary.LittleEndian, uint8(len(offset.File))); err != nil {
			return nil, err
		}
		if _, err := byteBuf.Write([]byte(offset.File)); err != nil {
			return nil, err
		}
		if err := binary.Write(byteBuf, binary.LittleEndian, offset.Offset); err != nil {
			return nil, err
		}
	}
	return byteBuf.Bytes(), nil
}
