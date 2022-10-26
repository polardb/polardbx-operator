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
	"errors"

	"github.com/google/uuid"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

const (
	IMMEDIATE_COMMIT_TIMESTAMP_LENGTH = 7
	ORIGINAL_COMMIT_TIMESTAMP_LENGTH  = 7
	ENCODED_COMMIT_TIMESTAMP_LENGTH   = 55
	TRANSACTION_LENGTH_MIN_LENGTH     = 1
	TRANSACTION_LENGTH_MAX_LENGTH     = 9
	ORIGINAL_SERVER_VERSION_LENGTH    = 4
	IMMEDIATE_SERVER_VERSION_LENGTH   = 4
	ENCODED_SERVER_VERSION_LENGTH     = 31
)

type GTIDLogEvent struct {
	GTIDFlags                uint8     `json:"gtid_flags,omitempty"`
	SID                      uuid.UUID `json:"sid,omitempty"`
	GNo                      uint64    `json:"gno,omitempty"`
	LtType                   uint8     `json:"lt_type,omitempty"`
	LastCommitted            uint64    `json:"last_committed,omitempty"`
	SequenceNumber           uint64    `json:"sequence_number,omitempty"`
	ImmediateCommitTimestamp uint64    `json:"immediate_commit_timestamp,omitempty"`
	OriginalCommitTimestamp  uint64    `json:"original_commit_timestamp,omitempty"`
	TransactionLength        uint64    `json:"transaction_length,omitempty"`
	OriginalServerVersion    uint32    `json:"original_server_version,omitempty"`
	ImmediateServerVersion   uint32    `json:"immediate_server_version,omitempty"`
}

func (e *GTIDLogEvent) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	return layout.Decl(
		layout.Number(&e.GTIDFlags),
		layout.UUID(&e.SID),
		layout.Number(&e.GNo),
		layout.Number(&e.LtType),
		layout.Area(layout.Infinite(), func(bs []byte) (int, error) {
			off := 0
			if e.LtType == spec.LOGICAL_TIMESTAMP_TYPECODE {
				n, err := layout.Decl(
					layout.Number(&e.LastCommitted),
					layout.Number(&e.SequenceNumber),
				).FromBlock(bs[off:])
				if err != nil {
					return 0, err
				}
				off += n

				// libbinlogevents/src/control_events.cpp
				if len(bs) >= off+IMMEDIATE_COMMIT_TIMESTAMP_LENGTH {
					var buf [8]byte
					copy(buf[:IMMEDIATE_COMMIT_TIMESTAMP_LENGTH], bs[off:])
					utils.ReadNumberLittleEndianHack(&e.ImmediateCommitTimestamp, buf[:])
					off += IMMEDIATE_COMMIT_TIMESTAMP_LENGTH

					if (e.ImmediateCommitTimestamp & (uint64(1) << ENCODED_COMMIT_TIMESTAMP_LENGTH)) != 0 {
						// Clear MSB
						e.ImmediateCommitTimestamp &= ^(uint64(1) << ENCODED_COMMIT_TIMESTAMP_LENGTH)

						// Read the original commit timestamp.
						if len(bs) >= off+ORIGINAL_COMMIT_TIMESTAMP_LENGTH {
							var buf [8]byte
							copy(buf[:ORIGINAL_COMMIT_TIMESTAMP_LENGTH], bs[off:])
							utils.ReadNumberLittleEndianHack(&e.OriginalCommitTimestamp, buf[:])
							off += ORIGINAL_COMMIT_TIMESTAMP_LENGTH
						} else {
							return 0, errors.New("not enough bytes for original_commit_timestamp")
						}
					} else {
						e.OriginalCommitTimestamp = e.ImmediateCommitTimestamp
					}
				}

				if len(bs) >= off+TRANSACTION_LENGTH_MIN_LENGTH {
					n, err := layout.PackedInt(&e.TransactionLength).FromBlock(bs[off:])
					if err != nil {
						return 0, err
					}
					off += n
				}

				if len(bs) >= off+IMMEDIATE_SERVER_VERSION_LENGTH {
					n, err := layout.Number(&e.ImmediateServerVersion).FromBlock(bs[off:])
					if err != nil {
						return 0, err
					}
					off += n

					if (e.ImmediateServerVersion & (uint32(1) << ENCODED_SERVER_VERSION_LENGTH)) != 0 {
						e.ImmediateServerVersion &= ^(uint32(1) << ENCODED_SERVER_VERSION_LENGTH)

						n, err = layout.Number(&e.OriginalServerVersion).FromBlock(bs[off:])
						if err != nil {
							return 0, err
						}

						off += n
					} else {
						e.OriginalServerVersion = e.ImmediateServerVersion
					}
				}
			}

			return off, nil
		}),
	)
}
