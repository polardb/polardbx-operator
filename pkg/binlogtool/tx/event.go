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
	"fmt"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
)

type EventType uint8

const (
	Begin    EventType = 1
	Prepare  EventType = 2
	Commit   EventType = 3
	Rollback EventType = 4
)

// Event for 2PC transaction events extracted from binlog.
// NOTE: HeartbeatRowsLogEvents is only exported when Type == Prepare and it changes the heartbeat table.
type Event struct {
	Raw                    event.LogEvent `json:"-"`
	HeartbeatRowsLogEvents []any          `json:"-"`
	Ts                     uint64         `json:"ts,omitempty"`
	File                   string         `json:"file,omitempty"`
	EndPos                 uint64         `json:"end_pos,omitempty"`
	Type                   EventType      `json:"type,omitempty"`
	XID                    uint64         `json:"xid,omitempty"`
}

func (e *Event) String() string {
	s := fmt.Sprintf("%s %d %s:%d %d", EventNameSimple(e.Type), e.XID,
		e.File, e.EndPos, e.Ts)
	if e.Raw != nil {
		s += " " + e.Raw.EventHeader().EventType()
	}
	if len(e.HeartbeatRowsLogEvents) > 0 {
		s += " *"
	}
	return s
}

func EventName(t EventType) string {
	switch t {
	case Begin:
		return "BEGIN"
	case Prepare:
		return "PREPARE"
	case Commit:
		return "COMMIT"
	case Rollback:
		return "ROLLBACK"
	default:
		panic("unrecognized")
	}
}

func EventNameSimple(t EventType) string {
	switch t {
	case Begin:
		return "S"
	case Prepare:
		return "P"
	case Commit:
		return "C"
	case Rollback:
		return "R"
	default:
		panic("unrecognized")
	}
}
