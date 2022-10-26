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
	"fmt"
	"io"

	"github.com/google/uuid"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

type GTIDInterval struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

type SIDAndGTIDInterval struct {
	SID       uuid.UUID      `json:"sid,omitempty"`
	Intervals []GTIDInterval `json:"intervals,omitempty"`
}

func (s *SIDAndGTIDInterval) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	var intervalLen uint64
	return layout.Decl(
		layout.UUID(&s.SID),
		layout.Number(&intervalLen),
		layout.Area(layout.Infinite(), func(r io.Reader) error {
			block := make([]byte, 16*intervalLen)
			if _, err := io.ReadFull(r, block); err != nil {
				return fmt.Errorf("unable to read: %w", err)
			}
			s.Intervals = make([]GTIDInterval, intervalLen)
			for i := 0; i < int(intervalLen); i++ {
				interval := &s.Intervals[i]
				utils.ReadNumberLittleEndianHack(&interval.Start, block[i*16:i*16+8])
				utils.ReadNumberLittleEndianHack(&interval.End, block[i*16+8:i*16+16])
			}
			return nil
		}),
	)
}

type PreviousGTIDsEvent struct {
	SIDAndGTIDIntervals []SIDAndGTIDInterval ` json:"sid_and_gtid_intervals,omitempty"`
}

func (e *PreviousGTIDsEvent) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	var sidsLen uint64
	return layout.Decl(
		layout.Number(&sidsLen),
		layout.Area(layout.Infinite(), func(r io.Reader) error {
			e.SIDAndGTIDIntervals = make([]SIDAndGTIDInterval, sidsLen)
			for i := 0; i < int(sidsLen); i++ {
				sg := &e.SIDAndGTIDIntervals[i]
				if err := sg.Layout(version, code, fde).FromStream(r); err != nil {
					return nil
				}
			}
			return nil
		}),
	)
}
