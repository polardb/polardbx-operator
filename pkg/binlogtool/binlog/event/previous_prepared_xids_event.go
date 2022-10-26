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
	"io"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"
)

type PreviousPreparedXIDsEvent struct {
	XIDs []XID `json:"xids,omitempty"`
}

func (e *PreviousPreparedXIDsEvent) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	var xidsLen uint32
	return layout.Decl(
		layout.Number(&xidsLen),
		layout.Area(layout.Infinite(), func(r io.Reader) error {
			e.XIDs = make([]XID, xidsLen)
			for i := 0; i < int(xidsLen); i++ {
				err := e.XIDs[i].Layout(version, code, fde).FromStream(r)
				if err != nil {
					return err
				}
			}
			return nil
		}),
	)
}
