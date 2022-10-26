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
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/str"
)

type XID struct {
	FormatID uint32  `json:"format_id,omitempty"`
	Gtrid    str.Str `json:"gtrid,omitempty"`
	Bqual    str.Str `json:"bqual,omitempty"`
}

func (x *XID) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	var gtridLen, bqualLen uint32
	return layout.Decl(
		layout.Number(&x.FormatID),
		layout.Number(&gtridLen),
		layout.Number(&bqualLen),
		layout.Bytes(&gtridLen, &x.Gtrid),
		layout.Bytes(&bqualLen, &x.Bqual),
	)
}

type XAPrepareEvent struct {
	OnePhase bool `json:"one_phase,omitempty"`
	XID      `json:"xid,omitempty"`
}

func (e *XAPrepareEvent) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	var gtridLen, bqualLen uint32
	return layout.Decl(
		layout.Bool(&e.OnePhase),
		layout.Number(&e.FormatID),

		layout.Number(&gtridLen),
		layout.Number(&bqualLen),
		layout.Bytes(&gtridLen, &e.Gtrid),
		layout.Bytes(&bqualLen, &e.Bqual),
	)
}
