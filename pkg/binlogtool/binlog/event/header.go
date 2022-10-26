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

import "github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"

// The v1 event header:
// +=====================================+
// | event  | timestamp         0 : 4    |
// | header +----------------------------+
// |        | type_code         4 : 1    |
// |        +----------------------------+
// |        | server_id         5 : 4    |
// |        +----------------------------+
// |        | event_length      9 : 4    |
// +=====================================+
// | event  | fixed part       13 : y    |
// | data   +----------------------------+
// |        | variable part              |
// +=====================================+
//
// header length = 13 bytes
// data length = (event_length - 13) bytes
// y is specific to the event type.

type LogEventHeaderV1 struct {
	Timestamp   uint32 `json:"timestamp"`
	TypeCode    uint8  `json:"type_code"`
	ServerID    uint32 `json:"server_id"`
	EventLength uint32 `json:"event_length"`
}

func (h *LogEventHeaderV1) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	return layout.Decl(
		layout.Number(&h.Timestamp),
		layout.Number(&h.TypeCode),
		layout.Number(&h.ServerID),
		layout.Number(&h.EventLength),
	)
}

// The v3 event structure:
// +=====================================+
// | event  | timestamp         0 : 4    |
// | header +----------------------------+
// |        | type_code         4 : 1    |
// |        +----------------------------+
// |        | server_id         5 : 4    |
// |        +----------------------------+
// |        | event_length      9 : 4    |
// |        +----------------------------+
// |        | next_position    13 : 4    |
// |        +----------------------------+
// |        | flags            17 : 2    |
// +=====================================+
// | event  | fixed part       19 : y    |
// | data   +----------------------------+
// |        | variable part              |
// +=====================================+
//
// header length = 19 bytes
// data length = (event_length - 19) bytes
// y is specific to the event type.

type LogEventHeaderV3 struct {
	LogEventHeaderV1 `json:",inline"`
	NextPosition     uint32 `json:"next_position"`
	Flags            uint16 `json:"flags"`
}

func (h *LogEventHeaderV3) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	return layout.Decl(
		layout.Number(&h.Timestamp),
		layout.Number(&h.TypeCode),
		layout.Number(&h.ServerID),
		layout.Number(&h.EventLength),
		layout.Number(&h.NextPosition),
		layout.Number(&h.Flags),
	)
}

// The v4 event structure:
//
// +=====================================+
// | event  | timestamp         0 : 4    |
// | header +----------------------------+
// |        | type_code         4 : 1    |
// |        +----------------------------+
// |        | server_id         5 : 4    |
// |        +----------------------------+
// |        | event_length      9 : 4    |
// |        +----------------------------+
// |        | next_position    13 : 4    |
// |        +----------------------------+
// |        | flags            17 : 2    |
// |        +----------------------------+
// |        | extra_headers    19 : x-19 |
// +=====================================+
// | event  | fixed part        x : y    |
// | data   +----------------------------+
// |        | variable part              |
// +=====================================+
//
// header length = x bytes
// data length = (event_length - x) bytes
// fixed data length = y bytes variable data length = (event_length - (x + y)) bytes
// x is given by the header_length field in the format description event (FDE). Currently, x is 19, so the extra_headers field is empty.
// y is specific to the event type, and is given by the FDE. The fixed-part length is the same for all events of a given type, but may vary for different event types.
// The fixed part of the event data is sometimes referred to as the "post-header" part. The variable part is sometimes referred to as the "payload" or "body."

// NOTE: The FDE (format description event) itself does not contain any extra headers.

type LogEventHeaderV4 struct {
	LogEventHeaderV3 `json:",inline"`
	ExtraHeaders     []byte `json:"extra_headers,omitempty"`
}

func (h *LogEventHeaderV4) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	return layout.Decl(
		layout.Number(&h.Timestamp),
		layout.Number(&h.TypeCode),
		layout.Number(&h.ServerID),
		layout.Number(&h.EventLength),
		layout.Number(&h.NextPosition),
		layout.Number(&h.Flags),
		layout.If(fde != nil, layout.Bytes(layout.Const(fde.HeaderLength-10), &h.ExtraHeaders)),
	)
}
