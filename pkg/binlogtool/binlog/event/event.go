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
	"fmt"
	"io"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/errs"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

func (h *LogEventHeaderV1) BinlogVersion() uint32 {
	return spec.V1
}

func (h *LogEventHeaderV3) BinlogVersion() uint32 {
	return spec.V3
}

func (h *LogEventHeaderV4) BinlogVersion() uint32 {
	return spec.V4
}

type LogEventHeader interface {
	BinlogVersion() uint32
	EventType() string
	EventTimestamp() uint32
	EventTypeCode() uint8
	EventServerID() uint32
	TotalEventLength() uint32
	EventEndPosition() uint32
}

func (h *LogEventHeaderV1) EventType() string {
	return spec.EventTypeName(h.EventTypeCode())
}

func (h *LogEventHeaderV1) EventTimestamp() uint32 {
	return h.Timestamp
}

func (h *LogEventHeaderV1) EventTypeCode() uint8 {
	return h.TypeCode
}

func (h *LogEventHeaderV1) EventServerID() uint32 {
	return h.ServerID
}

func (h *LogEventHeaderV1) TotalEventLength() uint32 {
	return h.EventLength
}

func (h *LogEventHeaderV1) EventEndPosition() uint32 {
	panic("not supported")
}

func (h *LogEventHeaderV3) EventEndPosition() uint32 {
	return h.NextPosition
}

type LogEventVersion interface {
	BinlogVersion() uint32
}

type LogEvent interface {
	LogEventVersion
	EventHeader() LogEventHeader
	EventData() any
}

func (h *RawLogEventV1) BinlogVersion() uint32 {
	return spec.V1
}

func (h *RawLogEventV3) BinlogVersion() uint32 {
	return spec.V3
}

func (h *RawLogEventV4) BinlogVersion() uint32 {
	return spec.V4
}

func (r *RawLogEventV1) EventHeader() LogEventHeader {
	return &r.Header
}

func (r *RawLogEventV3) EventHeader() LogEventHeader {
	return &r.Header
}

func (r *RawLogEventV4) EventHeader() LogEventHeader {
	return &r.Header
}

type RawLogEvent interface {
	LogEvent

	RawData() RawLogEventData
}

func (r *RawLogEventV1) EventData() any {
	return r.Data
}

func (r *RawLogEventV3) EventData() any {
	return r.Data
}

func (r *RawLogEventV4) EventData() any {
	return r.Data
}

func (r *RawLogEventV1) RawData() RawLogEventData {
	return r.Data
}

func (r *RawLogEventV3) RawData() RawLogEventData {
	return r.Data
}

func (r *RawLogEventV4) RawData() RawLogEventData {
	return r.Data
}

func NewRawLogEvent(header LogEventHeader, data RawLogEventData) LogEvent {
	switch h := header.(type) {
	case *LogEventHeaderV1:
		return &RawLogEventV1{
			Header: *h,
			Data:   data,
		}
	case *LogEventHeaderV3:
		return &RawLogEventV3{
			Header: *h,
			Data:   data,
		}
	case *LogEventHeaderV4:
		return &RawLogEventV4{
			Header: *h,
			Data:   data,
		}
	default:
		panic(errs.ErrUnrecognizedVersion)
	}
}

type LogEventV1[D any] struct {
	Header LogEventHeaderV1 `json:"header,omitempty"`
	Data   D                `json:"data,omitempty"`
}

func NewLogEventV1[D any](h LogEventHeaderV1, data D) LogEventV1[D] {
	return LogEventV1[D]{
		Header: h,
		Data:   data,
	}
}

func (e *LogEventV1[D]) BinlogVersion() uint32 {
	return spec.V1
}

func (e *LogEventV1[D]) EventHeader() LogEventHeader {
	return &e.Header
}

type LogEventV3[D any] struct {
	Header LogEventHeaderV3 `json:"header,omitempty"`
	Data   D                `json:"data,omitempty"`
}

func NewLogEventV3[D any](h LogEventHeaderV3, data D) LogEventV3[D] {
	return LogEventV3[D]{
		Header: h,
		Data:   data,
	}
}

func (e *LogEventV3[D]) BinlogVersion() uint32 {
	return spec.V3
}

func (e *LogEventV3[D]) EventHeader() LogEventHeader {
	return &e.Header
}

type LogEventV4[D any] struct {
	Header LogEventHeaderV4 `json:"header,omitempty"`
	Data   D                `json:"data,omitempty"`
}

func NewLogEventV4[D any](h LogEventHeaderV4, data D) LogEventV4[D] {
	return LogEventV4[D]{
		Header: h,
		Data:   data,
	}
}

func (e *LogEventV4[D]) BinlogVersion() uint32 {
	return spec.V4
}

func (e *LogEventV4[D]) EventHeader() LogEventHeader {
	return &e.Header
}

func (r *LogEventV1[D]) EventData() any {
	return &r.Data
}

func (r *LogEventV3[D]) EventData() any {
	return &r.Data
}

func (r *LogEventV4[D]) EventData() any {
	return &r.Data
}

type LayoutViewEvent[E any] interface {
	LayoutView
	*E
}

func NewLogEvent[E any](header LogEventHeader, event E) (LogEvent, error) {
	switch header.BinlogVersion() {
	case spec.V1:
		return &LogEventV1[E]{
			Header: *header.(*LogEventHeaderV1),
			Data:   event,
		}, nil
	case spec.V3:
		return &LogEventV3[E]{
			Header: *header.(*LogEventHeaderV3),
			Data:   event,
		}, nil
	case spec.V4:
		return &LogEventV4[E]{
			Header: *header.(*LogEventHeaderV4),
			Data:   event,
		}, nil
	default:
		return nil, errs.ErrUnrecognizedVersion
	}
}

var ErrBlockSizeNotMatch = errors.New("block size not match")

func ExtractLogEventFromBlock[E any, LV LayoutViewEvent[E]](fde *FormatDescriptionEvent, header LogEventHeader, block []byte, exact bool) (LogEvent, error) {
	var event E

	n, err := LV(&event).Layout(header.BinlogVersion(), header.EventTypeCode(), fde).FromBlock(block)
	if err != nil {
		return nil, fmt.Errorf("extract error: %w, event type: %s", err, header.EventType())
	}

	if exact && len(block) != n {
		return nil, fmt.Errorf("extract error: %w, event type: %s", ErrBlockSizeNotMatch, header.EventType())
	}

	return NewLogEvent(header, event)
}

func ExtractLogEventFromStream[E any, LV LayoutViewEvent[E]](fde *FormatDescriptionEvent, header LogEventHeader, r io.Reader, bodySize int, exact bool) (LogEvent, error) {
	var event E

	cr := utils.NewCountReader(io.LimitReader(r, int64(bodySize)))
	err := LV(&event).Layout(header.BinlogVersion(), header.EventTypeCode(), fde).FromStream(cr)
	if err != nil {
		return nil, fmt.Errorf("extract error: %w, event type: %s", err, header.EventType())
	}
	if exact && bodySize != cr.BytesRead() {
		return nil, fmt.Errorf("extract error: %w, event type: %s", ErrBlockSizeNotMatch, header.EventType())
	}

	return NewLogEvent(header, event)
}

func ExtractLogEventFromBlockWithLayoutCache[E any](event any, l *layout.Layout, header LogEventHeader, block []byte, exact bool) (LogEvent, error) {
	n, err := l.FromBlock(block)
	if err != nil {
		return nil, fmt.Errorf("extract error: %w, event type: %s", err, header.EventType())
	}

	if exact && len(block) != n {
		return nil, fmt.Errorf("extract error: %w, event type: %s", ErrBlockSizeNotMatch, header.EventType())
	}

	defer func() {
		// reset event
		var e E
		*(event).(*E) = e
	}()

	return NewLogEvent(header, *(event).(*E))
}

func ExtractLogEventFromStreamWithLayoutCache[E any](event any, l *layout.Layout, header LogEventHeader, r io.Reader, bodySize int, exact bool) (LogEvent, error) {
	cr := utils.NewCountReader(io.LimitReader(r, int64(bodySize)))
	err := l.FromStream(cr)
	if err != nil {
		return nil, fmt.Errorf("extract error: %w, event type: %s", err, header.EventType())
	}
	if exact && bodySize != cr.BytesRead() {
		return nil, fmt.Errorf("extract error: %w, event type: %s", ErrBlockSizeNotMatch, header.EventType())
	}

	defer func() {
		// reset event
		var e E
		*(event).(*E) = e
	}()

	return NewLogEvent(header, *(event).(*E))
}
