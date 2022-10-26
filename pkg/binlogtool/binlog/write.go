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

package binlog

import (
	"bufio"
	"encoding/binary"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/errs"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
	"hash/crc32"
	"io"
)

type RawLogEventWriter interface {
	WriteCommonHeader() error
	Write(ev event.LogEvent) error
	Flush() error
}

type rawLogEventWriter struct {
	w                       *bufio.Writer
	parseFirstEvent         bool
	binlogChecksumAlgorithm spec.BinlogChecksumAlgorithm
}

func (b *rawLogEventWriter) Flush() error {
	return b.w.Flush()
}

func (b *rawLogEventWriter) Write(ev event.LogEvent) error {
	headerBytes, err := b.WriteHeader(ev.EventHeader())
	if err != nil {
		return err
	}
	if err := b.WriteData(ev.EventData()); err != nil {
		return err
	}

	// Parse checksum algorithm
	if b.parseFirstEvent {
		if ev.EventHeader().EventTypeCode() == spec.FORMAT_DESCRIPTION_EVENT {
			data := ev.EventData().(event.RawLogEventData)
			algo := data[len(data)-1]
			b.binlogChecksumAlgorithm = spec.BinlogChecksumAlgorithm(algo)
		}
		b.parseFirstEvent = false
	}

	if b.binlogChecksumAlgorithm == spec.BinlogChecksumAlgorithmCrc32 {
		// Configure and write checksum
		var checksum uint32
		checksum = crc32.Update(checksum, crc32.IEEETable, headerBytes)
		checksum = crc32.Update(checksum, crc32.IEEETable, ev.EventData().(event.RawLogEventData))
		binary.Write(b.w, binary.LittleEndian, checksum)
	}

	return nil
}

func (b *rawLogEventWriter) WriteHeader(header event.LogEventHeader) ([]byte, error) {
	switch header.(type) {
	case *event.LogEventHeaderV1:
		return b.WriteLogEventHeaderV1(header.(*event.LogEventHeaderV1))
	case *event.LogEventHeaderV3:
		return b.WriteLogEventHeaderV3(header.(*event.LogEventHeaderV3))
	case *event.LogEventHeaderV4:
		return b.WriteLogEventHeaderV4(header.(*event.LogEventHeaderV4))
	default:
		panic(errs.ErrUnrecognizedVersion)
	}

	return nil, nil
}

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

func (b *rawLogEventWriter) WriteLogEventHeaderV1(header *event.LogEventHeaderV1) ([]byte, error) {
	headerBytes := make([]byte, 13)
	if err := binary.Write(b.w, binary.LittleEndian, header.Timestamp); err != nil {
		return headerBytes, err
	}
	binary.LittleEndian.PutUint32(headerBytes[0:4], header.Timestamp)
	if err := binary.Write(b.w, binary.LittleEndian, header.TypeCode); err != nil {
		return headerBytes, err
	}
	headerBytes[4] = header.TypeCode
	if err := binary.Write(b.w, binary.LittleEndian, header.ServerID); err != nil {
		return headerBytes, err
	}
	binary.LittleEndian.PutUint32(headerBytes[5:9], header.ServerID)
	if err := binary.Write(b.w, binary.LittleEndian, header.EventLength); err != nil {
		return headerBytes, err
	}
	binary.LittleEndian.PutUint32(headerBytes[9:13], header.EventLength)

	return headerBytes, nil
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

func (b *rawLogEventWriter) WriteLogEventHeaderV3(header *event.LogEventHeaderV3) ([]byte, error) {
	headerBytes := make([]byte, 19)
	if err := binary.Write(b.w, binary.LittleEndian, header.Timestamp); err != nil {
		return headerBytes, err
	}
	binary.LittleEndian.PutUint32(headerBytes[0:4], header.Timestamp)
	if err := binary.Write(b.w, binary.LittleEndian, header.TypeCode); err != nil {
		return headerBytes, err
	}
	headerBytes[4] = header.TypeCode
	if err := binary.Write(b.w, binary.LittleEndian, header.ServerID); err != nil {
		return headerBytes, err
	}
	binary.LittleEndian.PutUint32(headerBytes[5:9], header.ServerID)
	if err := binary.Write(b.w, binary.LittleEndian, header.EventLength); err != nil {
		return headerBytes, err
	}
	binary.LittleEndian.PutUint32(headerBytes[9:13], header.EventLength)
	if err := binary.Write(b.w, binary.LittleEndian, header.NextPosition); err != nil {
		return headerBytes, err
	}
	binary.LittleEndian.PutUint32(headerBytes[13:17], header.NextPosition)
	if err := binary.Write(b.w, binary.LittleEndian, header.Flags); err != nil {
		return headerBytes, err
	}
	binary.LittleEndian.PutUint16(headerBytes[17:19], header.Flags)
	return headerBytes, nil
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

func (b *rawLogEventWriter) WriteLogEventHeaderV4(header *event.LogEventHeaderV4) ([]byte, error) {
	headerLength := 19 + len(header.ExtraHeaders)
	headerBytes := make([]byte, headerLength)
	if err := binary.Write(b.w, binary.LittleEndian, header.Timestamp); err != nil {
		return headerBytes, err
	}
	binary.LittleEndian.PutUint32(headerBytes[0:4], header.Timestamp)
	if err := binary.Write(b.w, binary.LittleEndian, header.TypeCode); err != nil {
		return headerBytes, err
	}
	headerBytes[4] = header.TypeCode
	if err := binary.Write(b.w, binary.LittleEndian, header.ServerID); err != nil {
		return headerBytes, err
	}
	binary.LittleEndian.PutUint32(headerBytes[5:9], header.ServerID)
	if err := binary.Write(b.w, binary.LittleEndian, header.EventLength); err != nil {
		return headerBytes, err
	}
	binary.LittleEndian.PutUint32(headerBytes[9:13], header.EventLength)
	if err := binary.Write(b.w, binary.LittleEndian, header.NextPosition); err != nil {
		return headerBytes, err
	}
	binary.LittleEndian.PutUint32(headerBytes[13:17], header.NextPosition)
	if err := binary.Write(b.w, binary.LittleEndian, header.Flags); err != nil {
		return headerBytes, err
	}
	binary.LittleEndian.PutUint16(headerBytes[17:19], header.Flags)
	if err := binary.Write(b.w, binary.LittleEndian, header.ExtraHeaders); err != nil {
		return headerBytes, err
	}
	copy(headerBytes[19:headerLength], header.ExtraHeaders)

	return headerBytes, nil
}

func (b *rawLogEventWriter) WriteData(data any) error {
	switch data.(type) {
	case event.RawLogEventData:
		return b.WriteRawLogEventData(data.(event.RawLogEventData))
	default:
		panic(errs.ErrInvalidEvent)
	}
	return nil
}

func (b *rawLogEventWriter) WriteRawLogEventData(data event.RawLogEventData) error {
	if err := binary.Write(b.w, binary.LittleEndian, data); err != nil {
		return err
	}

	return nil
}

func (b *rawLogEventWriter) WriteCommonHeader() error {
	if err := binary.Write(b.w, binary.LittleEndian, spec.BINLOG_MAGIC); err != nil {
		return err
	}
	//commonHeaderBytes := make([]byte, 13)
	//if err := binary.Write(b.w, binary.LittleEndian, commonHeaderBytes); err != nil {
	//	return err
	//}
	// commonHeader := &event.LogEventHeaderV1{}
	return nil
	// return b.WriteHeader(commonHeader)
}

func NewRawLogEventWriter(w io.Writer) (RawLogEventWriter, error) {
	nw := bufio.NewWriter(w)
	return &rawLogEventWriter{
		w:                       nw,
		parseFirstEvent:         true,
		binlogChecksumAlgorithm: spec.BinlogChecksumAlgorithmOff,
	}, nil
}
