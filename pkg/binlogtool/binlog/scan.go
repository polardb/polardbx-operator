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
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"path/filepath"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/errs"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

var EOF = io.EOF

type LogEventHeaderFilter func(header event.LogEventHeader) bool

type EventOffset struct {
	File   string `json:"file,omitempty"`
	Offset uint64 `json:"offset,omitempty"`
}

func (o *EventOffset) String() string {
	return fmt.Sprintf("%s:%d", o.File, o.Offset)
}

type LogEventScanner interface {
	Next() (EventOffset, event.LogEvent, error)
}

type ScanMode byte

const (
	// ScanModeStrict if event parsing fails or the event payload isn't used up in parse,
	// an error will be returned.
	ScanModeStrict ScanMode = iota
	// ScanModeLoose returns error only if event parsing fails. If there's trailing bytes
	// not used in event payload, they are ignored.
	ScanModeLoose
	// ScanModeStrictFallback fallbacks to return a raw event when parse fails in strict mode.
	ScanModeStrictFallback
	// ScanModeLooseFallback fallbacks to return a raw event when parse fails in loose mode.
	ScanModeLooseFallback
	// ScanModeRaw does not parse any event body but return a raw event instead.
	ScanModeRaw
	// ScanModeHeaderOnly only returns header in body, thus no checksum is verified.
	ScanModeHeaderOnly
)

type logEventScanner struct {
	r io.Reader

	headerFilter LogEventHeaderFilter

	curOffset        uint64
	endPos           uint64
	binlogFile       string
	binlogVersion    uint32
	headerLength     uint32
	fde              *event.FormatDescriptionEvent
	checksum         string
	validateChecksum bool

	scanMode ScanMode

	firstEvent event.LogEvent
	hasFirst   bool

	lastErr error

	// probe to the next valid header. (only support v3, v4)
	probeHeaderSize uint

	eventLayoutCache  []*eventLayout
	headerLayoutCache eventLayout

	// header & body buf (for parse)
	headerBuf []byte
	bodyBuf   []byte
}

func (s *logEventScanner) HeaderLength() uint32 {
	return s.headerLength
}

func (s *logEventScanner) skipBytes(n int64) error {
	return utils.SkipBytes(s.r, n)
}

func (s *logEventScanner) isStrictParseMode() bool {
	return s.scanMode == ScanModeStrict || s.scanMode == ScanModeStrictFallback
}

func (s *logEventScanner) isFallbackMode() bool {
	return s.scanMode == ScanModeStrictFallback || s.scanMode == ScanModeLooseFallback
}

func (s *logEventScanner) isRawMode() bool {
	return s.scanMode == ScanModeRaw
}

func (s *logEventScanner) probeNextHeader(limit int) ([]byte, event.LogEventHeader, error) {
	if s.binlogVersion == spec.V1 {
		return nil, nil, errors.New("not supported")
	}
	if limit < 19 {
		return nil, nil, errors.New("limit too small")
	}

	// event length: 9 : 4
	// next position 13 : 4

	// bs is a ring buffer starts at index w, i.e.
	// bs[w:19] + bs[0:w] in the normal order.
	var bs [19]byte
	w := 0

	if _, err := io.ReadFull(s.r, bs[:]); err != nil {
		return nil, nil, err
	}
	limit -= 19

	for {
		// check if valid
		var eventLength, nextPos [4]byte
		if w+13 <= 19 {
			copy(eventLength[:], bs[w+9:w+13])
		} else if w+9 < 19 {
			copy(eventLength[:10-w], bs[w+9:19])
			copy(eventLength[10-w:], bs[0:w-6])
		} else {
			copy(eventLength[:], bs[w-10:w-6])
		}
		if w+17 <= 19 {
			copy(nextPos[:], bs[w+13:w+17])
		} else if w+13 < 19 {
			copy(nextPos[:6-w], bs[w+13:19])
			copy(nextPos[6-w:], bs[0:w-2])
		} else {
			copy(nextPos[:], bs[w-6:w-2])
		}

		var evLen, nPos uint32
		utils.ReadNumberLittleEndianHack(&evLen, eventLength[:])
		utils.ReadNumberLittleEndianHack(&nPos, nextPos[:])

		// found, parse and return.
		if uint32(s.curOffset+uint64(evLen)) == nPos {
			header := make([]byte, 19)
			copy(header[:19-w], bs[w:19])
			copy(header[19-w:], bs[0:w])

			hv3 := &event.LogEventHeaderV3{}
			if _, err := hv3.Layout(s.binlogVersion, 0, nil).FromBlock(header[:]); err != nil {
				return nil, nil, err
			}

			if s.binlogVersion == spec.V4 {
				h := &event.LogEventHeaderV4{LogEventHeaderV3: *hv3}
				headerLength := s.headerLengthOf(h.EventTypeCode())
				if headerLength > 19 {
					h.ExtraHeaders = make([]byte, headerLength-19)
					if _, err := io.ReadFull(s.r, h.ExtraHeaders); err != nil {
						return nil, nil, err
					}
					header = append(header, h.ExtraHeaders...)
				}
				return header, h, nil
			}
			return header, hv3, nil
		}

		// Exceeds limit.
		if limit <= 0 {
			break
		}

		// read next byte
		if _, err := io.ReadFull(s.r, bs[w:w+1]); err != nil {
			return nil, nil, err
		}
		w = (w + 1) % 19
		s.curOffset++
		limit--
	}

	return nil, nil, errors.New("probe header failed")
}

func (s *logEventScanner) readAndParseLogEventHeader() ([]byte, event.LogEventHeader, error) {
	if s.binlogVersion == spec.V4 {
		header := s.headerBuf[:19]
		if _, err := io.ReadFull(s.r, header); err != nil {
			return nil, nil, err
		}

		h := &event.LogEventHeaderV4{}
		if _, err := s.headerLayoutCache.l.FromBlock(header); err != nil {
			return nil, nil, err
		}
		h.LogEventHeaderV3 = *s.headerLayoutCache.event.(*event.LogEventHeaderV3)

		headerLength := s.headerLengthOf(h.EventTypeCode())
		if headerLength > 19 {
			h.ExtraHeaders = make([]byte, headerLength-19)
			if _, err := io.ReadFull(s.r, h.ExtraHeaders); err != nil {
				return nil, nil, err
			}
			header = append(header, h.ExtraHeaders...)
		}

		return header, h, nil
	} else {
		header := s.headerBuf[:s.headerLength]
		if _, err := io.ReadFull(s.r, header); err != nil {
			return nil, nil, err
		}

		switch s.binlogVersion {
		case spec.V1:
			h := &event.LogEventHeaderV1{}
			if _, err := fromBlockWith[event.LogEventHeaderV1](&s.headerLayoutCache, header, h); err != nil {
				return nil, nil, err
			}
			return header, h, nil
		case spec.V3:
			h := &event.LogEventHeaderV3{}
			if _, err := fromBlockWith[event.LogEventHeaderV3](&s.headerLayoutCache, header, h); err != nil {
				return nil, nil, err
			}
			return header, h, nil
		default:
			return nil, nil, errs.ErrUnrecognizedVersion
		}
	}
}

func (s *logEventScanner) headerLengthOf(typeCode uint8) uint32 {
	if s.binlogVersion == spec.V4 {
		if typeCode == spec.FORMAT_DESCRIPTION_EVENT || typeCode == spec.ROTATE_EVENT {
			return 19
		}
	}
	return s.headerLength
}

func (s *logEventScanner) readDataAndSkipFooter(headerBytes []byte, h event.LogEventHeader) ([]byte, error) {
	length := int64(h.TotalEventLength()) - int64(s.headerLengthOf(h.EventTypeCode()))
	if length < 0 {
		return nil, errs.ErrInvalidEvent
	}

	// Reuse buffer here.
	var data []byte
	if len(s.bodyBuf) < int(length) {
		data = make([]byte, length)
	} else {
		data = s.bodyBuf[:length]
	}

	if _, err := io.ReadFull(s.r, data); err != nil {
		return nil, err
	}

	if s.checksum == "crc32" {
		if len(data) < 4 {
			return nil, ErrBrokenBinlog
		}
		var expectedChecksum uint32
		utils.ReadNumberLittleEndianHack(&expectedChecksum, data[len(data)-4:])

		// Check CRC32.
		data = data[:len(data)-4]
		if s.validateChecksum {
			var checksum uint32
			checksum = crc32.Update(checksum, crc32.IEEETable, headerBytes)
			checksum = crc32.Update(checksum, crc32.IEEETable, data)
			if expectedChecksum != checksum {
				return nil, errors.New("checksum not match")
			}
		}
	}

	return data, nil
}

func (s *logEventScanner) skipDataAndFooter(h event.LogEventHeader) error {
	length := int64(h.TotalEventLength()) - int64(s.headerLengthOf(h.EventTypeCode()))
	if length < 0 {
		return errs.ErrInvalidEvent
	}
	return s.skipBytes(length)
}

func (s *logEventScanner) nextInterestedHeader() ([]byte, event.LogEventHeader, error) {
	for {
		var headerBytes []byte
		var logEventHeader event.LogEventHeader
		var err error
		if s.binlogVersion != spec.V1 && s.probeHeaderSize > 0 {
			// Only probe once.
			headerBytes, logEventHeader, err = s.probeNextHeader(int(s.probeHeaderSize))
			s.probeHeaderSize = 0
		} else {
			headerBytes, logEventHeader, err = s.readAndParseLogEventHeader()
		}
		if err != nil {
			return nil, nil, err
		}

		// quick check if it is bad event. (consider overflow)
		if uint32(s.curOffset+uint64(logEventHeader.TotalEventLength())) != logEventHeader.EventEndPosition() {
			return nil, nil, errors.New("bad event: offset not match")
		}

		if s.curOffset+uint64(logEventHeader.TotalEventLength()) > s.endPos {
			return nil, nil, EOF
		}

		if s.headerFilter == nil || s.headerFilter(logEventHeader) {
			return headerBytes, logEventHeader, nil
		}

		if s.checksum != "crc32" || !s.validateChecksum {
			if err := s.skipDataAndFooter(logEventHeader); err != nil {
				return nil, nil, err
			}
		} else {
			if _, err := s.readDataAndSkipFooter(headerBytes, logEventHeader); err != nil {
				return nil, nil, err
			}
		}
		s.curOffset += uint64(logEventHeader.TotalEventLength())
	}
}

func (s *logEventScanner) Next() (EventOffset, event.LogEvent, error) {
	e, err := s.next()
	return EventOffset{Offset: s.curOffset, File: s.binlogFile}, e, err
}

func (s *logEventScanner) newRawLogEvent(header event.LogEventHeader, rawData []byte) event.LogEvent {
	length := len(rawData)

	if length == 0 {
		return event.NewRawLogEvent(header, rawData)
	}

	// If they share the same region of data, we must copy the buf.
	if &rawData[0] == &s.bodyBuf[0] {
		buf := make([]byte, length)
		copy(buf, rawData)
		rawData = buf
	}

	return event.NewRawLogEvent(header, rawData)
}

func (s *logEventScanner) parseEvent(header event.LogEventHeader, rawData []byte) (event.LogEvent, error) {
	if s.isRawMode() {
		return s.newRawLogEvent(header, rawData), nil
	} else {
		ev, err := parseEventWithCache(s.eventLayoutCache, s.fde, header, rawData, s.isStrictParseMode())
		if err != nil && s.isFallbackMode() {
			return s.newRawLogEvent(header, rawData), nil
		}
		return ev, err
	}
}

func (s *logEventScanner) isScanModeHeaderOnly() bool {
	return s.scanMode == ScanModeHeaderOnly
}

func (s *logEventScanner) next() (ev event.LogEvent, err error) {
	if s.lastErr != nil {
		return nil, s.lastErr
	}

	defer func() {
		// Handle broken events.
		if err == io.ErrUnexpectedEOF {
			err = io.EOF
		}
		s.lastErr = err
	}()

	if s.hasFirst {
		s.hasFirst = false
		s.curOffset += uint64(s.firstEvent.EventHeader().TotalEventLength())
		if s.headerFilter == nil || s.headerFilter(s.firstEvent.EventHeader()) {
			return s.firstEvent, nil
		}
	}

	headerBytes, header, err := s.nextInterestedHeader()
	if err != nil {
		return nil, err
	}

	if s.isScanModeHeaderOnly() {
		if err := s.skipDataAndFooter(header); err != nil {
			return nil, err
		}
		s.curOffset += uint64(header.TotalEventLength())

		return event.NewRawLogEvent(header, nil), nil
	} else {
		rawData, err := s.readDataAndSkipFooter(headerBytes, header)
		if err != nil {
			return nil, err
		}

		s.curOffset += uint64(header.TotalEventLength())

		ev, err = s.parseEvent(header, rawData)
		if err != nil {
			return nil, err
		}
		return ev, nil
	}
}

type LogEventScannerOption func(s *logEventScanner)

func WithLogEventHeaderFilter(filter LogEventHeaderFilter) LogEventScannerOption {
	return func(s *logEventScanner) {
		if s.headerFilter == nil {
			s.headerFilter = filter
		} else {
			hf := s.headerFilter
			s.headerFilter = func(header event.LogEventHeader) bool {
				return hf(header) && filter(header)
			}
		}
	}
}

func WithInterestedLogEventTypes(eventTypes ...byte) LogEventScannerOption {
	var m [255]bool
	for _, et := range eventTypes {
		m[et] = true
	}
	return WithLogEventHeaderFilter(func(header event.LogEventHeader) bool {
		return m[header.EventTypeCode()]
	})
}

func WithScanMode(scanMode ScanMode) LogEventScannerOption {
	return func(s *logEventScanner) {
		s.scanMode = scanMode
	}
}

func EnableChecksumValidation(s *logEventScanner) {
	s.validateChecksum = true
}

func WithChecksumAlgorithm(checksum string) LogEventScannerOption {
	return func(s *logEventScanner) {
		s.checksum = checksum
	}
}

func WithFormatDescriptionEvent(fde *event.FormatDescriptionEvent) LogEventScannerOption {
	return func(s *logEventScanner) {
		s.fde = fde
	}
}

func WithBinlogFile(filename string) LogEventScannerOption {
	return func(s *logEventScanner) {
		s.binlogFile = filepath.Base(filename)
	}
}

func WithEndPos(endPos uint64) LogEventScannerOption {
	return func(s *logEventScanner) {
		s.endPos = endPos
	}
}

func WithProbeFirstHeader(probeSize uint) LogEventScannerOption {
	return func(s *logEventScanner) {
		s.probeHeaderSize = probeSize
	}
}

func withStartOffset(startOffset uint64) LogEventScannerOption {
	return func(s *logEventScanner) {
		s.curOffset = startOffset
	}
}

func withFirstEvent(e event.LogEvent, hasFirst bool) LogEventScannerOption {
	return func(s *logEventScanner) {
		s.firstEvent = e
		s.hasFirst = hasFirst
	}
}

func withoutLayoutCache() LogEventScannerOption {
	return func(s *logEventScanner) {
		s.eventLayoutCache = make([]*eventLayout, 256)
	}
}

func checkHeaderLength(binlogVersion uint32, headerLength uint32) {
	if binlogVersion == spec.V1 && headerLength != 13 {
		panic("invalid header length")
	}
	if binlogVersion == spec.V3 && headerLength != 19 {
		panic("invalid header length")
	}
	if binlogVersion == spec.V4 && headerLength < 19 {
		panic("invalid header length")
	}
}

func newLogEventScanner(r io.Reader, binlogVersion uint32, headerLength uint32, opts ...LogEventScannerOption) LogEventScanner {
	checkHeaderLength(binlogVersion, headerLength)

	s := &logEventScanner{
		r:                r,
		scanMode:         ScanModeStrictFallback,
		binlogVersion:    binlogVersion,
		headerLength:     headerLength,
		checksum:         "crc32",
		validateChecksum: false,
		endPos:           math.MaxUint64,
		curOffset:        4,
		headerBuf:        make([]byte, headerLength), // header buffer for parse
		bodyBuf:          make([]byte, 1024),         // default a 1024 byte buffer for parse
	}
	for _, opt := range opts {
		opt(s)
	}

	if s.binlogVersion == spec.V4 {
		if s.fde == nil {
			panic("bad scanner: fde must be provided")
		}
	}

	if s.eventLayoutCache == nil {
		s.eventLayoutCache = newEventBodyLayoutCache(s.binlogVersion, s.fde)
	}
	if s.binlogVersion == spec.V1 {
		s.headerLayoutCache = newEventLayout[event.LogEventHeaderV1](0, 0, nil)
	} else {
		s.headerLayoutCache = newEventLayout[event.LogEventHeaderV3](0, 0, nil)
	}

	return s
}

func newLogEventV1Scanner(r io.Reader, opts ...LogEventScannerOption) LogEventScanner {
	return newLogEventScanner(r, spec.V1, 13, opts...)
}

func newLogEventV3Scanner(r io.Reader, opts ...LogEventScannerOption) LogEventScanner {
	return newLogEventScanner(r, spec.V3, 19, opts...)
}

func newLogEventV4Scanner(r io.Reader, headerLength uint32, opts ...LogEventScannerOption) LogEventScanner {
	return newLogEventScanner(r, spec.V4, headerLength, opts...)
}

var (
	ErrBrokenBinlog = errors.New("broken binlog")
)

// The first event, according to https://dev.mysql.com/doc/internals/en/binary-log-versions.html.

// v1 start event:
// +=====================================+
// | event  | timestamp         0 : 4    |
// | header +----------------------------+
// |        | type_code         4 : 1    | = START_EVENT_V3 = 1
// |        +----------------------------+
// |        | server_id         5 : 4    |
// |        +----------------------------+
// |        | event_length      9 : 4    | = 69
// +=====================================+
// | event  | binlog_version   13 : 2    | = 1
// | data   +----------------------------+
// |        | server_version   15 : 50   |
// |        +----------------------------+
// |        | create_timestamp 65 : 4    |
// +=====================================+

// v3 start event:
// +=====================================+
// | event  | timestamp         0 : 4    |
// | header +----------------------------+
// |        | type_code         4 : 1    | = START_EVENT_V3 = 1
// |        +----------------------------+
// |        | server_id         5 : 4    |
// |        +----------------------------+
// |        | event_length      9 : 4    | = 75
// |        +----------------------------+
// |        | next_position    13 : 4    |
// |        +----------------------------+
// |        | flags            17 : 2    |
// +=====================================+
// | event  | binlog_version   19 : 2    | = 3
// | data   +----------------------------+
// |        | server_version   21 : 50   |
// |        +----------------------------+
// |        | create_timestamp 71 : 4    |
// +=====================================+

// v4 format description event
// +=====================================+
// | event  | timestamp         0 : 4    |
// | header +----------------------------+
// |        | type_code         4 : 1    | = FORMAT_DESCRIPTION_EVENT = 15
// |        +----------------------------+
// |        | server_id         5 : 4    |
// |        +----------------------------+
// |        | event_length      9 : 4    | >= 91
// |        +----------------------------+
// |        | next_position    13 : 4    |
// |        +----------------------------+
// |        | flags            17 : 2    |
// +=====================================+
// | event  | binlog_version   19 : 2    | = 4
// | data   +----------------------------+
// |        | server_version   21 : 50   |
// |        +----------------------------+
// |        | create_timestamp 71 : 4    |
// |        +----------------------------+
// |        | header_length    75 : 1    |
// |        +----------------------------+
// |        | post-header      76 : n    | = array of n bytes, one byte per event
// |        | lengths for all            |   type that the server knows about
// |        | event types                |
// +=====================================+

func headerLength(e event.FormatDescriptionEvent) uint8 {
	return e.HeaderLength
}

func checksumFromOptions(opts ...LogEventScannerOption) string {
	scanner := &logEventScanner{}
	for _, opt := range opts {
		opt(scanner)
	}
	return scanner.checksum
}

func isRawMode(opts ...LogEventScannerOption) bool {
	scanner := &logEventScanner{}
	for _, opt := range opts {
		opt(scanner)
	}
	return scanner.isRawMode()
}

func isValidateChecksum(opts ...LogEventScannerOption) bool {
	scanner := &logEventScanner{}
	for _, opt := range opts {
		opt(scanner)
	}
	return scanner.validateChecksum
}

func NewLogEventScanner(r io.Reader, opts ...LogEventScannerOption) (LogEventScanner, error) {
	return NewLogEventScannerWithStartOffset(r, 0, opts...)
}

func NewLogEventScannerWithStartOffset(r io.Reader, startOffset uint64, opts ...LogEventScannerOption) (LogEventScanner, error) {
	// Basic check of start offset, either 4 (after magic number) or >= 69 + 4 (after the first event).
	if startOffset > 0 {
		if startOffset < 4 {
			return nil, errors.New("invalid start offset: inside magic number")
		} else if startOffset != 4 && startOffset < 69+4 {
			return nil, errors.New("invalid start offset: inside the first event")
		}
	}

	// Read and check magic number.
	magicNumber := make([]byte, 4)
	_, err := io.ReadFull(r, magicNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to read magic number: %w", err)
	}
	if !bytes.Equal(magicNumber, spec.BINLOG_MAGIC[:]) {
		return nil, ErrBrokenBinlog
	}

	// Read the first event header.
	commonHeaderBytes := make([]byte, 13)
	_, err = io.ReadFull(r, commonHeaderBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read the first event: %w", err)
	}
	commonHeader := &event.LogEventHeaderV1{}
	_, err = commonHeader.Layout(0, 0, nil).FromBlock(commonHeaderBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read the first event: %w", err)
	}

	// Scan and parse the first.
	var firstEvent event.LogEvent
	// Read the left bytes.
	leftBytes := make([]byte, commonHeader.EventLength-13)
	if _, err := io.ReadFull(r, leftBytes); err != nil {
		return nil, fmt.Errorf("unable to read: %w", err)
	}
	{
		firstEventReader := io.MultiReader(bytes.NewBuffer(commonHeaderBytes), bytes.NewBuffer(leftBytes))

		opts := []LogEventScannerOption{
			WithScanMode(ScanModeStrict),
			// No necessary to build the cache.
			withoutLayoutCache(),
		}

		var firstEventScanner LogEventScanner
		if commonHeader.TypeCode == spec.START_EVENT_V3 {
			// No checksum algorithm in V1 and V3.
			opts = append(opts, WithChecksumAlgorithm("none"))
			if commonHeader.EventLength == 75 {
				firstEventScanner = newLogEventV3Scanner(firstEventReader, opts...)
			} else if commonHeader.EventLength == 69 {
				firstEventScanner = newLogEventV1Scanner(firstEventReader, opts...)
			} else {
				return nil, errs.ErrInvalidEvent
			}
		} else if commonHeader.TypeCode == spec.FORMAT_DESCRIPTION_EVENT {
			opts = append(opts,
				// Fake fde to scan the first event.
				WithFormatDescriptionEvent(&event.FormatDescriptionEvent{}),
				WithChecksumAlgorithm("none"), // force none
			)
			firstEventScanner = newLogEventV4Scanner(firstEventReader, 19, opts...)
		} else {
			return nil, errors.New("unrecognized binary log version")
		}

		_, firstEvent, err = firstEventScanner.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to read the first event: %w", err)
		}

		if firstEvent.EventHeader().EventEndPosition() != 4+firstEvent.EventHeader().TotalEventLength() {
			return nil, fmt.Errorf("invalid first event: offset not match")
		}
	}

	// Check if the start offset is greater than or equal to the first event end pos.
	currentOffset := uint64(firstEvent.EventHeader().EventEndPosition())
	if startOffset > 0 && startOffset != 4 && startOffset < currentOffset {
		return nil, errors.New("invalid start offset: inside the first event")
	}

	// Seek to the start position.

	if startOffset > currentOffset {
		if seeker, ok := r.(io.Seeker); ok {
			if _, err := seeker.Seek(int64(startOffset), io.SeekStart); err != nil {
				return nil, fmt.Errorf("unable to seek to offset %d: %w", startOffset, err)
			}
		} else {
			if err := utils.SkipBytes(r, int64(startOffset-currentOffset)); err != nil {
				return nil, fmt.Errorf("unable to seek to offset %d: %w", startOffset, err)
			}
		}
	}

	if startOffset >= currentOffset {
		opts = append(opts, withStartOffset(startOffset))
	}

	// Construct the scanner.
	hasFirst := startOffset < currentOffset

	switch commonHeader.TypeCode {
	case spec.FORMAT_DESCRIPTION_EVENT:
		formatDescriptionEvent := firstEvent.(*event.LogEventV4[event.FormatDescriptionEvent])
		// Validate the checksum.
		if isValidateChecksum(opts...) && formatDescriptionEvent.Data.ChecksumAlgorithm == spec.BinlogChecksumAlgorithmCrc32 {
			var expectedChecksum uint32
			utils.ReadNumberLittleEndianHack(&expectedChecksum, leftBytes[len(leftBytes)-4:])
			var checksum uint32
			checksum = crc32.Update(checksum, crc32.IEEETable, commonHeaderBytes)
			checksum = crc32.Update(checksum, crc32.IEEETable, leftBytes[:len(leftBytes)-4])
			if checksum != expectedChecksum {
				return nil, fmt.Errorf("invalid fde: checksum not match")
			}
		}

		opts = append(opts, WithFormatDescriptionEvent(&formatDescriptionEvent.Data))

		// Use the detected checksum algorithm.
		if formatDescriptionEvent.Data.ChecksumAlgorithm == spec.BinlogChecksumAlgorithmOff {
			opts = append(opts, WithChecksumAlgorithm("none"))
		} else if formatDescriptionEvent.Data.ChecksumAlgorithm == spec.BinlogChecksumAlgorithmCrc32 {
			opts = append(opts, WithChecksumAlgorithm("crc32"))
		}

		// Set the first event as raw event.
		if hasFirst {
			if isRawMode(opts...) {
				if formatDescriptionEvent.Data.ChecksumAlgorithm != spec.BinlogChecksumAlgorithmUndefined {
					firstEvent = event.NewRawLogEvent(firstEvent.EventHeader(), leftBytes[6:len(leftBytes)-4])
				} else {
					firstEvent = event.NewRawLogEvent(firstEvent.EventHeader(), leftBytes[6:])
				}
			}
			opts = append(opts, withFirstEvent(firstEvent, true))
		}

		headerLength := headerLength(formatDescriptionEvent.Data)
		return newLogEventV4Scanner(r, uint32(headerLength), opts...), nil
	case spec.START_EVENT_V3:
		// Events follow START_EVENT_V3 have no checksum algorithm.
		opts = append(opts, WithChecksumAlgorithm("none"))
		if commonHeader.EventLength == 75 {
			if hasFirst {
				if isRawMode(opts...) {
					firstEvent = event.NewRawLogEvent(firstEvent.EventHeader(), leftBytes[6:])
				}
				opts = append(opts, withFirstEvent(firstEvent, true))
			}
			return newLogEventV3Scanner(r, opts...), nil
		} else if commonHeader.EventLength == 69 {
			if hasFirst {
				if isRawMode(opts...) {
					firstEvent = event.NewRawLogEvent(firstEvent.EventHeader(), leftBytes)
				}
				opts = append(opts, withFirstEvent(firstEvent, true))
			}
			return newLogEventV1Scanner(r, opts...), nil
		} else {
			return nil, errs.ErrInvalidEvent
		}
	default:
		return nil, ErrBrokenBinlog
	}
}

type LogEventScanCloser interface {
	LogEventScanner
	io.Closer
}

type lazyScanCloser struct {
	f      func() (io.ReadCloser, LogEventScanner, error)
	rd     io.ReadCloser
	s      LogEventScanner
	closed bool
}

func (s *lazyScanCloser) Close() error {
	rd := s.rd

	s.closed = true
	s.s = nil
	s.rd = nil
	s.f = nil

	if rd != nil {
		return rd.Close()
	}

	return nil
}

func (s *lazyScanCloser) Next() (EventOffset, event.LogEvent, error) {
	if s.closed {
		return EventOffset{}, nil, errors.New("scanner closed")
	}

	if s.s == nil {
		var err error
		s.rd, s.s, err = s.f()
		if err != nil {
			return EventOffset{}, nil, err
		}
		s.f = nil
	}

	return s.s.Next()
}

func NewLazyLogEventScanCloser(rf func() (io.ReadCloser, error), startOffset uint64, opts ...LogEventScannerOption) LogEventScanCloser {
	if rf == nil {
		panic("reader generator must not be nil")
	}

	return &lazyScanCloser{
		closed: false,
		f: func() (io.ReadCloser, LogEventScanner, error) {
			r, err := rf()
			if err != nil {
				return nil, nil, err
			}
			s, err := NewLogEventScannerWithStartOffset(r, startOffset, opts...)
			if err != nil {
				_ = r.Close()
				return nil, nil, err
			}
			return r, s, nil
		},
	}
}
