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
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

func jsonPrettyFormat(d any) string {
	r, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(r)
}

func jsonFormat(d any) string {
	r, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	return string(r)
}

func printEvent(e event.LogEvent) {
	fmt.Printf("%s: %s\n", e.EventHeader().EventType(), jsonPrettyFormat(e))
}

func testNoRawEvents(t *testing.T, binlogFile string) {
	f, err := os.Open(binlogFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	scanner, err := NewLogEventScanner(utils.NewSeekableBufferReader(f))
	if err != nil {
		t.Fatal(err)
	}

	for {
		_, e, err := scanner.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		if _, ok := e.(event.RawLogEvent); ok {
			printEvent(e)
			t.Fatal("raw event")
		}
	}
}

func TestScanFileSpeed(t *testing.T) {
	const binlogFile = "/Users/shunjie.dsj/Documents/mysql-bin.000351"
	f, err := os.Open(binlogFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	buf := make([]byte, 4095)
	for {
		_, err := io.ReadFull(r, buf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return
			}
			t.Fatal(err)
		}
	}
}

func TestReadFromNetwork(t *testing.T) {
	const url = "http://localhost:8000/mysql-bin.000351"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	s, err := NewLogEventScanner(
		resp.Body,
		WithLogEventHeaderFilter(func(header event.LogEventHeader) bool {
			return true
		}),
		WithScanMode(ScanModeStrict),
		// EnableStreamBody,
		EnableChecksumValidation,
	)
	if err != nil {
		t.Fatal(err)
	}

	for {
		_, _, err := s.Next()
		if err != nil {
			if err == EOF {
				return
			}
			t.Fatal(err)
		}
	}
}

func TestNoRawEvents(t *testing.T) {
	const binlogFile = "/Users/shunjie.dsj/Documents/mysql-bin.000351"
	testNoRawEvents(t, binlogFile)
}

func TestNoRawEvents_MySQL80(t *testing.T) {
	const binlogFile = "/Users/shunjie.dsj/Documents/master-bin.000288"
	testNoRawEvents(t, binlogFile)
}

func TestBinlogFileScan(t *testing.T) {
	const binlogFile = "/Users/shunjie.dsj/Documents/mysql-bin.000351"
	f, err := os.Open(binlogFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	scanner, err := NewLogEventScanner(bufio.NewReader(f),
		WithLogEventHeaderFilter(func(header event.LogEventHeader) bool {
			// return true
			// return header.EventTypeCode() == spec.PREVIOUS_GTIDS_LOG_EVENT
			return header.EventTypeCode() == spec.QUERY_EVENT
			// return header.EventTypeCode() == spec.CONSENSUS_LOG_EVENT
			// return header.EventTypeCode() == spec.PREVIOUS_CONSENSUS_INDEX_LOG_EVENT
			// return header.EventTypeCode() == spec.CONSENSUS_CLUSTER_INFO_EVENT
			// return header.EventTypeCode() == spec.CONSENSUS_EMPTY_EVENT
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		// for {
		_, e, err := scanner.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		printEvent(e)
	}
}

func TestBinlogMySQL8_FileScan(t *testing.T) {
	const binlogFile = "/Users/shunjie.dsj/Documents/master-bin.000288"
	f, err := os.Open(binlogFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	scanner, err := NewLogEventScanner(utils.NewSeekableBufferReader(f),
		WithLogEventHeaderFilter(func(header event.LogEventHeader) bool {
			// return true
			return header.EventTypeCode() == spec.QUERY_EVENT
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	startTime := time.Now()

	eventCnt := 0
	// for i := 0; i < 100; i++ {
	for {
		_, _, err := scanner.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		// printEvent(e)

		eventCnt++
	}

	fmt.Println("Event count: ", eventCnt)
	fmt.Println("Time elapsed: ", time.Now().Sub(startTime))
}

func TestBinlogMySQL8_FileScanFromOffset(t *testing.T) {
	const binlogFile = "/Users/shunjie.dsj/Documents/master-bin.000288"
	f, err := os.Open(binlogFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	scanner, err := NewLogEventScannerWithStartOffset(
		utils.NewSeekableBufferReader(f),
		665,
		WithLogEventHeaderFilter(func(header event.LogEventHeader) bool {
			return true
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, ev, err := scanner.Next()
	if err != nil {
		t.Fatal(err)
	}

	if ev.EventHeader().EventTypeCode() != spec.TABLE_MAP_EVENT {
		t.Fatalf("expect TableMap event, but is %s", jsonPrettyFormat(ev))
	}
	if ev.EventHeader().EventEndPosition() != 768 {
		t.Fatalf("expect end pos %d, but is %s", 768, jsonPrettyFormat(ev))
	}
}

func TestReadFile(t *testing.T) {
	const binlogFile = "/Users/shunjie.dsj/Documents/mysql-bin.000351"
	f, err := os.Open(binlogFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	startTime := time.Now()

	io.ReadAll(bufio.NewReader(f))

	fmt.Println("Time elapsed: ", time.Now().Sub(startTime))
}

func TestBinlogFileScanSpeed(t *testing.T) {
	const binlogFile = "/Users/shunjie.dsj/Documents/mysql-bin.000351"
	f, err := os.Open(binlogFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	scanner, err := NewLogEventScanner(
		utils.NewSeekableBufferReader(f),
		WithLogEventHeaderFilter(func(header event.LogEventHeader) bool {
			// return false
			return true
			// return header.EventTypeCode() == spec.QUERY_EVENT
			// return header.EventTypeCode() == spec.TABLE_MAP_EVENT
		}),
		// EnableStreamBody,
	)
	if err != nil {
		t.Fatal(err)
	}

	startTime := time.Now()

	eventCnt := 0
	for {
		_, _, err := scanner.Next()
		if err == EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		eventCnt++
	}

	fmt.Println("Event count: ", eventCnt)
	fmt.Println("Time elapsed: ", time.Now().Sub(startTime))
}

func scanAndPrint(t *testing.T, binlog string, output string, n int) {
	s := NewLazyLogEventScanCloser(
		func() (io.ReadCloser, error) {
			f, err := os.Open(binlog)
			if err != nil {
				return nil, err
			}
			return utils.NewSeekableBufferReader(f), nil
		},
		0,
		EnableChecksumValidation,
		WithBinlogFile(binlog),
	)
	defer s.Close()

	of, err := os.OpenFile(output, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	if err != nil {
		t.Fatal(err)
	}
	defer of.Close()
	w := bufio.NewWriter(of)
	defer w.Flush()

	for i := 0; i < n; i++ {
		off, ev, err := s.Next()
		if err != nil {
			t.Fatal(err)
		}
		_, err = fmt.Fprintf(w, "%s %d: %s\n", ev.EventHeader().EventType(), off.Offset, jsonFormat(ev.EventData()))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestLogEventScanner_Next(t *testing.T) {
	const binlog = "/Users/shunjie.dsj/Shortcuts/polardbx-k8s/binlogs/galaxy-9f2z-dn-0-cand-0/mysql_bin.000001"
	// testNoRawEvents(t, binlog)
	scanAndPrint(t, binlog, "/tmp/event.log", 10000)
}

func TestBytesEqual(t *testing.T) {
	buf := make([]byte, 10)
	a, b := buf[:5], buf[:10]
	fmt.Printf("%v\n", &a[0] == &b[0])
}

func TestCrc32(t *testing.T) {
	var v = []byte{0x18, 0xa5, 0x11, 0x62, 0x0f, 0xcb, 0xb8, 0x16, 0x77, 0x78, 0x00, 0x00,
		0x00, 0x7c, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x00, 0x38, 0x2e, 0x30, 0x2e,
		0x31, 0x38, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x13, 0x00, 0x0d, 0x00, 0x08,
		0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x04, 0x00, 0x00, 0x00, 0x60, 0x00, 0x04,
		0x1a, 0x08, 0x00, 0x00, 0x00, 0x08, 0x08, 0x08, 0x02, 0x00, 0x00, 0x00, 0x0a,
		0x0a, 0x0a, 0x2a, 0x2a, 0x00, 0x12, 0x34, 0x00, 0x0a, 0x01}
	fmt.Printf("%x\n", crc32.Checksum(v, crc32.MakeTable(crc32.IEEE)))
	fmt.Printf("%x\n", crc32.Checksum(v, crc32.MakeTable(crc32.Castagnoli)))
	fmt.Printf("%x\n", crc32.Checksum(v, crc32.MakeTable(crc32.Koopman)))
}
