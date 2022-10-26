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
	"io"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
)

type MultiLogEventScanner interface {
	LogEventScanner
	io.Closer
}

type multiLogEventScanner struct {
	scanners []LogEventScanner
	curIdx   int
}

func (e *multiLogEventScanner) Close() error {
	for i := e.curIdx; i < len(e.scanners); i++ {
		_ = e.tryClose(e.scanners[i])
	}
	return nil
}

func (e *multiLogEventScanner) tryClose(s LogEventScanner) error {
	if closer, ok := s.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (e *multiLogEventScanner) Next() (EventOffset, event.LogEvent, error) {
	for {
		if e.curIdx >= len(e.scanners) {
			return EventOffset{}, nil, EOF
		}

		s := e.scanners[e.curIdx]
		offset, logEvent, err := s.Next()
		if err != nil {
			if err == EOF {
				_ = e.tryClose(s)
				e.curIdx++
				continue
			}
			return EventOffset{}, nil, err
		}
		return offset, logEvent, nil
	}
}

func NewMultiLogEventScanner(scanners ...LogEventScanner) MultiLogEventScanner {
	if len(scanners) == 0 {
		panic("no scanners provided")
	}

	return &multiLogEventScanner{
		scanners: scanners,
	}
}

type MultiLogEventScanCloser interface {
	MultiLogEventScanner
	io.Closer
}

func NewMultiLogEventScanCloser(scanClosers ...LogEventScanCloser) MultiLogEventScanCloser {
	if len(scanClosers) == 0 {
		panic("no scanners provided")
	}

	scanners := make([]LogEventScanner, 0, len(scanClosers))
	for _, s := range scanClosers {
		scanners = append(scanners, s)
	}

	return &multiLogEventScanner{
		scanners: scanners,
	}
}
