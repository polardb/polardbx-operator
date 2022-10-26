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
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
)

type asyncLogEventScanCloser struct {
	s     LogEventScanner
	close uint32
	ch    chan struct {
		EventOffset
		event.LogEvent
		error
	}
	wg sync.WaitGroup
}

func (s *asyncLogEventScanCloser) asyncScanAndClose(ctx context.Context) {
	defer func() {
		close(s.ch)
		if closer, ok := s.s.(io.Closer); ok {
			_ = closer.Close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			s.ch <- struct {
				EventOffset
				event.LogEvent
				error
			}{EventOffset{}, nil, ctx.Err()}
			return
		default:
		}

		if atomic.LoadUint32(&s.close) > 0 {
			s.ch <- struct {
				EventOffset
				event.LogEvent
				error
			}{EventOffset{}, nil, errors.New("scanner closed")}
			return
		}

		off, ev, err := s.s.Next()
		t := struct {
			EventOffset
			event.LogEvent
			error
		}{off, ev, err}
		s.ch <- t

		if err != nil {
			return
		}
	}
}

func (s *asyncLogEventScanCloser) start(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.asyncScanAndClose(ctx)
	}()
}

func (s *asyncLogEventScanCloser) Next() (EventOffset, event.LogEvent, error) {
	t := <-s.ch
	return t.EventOffset, t.LogEvent, t.error
}

func (s *asyncLogEventScanCloser) Close() error {
	atomic.StoreUint32(&s.close, 1)
	s.wg.Wait()
	return nil
}

func NewAsyncLogEventScanCloser(ctx context.Context, s LogEventScanner, bufSize int) LogEventScanCloser {
	as := &asyncLogEventScanCloser{
		s: s,
		ch: make(chan struct {
			EventOffset
			event.LogEvent
			error
		}, bufSize),
	}

	as.start(ctx)

	return as
}
