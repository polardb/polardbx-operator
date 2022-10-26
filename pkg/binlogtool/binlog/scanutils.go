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
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
)

type LogEventMutator func(off EventOffset, ev event.LogEvent) (event.LogEvent, error)

type mutateLogEventScanner struct {
	s       LogEventScanner
	mutator LogEventMutator
}

func (s *mutateLogEventScanner) Next() (EventOffset, event.LogEvent, error) {
	off, ev, err := s.s.Next()
	if err != nil {
		return EventOffset{}, nil, err
	}
	if ev, err = s.mutator(off, ev); err != nil {
		return EventOffset{}, nil, err
	}
	return off, ev, nil
}

func NewMutateLogEventScanner(s LogEventScanner, mutator LogEventMutator) LogEventScanner {
	return &mutateLogEventScanner{
		s:       s,
		mutator: mutator,
	}
}

type filterLogEventScanner struct {
	s          LogEventScanner
	predicates []LogEventFilterFunc
}

func (s *filterLogEventScanner) Next() (EventOffset, event.LogEvent, error) {
outer:
	for {
		off, ev, err := s.s.Next()
		if err != nil {
			return EventOffset{}, nil, err
		}
		for _, pred := range s.predicates {
			if !pred(off, ev) {
				continue outer
			}
		}
		return off, ev, nil
	}
}

type LogEventFilterFunc func(off EventOffset, ev event.LogEvent) bool

func NewFilterLogEventScanner(s LogEventScanner, predicates ...LogEventFilterFunc) LogEventScanner {
	return &filterLogEventScanner{
		s:          s,
		predicates: predicates,
	}
}

type limitedLogEventScanner struct {
	s     LogEventScanner
	skip  int
	count int
	index int
}

func (s *limitedLogEventScanner) Next() (EventOffset, event.LogEvent, error) {
	for s.index < s.skip {
		if _, _, err := s.s.Next(); err != nil {
			return EventOffset{}, nil, err
		}
		s.index++
	}

	if s.count >= 0 && s.index >= s.skip+s.count {
		return EventOffset{}, nil, EOF
	}

	off, ev, err := s.s.Next()
	if err != nil {
		return EventOffset{}, nil, err
	}

	s.index++
	return off, ev, nil
}

func NewLimitedLogEventScanner(s LogEventScanner, skip int, count int) LogEventScanner {
	return &limitedLogEventScanner{
		s:     s,
		skip:  skip,
		count: count,
		index: 0,
	}
}

type tailLogEventScanner struct {
	size     int
	offBuf   []EventOffset
	evBuf    []event.LogEvent
	bufPivot int
	bufLimit int
	lastErr  error
	s        LogEventScanner
}

func (ts *tailLogEventScanner) Next() (EventOffset, event.LogEvent, error) {
	if ts.size == 0 {
		return EventOffset{}, nil, EOF
	}

	// Read until EOF if not scanned.
	if ts.lastErr == nil {
		for {
			off, ev, err := ts.s.Next()
			if err != nil {
				ts.lastErr = err

				// Reset pivot if buffer is not fully used.
				if ts.bufLimit != ts.size {
					ts.bufPivot = 0
				}

				break
			}

			if ts.bufLimit < ts.size {
				ts.bufLimit++
				ts.offBuf[ts.bufPivot], ts.evBuf[ts.bufPivot] = off, ev
				ts.bufPivot = (ts.bufPivot + 1) % ts.size
			} else {
				ts.offBuf[ts.bufPivot], ts.evBuf[ts.bufPivot] = off, ev
				ts.bufPivot = (ts.bufPivot + 1) % ts.size
			}
		}
	}

	// Return events from buf. Returns the last error encountered.
	if ts.bufLimit > 0 {
		off, ev := ts.offBuf[ts.bufPivot], ts.evBuf[ts.bufPivot]
		ts.bufPivot = (ts.bufPivot + 1) % ts.size
		ts.bufLimit--
		return off, ev, nil
	} else {
		return EventOffset{}, nil, ts.lastErr
	}
}

func NewTailLogEventScanner(s LogEventScanner, size int) LogEventScanner {
	return &tailLogEventScanner{
		size:     size,
		offBuf:   make([]EventOffset, size),
		evBuf:    make([]event.LogEvent, size),
		bufPivot: 0,
		bufLimit: 0,
		lastErr:  nil,
		s:        s,
	}
}

type oneEventScanner struct {
	off *EventOffset
	ev  *event.LogEvent
}

func (s *oneEventScanner) Next() (EventOffset, event.LogEvent, error) {
	if s.off == nil {
		return EventOffset{}, nil, EOF
	}

	off, ev := *s.off, *s.ev
	s.off, s.ev = nil, nil

	return off, ev, nil
}

func NewOneEventScanner(off *EventOffset, ev *event.LogEvent) LogEventScanner {
	return &oneEventScanner{
		off: off,
		ev:  ev,
	}
}
