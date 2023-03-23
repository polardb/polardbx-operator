/*
Copyright 2021 Alibaba Group Holding Limited.

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

package filestream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	polarxIo "github.com/alibaba/polardbx-operator/pkg/util/io"
	"github.com/alibaba/polardbx-operator/pkg/util/math"
	"golang.org/x/sync/semaphore"
	"io"
	k8snet "k8s.io/apimachinery/pkg/util/net"
	net "net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Granularity  = 500 * time.Millisecond // 500ms
	TotalChanCap = 100
)

var (
	GlobalFlowControl FlowControl
)

type FlowControl interface {
	LimitFlow(reader io.Reader, writer io.Writer, notifyWriter io.Writer) (int64, error)
	Start()
	Stop()
}

type FlowControlManger struct {
	FlowControlConfig
	semaphore          *semaphore.Weighted
	totalFlowTokenChan chan int
	ctx                context.Context
	cancel             context.CancelFunc
}

func NewFlowControl(config FlowControlConfig) *FlowControlManger {
	if config.MaxFlow <= 0 || config.MinFlow <= 0 || config.TotalFlow <= 0 || config.BufferSize <= 0 || config.TotalFlow < config.MaxFlow {
		byteContent, _ := json.Marshal(&config)
		panic("invalid params " + string(byteContent))
	}
	maxConcurrent := int(config.TotalFlow / config.MinFlow)
	return &FlowControlManger{
		FlowControlConfig: config,
		semaphore:         semaphore.NewWeighted(int64(maxConcurrent)),
	}
}

type FlowControlConfig struct {
	MinFlow    float64 `json:"minFlow""`   //min flow speed, unit: bytes/s
	MaxFlow    float64 `json:"maxFlow"`    //max flow speed, unit: bytes/s
	TotalFlow  float64 `json:"totalFlow"`  // total flow speed, unit: bytes/s
	BufferSize int     `json:"bufferSize"` // transfer buffer size, unit: bytes
}

func (f *FlowControlManger) Start() {
	f.totalFlowTokenChan = make(chan int, TotalChanCap)
	ctx, cancel := context.WithCancel(context.Background())
	f.ctx = ctx
	f.cancel = cancel
	f.dispatchToken(f.TotalFlow, f.totalFlowTokenChan, f.ctx)
}

func (f *FlowControlManger) Stop() {
	if f.cancel != nil {
		f.cancel()
		f.cancel = nil
	}
}

func (f *FlowControlManger) LimitFlow(reader io.Reader, writer io.Writer, notifyWriter io.Writer) (int64, error) {
	if !f.tryAcquire() {
		return 0, errors.New(fmt.Sprintf("Throttled"))
	}
	ctx, cancel := context.WithCancel(context.Background())
	tokensChan := make(chan int, 2)
	defer func() {
		if cancel != nil {
			cancel()
		}
		f.release()
	}()
	f.dispatchToken(f.MaxFlow, tokensChan, ctx)
	var writtenLen uint64
	notifyWriterFunc := func() {
		if notifyWriter != nil {
			polarxIo.WriteUint64(notifyWriter, atomic.LoadUint64(&writtenLen))
		}
	}
	defer notifyWriterFunc()
	bufChan := make(chan []byte, 0)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			recover()
		}()
		defer wg.Done()
		for {
			bytes, ok := <-bufChan
			if !ok {
				break
			}
			len, err := writer.Write(bytes)
			if err != nil {
				close(bufChan)
				break
			}
			atomic.AddUint64(&writtenLen, uint64(len))
		}
	}()
	var err error
out:
	for {
		select {
		case _, ok := <-tokensChan:
			if !ok {
				break
			}
			select {
			case _, ok := <-f.totalFlowTokenChan:
				if !ok {
					break
				}
			case <-f.ctx.Done():
				err = errors.New("global Transferring task is stopped. Please retry it")
				break
			case <-ctx.Done():
				err = errors.New("local Transferring task is stopped. Please retry it")
				break
			}
		case <-f.ctx.Done():
			err = errors.New("global Transferring task is stopped. Please retry it")
			break
		case <-ctx.Done():
			err = errors.New("local Transferring task is stopped. Please retry it")
			break
		}
		var bufLen int
		buf := make([]byte, f.BufferSize)
		for readLen := 0; readLen < f.BufferSize; readLen += bufLen {
			conn, ok := reader.(net.Conn)
			if ok {
				conn.SetReadDeadline(time.Now().Add(time.Second))
			}
			bufLen, err = reader.Read(buf[readLen:])
			if bufLen > 0 {
				bufChan <- buf[readLen : readLen+bufLen]
			}
			if k8snet.IsTimeout(err) {
				notifyWriterFunc()
				err = nil
			}
			if k8snet.IsProbableEOF(err) {
				close(bufChan)
				break out
			}
			if err != nil {
				close(bufChan)
				break out
			}
		}
	}
	wg.Wait()
	return int64(atomic.LoadUint64(&writtenLen)), err
}

func (f *FlowControlManger) dispatchToken(expectFlow float64, tokenChan chan int, ctx context.Context) {
	go func(ctx context.Context) {
		defer func() {
			close(tokenChan)
		}()
		limitedFlow := expectFlow
		lastWakeupTs := int64(0)
		for {
			now := time.Now().UnixMilli()
			if now-lastWakeupTs >= Granularity.Milliseconds() {
				token := int(limitedFlow / float64(1*time.Second.Milliseconds()/Granularity.Milliseconds()) / float64(f.BufferSize))
				token = math.MaxInt(token, 1)
				for i := 0; i < token; i++ {
					select {
					case <-ctx.Done():
						return
					case tokenChan <- 1:
					}
				}
				lastWakeupTs = now
			}
			time.Sleep(time.Duration(math.MaxInt64(lastWakeupTs+Granularity.Milliseconds()-now, int64(0))) * time.Millisecond)
		}
	}(ctx)
}

func (f *FlowControlManger) tryAcquire() bool {
	return f.semaphore.TryAcquire(1)
}

func (f *FlowControlManger) release() {
	f.semaphore.Release(1)
}
