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
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/util/net"
	"sync"
	"testing"
	"time"
)

func TestFlowControl1(t *testing.T) {
	g := NewGomegaWithT(t)
	flowControlManager := NewFlowControl(FlowControlConfig{
		MaxFlow:    2, // 1 byte/s
		TotalFlow:  3,
		MinFlow:    1,
		BufferSize: 1,
	})
	data := [20]byte{}
	reader := bytes.NewReader(data[:])
	var byteBuffer bytes.Buffer
	writer := bufio.NewWriter(&byteBuffer)
	flowControlManager.Start()
	startTime := time.Now()
	flowControlManager.LimitFlow(reader, writer, nil)
	endTime := time.Now()
	timeDiffSeconds := int(endTime.UnixMilli()-startTime.UnixMilli()) / 1000
	g.Expect(timeDiffSeconds).Should(Equal(10))
	flowControlManager.Stop()
}

func TestFlowControl2(t *testing.T) {
	g := NewGomegaWithT(t)
	flowControlManager := NewFlowControl(FlowControlConfig{
		MaxFlow:    20, // 1 byte/s
		TotalFlow:  40,
		MinFlow:    2,
		BufferSize: 2,
	})
	flowControlManager.Start()
	var wg sync.WaitGroup
	startTime := time.Now()
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			data := [40]byte{}
			reader := bytes.NewReader(data[:])
			var byteBuffer bytes.Buffer
			writer := bufio.NewWriter(&byteBuffer)
			flowControlManager.LimitFlow(reader, writer, nil)
			wg.Done()
		}()
	}
	wg.Wait()
	endTime := time.Now()
	timeDiffSeconds := int(endTime.UnixMilli()-startTime.UnixMilli()) / 1000
	g.Expect(timeDiffSeconds).Should(Equal(2))
	flowControlManager.Stop()
}

func TestFlowControl3(t *testing.T) {
	g := NewGomegaWithT(t)
	flowControlManager := NewFlowControl(FlowControlConfig{
		MaxFlow:    1, // 1 byte/s
		TotalFlow:  40,
		MinFlow:    1,
		BufferSize: 2,
	})
	flowControlManager.Start()
	var wg sync.WaitGroup
	var finishWg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		finishWg.Add(1)
		go func() {
			data := [20]byte{}
			reader := bytes.NewReader(data[:])
			var byteBuffer bytes.Buffer
			writer := bufio.NewWriter(&byteBuffer)
			wg.Done()
			if _, err := flowControlManager.LimitFlow(reader, writer, nil); err != nil && !net.IsProbableEOF(err) {
				g.Expect(err).Should(BeEquivalentTo(errors.New("global Transferring task is stopped. Please retry it")))
			}
			finishWg.Done()
		}()
	}
	wg.Wait()
	time.Sleep(1 * time.Second)
	data := [20]byte{}
	reader := bytes.NewReader(data[:])
	var byteBuffer bytes.Buffer
	writer := bufio.NewWriter(&byteBuffer)
	_, err := flowControlManager.LimitFlow(reader, writer, nil)
	g.Expect(err).Should(BeEquivalentTo(errors.New("Throttled")))
	flowControlManager.Stop()
	finishWg.Wait()
}

func TestFlowControl4(t *testing.T) {
	g := NewGomegaWithT(t)
	flowControlManager := NewFlowControl(FlowControlConfig{
		MaxFlow:    20, // 1 byte/s
		TotalFlow:  40,
		MinFlow:    2,
		BufferSize: 2,
	})
	flowControlManager.Start()
	var wg sync.WaitGroup
	startTime := time.Now()
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			data := [40]byte{}
			reader := bytes.NewReader(data[:])
			var byteBuffer bytes.Buffer
			writer := bufio.NewWriter(&byteBuffer)
			var b bytes.Buffer
			foo := bufio.NewWriter(&b)
			flowControlManager.LimitFlow(reader, writer, foo)
			foo.Flush()
			sentCount := binary.BigEndian.Uint64(b.Bytes()[b.Len()-8:])
			g.Expect(sentCount).Should(BeEquivalentTo(40))
			wg.Done()
		}()
	}
	wg.Wait()
	endTime := time.Now()
	timeDiffSeconds := int(endTime.UnixMilli()-startTime.UnixMilli()) / 1000
	g.Expect(timeDiffSeconds).Should(Equal(4))
	flowControlManager.Stop()
}
