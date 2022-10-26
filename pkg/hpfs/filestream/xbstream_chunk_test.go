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
	"bytes"
	"encoding/binary"
	"fmt"
	. "github.com/onsi/gomega"
	"os"
	"testing"
)

func TestXbStream(t *testing.T) {
	g := NewGomegaWithT(t)
	fd, _ := os.OpenFile("./chunktest.xbstream", os.O_RDONLY, 0664)
	xbstreamChunk := NewChunk(fd)
	xbstreamChunk.Read()
	xbstreamChunk.crc32.Write(xbstreamChunk.data)
	sum32 := xbstreamChunk.crc32.Sum32()
	g.Expect(sum32).Should(BeEquivalentTo(xbstreamChunk.checkSum))
	xbstreamChunk2 := NewChunk(fd)
	xbstreamChunk2.Read()
	g.Expect(xbstreamChunk2.IsEof()).Should(BeEquivalentTo(true))
	fmt.Println(sum32)
}

func TestXbStreamSparse(t *testing.T) {
	g := NewGomegaWithT(t)
	var byteBuf bytes.Buffer
	byteBuf.Write([]byte(XbStreamChunkMagic))
	byteBuf.WriteByte(0)
	byteBuf.WriteByte(byte(XbChunkTypeSparse))
	var lenBytes [8]byte
	binary.LittleEndian.PutUint32(lenBytes[:4], 4)
	byteBuf.Write(lenBytes[:4])
	byteBuf.WriteString("busu")
	binary.LittleEndian.PutUint32(lenBytes[:4], 2)
	byteBuf.Write(lenBytes[:4])
	binary.LittleEndian.PutUint64(lenBytes[:], 5)
	byteBuf.Write(lenBytes[:])
	binary.LittleEndian.PutUint64(lenBytes[:], 6)
	byteBuf.Write(lenBytes[:])
	binary.LittleEndian.PutUint32(lenBytes[:4], 1)
	byteBuf.Write(lenBytes[:4])
	byteBuf.Write(lenBytes[:4])
	byteBuf.Write(lenBytes[:4])
	byteBuf.Write(lenBytes[:4])
	byteBuf.Write(lenBytes[:4])
	byteBuf.WriteString("busu5")
	xbstreamChunk := NewChunk(bytes.NewReader(byteBuf.Bytes()))
	xbstreamChunk.Read()
	g.Expect(xbstreamChunk.IsEof()).Should(BeEquivalentTo(false))
	g.Expect(xbstreamChunk.rawLength).Should(BeEquivalentTo(byteBuf.Len()))
	fmt.Println()
}
