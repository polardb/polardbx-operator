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
	"hash"
	"hash/crc32"
	"io"
)

type ChunkType byte

const (
	XbChunkTypeUnknown ChunkType = '\n'
	XbChunkTypePayload ChunkType = 'P'
	XbChunkTypeSparse  ChunkType = 'S'
	XbChunkTypeEof     ChunkType = 'E'
)

const (
	XbStreamChunkMagic = "XBSTCK01"
	XbStreamFlagIgnore = 0x01
)

var (
	// ChunkHeaderLength  Magic + flags + type + path len
	ChunkHeaderLength     = len(XbStreamChunkMagic) + 1 + 1 + 4
	ChunkTypeOffset       = len(XbStreamChunkMagic) + 1
	ChunkPathLengthOffset = len(XbStreamChunkMagic) + 1 + 1
)

type SparseChunk struct {
	skip uint32
	len  uint32
}

type XbRStreamChunk struct {
	flags              byte
	chunkType          ChunkType
	pathLength         uint32
	path               string
	length             uint64
	rawLength          uint64
	offset             uint64
	checkSumOffset     uint64
	data               []byte
	checkSum           uint32
	checkSumPart       uint64
	bufLen             uint64
	sparseMapAllocSize uint64
	sparseMapSize      uint64
	sparseMap          []SparseChunk
	reader             io.Reader
	crc32              hash.Hash32
}

func NewChunk(reader io.Reader) *XbRStreamChunk {
	return &XbRStreamChunk{
		reader: reader,
	}
}

func (x *XbRStreamChunk) IsEof() bool {
	return x.chunkType == XbChunkTypeEof
}

func (x *XbRStreamChunk) Read() error {
	headerBytes, err := ReadBytes(x.reader, uint64(ChunkHeaderLength))
	if err != nil {
		return err
	}
	index := uint64(0)
	if bytes.Compare(headerBytes[:8], []byte(XbStreamChunkMagic)) != 0 {
		return fmt.Errorf("failed to read chunk header")
	}
	index += 8
	x.flags = headerBytes[index]
	index += 1
	x.chunkType = validateChunkType(headerBytes[index])
	index += 1
	if (x.chunkType == XbChunkTypeUnknown) && (x.flags&XbStreamFlagIgnore) == 0 {
		return fmt.Errorf("unknown chunk type")
	}
	x.pathLength = binary.LittleEndian.Uint32(headerBytes[index : index+4])
	index += 4
	x.path = ""
	if x.pathLength > 0 {
		pathBytes, err := ReadBytes(x.reader, uint64(x.pathLength))
		index += uint64(x.pathLength)
		if err != nil {
			return err
		}
		x.path = string(pathBytes)
	}
	if x.chunkType == XbChunkTypeEof {
		x.rawLength = index
		return nil
	}
	if x.chunkType == XbChunkTypeSparse {
		sparseMapSizeBytes, err := ReadBytes(x.reader, 4)
		index += 4
		if err != nil {
			return err
		}
		x.sparseMapSize = uint64(binary.LittleEndian.Uint32(sparseMapSizeBytes))
	} else {
		x.sparseMapSize = 0
	}
	payLoadMetaBytes, err := ReadBytes(x.reader, 16)
	index += 16
	if err != nil {
		return err
	}
	x.length = binary.LittleEndian.Uint64(payLoadMetaBytes[:8])
	x.offset = binary.LittleEndian.Uint64(payLoadMetaBytes[8:])
	checkSumBytes, err := ReadBytes(x.reader, 4)
	if err != nil {
		return err
	}
	index += 4
	x.checkSum = binary.LittleEndian.Uint32(checkSumBytes)
	x.rawLength = index + x.length + x.sparseMapSize*4*2
	if x.crc32 == nil {
		x.crc32 = crc32.NewIEEE()
	}
	if x.sparseMapSize > 0 {
		x.sparseMap = make([]SparseChunk, x.sparseMapSize)
		for i := 0; i < int(x.sparseMapSize); i++ {
			mapBytes, err := ReadBytes(x.reader, 8)
			if err != nil {
				return err
			}
			x.crc32.Write(mapBytes)
			x.sparseMap[i].skip = binary.LittleEndian.Uint32(mapBytes[:4])
			x.sparseMap[i].len = binary.LittleEndian.Uint32(mapBytes[4:])
		}
	}
	if x.length > 0 {
		x.data, err = ReadBytes(x.reader, x.length)
		if err != nil {
			return err
		}
	}
	return nil
}

func validateChunkType(chunkType byte) ChunkType {
	resultChunkType := ChunkType(chunkType)
	switch resultChunkType {
	case XbChunkTypePayload, XbChunkTypeSparse, XbChunkTypeEof:
		//do nothing
	default:
		resultChunkType = XbChunkTypeEof
	}
	return resultChunkType
}
