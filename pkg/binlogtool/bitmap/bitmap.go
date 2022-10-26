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

package bitmap

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type Bitmap struct {
	block []byte
	size  int
}

func NewBitmap(block []byte, size int) Bitmap {
	if size == 0 {
		panic("size is zero")
	}
	if block == nil {
		block = make([]byte, (size+7)/8)
	} else {
		if len(block) < (size+7)/8 {
			panic("not enough bytes")
		}
	}
	return Bitmap{
		block: block,
		size:  size,
	}
}

func bitsOne(x byte) int {
	c := 0
	for x != 0 {
		x &= x - 1
		c++
	}
	return c
}

func (m Bitmap) MarshalJSON() ([]byte, error) {
	if m.size == 0 {
		return json.Marshal(nil)
	}
	return json.Marshal(m.String())
}

func (m Bitmap) Len() int {
	return m.size
}

func (m Bitmap) Get(index int) bool {
	if index >= m.size {
		panic(fmt.Sprintf("index overflow: %d, size: %d", index, m.size))
	}
	return (m.block[index>>3] & (byte(1) << (index & 0x7))) != 0
}

func (m Bitmap) Set(index int, value bool) {
	if index >= m.size {
		panic(fmt.Sprintf("index overflow: %d, size: %d", index, m.size))
	}
	if value {
		m.block[index>>3] |= byte(1) << (index & 0x7)
	} else {
		m.block[index>>3] &= ^(byte(1) << (index & 0x7))
	}
}

func (m Bitmap) CountOnesBeforeIndex(index int) int {
	if !m.Get(index) {
		return -1
	}
	// Count 1 before index.
	c := 0
	for i := 0; i < index/8; i++ {
		c += bitsOne(m.block[i])
	}
	if index&0x7 != 0 {
		c += bitsOne(m.block[index/8] >> (8 - (index & 0x7)))
	}
	return c
}

func (m Bitmap) NumBitsSet() int {
	c := 0
	for i := 0; i < len(m.block)-1; i++ {
		c += bitsOne(m.block[i])
	}
	if m.size&0x7 == 0 {
		c += bitsOne(m.block[len(m.block)-1])
	} else {
		c += bitsOne(m.block[len(m.block)-1] >> (8 - (m.size & 0x7)))
	}
	return c
}

func (m Bitmap) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 2+m.Len()))
	buf.WriteString("0b")
outer:
	for bi, b := range m.block {
		for i := 0; i < 8; i++ {
			if (bi*8)+i >= m.size {
				break outer
			}
			if (b & (1 << (7 - i))) != 0 {
				buf.WriteRune('1')
			} else {
				buf.WriteRune('0')
			}
		}
	}

	return buf.String()
}
