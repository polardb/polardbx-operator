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

package security

import (
	"bytes"
	"errors"
)

var (
	ErrNotAlignedToBlock = errors.New("not aligned to block")
	ErrInvalidPadding    = errors.New("invalid padding")
)

type PaddingScheme interface {
	Pad(data []byte) []byte
	Strip(data []byte) ([]byte, error)
}

type pkcs7 struct {
	blockSize int
}

func (p *pkcs7) Pad(data []byte) []byte {
	padLen := p.blockSize - len(data)%p.blockSize
	if padLen == 0 {
		padLen = p.blockSize
	}
	padding := bytes.Repeat([]byte{byte(padLen)}, padLen)
	return append(data, padding...)
}

func (p *pkcs7) Strip(data []byte) ([]byte, error) {
	length := len(data)
	if length%p.blockSize > 0 {
		return nil, ErrNotAlignedToBlock
	}
	padLen := int(data[length-1])
	if padLen > p.blockSize || padLen > length {
		return nil, ErrInvalidPadding
	}
	for i := length - padLen; i < length; i++ {
		if data[i] != byte(padLen) {
			return nil, ErrInvalidPadding
		}
	}
	return data[:length-padLen], nil
}

func NewPkcs5() PaddingScheme {
	return &pkcs7{
		blockSize: 8,
	}
}

func NewPkcs7(blockSize int) PaddingScheme {
	if blockSize <= 0 || blockSize > 0xff {
		panic("pkcs7: block size must be in [1, 255]")
	}
	return &pkcs7{
		blockSize: blockSize,
	}
}
