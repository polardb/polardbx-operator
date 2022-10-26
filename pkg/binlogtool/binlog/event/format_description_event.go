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

package event

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
)

// FDE data part.
// https://dev.mysql.com/doc/internals/en/event-data-for-specific-event-types.html

type FormatDescriptionEvent struct {
	BinlogVersion     uint16                       `json:"binlog_version,omitempty"`
	ServerVersion     string                       `json:"server_version,omitempty"`
	CreateTimestamp   uint32                       `json:"create_timestamp,omitempty"`
	HeaderLength      uint8                        `json:"header_length,omitempty"`
	PostHeaderLength  []uint8                      `json:"post_header_length,omitempty"`
	ChecksumAlgorithm spec.BinlogChecksumAlgorithm `json:"checksum_algorithm"`
}

func str2uint(s string) int {
	r := 0
	for _, c := range []byte(s) {
		x := int(c - '0')
		if x > 9 {
			break
		}
		r = r*10 + x
	}
	return r
}

func (e *FormatDescriptionEvent) ServerVersionProduct() (int, error) {
	s := strings.Split(e.ServerVersion, ".")
	if len(s) < 3 {
		return 0, errors.New("invalid server version")
	}
	major, err := strconv.Atoi(s[0])
	if err != nil {
		return 0, errors.New("invalid server version")
	}
	minor, err := strconv.Atoi(s[1])
	if err != nil {
		return 0, errors.New("invalid server version")
	}
	patch := str2uint(s[2])
	if err != nil {
		return 0, errors.New("invalid server version")
	}
	if major <= 0 || minor < 0 || patch < 0 || major > 255 || minor > 255 || patch > 255 {
		return 0, errors.New("invalid server version")
	}
	return major<<16 + minor<<8 + patch, nil
}

func (e *FormatDescriptionEvent) Layout(version uint32, code byte, fde *FormatDescriptionEvent) *layout.Layout {
	return layout.Decl(
		layout.Number(&e.BinlogVersion),
		layout.Area(layout.Const(50), func(bs []byte) (int, error) {
			// Assume it is always utf-8 encoded
			nullIdx := bytes.IndexByte(bs, 0)
			if nullIdx < 0 {
				nullIdx = 50
			}
			e.ServerVersion = string(bs[:nullIdx])
			return 50, nil
		}),
		layout.Number(&e.CreateTimestamp),
		layout.Number(&e.HeaderLength),
		layout.Area(layout.Infinite(), func(bs []byte) (int, error) {
			p, err := e.ServerVersionProduct()
			if err != nil {
				return 0, fmt.Errorf("failed to get server version split product: %w", err)
			}
			if p < spec.CHECKSUM_VERSION_PRODUCT {
				e.ChecksumAlgorithm = spec.BinlogChecksumAlgorithmUndefined
				e.PostHeaderLength = make([]byte, len(bs))
				copy(e.PostHeaderLength, bs)
			} else {
				if len(bs) < 5 {
					return 0, errors.New("invalid post header block")
				}
				// Here bs contains the checksum part. We must exclude it.
				alg := bs[len(bs)-5]
				if alg != spec.BINLOG_CHECKSUM_ALG_OFF && alg != spec.BINLOG_CHECKSUM_ALG_CRC32 {
					return 0, errors.New("invalid checksum algorithm")
				}
				e.ChecksumAlgorithm = spec.BinlogChecksumAlgorithm(alg)
				e.PostHeaderLength = make([]byte, len(bs)-5)
				copy(e.PostHeaderLength, bs[:len(bs)-5])
			}

			return len(bs), nil
		}),
	)
}

func (e *FormatDescriptionEvent) GetPostHeaderLength(code uint8, defaultVal uint8) uint8 {
	if e == nil || len(e.PostHeaderLength) < int(code) {
		return defaultVal
	}
	return e.PostHeaderLength[code-1]
}
