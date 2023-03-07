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

package utils

import (
	"bufio"
	"hash/crc32"
	"io"
)

type SeekableBufferReader struct {
	rd     io.ReadSeekCloser
	r      *bufio.Reader
	offset int64
}

func (r *SeekableBufferReader) Close() error {
	rd := r.rd
	r.r.Reset(nil)
	r.rd = nil
	r.offset = 0
	if rd != nil {
		return rd.Close()
	}
	return nil
}

func (r *SeekableBufferReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	r.offset += int64(n)
	return
}

func (r *SeekableBufferReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart && offset == r.offset {
		return r.offset, nil
	}

	if whence == io.SeekCurrent {
		if offset == 0 {
			return r.offset, nil
		}

		// If seek relative to current and buffer covers, we skip in buffer.
		// Otherwise, we rewrite the seek.
		if offset > 0 && int64(r.r.Buffered()) >= offset {
			_, _ = r.r.Discard(int(offset))
			r.offset += offset
			return r.offset, nil
		} else {
			offset = r.offset + offset
			whence = io.SeekStart
		}
	}

	off, err := r.rd.Seek(offset, whence)
	if err != nil {
		return off, err
	}
	r.r.Reset(r.rd)
	r.offset = off
	return off, nil
}

func NewSeekableBufferReader(rd io.ReadSeekCloser) *SeekableBufferReader {
	return &SeekableBufferReader{
		rd: rd,
		r:  bufio.NewReaderSize(rd, 8192),
	}
}

func NewSeekableBufferReaderSize(rd io.ReadSeekCloser, size int) *SeekableBufferReader {
	return &SeekableBufferReader{
		rd: rd,
		r:  bufio.NewReaderSize(rd, size),
	}
}

func SkipBytes(r io.Reader, n int64) error {
	if n == 0 {
		return nil
	}
	if seeker, ok := r.(io.Seeker); ok {
		_, err := seeker.Seek(n, io.SeekCurrent)
		return err
	} else {
		_, err := io.CopyN(io.Discard, r, n)
		return err
	}
}

type CountReader struct {
	r   io.Reader
	cnt int
}

func (r *CountReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	r.cnt += n
	return n, err
}

func (r *CountReader) BytesRead() int {
	return r.cnt
}

func NewCountReader(r io.Reader) *CountReader {
	return &CountReader{r: r}
}

type CRC32Reader struct {
	r        io.Reader
	checksum uint32
}

func (r *CRC32Reader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	if err == nil {
		r.checksum = crc32.Update(r.checksum, crc32.IEEETable, p[:n])
	}
	return
}

func (r *CRC32Reader) Checksum() uint32 {
	return r.checksum
}

func NewCRC32Reader(r io.Reader, checksum uint32) *CRC32Reader {
	return &CRC32Reader{
		r:        r,
		checksum: checksum,
	}
}
