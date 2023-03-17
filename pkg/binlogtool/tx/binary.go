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

package tx

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"path"
	"strconv"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"
)

var magicNumber = [4]byte{0xa9, 0x6f, 0x8b, 0xca}

// Binary format.
//
// MAGIC NUMBER						4 bytes
// FILE NAME MAP SIZE				1 bytes
// REPEAT
//		FILE NAME LEN				1 byte
//		FILE NAME					len bytes
// REPEAT
//		EVENT TYPE					1 byte
//		FILE ID						1 byte
//		FILE OFFSET					4 bytes
//		TXID						8 bytes
//		COMMIT TIMESTAMP			8 bytes (if event is commit)

type BinaryTransactionEventWriter interface {
	Write(ev Event) error
	Flush() error
}

type binaryTransactionEventWriter struct {
	w           *gzip.Writer
	files       []string
	filesMap    map[string]uint8
	headerWrote bool
}

func (b *binaryTransactionEventWriter) Flush() error {
	return b.w.Close()
}

func (b *binaryTransactionEventWriter) writeHeader() error {
	_, err := b.w.Write(magicNumber[:])
	if err != nil {
		return err
	}

	b.filesMap = make(map[string]uint8)

	err = binary.Write(b.w, binary.LittleEndian, uint8(len(b.files)))
	if err != nil {
		return err
	}

	for i, file := range b.files {
		b.filesMap[file] = uint8(i)
		fileBytes := []byte(file)
		err = binary.Write(b.w, binary.LittleEndian, uint8(len(fileBytes)))
		if err != nil {
			return err
		}
		_, err = b.w.Write(fileBytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *binaryTransactionEventWriter) Write(ev Event) error {
	if !b.headerWrote {
		if err := b.writeHeader(); err != nil {
			return err
		}
		b.headerWrote = true
	}

	fileId := b.filesMap[ev.File]
	if err := binary.Write(b.w, binary.LittleEndian, uint8(ev.Type)); err != nil {
		return err
	}
	if err := binary.Write(b.w, binary.LittleEndian, fileId); err != nil {
		return err
	}
	if err := binary.Write(b.w, binary.LittleEndian, ev.EndPos); err != nil {
		return err
	}
	if err := binary.Write(b.w, binary.LittleEndian, ev.XID); err != nil {
		return err
	}
	if ev.Type == Commit {
		if err := binary.Write(b.w, binary.LittleEndian, ev.Ts); err != nil {
			return err
		}
	}

	return nil
}

func NewBinaryTransactionEventWriter(w io.Writer, files []string) (BinaryTransactionEventWriter, error) {
	if len(files) > 255 {
		return nil, errors.New("too many files")
	}
	baseNames := make([]string, 0, len(files))
	for _, f := range files {
		f = path.Base(f)
		if len(f) > 255 {
			return nil, errors.New("file name is too long")
		}
		baseNames = append(baseNames, f)
	}

	gw, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}

	return &binaryTransactionEventWriter{
		w:     gw,
		files: baseNames,
	}, nil
}

type binaryTransactionEventParser struct {
	r     io.Reader
	files []string
}

func (p *binaryTransactionEventParser) tryClose() error {
	if closer, ok := p.r.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (p *binaryTransactionEventParser) readAndCheckMagicNumber() error {
	var m [4]byte
	_, err := io.ReadFull(p.r, m[:])
	if err != nil {
		return err
	}
	if !bytes.Equal(magicNumber[:], m[:]) {
		return errors.New("corrupted binary events: magic number not match")
	}
	return nil
}

func (p *binaryTransactionEventParser) readFileIds() error {
	var fileLen uint8
	if err := layout.Number(&fileLen).FromStream(p.r); err != nil {
		return err
	}
	p.files = make([]string, fileLen)

	var fileNameLen uint8
	var fileName []byte
	l := layout.Decl(layout.Number(&fileNameLen), layout.Bytes(&fileNameLen, &fileName))
	for i := 0; i < int(fileLen); i++ {
		if err := l.FromStream(p.r); err != nil {
			return err
		}
		p.files[i] = string(fileName)
	}

	return nil
}

func isUnderlyingErr(err error, expect error) bool {
	for {
		e := errors.Unwrap(err)
		if e == nil {
			break
		}
		err = e
	}
	return err == expect
}

func (p *binaryTransactionEventParser) Parse(h EventHandler) error {
	defer p.tryClose()

	if err := p.readAndCheckMagicNumber(); err != nil {
		return err
	}

	if err := p.readFileIds(); err != nil {
		return err
	}

	var ev Event
	var fileId uint8
	l := layout.Decl(
		layout.Number(&ev.Type),
		layout.Number(&fileId),
		layout.Number(&ev.EndPos),
		layout.Number(&ev.XID),
	)

	for {
		if err := l.FromStream(p.r); err != nil {
			if isUnderlyingErr(err, io.EOF) {
				return nil
			}
		}

		if int(fileId) > len(p.files) {
			return errors.New("unknown file id: " + strconv.Itoa(int(fileId)))
		}
		ev.File = p.files[fileId]
		if ev.Type == Commit {
			if err := binary.Read(p.r, binary.LittleEndian, &ev.Ts); err != nil {
				return err
			}
		} else {
			ev.Ts = 0
		}

		if err := h(&ev); err != nil {
			if err == StopParse {
				return nil
			}
			return err
		}
	}
}

func NewBinaryTransactionEventParser(r io.Reader) (TransactionEventParser, error) {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	return &binaryTransactionEventParser{
		r: gr,
	}, nil
}
