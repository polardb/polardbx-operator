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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

const ClientCopyBufferSize = (1 << 20) * 5

type FileClient struct {
	host        string
	port        int
	flowControl FlowControl
	returnConn  net.Conn
}

func NewFileClient(host string, port int, flowControl FlowControl) *FileClient {
	return &FileClient{
		host:        host,
		port:        port,
		flowControl: flowControl,
	}
}

func (f *FileClient) addr() string {
	return fmt.Sprintf("%s:%d", f.host, f.port)
}

func (f *FileClient) copy(reader io.Reader, writer io.Writer) (written int64, err error) {
	if f.flowControl != nil {
		return f.flowControl.LimitFlow(reader, writer, nil)
	} else {
		return io.CopyBuffer(writer, reader, make([]byte, ClientCopyBufferSize))
	}
}

func (f *FileClient) Upload(reader io.Reader, actionMetadata ActionMetadata) (int64, error) {
	var conn net.Conn
	var err error
	conn, err = net.Dial("tcp", f.addr())
	if err != nil {
		fmt.Fprint(os.Stderr, "Failed to connect"+f.addr())
		return 0, err
	}
	defer conn.Close()
	f.writeMagicNumber(conn)
	f.writeMetadata(conn, actionMetadata)
	if f.returnConn != nil {
		go func() {
			io.Copy(f.returnConn, conn)
		}()
	}
	written, err := f.copy(reader, conn)
	if f.returnConn == nil {
		var lastWrittenLen int64
		for {
			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			len, err := ReadInt64(conn)
			if err != nil {
				return lastWrittenLen, err
			}
			lastWrittenLen = len
			if lastWrittenLen == written {
				return lastWrittenLen, nil
			}
		}
	}
	return 0, nil
}

func (f *FileClient) Check(actionMetadata ActionMetadata) error {
	conn, err := net.Dial("tcp", f.addr())
	if err != nil {
		fmt.Fprint(os.Stderr, "Failed to connect"+f.addr())
		return err
	}
	defer conn.Close()
	f.writeMagicNumber(conn)
	actionMetadata.Action = CheckTask
	f.writeMetadata(conn, actionMetadata)
	buf := make([]byte, 1)
	for {
		cnt, err := conn.Read(buf)
		if err != nil {
			return err
		}
		if cnt == 1 {
			if buf[0] == 0 {
				return nil
			} else if buf[0] == 1 {
				return errors.New("task failed")
			}
		}
	}
}

func (f *FileClient) Download(writer io.Writer, actionMetadata ActionMetadata) (int64, error) {
	conn, err := net.Dial("tcp", f.addr())
	if err != nil {
		fmt.Fprint(os.Stderr, "Failed to connect"+f.addr())
		return 0, err
	}
	defer conn.Close()
	f.writeMagicNumber(conn)
	f.writeMetadata(conn, actionMetadata)
	if actionMetadata.redirect {
		len, err := f.copy(conn, writer)
		return len, err
	}
	bytes, err := ReadBytes(conn, 8)
	len := binary.BigEndian.Uint64(bytes)
	copiedLen, err := f.copy(conn, writer)
	if err != nil {
		return copiedLen, err
	}
	if copiedLen != int64(len) {
		return copiedLen, errors.New(fmt.Sprintf("not complete copiedLen %d  len %d", copiedLen, len))
	}
	return copiedLen, nil
}

func (f *FileClient) writeMagicNumber(conn net.Conn) {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, MagicNumber)
	conn.Write(bytes)
}

func (f *FileClient) writeMetadata(conn net.Conn, actionMetadata ActionMetadata) {
	metadataBytes := []byte(actionMetadata.ToString())
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(len(metadataBytes)))
	conn.Write(bytes)
	conn.Write(metadataBytes)
}
