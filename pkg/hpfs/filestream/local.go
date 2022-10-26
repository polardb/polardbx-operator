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
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	FallocateArchFileName = "fallocatetest"
	FALLOC_FL_KEEP_SIZE   = 0x01
	FALLOC_FL_PUNCH_HOLE  = 0x02
)

func IsFallocateSupported(fp string) bool {
	archFilepath := filepath.Join(filepath.Dir(fp), FallocateArchFileName)
	file, err := os.OpenFile(archFilepath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		fmt.Println(err)
		return false
	}
	defer func() {
		file.Close()
		os.Remove(archFilepath)
	}()
	err = Fallocate(int(file.Fd()), FALLOC_FL_KEEP_SIZE|FALLOC_FL_PUNCH_HOLE, 0, 1)
	if err != nil {
		return false
	}
	return true
}

type File struct {
	Filepath           string
	LastSeek           uint64
	OsFile             *os.File
	FallocateSupported bool
}

func NewFile(filepath string) *File {
	return &File{
		Filepath: filepath,
	}
}

func (f *File) Open() (err error) {
	f.OsFile, err = os.OpenFile(f.Filepath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	return
}

func (f *File) Write(buf []byte, len uint64) (err error) {
	f.LastSeek = 0
	_, err = f.OsFile.Write(buf[:len])
	return
}

func (f *File) CurOffset() (int64, error) {
	pos, err := f.OsFile.Seek(0, io.SeekCurrent)
	return pos, err
}

func (f *File) Close() {
	if f.OsFile != nil {
		if f.LastSeek > 0 {
			_, err := f.OsFile.Seek(-1, io.SeekCurrent)
			if err == nil {
				f.OsFile.Write([]byte{0})
			}
		}
		f.LastSeek = 0
		f.OsFile.Close()
	}
}

func (f *File) WriteSparse(sparseMap []SparseChunk, data []byte) error {
	var index uint32 = 0
	for _, sparseChunk := range sparseMap {
		fOffset, err := f.CurOffset()
		if err != nil {
			return fmt.Errorf("failed to get cur offset %s", err.Error())
		}
		_, err = f.OsFile.Seek(int64(sparseChunk.skip), io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("failed to get cur offset %s", err.Error())
		}
		if sparseChunk.len > 0 {
			_, err = f.OsFile.Write(data[index : index+sparseChunk.len])
			index += sparseChunk.len
			if err != nil {
				return fmt.Errorf("failed to writedata %s", err.Error())
			}
		}
		if f.FallocateSupported && sparseChunk.skip > 0 {
			ferr := Fallocate(int(f.OsFile.Fd()), FALLOC_FL_KEEP_SIZE|FALLOC_FL_PUNCH_HOLE, fOffset, int64(sparseChunk.skip))
			if ferr != nil {
				fmt.Printf("ferr %s , fOffset %d skip %d\n", ferr.Error(), fOffset, sparseChunk.skip)
			}
		}
		if err != nil {
			return fmt.Errorf("failed to write sparse data %s", err.Error())
		}
	}
	f.LastSeek = 0
	if len(sparseMap) > 0 {
		if sparseMap[len(sparseMap)-1].len == 0 {
			f.LastSeek = uint64(sparseMap[len(sparseMap)-1].skip)
		}
	}
	return nil
}
