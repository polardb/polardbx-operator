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

package meta

import (
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
)

var (
	ErrInvalidBinaryLogFilenameFormat = errors.New("invalid binlog filename format")
)

// ParseBinaryLogFileName parses the binlog filename into two parts: basename and index.
// It returns an error if the filename does not obey the format.
func ParseBinaryLogFileName(filename string) (string, int, error) {
	dotIndex := strings.LastIndexByte(filename, '.')
	if dotIndex < 0 {
		return "", 0, ErrInvalidBinaryLogFilenameFormat
	}
	basename, suffix := filename[:dotIndex], filename[dotIndex+1:]
	index, err := strconv.Atoi(suffix)
	if err != nil {
		return "", 0, ErrInvalidBinaryLogFilenameFormat
	}
	return basename, index, nil
}

type BinlogFile struct {
	File     string
	Basename string
	Index    int
}

func (f *BinlogFile) FileName() string {
	return path.Base(f.File)
}

func (f *BinlogFile) String() string {
	return fmt.Sprintf("%s.%06d", f.Basename, f.Index)
}

func ParseBinlogFile(file string) (BinlogFile, error) {
	filename := path.Base(file)
	basename, index, err := ParseBinaryLogFileName(filename)
	if err != nil {
		return BinlogFile{}, err
	}
	return BinlogFile{File: file, Basename: basename, Index: index}, nil
}

func ParseBinlogFilesAndSortByIndex(files ...string) ([]BinlogFile, error) {
	// Parse filenames.
	parsed := make([]BinlogFile, len(files))
	var err error
	for i, filename := range files {
		parsed[i], err = ParseBinlogFile(filename)
		if err != nil {
			return nil, fmt.Errorf("failed to parse filename: %w", err)
		}
	}

	sort.Slice(parsed, func(i, j int) bool {
		a, b := &parsed[i], &parsed[j]
		return a.Index < b.Index
	})

	return parsed, nil
}
