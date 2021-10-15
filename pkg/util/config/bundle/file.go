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

package bundle

import (
	"bytes"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
)

type fileSystemBundle struct {
	path string
}

func (b *fileSystemBundle) List() ([]string, error) {
	stat, err := os.Stat(b.path)
	if err != nil {
		return nil, err
	}
	if !stat.IsDir() {
		return []string{}, nil
	}
	f, err := os.Open(b.path)

	entries, err := f.ReadDir(-1)
	subKeys := make([]string, 0, len(entries))
	for _, entry := range entries {
		// Only regular file or symlinked regular file.
		if entry.IsDir() {
			continue
		} else if entry.Type()&fs.ModeSymlink > 0 {
			linkedFileStat, err := os.Stat(path.Join(b.path, entry.Name()))
			if err != nil {
				return nil, err
			}
			if !linkedFileStat.Mode().IsRegular() {
				continue
			}
		} else if !entry.Type().IsRegular() {
			continue
		}
		subKeys = append(subKeys, entry.Name())
	}
	return subKeys, nil
}

func (b *fileSystemBundle) ListPaths() ([]string, error) {
	stat, err := os.Stat(b.path)
	if err != nil {
		return nil, err
	}
	if !stat.IsDir() {
		return []string{}, nil
	}
	f, err := os.Open(b.path)

	entries, err := f.ReadDir(-1)
	subKeys := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		subKeys = append(subKeys, entry.Name())
	}
	return subKeys, nil
}

func (b *fileSystemBundle) Get(key string) (io.Reader, error) {
	content, err := ioutil.ReadFile(path.Join(b.path, key))
	if err != nil {
		if err == os.ErrNotExist {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return bytes.NewBuffer(content), nil
}

func (b *fileSystemBundle) Path(subPath string) (Bundle, error) {
	targetPath := path.Join(b.path, subPath)
	if _, err := os.Stat(targetPath); err != nil {
		if err == os.ErrNotExist {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return &fileSystemBundle{
		path: targetPath,
	}, nil
}

func NewFileSystemBundle(path string) Bundle {
	return &fileSystemBundle{
		path: path,
	}
}
