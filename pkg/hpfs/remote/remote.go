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

package remote

import (
	"context"
	"errors"
	"io"
)

type FileTask interface {
	Progress() int
	Wait() error
}

type FileService interface {
	DeleteFile(ctx context.Context, path string, auth, params map[string]string) error
	UploadFile(ctx context.Context, reader io.Reader, path string, auth, params map[string]string) (FileTask, error)
	DownloadFile(ctx context.Context, writer io.Writer, path string, auth, params map[string]string) (FileTask, error)
}

type fileTask struct {
	ctx      context.Context
	progress int32
	errC     chan error
}

func newFileTask(ctx context.Context) *fileTask {
	return &fileTask{
		ctx:  ctx,
		errC: make(chan error, 1),
	}
}

func (f *fileTask) Progress() int {
	return int(f.progress)
}

func (f *fileTask) Wait() error {
	return <-f.errC
}

func (f *fileTask) complete(err error) {
	f.progress = 100
	if err != nil {
		f.errC <- err
	}
	close(f.errC)
}

var fileServices = make(map[string]FileService)

func RegisterFileService(protocol string, service FileService) error {
	if _, ok := fileServices[protocol]; ok {
		return errors.New("duplicate found: " + protocol)
	}
	fileServices[protocol] = service
	return nil
}

func MustRegisterFileService(protocol string, service FileService) {
	if err := RegisterFileService(protocol, service); err != nil {
		panic(err)
	}
}

func GetFileService(protocol string) (FileService, error) {
	if fs, ok := fileServices[protocol]; ok {
		return fs, nil
	} else {
		return nil, errors.New("unknown protocol: " + protocol)
	}
}
