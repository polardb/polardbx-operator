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
	"fmt"
	"io"
	"path/filepath"

	"github.com/colinmarc/hdfs"
)

func init() {
	MustRegisterFileService("hdfs", &hdfsFs{})
}

type hdfsFs struct{}

type hdfsContext struct {
	ctx context.Context

	addr     string
	username string
}

func getAddr(auth map[string]string) string {
	if addr, ok := auth["addr"]; ok {
		return addr
	}
	return fmt.Sprintf("%s:%s", auth["host"], auth["port"])
}

func newHdfsContext(ctx context.Context, auth, params map[string]string) *hdfsContext {
	return &hdfsContext{
		ctx:      ctx,
		addr:     getAddr(auth),
		username: auth["username"],
	}
}

func (h *hdfsFs) newClient(hdfsCtx *hdfsContext) (*hdfs.Client, error) {
	return hdfs.NewClient(hdfs.ClientOptions{
		Addresses: []string{hdfsCtx.addr},
		User:      hdfsCtx.username,
	})
}

func (h *hdfsFs) DeleteFile(ctx context.Context, path string, auth, params map[string]string) error {
	hdfsCtx := newHdfsContext(ctx, auth, params)

	client, err := h.newClient(hdfsCtx)
	if err != nil {
		return fmt.Errorf("failed new hdfs client: %w", err)
	}

	defer client.Close()
	return client.Remove(path)
}

func (h *hdfsFs) UploadFile(ctx context.Context, reader io.Reader, path string, auth, params map[string]string) (FileTask, error) {
	hdfsCtx := newHdfsContext(ctx, auth, params)

	client, err := h.newClient(hdfsCtx)
	if err != nil {
		return nil, fmt.Errorf("failed new hdfs client: %w", err)
	}

	ft := newFileTask(ctx)
	go func() {
		defer client.Close()

		err := client.MkdirAll(filepath.Base(path), 0755)
		if err != nil {
			ft.complete(fmt.Errorf("failed to create dirs: %w", err))
			return
		}

		f, err := client.Create(path)
		if err != nil {
			ft.complete(fmt.Errorf("failed to create file: %w", err))
			return
		}

		if _, err := io.Copy(f, reader); err != nil {
			ft.complete(fmt.Errorf("failed to copy contents: %w", err))
			return
		}
		ft.complete(nil)
	}()

	return ft, nil
}

func (h *hdfsFs) DownloadFile(ctx context.Context, writer io.Writer, path string, auth, params map[string]string) (FileTask, error) {
	hdfsCtx := newHdfsContext(ctx, auth, params)

	client, err := h.newClient(hdfsCtx)
	if err != nil {
		return nil, fmt.Errorf("failed new hdfs client: %w", err)
	}

	ft := newFileTask(ctx)
	go func() {
		defer client.Close()

		f, err := client.Open(path)
		if err != nil {
			ft.complete(fmt.Errorf("failed to open file: %w", err))
			return
		}

		if _, err := io.Copy(writer, f); err != nil {
			ft.complete(fmt.Errorf("failed to copy content: %w", err))
			return
		}
		ft.complete(nil)
	}()

	return ft, nil
}
