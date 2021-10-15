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
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

func init() {
	MustRegisterFileService("aliyun-oss", &aliyunOssFs{})
}

type aliyunOssFs struct{}

func (o *aliyunOssFs) newClient(ossCtx *aliyunOssContext) (*oss.Client, error) {
	return oss.New(ossCtx.endpoint, ossCtx.accessKey, ossCtx.accessSecret)
}

func (o *aliyunOssFs) DeleteFile(ctx context.Context, path string, auth, params map[string]string) error {
	ossCtx, err := newAliyunOssContext(ctx, auth, params)
	if err != nil {
		return err
	}

	client, err := o.newClient(ossCtx)
	if err != nil {
		return fmt.Errorf("failed to create oss client: %w", err)
	}
	bucket, err := client.Bucket(ossCtx.bucket)
	if err != nil {
		return fmt.Errorf("failed to open oss bucket: %w", err)
	}

	return bucket.DeleteObject(path)
}

type ossProgressListener4FileTask struct {
	*fileTask
}

func (l *ossProgressListener4FileTask) ProgressChanged(event *oss.ProgressEvent) {
	l.progress = int32(event.ConsumedBytes * 100 / event.TotalBytes)
}

func (o *aliyunOssFs) UploadFile(ctx context.Context, reader io.Reader, path string, auth, params map[string]string) (FileTask, error) {
	ossCtx, err := newAliyunOssContext(ctx, auth, params)
	if err != nil {
		return nil, err
	}

	client, err := o.newClient(ossCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create oss client: %w", err)
	}
	bucket, err := client.Bucket(ossCtx.bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to open oss bucket: %w", err)
	}

	ft := newFileTask(ctx)
	go func() {
		opts := []oss.Option{
			oss.Progress(&ossProgressListener4FileTask{fileTask: ft}),
		}

		// Expires at specified time.
		if ossCtx.retentionTime > 0 {
			opts = append(opts, oss.Expires(time.Now().Add(ossCtx.retentionTime)))
		}

		ft.complete(bucket.PutObject(path, reader, opts...))
	}()

	return ft, nil
}

func (o *aliyunOssFs) DownloadFile(ctx context.Context, writer io.Writer, path string, auth, params map[string]string) (FileTask, error) {
	ossCtx, err := newAliyunOssContext(ctx, auth, params)
	if err != nil {
		return nil, err
	}

	client, err := o.newClient(ossCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create oss client: %w", err)
	}
	bucket, err := client.Bucket(ossCtx.bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to open oss bucket: %w", err)
	}

	ft := newFileTask(ctx)
	go func() {
		opts := []oss.Option{
			oss.Progress(&ossProgressListener4FileTask{fileTask: ft}),
		}

		r, err := bucket.GetObject(path, opts...)
		if err != nil {
			ft.complete(fmt.Errorf("failed to get object: %w", err))
			return
		}
		defer r.Close()

		if _, err := io.Copy(writer, r); err != nil {
			ft.complete(fmt.Errorf("failed to copy content: %w", err))
			return
		}

		ft.complete(nil)
	}()

	return ft, nil
}

type aliyunOssContext struct {
	ctx context.Context

	endpoint      string
	accessKey     string
	accessSecret  string
	bucket        string
	retentionTime time.Duration
}

func newAliyunOssContext(ctx context.Context, auth, params map[string]string) (*aliyunOssContext, error) {
	ossCtx := &aliyunOssContext{
		ctx:          ctx,
		endpoint:     auth["endpoint"],
		accessKey:    auth["access_key"],
		accessSecret: auth["access_secret"],
		bucket:       params["bucket"],
	}

	if t, ok := params["retention-time"]; ok {
		d, err := time.ParseDuration(t)
		if err != nil {
			return nil, fmt.Errorf("format error for retention-time: %w", err)
		}
		ossCtx.retentionTime = d
	} else {
		ossCtx.retentionTime = 0
	}

	return ossCtx, nil
}
