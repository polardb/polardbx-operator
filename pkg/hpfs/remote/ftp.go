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
	"io"

	"github.com/jlaffaye/ftp"
)

func init() {
	MustRegisterFileService("ftp", &ftpFs{})
}

type ftpFs struct{}

type ftpContext struct {
	ctx      context.Context
	addr     string
	username string
	password string
}

func newFtpContext(ctx context.Context, auth, params map[string]string) *ftpContext {
	if ctx == nil {
		ctx = context.Background()
	}
	return &ftpContext{
		ctx:      ctx,
		addr:     auth["addr"],
		username: auth["username"],
		password: auth["password"],
	}
}

func (f *ftpFs) newFtpClient(ftpCtx *ftpContext) (*ftp.ServerConn, error) {
	c, err := ftp.Dial(ftpCtx.addr, ftp.DialWithContext(ftpCtx.ctx))
	if err != nil {
		return nil, err
	}

	err = c.Login(ftpCtx.username, ftpCtx.password)
	if err != nil {
		defer c.Quit()
		return nil, err
	}

	return c, nil
}

func (f *ftpFs) DeleteFile(ctx context.Context, path string, auth, params map[string]string) error {
	c, err := f.newFtpClient(newFtpContext(ctx, auth, params))
	if err != nil {
		return err
	}
	defer c.Quit()

	return c.Delete(path)
}

func (f *ftpFs) UploadFile(ctx context.Context, reader io.Reader, path string, auth, params map[string]string) (FileTask, error) {
	c, err := f.newFtpClient(newFtpContext(ctx, auth, params))
	if err != nil {
		return nil, err
	}

	ft := newFileTask(ctx)
	go func() {
		defer c.Quit()

		err := c.Stor(path, reader)
		ft.complete(err)
	}()

	return ft, nil
}

func (f *ftpFs) DownloadFile(ctx context.Context, writer io.Writer, path string, auth, params map[string]string) (FileTask, error) {
	c, err := f.newFtpClient(newFtpContext(ctx, auth, params))
	if err != nil {
		return nil, err
	}

	ft := newFileTask(ctx)
	go func() {
		defer c.Quit()

		r, err := c.Retr(path)
		if err != nil {
			ft.complete(err)
		}
		defer r.Close()

		// Copy from response to writer.
		_, err = io.Copy(writer, r)

		ft.complete(err)
	}()

	return ft, nil
}

func (f *ftpFs) DeleteExpiredFile(ctx context.Context, path string, auth, params map[string]string) (FileTask, error) {
	panic("Not implemented")
}

func (f *ftpFs) ListFiles(ctx context.Context, writer io.Writer, path string, auth, params map[string]string) (FileTask, error) {
	panic("Not implemented")
}

func (o *ftpFs) ListAllFiles(ctx context.Context, path string, auth, params map[string]string) (FileTask, error) {
	panic("Not implemented")
}
