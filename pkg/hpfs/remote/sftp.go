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
	"os"
	"strconv"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func init() {
	MustRegisterFileService("sftp", &sftpFs{})
}

type sftpFs struct{}

type sftpContext struct {
	ctx context.Context

	host     string
	port     int
	username string
	password string
}

func newSftpContext(ctx context.Context, auth, params map[string]string) (*sftpContext, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	port, err := strconv.Atoi(auth["port"])
	if err != nil {
		return nil, fmt.Errorf("invalid port: %w", err)
	}
	return &sftpContext{
		ctx:      ctx,
		host:     auth["host"],
		port:     port,
		username: auth["username"],
		password: auth["password"],
	}, nil
}

func (s *sftpFs) newSshConn(sftpCtx *sftpContext) (*ssh.Client, error) {
	return ssh.Dial("tcp", fmt.Sprintf("%s:%d", sftpCtx.host, sftpCtx.port), &ssh.ClientConfig{
		User: sftpCtx.username,
		Auth: []ssh.AuthMethod{
			ssh.Password(sftpCtx.password),
		},
		Timeout: 2 * time.Second,
	})
}

func (s *sftpFs) DeleteFile(ctx context.Context, path string, auth, params map[string]string) error {
	sftpCtx, err := newSftpContext(ctx, auth, params)
	if err != nil {
		return err
	}

	conn, err := s.newSshConn(sftpCtx)
	if err != nil {
		return fmt.Errorf("ssh connection failure: %w", err)
	}
	defer conn.Close()

	client, err := sftp.NewClient(conn)
	if err != nil {
		return fmt.Errorf("failed to create sftp client: %w", err)
	}
	defer client.Close()

	return client.Remove(path)
}

func (s *sftpFs) UploadFile(ctx context.Context, reader io.Reader, path string, auth, params map[string]string) (FileTask, error) {
	sftpCtx, err := newSftpContext(ctx, auth, params)
	if err != nil {
		return nil, err
	}

	conn, err := s.newSshConn(sftpCtx)
	if err != nil {
		return nil, fmt.Errorf("ssh connection failure: %w", err)
	}

	ft := newFileTask(ctx)
	go func() {
		defer conn.Close()

		client, err := sftp.NewClient(conn)
		if err != nil {
			ft.complete(fmt.Errorf("failed to create sftp client: %w", err))
			return
		}
		defer client.Close()

		f, err := client.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
		if err != nil {
			ft.complete(fmt.Errorf("failed to open remote file: %w", err))
			return
		}

		if _, err := io.Copy(f, reader); err != nil {
			ft.complete(fmt.Errorf("failed to copy file: %w", err))
		}
		ft.complete(nil)
	}()

	return ft, nil
}

func (s *sftpFs) DownloadFile(ctx context.Context, writer io.Writer, path string, auth, params map[string]string) (FileTask, error) {
	sftpCtx, err := newSftpContext(ctx, auth, params)
	if err != nil {
		return nil, err
	}

	conn, err := s.newSshConn(sftpCtx)
	if err != nil {
		return nil, fmt.Errorf("ssh connection failure: %w", err)
	}

	ft := newFileTask(ctx)
	func() {
		defer conn.Close()

		client, err := sftp.NewClient(conn)
		if err != nil {
			ft.complete(fmt.Errorf("failed to create sftp client: %w", err))
			return
		}
		defer client.Close()

		f, err := client.OpenFile(path, os.O_RDONLY)
		if err != nil {
			ft.complete(fmt.Errorf("failed to open remote file: %w", err))
			return
		}

		if _, err := io.Copy(writer, f); err != nil {
			ft.complete(fmt.Errorf("failed to copy file: %w", err))
		}
		ft.complete(nil)
	}()

	return ft, nil
}
