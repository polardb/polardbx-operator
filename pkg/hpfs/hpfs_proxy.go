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

package hpfs

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	protobuf "github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"

	"github.com/alibaba/polardbx-operator/pkg/hpfs/discovery"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/local"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/proto"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/task"

	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

type proxy struct {
	logr.Logger

	discovery.HostDiscovery

	local proto.HpfsServiceServer
}

func (p *proxy) GetWatcherInfoHash(ctx context.Context, request *proto.GetWatcherInfoHashRequest) (*proto.GetWatcherInfoHashResponse, error) {
	resp, err := p.executeOnHost(ctx, "GetWatcherInfoHash", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.GetWatcherInfoHash(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.GetWatcherInfoHash(ctx, request)
		},
	)
	if err != nil {
		return nil, err
	}
	return resp.(*proto.GetWatcherInfoHashResponse), err
}

func (p *proxy) OpenBackupBinlog(ctx context.Context, request *proto.OpenBackupBinlogRequest) (*proto.OpenBackupBinlogResponse, error) {
	resp, err := p.executeOnHost(ctx, "OpenBackupBinlog", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.OpenBackupBinlog(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.OpenBackupBinlog(ctx, request)
		},
	)
	if err != nil {
		return nil, err
	}
	return resp.(*proto.OpenBackupBinlogResponse), err
}

func (p *proxy) CloseBackupBinlog(ctx context.Context, request *proto.CloseBackupBinlogRequest) (*proto.CloseBackupBinlogResponse, error) {
	resp, err := p.executeOnHost(ctx, "CloseBackupBinlog", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.CloseBackupBinlog(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.CloseBackupBinlog(ctx, request)
		},
	)
	if err != nil {
		return nil, err
	}
	return resp.(*proto.CloseBackupBinlogResponse), err
}

func (p *proxy) UploadLatestBinlogFile(ctx context.Context, request *proto.UploadLatestBinlogFileRequest) (*proto.UploadLatestBinlogFileResponse, error) {
	resp, err := p.executeOnHost(ctx, "UploadLatestBinlogFile", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.UploadLatestBinlogFile(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.UploadLatestBinlogFile(ctx, request)
		},
	)
	if err != nil {
		return nil, err
	}
	return resp.(*proto.UploadLatestBinlogFileResponse), err
}

func (p *proxy) DeleteBinlogFilesBefore(ctx context.Context, request *proto.DeleteBinlogFilesBeforeRequest) (*proto.DeleteBinlogFilesBeforeResponse, error) {
	resp, err := p.executeOnHost(ctx, "DeleteBinlogFilesBefore", nil, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.DeleteBinlogFilesBefore(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.DeleteBinlogFilesBefore(ctx, request)
		},
	)
	if err != nil {
		return nil, err
	}
	return resp.(*proto.DeleteBinlogFilesBeforeResponse), err
}

func (p *proxy) executeOnHost(ctx context.Context, api string, host *proto.Host, request protobuf.Message,
	remoteFn func(c proto.HpfsServiceClient) (protobuf.Message, error),
	localFn func(s proto.HpfsServiceServer) (protobuf.Message, error)) (protobuf.Message, error) {
	log := p.WithValues("trace", uuid.New().String()).WithValues("api", api)
	log.Info("receive request", "request", request.String())

	if host == nil || p.IsLocal(host.NodeName) {
		// Execute on local service.
		resp, err := localFn(p.local)
		p.withResponse(log, resp, err).Info("return response")
		return resp, err
	} else {
		log.Info("not local, proxy request", "host", host.NodeName)

		// Execute on remote service.
		log = log.WithValues("proxy", true)
		host, err := p.GetHost(host.NodeName)
		if err != nil {
			return nil, fmt.Errorf("proxy failed: %w", err)
		}

		// New client and proxy the request and response
		conn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", host.HpfsHost, host.HpfsPort),
			grpc.WithInsecure())
		if err != nil {
			return nil, fmt.Errorf("proxy failed: %w", err)
		}
		defer conn.Close()
		c := proto.NewHpfsServiceClient(conn)

		resp, err := remoteFn(c)
		if err != nil {
			return nil, fmt.Errorf("proxy failed: %w", err)
		}

		p.withResponse(log, resp, err).Info("proxy: return response")
		return resp, err
	}
}

func (p *proxy) ControlCgroupsBlkio(ctx context.Context, request *proto.ControlCgroupsBlkioRequest) (*proto.ControlCgroupsBlkioResponse, error) {
	resp, err := p.executeOnHost(ctx, "ControlCgroupsBlkio", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.ControlCgroupsBlkio(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.ControlCgroupsBlkio(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.ControlCgroupsBlkioResponse), err
}

func (p *proxy) CancelAsyncTask(ctx context.Context, request *proto.CancelAsyncTaskRequest) (*proto.CancelAsyncTaskResponse, error) {
	resp, err := p.executeOnHost(ctx, "CancelAsyncTask", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.CancelAsyncTask(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.CancelAsyncTask(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.CancelAsyncTaskResponse), err
}

func (p *proxy) ShowAsyncTaskStatus(ctx context.Context, request *proto.ShowAsyncTaskStatusRequest) (*proto.ShowAsyncTaskStatusResponse, error) {
	resp, err := p.executeOnHost(ctx, "ShowAsyncTaskStatus", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.ShowAsyncTaskStatus(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.ShowAsyncTaskStatus(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.ShowAsyncTaskStatusResponse), err
}

func (p *proxy) DeleteRemoteFile(ctx context.Context, request *proto.DeleteRemoteFileRequest) (*proto.DeleteRemoteFileResponse, error) {
	resp, err := p.executeOnHost(ctx, "ShowAsyncTaskStatus", nil, request,
		nil,
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.DeleteRemoteFile(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.DeleteRemoteFileResponse), err
}

func (p *proxy) ListDirectory(ctx context.Context, request *proto.ListDirectoryRequest) (*proto.ListDirectoryResponse, error) {
	resp, err := p.executeOnHost(ctx, "ListDirectory", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.ListDirectory(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.ListDirectory(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.ListDirectoryResponse), err
}

func (p *proxy) withResponse(log logr.Logger, message protobuf.Message, err error) logr.Logger {
	if err != nil {
		return log.WithValues("err", err.Error())
	} else if message != nil {
		return log.WithValues("response", message.String())
	} else {
		return log
	}
}

func (p *proxy) CreateDirectory(ctx context.Context, request *proto.CreateDirectoryRequest) (*proto.CreateDirectoryResponse, error) {
	resp, err := p.executeOnHost(ctx, "CreateDirectory", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.CreateDirectory(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.CreateDirectory(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.CreateDirectoryResponse), err
}

func (p *proxy) CreateFile(ctx context.Context, request *proto.CreateFileRequest) (*proto.CreateFileResponse, error) {
	resp, err := p.executeOnHost(ctx, "CreateFile", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.CreateFile(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.CreateFile(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.CreateFileResponse), err
}

func (p *proxy) CreateSymbolicLink(ctx context.Context, request *proto.CreateSymbolicLinkRequest) (*proto.CreateSymbolicLinkResponse, error) {
	resp, err := p.executeOnHost(ctx, "CreateSymbolicLink", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.CreateSymbolicLink(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.CreateSymbolicLink(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.CreateSymbolicLinkResponse), err
}

func (p *proxy) RemoveDirectory(ctx context.Context, request *proto.RemoveDirectoryRequest) (*proto.RemoveDirectoryResponse, error) {
	resp, err := p.executeOnHost(ctx, "RemoveDirectory", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.RemoveDirectory(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.RemoveDirectory(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.RemoveDirectoryResponse), err
}

func (p *proxy) RemoveFile(ctx context.Context, request *proto.RemoveFileRequest) (*proto.RemoveFileResponse, error) {
	resp, err := p.executeOnHost(ctx, "RemoveFile", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.RemoveFile(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.RemoveFile(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.RemoveFileResponse), err
}

func (p *proxy) TruncateFile(ctx context.Context, request *proto.TruncateFileRequest) (*proto.TruncateFileResponse, error) {
	resp, err := p.executeOnHost(ctx, "TruncateFile", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.TruncateFile(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.TruncateFile(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.TruncateFileResponse), err
}

func (p *proxy) DownloadFiles(ctx context.Context, request *proto.DownloadRequest) (*proto.DownloadResponse, error) {
	resp, err := p.executeOnHost(ctx, "DownloadFiles", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.DownloadFiles(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.DownloadFiles(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.DownloadResponse), err
}

func (p *proxy) UploadFiles(ctx context.Context, request *proto.UploadRequest) (*proto.UploadResponse, error) {
	resp, err := p.executeOnHost(ctx, "UploadFiles", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.UploadFiles(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.UploadFiles(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.UploadResponse), err
}

func (p *proxy) TransferFiles(ctx context.Context, request *proto.TransferRequest) (*proto.TransferResponse, error) {
	resp, err := p.executeOnHost(ctx, "TransferFiles", request.DestHost, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.TransferFiles(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.TransferFiles(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.TransferResponse), err
}

func (p *proxy) ShowDiskUsage(ctx context.Context, request *proto.ShowDiskUsageRequest) (*proto.ShowDiskUsageResponse, error) {
	resp, err := p.executeOnHost(ctx, "ShowDiskUsage", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.ShowDiskUsage(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.ShowDiskUsage(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.ShowDiskUsageResponse), err
}

func (p *proxy) ShowDiskInfo(ctx context.Context, request *proto.ShowDiskInfoRequest) (*proto.ShowDiskInfoResponse, error) {
	resp, err := p.executeOnHost(ctx, "ShowDiskInfo", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.ShowDiskInfo(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.ShowDiskInfo(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.ShowDiskInfoResponse), err
}

func (p *proxy) ListLocalBinlogList(ctx context.Context, request *proto.ListLocalBinlogListRequest) (*proto.ListLocalBinlogListResponse, error) {
	resp, err := p.executeOnHost(ctx, "ListLocalBinlogList", request.Host, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.ListLocalBinlogList(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.ListLocalBinlogList(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.ListLocalBinlogListResponse), err
}

func (p *proxy) ListRemoteBinlogList(ctx context.Context, request *proto.ListRemoteBinlogListRequest) (*proto.ListRemoteBinlogListResponse, error) {
	resp, err := p.executeOnHost(ctx, "ListRemoteBinlogList", nil, request,
		func(c proto.HpfsServiceClient) (protobuf.Message, error) {
			return c.ListRemoteBinlogList(ctx, request)
		},
		func(s proto.HpfsServiceServer) (protobuf.Message, error) {
			return s.ListRemoteBinlogList(ctx, request)
		},
	)

	if err != nil {
		return nil, err
	}
	return resp.(*proto.ListRemoteBinlogListResponse), err
}

func NewHpfsServiceServer(hostDiscovery discovery.HostDiscovery, localFileService local.LocalFileService, taskManager task.Manager) proto.HpfsServiceServer {
	logger := zap.New(zap.UseDevMode(true))

	// Setup the hpfs service.
	hpfsService := &rpcService{
		Logger:           logger.WithName("hpfs"),
		hostDiscovery:    hostDiscovery,
		localFileService: localFileService,
	}
	if err := hpfsService.init(); err != nil {
		logger.Error(err, "unable to init")
		panic(err)
	}

	hpfsService.taskEngine = task.NewEngine(taskManager, hpfsService.executeAsyncTask,
		logger.WithName("task").WithName("engine"))

	// Recover old tasks.
	if err := hpfsService.taskEngine.Recover(); err != nil {
		logger.Error(err, "unable to recover")
		panic(fmt.Errorf("unable to recover: %w", err))
	}

	// Wrap with proxy and return.
	return &proxy{
		Logger:        logger.WithName("hpfs").WithName("proxy"),
		HostDiscovery: hostDiscovery,
		local:         hpfsService,
	}
}
