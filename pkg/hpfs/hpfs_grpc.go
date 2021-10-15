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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/user"
	"path"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	protobuf "github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/alibaba/polardbx-operator/pkg/hpfs/discovery"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/local"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/proto"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/remote"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/task"
)

const (
	OpTransfer = "Transfer"
	OpDownload = "Download"
	OpUpload   = "Upload"
)

type rpcService struct {
	logr.Logger

	hostDiscovery    discovery.HostDiscovery
	localFileService local.LocalFileService
	taskEngine       task.Engine

	// Systemd style. Refer to https://github.com/kubernetes/kubernetes/blob/ea07644522/pkg/kubelet/cm/cgroup_manager_linux.go#L53.
	systemdStyleCgroup bool
}

func (r *rpcService) init() error {
	ok, err := r.localFileService.IsExists("/sys/fs/cgroup/blkio/kubepods.slice")
	if err != nil {
		return err
	}
	if ok {
		r.systemdStyleCgroup = true
	}
	return nil
}

func (r *rpcService) ok(msg string) *proto.Status {
	return &proto.Status{
		Code:    proto.Status_OK,
		Message: msg,
	}
}

func (r *rpcService) fail(err error) *proto.Status {
	code := proto.Status_UNKNOWN
	if os.IsExist(err) {
		code = proto.Status_EXISTS
	} else if os.IsNotExist(err) {
		code = proto.Status_NOT_EXIST
	} else if os.IsPermission(err) {
		code = proto.Status_PERMISSION_DENIED
	} else if err == task.ErrTaskNotFound {
		code = proto.Status_NOT_EXIST
	} else if err == task.ErrTaskCanceled {
		code = proto.Status_INVALID
	} else if err == task.ErrTaskAlreadyRun {
		code = proto.Status_INVALID
	} else if err == task.ErrTaskNotSubmitted {
		code = proto.Status_INVALID
	}

	return &proto.Status{
		Code:    code,
		Message: err.Error(),
	}
}

func (r *rpcService) invalid(msg string) *proto.Status {
	return &proto.Status{
		Code:    proto.Status_INVALID,
		Message: msg,
	}
}

func (r *rpcService) handleOwnerAndGroup(fileRequest *proto.FileRequest) error {
	uid, gid := -1, -1

	// Change gid and uid if specified
	if fileRequest.GetGroup() != nil {
		if _, ok := fileRequest.Group.(*proto.FileRequest_GroupName); ok {
			groupName := fileRequest.GetGroupName()
			group, err := user.LookupGroup(groupName)
			if err != nil {
				return err
			}
			gid, _ = strconv.Atoi(group.Gid)
		} else if _, ok := fileRequest.Group.(*proto.FileRequest_Gid); ok {
			gid = int(fileRequest.GetGid())
		}
	}
	if fileRequest.GetOwner() != nil {
		if _, ok := fileRequest.Owner.(*proto.FileRequest_User); ok {
			username := fileRequest.GetUser()
			u, err := user.Lookup(username)
			if err != nil {
				return err
			}
			uid, _ = strconv.Atoi(u.Uid)
		} else if _, ok := fileRequest.Owner.(*proto.FileRequest_Uid); ok {
			uid = int(fileRequest.GetUid())
		}
	}

	if uid >= 0 || gid >= 0 {
		return r.localFileService.Lchown(fileRequest.Path, uid, gid)
	}
	return nil
}

func (r *rpcService) CreateDirectory(ctx context.Context, request *proto.CreateDirectoryRequest) (*proto.CreateDirectoryResponse, error) {
	dir := request.Directory
	if dir == nil {
		return &proto.CreateDirectoryResponse{Status: r.invalid("dir is nil")}, nil
	}

	// Create directory with options
	var err error
	opts := request.Options
	if opts != nil && opts.CreateParentDirectories {
		err = r.localFileService.CreateDirectories(dir.Path, os.FileMode(dir.Mode))
	} else {
		err = r.localFileService.CreateDirectory(dir.Path, os.FileMode(dir.Mode))
	}

	if err != nil {
		if opts != nil && opts.IgnoreIfExists && os.IsExist(err) {
			return &proto.CreateDirectoryResponse{Status: r.ok("ignore exists")}, nil
		} else {
			return &proto.CreateDirectoryResponse{Status: r.fail(err)}, nil
		}
	}

	// Set owner and group if specified
	if err := r.handleOwnerAndGroup(dir); err != nil {
		return &proto.CreateDirectoryResponse{Status: r.fail(err)}, nil
	}

	return &proto.CreateDirectoryResponse{Status: r.ok("")}, nil
}

func (r *rpcService) CreateFile(ctx context.Context, request *proto.CreateFileRequest) (*proto.CreateFileResponse, error) {
	file := request.File
	if file == nil {
		return &proto.CreateFileResponse{Status: r.invalid("file is nil")}, nil
	}

	opts := request.Options

	exists, err := r.localFileService.IsExists(file.Path)
	if err != nil {
		return &proto.CreateFileResponse{Status: r.fail(err)}, nil
	}
	if exists {
		if opts != nil && opts.OverwriteIfExists {
			// Delete and recreate
			err := r.localFileService.DeleteFile(file.Path)
			if err != nil {
				return &proto.CreateFileResponse{Status: r.fail(err)}, nil
			}
			err = r.localFileService.CreateFile(file.Path, os.FileMode(file.Mode))
			if err != nil {
				return &proto.CreateFileResponse{Status: r.fail(err)}, nil
			}
		} else if opts != nil && opts.IgnoreIfExists {
			return &proto.CreateFileResponse{Status: r.ok("ignore exists")}, nil
		}
	} else {
		// Create file and handle errors
		err := r.localFileService.CreateFile(file.Path, os.FileMode(file.Mode))
		if err != nil {
			return &proto.CreateFileResponse{Status: r.fail(err)}, nil
		}
	}

	// Set the content of the file if specified
	if len(request.Content) > 0 {
		err = r.localFileService.WriteFile(file.Path, []byte(request.Content))
		if err != nil {
			return &proto.CreateFileResponse{Status: r.fail(err)}, nil
		}
	}

	// Set owner and group if specified
	if err := r.handleOwnerAndGroup(file); err != nil {
		return &proto.CreateFileResponse{Status: r.fail(err)}, nil
	}

	return &proto.CreateFileResponse{Status: r.ok("")}, nil
}

func (r *rpcService) CreateSymbolicLink(ctx context.Context, request *proto.CreateSymbolicLinkRequest) (*proto.CreateSymbolicLinkResponse, error) {
	linkPath := request.LinkPath
	if linkPath == nil {
		return &proto.CreateSymbolicLinkResponse{Status: r.invalid("file is nil")}, nil
	}

	opts := request.Options

	// Create file and handle errors
	err := r.localFileService.CreateSymlink(request.DestPath, linkPath.Path)
	if err != nil {
		if opts == nil {
			return &proto.CreateSymbolicLinkResponse{Status: r.fail(err)}, nil
		}
		if os.IsExist(err) {
			if opts.IgnoreIfExists {
				return &proto.CreateSymbolicLinkResponse{Status: r.ok("ignore exists")}, nil
			} else {
				return &proto.CreateSymbolicLinkResponse{Status: r.fail(err)}, nil
			}
		}
	}

	// Set owner and group if specified
	if err := r.handleOwnerAndGroup(linkPath); err != nil {
		return &proto.CreateSymbolicLinkResponse{Status: r.fail(err)}, nil
	}

	return &proto.CreateSymbolicLinkResponse{Status: r.ok("")}, nil
}

func (r *rpcService) RemoveDirectory(ctx context.Context, request *proto.RemoveDirectoryRequest) (*proto.RemoveDirectoryResponse, error) {
	opts := request.Options

	// Remove directory
	err := r.localFileService.DeleteDirectory(request.Path, opts != nil && opts.Recursive)
	if err != nil {
		if os.IsNotExist(err) && opts != nil && opts.IgnoreIfNotExists {
			return &proto.RemoveDirectoryResponse{Status: r.ok("ignore not exists")}, nil
		} else {
			return &proto.RemoveDirectoryResponse{Status: r.fail(err)}, nil
		}
	}

	return &proto.RemoveDirectoryResponse{Status: r.ok("")}, nil
}

func (r *rpcService) RemoveFile(ctx context.Context, request *proto.RemoveFileRequest) (*proto.RemoveFileResponse, error) {
	opts := request.Options

	// Remove file
	err := r.localFileService.DeleteFile(request.Path)
	if err != nil {
		if os.IsNotExist(err) && opts != nil && opts.IgnoreIfNotExists {
			return &proto.RemoveFileResponse{Status: r.ok("ignore not exists")}, nil
		} else {
			return &proto.RemoveFileResponse{Status: r.fail(err)}, nil
		}
	}

	return &proto.RemoveFileResponse{Status: r.ok("")}, nil
}

func (r *rpcService) TruncateFile(ctx context.Context, request *proto.TruncateFileRequest) (*proto.TruncateFileResponse, error) {
	opts := request.Options

	// Truncate file
	err := r.localFileService.TruncateFile(request.Path, 0)
	if err != nil {
		if os.IsNotExist(err) && opts != nil && opts.IgnoreIfNotExists {
			return &proto.TruncateFileResponse{Status: r.ok("ignore not exists")}, nil
		} else {
			return &proto.TruncateFileResponse{Status: r.fail(err)}, nil
		}
	}

	return &proto.TruncateFileResponse{Status: r.ok("")}, nil
}

func (r *rpcService) DownloadFiles(ctx context.Context, request *proto.DownloadRequest) (*proto.DownloadResponse, error) {
	// Check request
	if request.Tasks == nil || len(request.Tasks) == 0 {
		return &proto.DownloadResponse{Status: r.invalid("empty tasks")}, nil
	}
	for _, dt := range request.Tasks {
		if len(dt.Path) == 0 || dt.Source == nil {
			return &proto.DownloadResponse{Status: r.invalid("invalid task")}, nil
		}
	}

	if request.AsyncTask == nil || len(request.AsyncTask.TraceId) == 0 {
		// New trace id.
		request.AsyncTask = &proto.AsyncTaskRequest{TraceId: uuid.New().String()}
	}

	// Find task by trace id.
	t, err := r.taskEngine.Get(request.AsyncTask.TraceId)
	if err != nil {
		return &proto.DownloadResponse{
			Status: r.fail(err),
			Task:   &proto.AsyncTask{TraceId: request.AsyncTask.TraceId},
		}, nil
	}
	if t == nil {
		t = &task.Task{
			TraceId:   request.AsyncTask.TraceId,
			Operation: OpDownload,
			Details:   request.String(),
		}
	}

	// Submit the task to engine.
	if t, err = r.taskEngine.Submit(t); err != nil {
		return &proto.DownloadResponse{
			Status: r.fail(err),
			Task:   &proto.AsyncTask{TraceId: request.AsyncTask.TraceId},
		}, nil
	}

	// Submitted.
	return &proto.DownloadResponse{Status: r.ok(""), Task: &proto.AsyncTask{TraceId: request.AsyncTask.TraceId}}, nil
}

func (r *rpcService) UploadFiles(ctx context.Context, request *proto.UploadRequest) (*proto.UploadResponse, error) {
	// Check request
	if len(request.Path) == 0 {
		return &proto.UploadResponse{Status: r.invalid("invalid path")}, nil
	}
	if request.Target == nil || request.Target.Endpoint == nil {
		return &proto.UploadResponse{Status: r.invalid("invalid target")}, nil
	}

	if request.AsyncTask == nil || len(request.AsyncTask.TraceId) == 0 {
		// New trace id.
		request.AsyncTask = &proto.AsyncTaskRequest{TraceId: uuid.New().String()}
	}
	// Find task by trace id.
	t, err := r.taskEngine.Get(request.AsyncTask.TraceId)
	if err != nil {
		return &proto.UploadResponse{
			Status: r.fail(err),
			Task:   &proto.AsyncTask{TraceId: request.AsyncTask.TraceId},
		}, nil
	}
	if t == nil {
		t = &task.Task{
			TraceId:   request.AsyncTask.TraceId,
			Operation: OpUpload,
			Details:   request.String(),
		}
	}

	// Submit
	if t, err = r.taskEngine.Submit(t); err != nil {
		return &proto.UploadResponse{
			Status: r.fail(err),
			Task:   &proto.AsyncTask{TraceId: request.AsyncTask.TraceId},
		}, nil
	}

	// Submitted.
	return &proto.UploadResponse{Status: r.ok(""), Task: &proto.AsyncTask{TraceId: request.AsyncTask.TraceId}}, nil
}

func (r *rpcService) DeleteRemoteFile(ctx context.Context, request *proto.DeleteRemoteFileRequest) (*proto.DeleteRemoteFileResponse, error) {
	fs, err := remote.GetFileService(request.Target.Protocol)
	if err != nil {
		return &proto.DeleteRemoteFileResponse{Status: r.fail(err)}, nil
	}

	err = fs.DeleteFile(ctx, request.Target.Path, request.Target.Auth, request.Target.Other)
	if err != nil {
		return &proto.DeleteRemoteFileResponse{Status: r.fail(err)}, nil
	}

	return &proto.DeleteRemoteFileResponse{Status: r.ok("")}, nil
}

func (r *rpcService) TransferFiles(ctx context.Context, request *proto.TransferRequest) (*proto.TransferResponse, error) {
	// Check request
	if request.SrcHost == nil || request.DestHost == nil {
		return &proto.TransferResponse{Status: r.invalid("invalid src/dest host")}, nil
	}
	if len(request.SrcPath) == 0 || len(request.DestPath) == 0 {
		return &proto.TransferResponse{Status: r.invalid("invalid src/dest path")}, nil
	}

	if request.AsyncTask == nil || len(request.AsyncTask.TraceId) == 0 {
		// New trace id.
		request.AsyncTask = &proto.AsyncTaskRequest{TraceId: uuid.New().String()}
	}

	// Find task by trace id.
	t, err := r.taskEngine.Get(request.AsyncTask.TraceId)
	if err != nil {
		return &proto.TransferResponse{
			Status: r.fail(err),
			Task:   &proto.AsyncTask{TraceId: request.AsyncTask.TraceId},
		}, nil
	}
	if t == nil {
		t = &task.Task{
			TraceId:   request.AsyncTask.TraceId,
			Operation: OpTransfer,
			Details:   request.String(),
		}
	}

	// Submit
	if t, err = r.taskEngine.Submit(t); err != nil {
		if err == task.ErrTaskAlreadyRun || err == task.ErrTaskCanceled {
			return &proto.TransferResponse{
				Status: r.ok(err.Error()),
				Task:   &proto.AsyncTask{TraceId: request.AsyncTask.TraceId},
			}, nil
		}

		return &proto.TransferResponse{
			Status: r.fail(err),
			Task:   &proto.AsyncTask{TraceId: request.AsyncTask.TraceId},
		}, nil
	}

	// Submitted.
	return &proto.TransferResponse{Status: r.ok(""), Task: &proto.AsyncTask{TraceId: request.AsyncTask.TraceId}}, nil
}

var taskStatusMap = map[task.TaskStatus]proto.TaskStatus{
	task.Pending:   proto.TaskStatus_PENDING,
	task.Running:   proto.TaskStatus_RUNNING,
	task.Complete:  proto.TaskStatus_SUCCESS,
	task.Error:     proto.TaskStatus_FAILED,
	task.Canceling: proto.TaskStatus_CANCELING,
	task.Cancel:    proto.TaskStatus_CANCELED,
}

func (r *rpcService) ShowAsyncTaskStatus(ctx context.Context, request *proto.ShowAsyncTaskStatusRequest) (*proto.ShowAsyncTaskStatusResponse, error) {
	t, err := r.taskEngine.Get(request.Task.TraceId)
	if err != nil {
		return &proto.ShowAsyncTaskStatusResponse{Status: r.fail(err)}, nil
	}

	if t != nil {
		resp := &proto.ShowAsyncTaskStatusResponse{Status: r.ok("")}

		var ok bool
		resp.TaskStatus, ok = taskStatusMap[t.Status]
		if !ok {
			resp.TaskStatus = proto.TaskStatus_UNKNOWN
		}

		resp.Progress = float64(t.Progress)
		return resp, nil
	} else {
		return &proto.ShowAsyncTaskStatusResponse{
			Status: r.invalid("task not found"),
		}, nil
	}
}

func (r *rpcService) CancelAsyncTask(ctx context.Context, request *proto.CancelAsyncTaskRequest) (*proto.CancelAsyncTaskResponse, error) {
	t, err := r.taskEngine.Get(request.Task.TraceId)
	if err != nil {
		return &proto.CancelAsyncTaskResponse{Status: r.fail(err)}, nil
	}

	if t != nil {
		if err := r.taskEngine.Cancel(t); err != nil {
			return &proto.CancelAsyncTaskResponse{Status: r.fail(err)}, nil
		}
		return &proto.CancelAsyncTaskResponse{Status: r.ok("")}, nil
	} else {
		return &proto.CancelAsyncTaskResponse{Status: r.ok("task not found")}, nil
	}
}

func (r *rpcService) ShowDiskUsage(ctx context.Context, request *proto.ShowDiskUsageRequest) (*proto.ShowDiskUsageResponse, error) {
	usedBytes, err := r.localFileService.GetDiskUsage(request.Path)
	if err != nil {
		return &proto.ShowDiskUsageResponse{Status: r.fail(err)}, nil
	}

	return &proto.ShowDiskUsageResponse{Status: r.ok(""), Size: usedBytes}, nil
}

func (r *rpcService) ShowDiskInfo(ctx context.Context, request *proto.ShowDiskInfoRequest) (*proto.ShowDiskInfoResponse, error) {
	di, err := r.localFileService.GetDiskInfo(request.GetPath())
	if err != nil {
		return &proto.ShowDiskInfoResponse{Status: r.fail(err)}, err
	}

	return &proto.ShowDiskInfoResponse{
		Status: r.ok(""),
		Info: &proto.DiskInfo{
			Total:  di.Total,
			Free:   di.Free,
			Used:   di.Used,
			Files:  di.Files,
			FFree:  di.Ffree,
			FsType: di.FSType,
		},
	}, nil
}

func (r *rpcService) ListDirectory(ctx context.Context, request *proto.ListDirectoryRequest) (*proto.ListDirectoryResponse, error) {
	files, err := r.localFileService.ListDirectory(request.Path)
	if err != nil {
		return &proto.ListDirectoryResponse{Status: r.fail(err)}, nil
	}

	pFiles := make([]*proto.FileInfo, 0, len(files))
	for _, f := range files {
		pFiles = append(pFiles, &proto.FileInfo{
			Name:    f.Name(),
			Size:    uint64(f.Size()),
			Mode:    uint32(f.Mode()),
			ModTime: timestamppb.New(f.ModTime()),
			IsDir:   f.IsDir(),
		})
	}

	return &proto.ListDirectoryResponse{Status: r.ok(""), Files: pFiles}, nil
}

func (r *rpcService) executeAsyncTask(ctx context.Context, t *task.Task) error {
	logger := r.Logger.WithValues("operation", t.Operation)

	switch t.Operation {
	case OpTransfer:
		request := proto.TransferRequest{}
		err := protobuf.UnmarshalText(t.Details, &request)
		if err != nil {
			return err
		}
		return r.transferFiles(ctx, logger, &request)
	case OpDownload:
		panic("unsupported")
	case OpUpload:
		panic("unsupported")
	default:
		panic("unrecognized operation")
	}
}

func (r *rpcService) transferFiles(ctx context.Context, logger logr.Logger, request *proto.TransferRequest) error {
	// For debug purpose
	if request.SrcHost.NodeName == "sleep" {
		sleepSeconds, err := strconv.Atoi(request.SrcPath)
		if err != nil {
			return err
		}

		cmd := exec.CommandContext(ctx, "sleep", strconv.Itoa(sleepSeconds))
		return cmd.Run()
	}

	srcHost, err := r.hostDiscovery.GetHost(request.SrcHost.NodeName)
	if err != nil {
		return fmt.Errorf("failed to get src host info: %w", err)
	}

	srcPath, destPath := request.SrcPath, request.DestPath
	rsyncOpts := []string{
		"-a",
	}
	if !r.hostDiscovery.IsLocal(request.SrcHost.NodeName) {
		srcPath = fmt.Sprintf("root@%s:%s", srcHost.HpfsHost, request.SrcPath)
		rsyncOpts = append(rsyncOpts, "--rsh", fmt.Sprintf("ssh -o StrictHostKeyChecking=no -p %d", srcHost.SshPort))
	}
	rsyncOpts = append(rsyncOpts, srcPath, destPath)

	// Do execute rsync with context
	cmd := exec.CommandContext(ctx, "rsync", rsyncOpts...)
	return cmd.Run()
}

var blkioKeyFileMap = map[proto.BlkioKey]string{
	proto.BlkioKey_BPS_READ:   "blkio.throttle.read_bps_device",
	proto.BlkioKey_BPS_WRITE:  "blkio.throttle.write_bps_device",
	proto.BlkioKey_IOPS_READ:  "blkio.throttle.read_iops_device",
	proto.BlkioKey_IOPS_WRITE: "blkio.throttle.write_iops_device",
}

func (r *rpcService) getCgroupBlkioPath(podUid string) (string, error) {
	// Find the pod's blkio cgroups path.
	const cgroupRoot = "/sys/fs/cgroup"

	if r.systemdStyleCgroup {
		// Systemd style needs the replace.
		podUid = strings.ReplaceAll(podUid, "-", "_")

		kubePodBlkioCgroupPath := path.Join(cgroupRoot, "blkio", "kubepods.slice")
		kubePodsQosLevelPath := []string{"besteffort", "burstable", ""}
		podBlkioCgroupPath := ""
		for _, levelPath := range kubePodsQosLevelPath {
			subPath := "kubepods-" + levelPath + ".slice"
			endDir := "kubepods-" + levelPath + "-pod" + podUid + ".slice"
			if len(levelPath) == 0 {
				subPath = ""
				endDir = "kubepods-pod" + podUid + ".slice"
			}
			p := path.Join(kubePodBlkioCgroupPath, subPath, endDir)
			if ok, err := r.localFileService.IsExists(p); err != nil {

			} else if ok {
				podBlkioCgroupPath = p
				break
			}
		}

		return podBlkioCgroupPath, nil
	} else {
		kubePodBlkioCgroupPath := path.Join(cgroupRoot, "blkio", "kubepods")
		kubePodsQosLevelPath := []string{"besteffort", "burstable", ""}
		podBlkioCgroupPath := ""
		for _, levelPath := range kubePodsQosLevelPath {
			p := path.Join(kubePodBlkioCgroupPath, levelPath, "pod"+podUid)
			if ok, err := r.localFileService.IsExists(p); err != nil {

			} else if ok {
				podBlkioCgroupPath = p
				break
			}
		}

		return podBlkioCgroupPath, nil
	}
}

func (r *rpcService) ControlCgroupsBlkio(ctx context.Context, request *proto.ControlCgroupsBlkioRequest) (*proto.ControlCgroupsBlkioResponse, error) {
	// Check request
	if len(request.PodUid) == 0 {
		return &proto.ControlCgroupsBlkioResponse{Status: r.invalid("empty pod uid")}, nil
	}

	if request.Controls == nil || len(request.Controls) == 0 {
		return &proto.ControlCgroupsBlkioResponse{Status: r.ok("")}, nil
	}

	validateCtrl := func(ctrl *proto.BlkioCtrl) error {
		if ctrl.Device == nil {
			return errors.New("invalid device")
		}
		return nil
	}

	for _, ctrl := range request.Controls {
		if err := validateCtrl(ctrl); err != nil {
			return &proto.ControlCgroupsBlkioResponse{Status: r.fail(err)}, nil
		}
	}

	podBlkioCgroupPath, err := r.getCgroupBlkioPath(request.PodUid)
	if err != nil {
		return &proto.ControlCgroupsBlkioResponse{Status: r.fail(err)}, nil
	}
	if len(podBlkioCgroupPath) == 0 {
		return &proto.ControlCgroupsBlkioResponse{Status: r.invalid("pod's cgroup path not found")}, nil
	}

	r.Info("found pod's blkio cgroups path", "pod", request.PodUid, "blkio-path", podBlkioCgroupPath)

	// Get all devices' major:minor number
	getDeviceMajorMinorNumber := func(ctrl *proto.BlkioCtrl) (string, error) {
		s, ok := ctrl.GetDevice().(*proto.BlkioCtrl_MajorMinor)
		if ok {
			return s.MajorMinor, nil
		}

		deviceName, ok := ctrl.GetDevice().(*proto.BlkioCtrl_DeviceName)
		if ok {
			major, minor, err := r.localFileService.GetMajorMinorNumber(path.Join("/dev", deviceName.DeviceName), true)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%d:%d", major, minor), nil
		}

		p, ok := ctrl.GetDevice().(*proto.BlkioCtrl_Path)
		if ok {
			major, minor, err := r.localFileService.GetMajorMinorNumber(p.Path, true)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%d:%d", major, minor), nil
		}

		return "", errors.New("invalid device")
	}

	majorMinors := make([]string, 0, len(request.Controls))
	for _, ctrl := range request.Controls {
		majorMinor, err := getDeviceMajorMinorNumber(ctrl)
		if err != nil {
			return &proto.ControlCgroupsBlkioResponse{Status: r.ok("")}, nil
		}
		majorMinors = append(majorMinors, majorMinor)
	}

	// update cgroups blkio
	for i, ctrl := range request.Controls {
		if ctrl.Value < 0 {
			continue
		}
		cgroupFile := path.Join(podBlkioCgroupPath, blkioKeyFileMap[ctrl.Key])
		mm := majorMinors[i]
		// <major>:<minor> <limit>
		value := fmt.Sprintf("%s %d", mm, ctrl.Value)
		r.Info("writing blkio cgroups value", "pod", request.PodUid, "blkio-path", cgroupFile, "value", value)
		err := ioutil.WriteFile(cgroupFile, []byte(fmt.Sprintf("%s %d", mm, ctrl.Value)), 000)
		if err != nil {
			r.Error(err, "failed to write blkio cgroups value", "pod", request.PodUid, "blkio-path", cgroupFile, "value", value)
			return &proto.ControlCgroupsBlkioResponse{Status: r.fail(err)}, nil
		}
	}

	return &proto.ControlCgroupsBlkioResponse{Status: r.ok("")}, nil
}
