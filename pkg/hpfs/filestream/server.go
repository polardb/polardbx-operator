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
	"archive/tar"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/remote"
	polarxJson "github.com/alibaba/polardbx-operator/pkg/util/json"
	polarxMap "github.com/alibaba/polardbx-operator/pkg/util/map"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"io"
	"net"
	"os"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"strings"
	"sync"
	"time"
)

const (
	NetType           = "tcp4"
	MagicNumber       = 99966
	MagicNumberLen    = 4
	TaskStateDoing    = "Doing"
	TaskStateNotFound = "NotFound"
	TaskStateSuccess  = "Success"
	TaskStateFailed   = "Failed"
)

var OssParams = map[string]string{"write_len": "true"}

var TaskMap = sync.Map{}

type SftpConfigType struct {
	Host      string
	Port      int
	User      string
	Password  string
	Supported bool
}

type FileServer struct {
	host         string
	port         int
	fileRootPath string
	flowControl  FlowControl
	logger       logr.Logger
	listener     io.Closer
}

func NewFileServer(host string, port int, fileRootPath string, flowControl FlowControl) *FileServer {
	logger := zap.New(zap.UseDevMode(true))
	return &FileServer{
		host:         host,
		port:         port,
		fileRootPath: fileRootPath,
		flowControl:  flowControl,
		logger:       logger.WithName("FileServer").WithValues("host", host, "port", port),
	}
}

func (f *FileServer) Start() error {
	InitConfig()
	go func() {
		for {
			time.Sleep(70 * time.Second)
			ReloadConfig()
		}
	}()
	listen, err := net.Listen(NetType, fmt.Sprintf("%s:%d", f.host, f.port))
	if err != nil {
		f.logger.Error(err, "Failed to listen")
		return err
	}
	f.listener = listen
	defer listen.Close()
	for {
		conn, err := listen.Accept()
		if err != nil {
			f.logger.Error(err, "Failed to Accept, exit listen")
			return err
		}
		go f.handleRequest(conn)
	}
}

func (f *FileServer) Stop() {
	f.listener.Close()
}

func (f *FileServer) handleRequest(conn net.Conn) error {
	defer conn.Close()
	defer func() {
		err := recover()
		if err != nil {
			f.logger.Error(errors.New(polarxJson.Convert2JsonString(err)), "")
		}
	}()
	if err := f.checkMagicNumber(conn); err != nil {
		f.logger.Error(err, "Failed to check magic number")
		return err
	}
	metadata, err := f.readMetadata(conn)
	if err != nil {
		f.logger.Error(err, "Failed to read metadata")
		return err
	}
	logger := f.logger.WithValues("metadata", polarxJson.Convert2JsonString(metadata), "requestId", uuid.New().String())
	if strings.Contains(metadata.InstanceId, "/") || strings.Contains(metadata.InstanceId, "\\") {
		err := errors.New("invalid instanceId")
		logger.Error(err, "failed to check instanceId")
		return err
	}
	logger.Info("receive request")
	defer func() {
		go f.clearTaskLater(metadata)
	}()
	switch strings.ToLower(string(metadata.Action)) {
	case strings.ToLower(string(UploadLocal)):
		f.markTask(logger, metadata, TaskStateDoing)
		err := f.processUploadLocal(logger, metadata, conn)
		f.processTaskResult(err, metadata)
	case strings.ToLower(string(UploadRemote)):
		f.markTask(logger, metadata, TaskStateDoing)
		err := f.processUploadRemote(logger, metadata, conn)
		f.processTaskResult(err, metadata)
	case strings.ToLower(string(DownloadLocal)):
		f.processDownloadLocal(logger, metadata, conn)
	case strings.ToLower(string(DownloadRemote)):
		f.processDownloadRemote(logger, metadata, conn)
	case strings.ToLower(string(UploadOss)):
		f.markTask(logger, metadata, TaskStateDoing)
		err := f.processUploadOss(logger, metadata, conn)
		f.processTaskResult(err, metadata)
	case strings.ToLower(string(DownloadOss)):
		f.processDownloadOss(logger, metadata, conn)
	case strings.ToLower(string(UploadSsh)):
		f.markTask(logger, metadata, TaskStateDoing)
		err := f.processUploadSsh(logger, metadata, conn)
		f.processTaskResult(err, metadata)
	case strings.ToLower(string(DownloadSsh)):
		f.processDownloadSsh(logger, metadata, conn)
	case strings.ToLower(string(CheckTask)):
		f.processCheckTask(logger, metadata, conn)
	default:
		err := errors.New("invalid action" + string(metadata.Action))
		f.logger.Error(err, "")
	}
	logger.Info("finish request")
	return nil
}

func (f *FileServer) markTask(logger logr.Logger, metadata ActionMetadata, value string) {
	TaskMap.Store(metadata.RequestId, value)
}

func (f *FileServer) processTaskResult(err error, metadata ActionMetadata) {
	taskState := TaskStateSuccess
	if err != nil {
		taskState = TaskStateFailed
	}
	TaskMap.Store(metadata.RequestId, taskState)
}

func (f *FileServer) clearTaskLater(metadata ActionMetadata) {
	time.Sleep(1 * time.Minute)
	TaskMap.Delete(metadata.RequestId)
}

func (f *FileServer) processCheckTask(logger logr.Logger, metadata ActionMetadata, conn net.Conn) {
	for {
		val, ok := TaskMap.Load(metadata.RequestId)
		if !ok {
			conn.Write([]byte{1})
			logger.Error(errors.New("failed to load task"), "", "requestId", metadata.RequestId)
			return
		}
		if val == TaskStateDoing {
			time.Sleep(2 * time.Second)
			conn.Write([]byte{2})
			continue
		}
		if val == TaskStateSuccess {
			conn.Write([]byte{0})
			break
		}
		conn.Write([]byte{1})
		break
	}

}

func getSshConn(sink Sink) (*ssh.Client, error) {
	config := &ssh.ClientConfig{
		User: sink.User,
		Auth: []ssh.AuthMethod{
			ssh.Password(sink.Password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	sshConn, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", sink.Host, sink.Port), config)
	if err != nil {
		return nil, err
	}
	return sshConn, nil
}

func (f *FileServer) processUploadSsh(logger logr.Logger, metadata ActionMetadata, conn net.Conn) error {
	sink, err := GetSink(metadata.Sink, SinkTypeSftp)
	if err != nil {
		logger.Error(err, "fail to get sink", "sinkName", metadata.Sink)
		return err
	}
	if metadata.Filepath == "" {
		filepath := filepath.Join(sink.RootPath, metadata.InstanceId, metadata.Filename)
		metadata.Filepath = filepath
	}
	destFilepath := metadata.Filepath
	sshConn, err := getSshConn(*sink)
	if err != nil {
		logger.Error(err, "failed to get ssh conn")
		return err
	}
	defer sshConn.Close()
	client, err := sftp.NewClient(sshConn)
	if err != nil {
		return err
	}
	defer client.Close()
	if err != nil {
		logger.Error(err, "failed to get sftp client")
		return err
	}
	err = client.MkdirAll(filepath.Dir(destFilepath))
	if err != nil {
		logger.Error(err, "failed to MkDirAll", "destFilepath", destFilepath)
		return err
	}
	destFile, err := client.Create(destFilepath)
	if err != nil {
		logger.Error(err, "failed to create dest file", "destFilepath", destFilepath)
		return err
	}
	defer destFile.Close()
	len, err := f.flowControl.LimitFlow(conn, destFile, conn)
	logger.Info("limitFlow", "len", len)
	return nil
}

func (f *FileServer) processDownloadSsh(logger logr.Logger, metadata ActionMetadata, writer io.Writer) error {
	sink, err := GetSink(metadata.Sink, SinkTypeSftp)
	if err != nil {
		logger.Error(err, "fail to get sink", "sinkName", metadata.Sink)
		return err
	}
	if metadata.Filepath == "" {
		dirPath := filepath.Join(sink.RootPath, metadata.InstanceId)
		filepath := filepath.Join(dirPath, metadata.Filename)
		metadata.Filepath = filepath
	}
	destFilepath := metadata.Filepath
	sshConn, err := getSshConn(*sink)
	if err != nil {
		logger.Error(err, "failed to get ssh conn")
		return err
	}
	defer sshConn.Close()
	client, err := sftp.NewClient(sshConn)
	if err != nil {
		return err
	}
	defer client.Close()
	if err != nil {
		logger.Error(err, "failed to get sftp client")
		return err
	}
	if err != nil {
		logger.Error(err, "failed to get sftp client")
		return err
	}
	fd, err := client.OpenFile(destFilepath, os.O_RDONLY)
	if err != nil {
		logger.Error(err, "Failed to open file")
		return err
	}
	defer func() {
		fd.Close()
	}()
	fileInfo, err := client.Stat(metadata.Filepath)
	if err != nil {
		logger.Error(err, "Failed to stat file")
		return err
	}
	size := fileInfo.Size()
	sizeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeBytes, uint64(size))
	writer.Write(sizeBytes[:])
	len, _ := f.flowControl.LimitFlow(fd, writer, nil)
	logger.Info("limitFlow", "len", len)
	return nil
}

func getOssAuth(sink Sink) map[string]string {
	return map[string]string{
		"endpoint":      sink.Endpoint,
		"access_key":    sink.AccessKey,
		"access_secret": sink.AccessSecret,
	}
}

func (f *FileServer) processUploadOss(logger logr.Logger, metadata ActionMetadata, conn net.Conn) error {
	sink, err := GetSink(metadata.Sink, SinkTypeOss)
	if err != nil {
		logger.Error(err, "fail to get sink", "sinkName", metadata.Sink)
		return err
	}
	fileService, err := remote.GetFileService("aliyun-oss")
	if err != nil {
		logger.Error(err, "Failed to get file service of aliyun-oss")
		return err
	}
	if metadata.Filepath == "" {
		filepath := filepath.Join(metadata.InstanceId, metadata.Filename)
		metadata.Filepath = filepath
	}
	reader, writer := io.Pipe()
	defer func() {
		reader.Close()
		writer.Close()
	}()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			writer.Close()
			wg.Done()
		}()
		len, _ := f.flowControl.LimitFlow(conn, writer, conn)
		logger.Info("limitFlow", "len", len)
	}()
	ctx := context.Background()
	nowOssParams := polarxMap.MergeMap(map[string]string{
		"retention-time": metadata.RetentionTime,
	}, OssParams, false).(map[string]string)
	if nowOssParams["retention-time"] == "" {
		delete(nowOssParams, "retention-time")
	}
	if metadata.OssBufferSize != "" {
		nowOssParams["limit_reader_size"] = metadata.OssBufferSize
	}
	nowOssParams["bucket"] = sink.Bucket
	ossAuth := getOssAuth(*sink)
	ft, err := fileService.UploadFile(ctx, reader, metadata.Filepath, ossAuth, nowOssParams)
	if err != nil {
		logger.Error(err, "Failed to upload file to oss")
		return err
	}
	err = ft.Wait()
	reader.Close()
	if err != nil {
		logger.Error(err, "Failed to upload file to oss after wait")
		return err
	}
	wg.Wait()
	return nil
}

func (f *FileServer) processDownloadOss(logger logr.Logger, metadata ActionMetadata, conn net.Conn) error {
	sink, err := GetSink(metadata.Sink, SinkTypeOss)
	if err != nil {
		logger.Error(err, "fail to get sink", "sinkName", metadata.Sink)
		return err
	}
	fileService, err := remote.GetFileService("aliyun-oss")
	if err != nil {
		logger.Error(err, "Failed to get file service of aliyun-oss")
		return err
	}
	if metadata.Filepath == "" {
		filepath := filepath.Join(metadata.InstanceId, metadata.Filename)
		metadata.Filepath = filepath
	}
	reader, writer := io.Pipe()
	defer func() {
		writer.Close()
		reader.Close()
	}()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			reader.Close()
			wg.Done()
		}()
		len, _ := f.flowControl.LimitFlow(reader, conn, nil)
		logger.Info("limitFlow", "len", len)
	}()
	ctx := context.Background()
	nowOssParams := polarxMap.MergeMap(map[string]string{}, OssParams, false).(map[string]string)
	nowOssParams["bucket"] = sink.Bucket
	ossAuth := getOssAuth(*sink)
	ft, err := fileService.DownloadFile(ctx, writer, metadata.Filepath, ossAuth, nowOssParams)
	if err != nil {
		logger.Error(err, "Failed to download file from oss ")
		return err
	}
	err = ft.Wait()
	writer.Close()
	if err != nil {
		logger.Error(err, "Failed to download file from oss ")
		return err
	}
	wg.Wait()
	return nil
}

func (f *FileServer) processUploadRemote(logger logr.Logger, metadata ActionMetadata, conn net.Conn) error {
	host, port := ParseNetAddr(metadata.RedirectAddr)
	fileClient := NewFileClient(host, port, f.flowControl)
	remoteMetadata := metadata
	remoteMetadata.Action = ActionLocal2Remote2[metadata.Action]
	remoteMetadata.RedirectAddr = ""
	fileClient.returnConn = conn
	_, err := fileClient.Upload(conn, remoteMetadata)
	if err != nil {
		logger.Error(err, fmt.Sprintf("Failed to Upload remotely %s", metadata.RedirectAddr))
		return err
	}
	return nil
}

func (f *FileServer) processUploadLocal(logger logr.Logger, metadata ActionMetadata, conn net.Conn) error {
	dirPath := filepath.Join(f.fileRootPath, metadata.InstanceId)
	if err := f.createDirIfNotExists(dirPath); err != nil {
		logger.Error(err, "Failed to create dir ", "dirFilePath", dirPath)
		return err
	}

	switch metadata.Stream {
	case StreamNormal:
		return f.writeNormalFile(logger, dirPath, metadata, conn)
	case StreamTar:
		return f.writeTarFiles(logger, dirPath, metadata, conn)
	case StreamXBStream:
		return f.writeXBStreamFiles(logger, dirPath, metadata, conn)
	default:
		return errors.New("invalid stream")
	}

}

func (f *FileServer) writeNormalFile(logger logr.Logger, dirPath string, metadata ActionMetadata, conn net.Conn) error {
	filepath := filepath.Join(dirPath, metadata.Filename)
	fd, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer func() {
		fd.Close()
	}()
	len, err := f.flowControl.LimitFlow(conn, fd, conn)
	logger.Info("limitFlow", "len", len)
	return err
}

func (f *FileServer) writeTarFiles(logger logr.Logger, dirPath string, metadata ActionMetadata, conn net.Conn) error {
	dirFilePath := filepath.Join(dirPath, metadata.Filename)
	if err := f.createDirIfNotExists(dirFilePath); err != nil {
		logger.Error(err, "Failed to create dir ", "dirFilePath", dirPath)
		return err
	}
	pipeReader, pipeWriter := io.Pipe()
	defer pipeWriter.Close()

	go func() {
		defer pipeReader.Close()
		tr := tar.NewReader(pipeReader)
		copyBuffer := make([]byte, 1<<20)
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break // End of archive
			}
			if err != nil {
				logger.Error(err, "")
				break
			}
			headerFilePath := filepath.Join(dirFilePath, hdr.Name)
			logger.Info("Contents", "headerFilePath", headerFilePath)
			err = f.createDirIfNotExists(filepath.Dir(headerFilePath))
			if err != nil {
				logger.Error(err, "")
				break
			}
			fd, err := os.Create(headerFilePath)
			if err != nil {
				logger.Error(err, "")
				break
			}
			if _, err := io.CopyBuffer(fd, tr, copyBuffer); err != nil {
				fd.Close()
				logger.Error(err, "")
				break
			}
			fd.Close()
		}
	}()
	len, err := f.flowControl.LimitFlow(conn, pipeWriter, conn)
	logger.Info("limitFlow", "len", len)
	return err
}

func (f *FileServer) writeXBStreamFiles(logger logr.Logger, dirPath string, metadata ActionMetadata, conn net.Conn) error {
	dirFilePath := filepath.Join(dirPath, metadata.Filename)
	if err := f.createDirIfNotExists(dirFilePath); err != nil {
		logger.Error(err, "Failed to create dir ", "dirFilePath", dirPath)
		return err
	}
	pipeReader, pipeWriter := io.Pipe()
	defer pipeWriter.Close()

	go func() {
		defer pipeReader.Close()
		files := make(map[string]*os.File)
		defer func() {
			for _, val := range files {
				if val != nil {
					val.Close()
				}
			}
		}()
		var fallocate *bool
		path2File := map[string]*File{}
		defer func() {
			for _, v := range path2File {
				v.Close()
			}
		}()
		for {
			chunk := NewChunk(pipeReader)
			err := chunk.Read()
			if err == io.EOF {
				break // End of archive
			}
			if err != nil {
				logger.Error(err, "")
				break
			}

			chunkFilepath := filepath.Join(dirFilePath, chunk.path)
			if chunk.chunkType == XbChunkTypeUnknown {
				continue
			}
			file, ok := path2File[chunkFilepath]
			if !ok {
				err = f.createDirIfNotExists(filepath.Dir(chunkFilepath))
				if err != nil {
					logger.Error(err, "")
					break
				}
				file = NewFile(chunkFilepath)
				if err := file.Open(); err != nil {
					logger.Error(err, "Failed to open file", "chunkFilepath", chunkFilepath)
					break
				}
				path2File[chunkFilepath] = file
				if fallocate == nil {
					ret := IsFallocateSupported(chunkFilepath)
					if ret {
						logger.Info("Fallocate is supported")
					} else {
						logger.Info("Fallocate is not supported")
					}
					fallocate = &ret

				}
				file.FallocateSupported = *fallocate
			}
			if chunk.IsEof() {
				file, ok := path2File[chunkFilepath]
				if !ok {
					panic(fmt.Sprintf("Fail to get file obj %s", chunkFilepath))
				}
				file.Close()
				delete(path2File, chunkFilepath)
				continue
			}
			if chunk.chunkType == XbChunkTypePayload {
				err := file.Write(chunk.data, uint64(len(chunk.data)))
				if err != nil {
					logger.Error(err, "Failed to write", "chunkFilepath", chunkFilepath)
					break
				}
			} else if chunk.chunkType == XbChunkTypeSparse {
				err := file.WriteSparse(chunk.sparseMap, chunk.data)
				if err != nil {
					logger.Error(err, "Failed to write sparse", "chunkFilepath", chunkFilepath)
					break
				}
			}
		}
	}()
	len, _ := f.flowControl.LimitFlow(conn, pipeWriter, conn)
	logger.Info("limitFlow", "len", len)
	return nil
}

func (f *FileServer) createDirIfNotExists(dirPath string) error {
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return err
	}
	return nil
}

func (f *FileServer) processDownloadLocal(logger logr.Logger, metadata ActionMetadata, writer io.Writer) error {
	if metadata.Filepath == "" {
		dirPath := filepath.Join(f.fileRootPath, metadata.InstanceId)
		filepath := filepath.Join(dirPath, metadata.Filename)
		metadata.Filepath = filepath
	}
	fd, err := os.OpenFile(metadata.Filepath, os.O_RDONLY, 0664)
	if err != nil {
		logger.Error(err, "Failed to open file")
		return err
	}
	defer func() {
		fd.Close()
	}()
	fileInfo, err := os.Stat(metadata.Filepath)
	if err != nil {
		logger.Error(err, "Failed to stat file")
		return err
	}
	size := fileInfo.Size()
	sizeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeBytes, uint64(size))
	writer.Write(sizeBytes[:])
	len, _ := f.flowControl.LimitFlow(fd, writer, nil)
	logger.Info("limitFlow", "len", len)
	return nil
}

func (f *FileServer) processDownloadRemote(logger logr.Logger, metadata ActionMetadata, conn net.Conn) error {
	host, port := ParseNetAddr(metadata.RedirectAddr)
	fileClient := NewFileClient(host, port, f.flowControl)
	remoteMetadata := metadata
	remoteMetadata.Action = ActionLocal2Remote2[metadata.Action]
	remoteMetadata.redirect = true
	_, err := fileClient.Download(conn, remoteMetadata)
	if err != nil {
		logger.Error(err, fmt.Sprintf("Failed to download remotely %s", metadata.RedirectAddr))
		return err
	}
	return nil
}

func (f *FileServer) readNumber(reader io.Reader, byteLen int) (uint32, error) {
	bytes, err := ReadBytes(reader, uint64(byteLen))
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(bytes), nil
}

func (f *FileServer) checkMagicNumber(reader io.Reader) error {
	magicNumber, err := f.readNumber(reader, MagicNumberLen)
	if err != nil {
		return err
	}
	if magicNumber != MagicNumber {
		return errors.New("invalid magic number")
	}
	return nil
}

func (f *FileServer) readMetadata(reader io.Reader) (actionMeta ActionMetadata, err error) {
	metadataByteLen, err := f.readNumber(reader, MetaDataLenLen)
	if err != nil {
		return
	}
	bytes, err := ReadBytes(reader, uint64(metadataByteLen))
	if err != nil {
		return
	}
	metadata := strings.Split(string(bytes), ",")
	if len(metadata) != MetaFiledLen {
		err = errors.New("invalid metadata")
		return
	}
	actionMeta = ActionMetadata{
		Action:        Action(metadata[MetadataActionOffset]),
		InstanceId:    metadata[MetadataInstanceIdOffset],
		Filename:      metadata[MetadataFilenameOffset],
		RedirectAddr:  metadata[MetadataRedirectOffset],
		Filepath:      metadata[MetadataFilepathOffset],
		RetentionTime: metadata[MetadataRetentionTimeOffset],
		Stream:        metadata[MetadataStreamOffset],
		Sink:          metadata[MetadataSinkOffset],
		RequestId:     metadata[MetadataRequestIdOffset],
		OssBufferSize: metadata[MetadataOssBufferSizeOffset],
	}
	return
}
