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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/common"
	polarxIo "github.com/alibaba/polardbx-operator/pkg/util/io"
	polarxPath "github.com/alibaba/polardbx-operator/pkg/util/path"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/eapache/queue"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	LimitedReaderSize = 1 << 20 * 600 //600MB
	MaxPartSize       = (1 << 30) * 5 //5GB
)

func init() {
	MustRegisterFileService("aliyun-oss", &aliyunOssFs{})
}

type aliyunOssFs struct{}

func (o *aliyunOssFs) newClient(ossCtx *aliyunOssContext) (*oss.Client, error) {
	return oss.New(ossCtx.endpoint, ossCtx.accessKey, ossCtx.accessSecret, oss.Timeout(10, 3600*2))
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

	recursive, err := strconv.ParseBool(params["recursive"])
	if err != nil {
		return fmt.Errorf("invalid value for param 'recursive': %w", err)
	}
	if !recursive {
		// Only tend to delete a single object
		return bucket.DeleteObject(path)
	}

	// Path points to a single object
	isFile, err := bucket.IsObjectExist(path)
	if err != nil {
		return fmt.Errorf("failed to check object: %w", err)
	}
	if isFile {
		return bucket.DeleteObject(path)
	}

	// Path may point to a directory (or does not exist)
	if path != "" && path[len(path)-1] != '/' { // The path may be trimmed
		path += "/"
	}
	marker := oss.Marker("")
	prefix := oss.Prefix(path)
	for {
		result, err := bucket.ListObjects(prefix, marker)
		if err != nil {
			return fmt.Errorf("failed to list object with prefix '%s', error: '%v'", path, err)
		}

		objectKeys := make([]string, 0, len(result.Objects))
		for _, object := range result.Objects {
			objectKeys = append(objectKeys, object.Key)
		}

		if len(objectKeys) > 0 { // Check to avoid 404 error
			_, err := bucket.DeleteObjects(objectKeys)
			if err != nil {
				return fmt.Errorf("failed to delete objects '%s', error: '%v'", objectKeys, err)
			}
		}

		if !result.IsTruncated { // Finish listing
			break
		}
		marker = oss.Marker(result.NextMarker)
	}
	return nil
}

func (o aliyunOssFs) DeleteExpiredFile(ctx context.Context, path string, auth, params map[string]string) (FileTask, error) {
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
		err := o.ListFileWithDeadline(path, bucket, ossCtx.deadline, func(objs []string) error {
			_, err := bucket.DeleteObjects(objs)
			if err != nil {
				return err
			}
			for _, obj := range objs {
				if val, ok := ctx.Value(common.AffectedFiles).(*[]string); ok {
					*val = append(*val, obj)
				}
			}
			return nil
		})
		ft.complete(err)
	}()
	return ft, nil
}

type ossProgressListener4FileTask struct {
	*fileTask
}

func (l *ossProgressListener4FileTask) ProgressChanged(event *oss.ProgressEvent) {
	l.progress = int32(event.ConsumedBytes * 100 / event.TotalBytes)
}

func (o *aliyunOssFs) UploadFileNormally(ctx context.Context, reader io.Reader, path string, auth, params map[string]string) (FileTask, error) {
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

func (o *aliyunOssFs) UploadFile(ctx context.Context, reader io.Reader, path string, auth, params map[string]string) (FileTask, error) {
	ossCtx, err := newAliyunOssContext(ctx, auth, params)
	var limitReaderSize int64 = LimitedReaderSize
	val, ok := params["limit_reader_size"]
	if ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			panic(err)
		}
		limitReaderSize = parsedVal
	}
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
		if err != nil {
			ft.complete(err)
			return
		}

		var partIndex int = 1
		imur, err := bucket.InitiateMultipartUpload(path, opts...)
		if err != nil {
			ft.complete(err)
			return
		}
		//complete := false
		defer func() {
			bucket.AbortMultipartUpload(imur)
		}()
		pipeReader, pipeWriter := io.Pipe()
		defer pipeReader.Close()
		var actualSize int64
		go func() {
			defer pipeWriter.Close()
			buff := make([]byte, ossCtx.bufferSize)
			written, err := io.CopyBuffer(pipeWriter, reader, buff)
			if written > 0 {
				if written%limitReaderSize != 0 {
					toFillBytes := limitReaderSize - (written % limitReaderSize)
					buff = make([]byte, ossCtx.bufferSize)
					for {
						if toFillBytes == 0 {
							break
						}
						if toFillBytes > ossCtx.bufferSize {
							pipeWriter.Write(buff)
							toFillBytes -= ossCtx.bufferSize
						} else {
							pipeWriter.Write(buff[:toFillBytes])
							toFillBytes -= toFillBytes
						}
					}
				}
				atomic.StoreInt64(&actualSize, written)
			}
			if err != nil {
				ft.complete(err)
				return
			}
		}()
		var uploadedLen int64
		parts := make([]oss.UploadPart, 0)
		emptyBytes := make([]byte, 0)
		for {
			_, err := pipeReader.Read(emptyBytes)
			if err != nil {
				break
			}
			limitedReader := io.LimitReader(pipeReader, limitReaderSize)
			uploadPart, err := bucket.UploadPart(imur, limitedReader, limitReaderSize, partIndex, opts...)
			partIndex++
			uploadedLen += limitReaderSize
			if err != nil {
				break
			}
			parts = append(parts, uploadPart)
		}

		if len(parts) > 0 {
			_, err = bucket.CompleteMultipartUpload(imur, parts, opts...)
			if err != nil {
				ft.complete(err)
				return
			}
			totalSize := atomic.LoadInt64(&actualSize)
			SetTags(bucket, path, actualSize)
			ft.complete(nil)
			var copyPosition int64
			pageNumber := 1
			copiedParts := make([]oss.UploadPart, 0)
			if totalSize%limitReaderSize != 0 {
				imur, err = bucket.InitiateMultipartUpload(path, opts...)
				if err != nil {
					return
				}
				for {
					partSize := totalSize - copyPosition
					if partSize >= MaxPartSize {
						partSize = MaxPartSize
					}
					copiedUploadPart, err := bucket.UploadPartCopy(imur, bucket.BucketName, path, copyPosition, partSize, pageNumber, opts...)
					if err != nil {
						return
					}
					pageNumber++
					copiedParts = append(copiedParts, copiedUploadPart)
					copyPosition += partSize
					if copyPosition == totalSize {
						_, err = bucket.CompleteMultipartUpload(imur, copiedParts, opts...)
						if err != nil {
							return
						}
						SetTags(bucket, path, actualSize)
						break
					}
				}
			}
		} else {
			ft.complete(nil)
		}
	}()
	return ft, nil
}

func SetTags(bucket *oss.Bucket, objKey string, actualSize int64) {
	uploaderTag := oss.Tag{
		Key:   "uploader",
		Value: "hpfs",
	}
	sizeTag := oss.Tag{
		Key:   "size",
		Value: strconv.FormatInt(actualSize, 10),
	}
	tagging := oss.Tagging{
		Tags: []oss.Tag{uploaderTag, sizeTag},
	}
	bucket.PutObjectTagging(objKey, tagging)
}

func GetActualSizeFromTags(bucket *oss.Bucket, objKey string) int64 {
	hpfsUpload := false
	var size int64 = -1
	tagResult, err := bucket.GetObjectTagging(objKey)
	if err != nil {
		return size
	}
	if tagResult.Tags != nil && len(tagResult.Tags) > 2 {
		sizeStr := ""
		for _, tag := range tagResult.Tags {
			if tag.Key == "uploader" && tag.Value == "hpfs" {
				hpfsUpload = true
			}
			if tag.Key == "size" {
				sizeStr = tag.Value
			}
		}
		if hpfsUpload && sizeStr != "" {
			size, err = strconv.ParseInt(sizeStr, 10, 64)
			if err != nil {
				size = -1
			}
		}
	}
	if !hpfsUpload {
		size = -1
	}
	return size
}

func (o *aliyunOssFs) UploadFileTmpFile(ctx context.Context, reader io.Reader, path string, auth, params map[string]string) (FileTask, error) {
	ossCtx, err := newAliyunOssContext(ctx, auth, params)
	if err != nil {
		return nil, err
	}
	if !ossCtx.useTmpFile {
		return o.UploadFileNormally(ctx, reader, path, auth, params)
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
		imur, err := bucket.InitiateMultipartUpload(path, opts...)
		complete := false
		if err != nil {
			ft.complete(err)
			return
		}
		defer func() {
			if !complete {
				bucket.AbortMultipartUpload(imur, opts...)
			}
		}()

		parts := make([]oss.UploadPart, 0)

		tmpFileChan := make(chan string)
		tmpFileDir := filepath.Join("/tmp/oss", ossCtx.bucket)
		err = os.MkdirAll(tmpFileDir, os.ModePerm)
		if err != nil {
			ft.complete(err)
			return
		}
		defer func() {
			os.RemoveAll(tmpFileDir)
		}()
		go func() {
			defer close(tmpFileChan)
			fileIndex := 0
			var writeTmpFile *os.File
			defer func() {
				if writeTmpFile != nil {
					writeTmpFile.Close()
				}
			}()
			for {
				tempFilePath := filepath.Join(tmpFileDir, strconv.FormatInt(int64(fileIndex), 10))
				writeTmpFile, err = os.OpenFile(tempFilePath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
				fileIndex++
				if err != nil {
					ft.complete(err)
					return
				}
				copiedNum, _ := io.CopyN(writeTmpFile, reader, ossCtx.bufferSize)
				writeTmpFile.Close()
				writeTmpFile = nil
				if copiedNum > 0 {
					tmpFileChan <- tempFilePath
				}
				if copiedNum < ossCtx.bufferSize {
					return
				}
			}
		}()
		var readTmpFile *os.File
		defer func() {
			if readTmpFile != nil {
				readTmpFile.Close()
			}
		}()
		for {
			tempFilePath, ok := <-tmpFileChan
			if !ok {
				break
			}
			fileInfo, err := os.Stat(tempFilePath)
			if err != nil {
				ft.complete(err)
				return
			}
			readTmpFile, err = os.OpenFile(tempFilePath, os.O_RDWR, 0644)
			if err != nil {
				ft.complete(err)
				return
			}
			uploadPart, err := bucket.UploadPart(imur, readTmpFile, fileInfo.Size(), len(parts)+1, opts...)
			readTmpFile.Close()
			readTmpFile = nil
			os.Remove(tempFilePath)
			if err != nil {
				ft.complete(err)
				return
			}
			parts = append(parts, uploadPart)
		}
		_, err = bucket.CompleteMultipartUpload(imur, parts, opts...)
		if err != nil {
			complete = true
		}
		ft.complete(err)
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
		var bytesCount int64
		if ossCtx.writeLen {
			r, err := bucket.GetObjectMeta(path)
			if err != nil {
				ft.complete(fmt.Errorf("failed to get object meta: %w", err))
				return
			}
			contentLength := r.Get("Content-Length")
			bytesCount, _ = strconv.ParseInt(contentLength, 10, 64)
			actualSize := GetActualSizeFromTags(bucket, path)
			if actualSize != -1 {
				bytesCount = actualSize
			}
			polarxIo.WriteUint64(writer, uint64(bytesCount))
		}

		r, err := bucket.GetObject(path)
		if err != nil {
			ft.complete(fmt.Errorf("failed to get object: %w", err))
			return
		}
		if ossCtx.writeLen {
			_, err = io.CopyN(writer, r, bytesCount)
		} else {
			_, err = io.Copy(writer, r)
		}
		if err != nil {
			ft.complete(fmt.Errorf("failed to copy content: %w", err))
			return
		}
		ft.complete(nil)
	}()

	return ft, nil
}

func (o *aliyunOssFs) ListFiles(ctx context.Context, writer io.Writer, path string, auth, params map[string]string) (FileTask, error) {
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
		// list entries in path use oss sdk
		entryNames := make([]string, 0)
		marker := ""
		prefix := oss.Prefix(path)
		delimiter := oss.Delimiter("/")
		for {
			lsRes, err := bucket.ListObjects(oss.Marker(marker), prefix, delimiter)
			if err != nil {
				ft.complete(fmt.Errorf("failed to list oss objects in path %s: %w", path, err))
				return
			}
			for _, object := range lsRes.Objects { // file
				entryNames = append(entryNames, polarxPath.GetBaseNameFromPath(object.Key))
			}
			for _, dir := range lsRes.CommonPrefixes { // subdirectory
				entryNames = append(entryNames, polarxPath.GetBaseNameFromPath(dir))
			}
			if lsRes.IsTruncated {
				marker = lsRes.NextMarker
			} else {
				break
			}
		}

		// parse entry slice and send response
		encodedEntryNames, err := json.Marshal(entryNames)
		if err != nil {
			ft.complete(fmt.Errorf("failed to encode entry name slice,: %w", err))
			return
		}
		if ossCtx.writeLen {
			bytesCount := int64(len(encodedEntryNames))
			err := polarxIo.WriteUint64(writer, uint64(bytesCount))
			if err != nil {
				ft.complete(fmt.Errorf("failed to send content bytes count: %w", err))
				return
			}
			_, err = io.CopyN(writer, bytes.NewReader(encodedEntryNames), bytesCount)
		} else {
			_, err = io.Copy(writer, bytes.NewReader(encodedEntryNames))
		}
		if err != nil {
			ft.complete(fmt.Errorf("failed to copy content: %w", err))
			return
		}
		ft.complete(nil)
	}()
	return ft, nil
}

func (o *aliyunOssFs) ListFileWithDeadline(path string, bucket *oss.Bucket, deadline int64, callback func([]string) error) error {
	marker := ""
	delimiter := oss.Delimiter("/")
	fileQueue := queue.New()
	fileQueue.Add([]string{path, marker})
	for fileQueue.Length() != 0 {
		element := fileQueue.Remove().([]string)
		lsRes, err := bucket.ListObjects(oss.Marker(element[1]), oss.Prefix(element[0]), delimiter)
		if err != nil {
			return err
		}
		if lsRes.IsTruncated {
			fileQueue.Add([]string{element[0], lsRes.NextMarker})
		}
		for _, commonPrefix := range lsRes.CommonPrefixes {
			fileQueue.Add([]string{commonPrefix, ""})
		}
		objs := make([]string, 0)
		for _, obj := range lsRes.Objects {
			if obj.LastModified.Unix() < deadline {
				// delete it
				objs = append(objs, obj.Key)
			}
		}
		if len(objs) > 0 {
			err := callback(objs)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *aliyunOssFs) ListAllFiles(ctx context.Context, path string, auth, params map[string]string) (FileTask, error) {
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
		err := o.ListFileWithDeadline(path, bucket, ossCtx.deadline, func(objs []string) error {
			for _, obj := range objs {
				if val, ok := ctx.Value(common.AffectedFiles).(*[]string); ok {
					*val = append(*val, obj)
				}
			}
			return nil
		})
		ft.complete(err)
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
	writeLen      bool
	bufferSize    int64
	useTmpFile    bool
	deadline      int64
}

func newAliyunOssContext(ctx context.Context, auth, params map[string]string) (*aliyunOssContext, error) {
	var writeLen bool
	if val, ok := params["write_len"]; ok {
		toWriteLenVal, err := strconv.ParseBool(val)
		if err != nil {
			return nil, err
		}
		writeLen = toWriteLenVal
	}
	var bufferSize int64 = 1 << 20 * 50 //50MB
	if val, ok := params["buffer_size"]; ok {
		toBufferSize, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, err
		}
		bufferSize = toBufferSize
	}
	var useTmpFile bool = true
	if val, ok := params["use_tmp_file"]; ok {
		toUseTmpFile, err := strconv.ParseBool(val)
		if err != nil {
			return nil, err
		}
		useTmpFile = toUseTmpFile
	}
	var deadline int64 = 0
	if val, ok := params["deadline"]; ok {
		parsedDeadline, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, err
		}
		deadline = parsedDeadline
	}
	ossCtx := &aliyunOssContext{
		ctx:          ctx,
		endpoint:     auth["endpoint"],
		accessKey:    auth["access_key"],
		accessSecret: auth["access_secret"],
		bucket:       params["bucket"],
		writeLen:     writeLen,
		bufferSize:   bufferSize,
		useTmpFile:   useTmpFile,
		deadline:     deadline,
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
