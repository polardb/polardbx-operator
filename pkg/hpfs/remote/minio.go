package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/common"
	polarxIo "github.com/alibaba/polardbx-operator/pkg/util/io"
	polarxPath "github.com/alibaba/polardbx-operator/pkg/util/path"
	"github.com/eapache/queue"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/tags"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	MinioLimitedReaderSize = 1 << 20 * 600 //600MB
	MinioMaxPartSize       = (1 << 30) * 5 //5GB
)

func init() {
	MustRegisterFileService("s3", &minioFs{})
}

type minioFs struct{}

type minioContext struct {
	ctx context.Context

	accessKey        string
	secretKey        string
	endpoint         string
	useSSL           bool
	bucket           string
	retentionTime    time.Duration
	writeLen         bool
	bufferSize       int64
	useTmpFile       bool
	deadline         int64
	bucketLookupType minio.BucketLookupType
}

func bucketLookupType2string(lookupType minio.BucketLookupType) string {
	switch lookupType {
	case minio.BucketLookupAuto:
		return "auto"
	case minio.BucketLookupDNS:
		return "dns"
	case minio.BucketLookupPath:
		return "path"
	default:
		return "auto"
	}
}
func string2bucketLookupType(lookupType string) minio.BucketLookupType {
	switch strings.ToLower(lookupType) {
	case "auto":
		return minio.BucketLookupAuto
	case "dns":
		return minio.BucketLookupDNS
	case "path":
		return minio.BucketLookupPath
	default:
		return minio.BucketLookupAuto
	}
}

func newMinioContext(ctx context.Context, auth, params map[string]string) (*minioContext, error) {
	var writeLen bool
	if val, ok := params["write_len"]; ok {
		toWriteLenVal, err := strconv.ParseBool(val)
		if err != nil {
			return nil, err
		}
		writeLen = toWriteLenVal
	}
	var useSSL bool = false
	if val, ok := auth["useSSL"]; ok {
		touseSSLVal, err := strconv.ParseBool(val)
		if err != nil {
			return nil, err
		}
		useSSL = touseSSLVal
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
	var bucketLookupType minio.BucketLookupType = string2bucketLookupType(params["bucket_lookup_type"])

	minioCtx := &minioContext{
		ctx:              ctx,
		endpoint:         auth["endpoint"],
		accessKey:        auth["access_key"],
		secretKey:        auth["secret_key"],
		bucket:           params["bucket"],
		useSSL:           useSSL,
		writeLen:         writeLen,
		bufferSize:       bufferSize,
		useTmpFile:       useTmpFile,
		deadline:         deadline,
		bucketLookupType: bucketLookupType,
	}

	if t, ok := params["retention-time"]; ok {
		d, err := time.ParseDuration(t)
		if err != nil {
			return nil, fmt.Errorf("format error for retention-time: %w", err)
		}
		minioCtx.retentionTime = d
	} else {
		minioCtx.retentionTime = 0
	}

	return minioCtx, nil
}

func (m *minioFs) newClient(minioCtx *minioContext) (*minio.Client, error) {
	return minio.New(minioCtx.endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(minioCtx.accessKey, minioCtx.secretKey, ""),
		Secure:       minioCtx.useSSL,
		BucketLookup: minioCtx.bucketLookupType,
	})
}
func (m *minioFs) newCore(minioCtx *minioContext) (*minio.Core, error) {
	return minio.NewCore(minioCtx.endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(minioCtx.accessKey, minioCtx.secretKey, ""),
		Secure:       minioCtx.useSSL,
		BucketLookup: minioCtx.bucketLookupType,
	})
}

func (m minioFs) DeleteFile(ctx context.Context, path string, auth, params map[string]string) error {
	minioCtx, err := newMinioContext(ctx, auth, params)
	if err != nil {
		return err
	}
	client, err := m.newCore(minioCtx)
	if err != nil {
		return fmt.Errorf("failed to create minio client: %w", err)
	}

	recursive, err := strconv.ParseBool(params["recursive"])
	if err != nil {
		return fmt.Errorf("invalid value for param 'recursive': %w", err)
	}
	if !recursive {
		// Only tend to delete a single object
		return client.RemoveObject(ctx, minioCtx.bucket, path, minio.RemoveObjectOptions{})
	}

	// Path points to a single file
	_, err = client.StatObject(ctx, minioCtx.bucket, path, minio.StatObjectOptions{})
	if err == nil {
		return client.RemoveObject(ctx, minioCtx.bucket, path, minio.RemoveObjectOptions{GovernanceBypass: true})
	}

	// Path may point to a directory (or does not exist)
	for marker := ""; ; {
		result, err := client.ListObjects(minioCtx.bucket, path, marker, "", 1000)
		if err != nil {
			return fmt.Errorf("failed to list objects in path '%s', error: '%w'", path, err)
		}

		objectsCh := make(chan minio.ObjectInfo)
		go func() {
			defer close(objectsCh)
			for _, objectInfo := range result.Contents {
				objectsCh <- objectInfo
			}
		}()

		errCh := client.RemoveObjects(ctx, minioCtx.bucket, objectsCh, minio.RemoveObjectsOptions{GovernanceBypass: true})
		for removeErr := range errCh {
			return fmt.Errorf("failed to delete object '%s', error: '%w'", removeErr.ObjectName, removeErr.Err)
		}

		if !result.IsTruncated {
			break
		}
		marker = result.NextMarker
	}
	return nil
}

func (m minioFs) DeleteExpiredFile(ctx context.Context, path string, auth, params map[string]string) (FileTask, error) {
	minioCtx, err := newMinioContext(ctx, auth, params)
	if err != nil {
		return nil, err
	}
	client, err := m.newCore(minioCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}
	ft := newFileTask(ctx)
	go func() {
		err := m.ListMinioFileWithDeadline(client, minioCtx, path, func(objs []string) error {
			for _, obj := range objs {
				err := client.RemoveObject(ctx, minioCtx.bucket, obj, minio.RemoveObjectOptions{GovernanceBypass: true})
				if err != nil {
					return err
				}
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

func (m *minioFs) UploadFile(ctx context.Context, reader io.Reader, path string, auth, params map[string]string) (FileTask, error) {
	minioCtx, err := newMinioContext(ctx, auth, params)
	// params["limit_reader_size"]: 客户端传来的文件大小或单个分段的最大大小
	// params["single_part_max_size"]: hpfs configmap 传入，单个分段的最大大小
	// MinioLimitedReaderSize: 单个分段的最大大小的安全值
	// 取三者中的最小值(>0)
	var limitReaderSize int64 = MinioLimitedReaderSize
	var val int64
	val, _ = strconv.ParseInt(params["limit_reader_size"], 10, 64)
	if val > 0 && val < limitReaderSize {
		limitReaderSize = val
	}
	val, _ = strconv.ParseInt(params["single_part_max_size"], 10, 64)
	if val > 0 && val < limitReaderSize {
		limitReaderSize = val
	}
	fmt.Printf("UploadFile: limitReaderSize = %d, path = %s\n", limitReaderSize, path)

	client, err := m.newCore(minioCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	ft := newFileTask(ctx)
	go func() {
		opts := minio.PutObjectOptions{}
		// Expires at specified time.
		if minioCtx.retentionTime > 0 {
			opts = minio.PutObjectOptions{RetainUntilDate: time.Now().Add(minioCtx.retentionTime)}
		}
		if err != nil {
			ft.complete(err)
			return
		}
		var partIndex int = 1
		uploadID, err := client.NewMultipartUpload(ctx, minioCtx.bucket, path, opts)
		if err != nil {
			ft.complete(err)
			return
		}
		defer func() {
			client.AbortMultipartUpload(ctx, minioCtx.bucket, path, uploadID)
		}()
		pipeReader, pipeWriter := io.Pipe()
		defer pipeReader.Close()
		var actualSize int64
		go func() {
			defer pipeWriter.Close()
			buff := make([]byte, minioCtx.bufferSize)
			written, err := io.CopyBuffer(pipeWriter, reader, buff)
			if written > 0 {
				if written%limitReaderSize != 0 {
					toFillBytes := limitReaderSize - (written % limitReaderSize)
					buff = make([]byte, minioCtx.bufferSize)
					for {
						if toFillBytes == 0 {
							break
						}
						if toFillBytes > minioCtx.bufferSize {
							pipeWriter.Write(buff)
							toFillBytes -= minioCtx.bufferSize
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
		partsInfo := make(map[int]minio.ObjectPart)
		emptyBytes := make([]byte, 0)
		for {
			_, err := pipeReader.Read(emptyBytes)
			if err != nil {
				break
			}
			limitedReader := io.LimitReader(pipeReader, limitReaderSize)
			uploadPart, err := client.PutObjectPart(ctx, minioCtx.bucket, path, uploadID, partIndex, limitedReader, limitReaderSize, "", "", nil)
			if errResponse, ok := err.(minio.ErrorResponse); ok {
				fmt.Printf("PutObjectPart Error: %s, bucketLookupType=%s, bucket=%s, path=%s, uploadID=%s, partIndex=%d, limitReaderSize=%d, remoteRequestID=%s\n",
					err, bucketLookupType2string(minioCtx.bucketLookupType), minioCtx.bucket, path, uploadID, partIndex, limitReaderSize, errResponse.RequestID)
			}
			partsInfo[partIndex] = uploadPart
			partIndex++
			uploadedLen += limitReaderSize
			if err != nil {
				break
			}
		}
		if len(partsInfo) > 0 {
			completeParts := make([]minio.CompletePart, 0)
			for i := 1; i < partIndex; i++ {
				part := partsInfo[i]
				completeParts = append(completeParts, minio.CompletePart{
					ETag:       part.ETag,
					PartNumber: part.PartNumber,
				})
			}
			_, err = client.CompleteMultipartUpload(ctx, minioCtx.bucket, path, uploadID, completeParts)
			if err != nil {
				if errResponse, ok := err.(minio.ErrorResponse); ok {
					fmt.Printf("CompleteMultipartUpload Error: %s, bucketLookupType=%s, bucket=%s, path=%s, uploadID=%s, remoteRequestID=%s, completeParts=%v\n",
						err, bucketLookupType2string(minioCtx.bucketLookupType), minioCtx.bucket, path, uploadID, errResponse.RequestID, completeParts)
				}
				ft.complete(err)
				return
			}
			totalSize := atomic.LoadInt64(&actualSize)
			SetMinioTags(client, ctx, minioCtx, path, actualSize)
			ft.complete(nil)

			var copyPosition int64
			pageNumber := 1
			copiedParts := make([]minio.CompletePart, 0)
			if totalSize%limitReaderSize != 0 {
				uploadID, err := client.NewMultipartUpload(ctx, minioCtx.bucket, path, opts)
				if err != nil {
					return
				}
				metadata := make(map[string]string)
				for {
					// SinglePartMaxSize is for multi-parts upload, copy parts uses MinioMaxPartSize
					partSize := totalSize - copyPosition
					if partSize >= MinioMaxPartSize {
						partSize = MinioMaxPartSize
					}
					copiedUploadPart, err := client.CopyObjectPart(ctx, minioCtx.bucket, path, minioCtx.bucket, path, uploadID,
						pageNumber, copyPosition, partSize, metadata)
					if err != nil {
						if errResponse, ok := err.(minio.ErrorResponse); ok {
							fmt.Printf("CopyObjectPart<copy> Error: %s, bucketLookupType=%s, srcBucket=%s, srcPath=%s, dstBucket=%s, dstPath=%s, uploadID=%s, partIndex=%d, startOffset=%d, length=%d, remoteRequestID=%s\n",
								err, bucketLookupType2string(minioCtx.bucketLookupType), minioCtx.bucket, path, minioCtx.bucket, path, uploadID, pageNumber, copyPosition, partSize, errResponse.RequestID)
						}
						return
					}
					pageNumber++
					copiedParts = append(copiedParts, copiedUploadPart)
					copyPosition += partSize
					if copyPosition == totalSize {
						_, err = client.CompleteMultipartUpload(ctx, minioCtx.bucket, path, uploadID, copiedParts)
						if err != nil {
							if errResponse, ok := err.(minio.ErrorResponse); ok {
								fmt.Printf("CompleteMultipartUpload<Copy> Error: %s, bucketLookupType=%s, bucket=%s, path=%s, uploadID=%s, remoteRequestID=%s, completeParts=%v\n",
									err, bucketLookupType2string(minioCtx.bucketLookupType), minioCtx.bucket, path, uploadID, errResponse.RequestID, copiedParts)
							}
							return
						}
						SetMinioTags(client, ctx, minioCtx, path, actualSize)
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

func SetMinioTags(client *minio.Core, ctx context.Context, minioCtx *minioContext, objectName string, actualSize int64) {
	tagMap := make(map[string]string)
	tagMap["uploader"] = "hpfs"
	tagMap["size"] = strconv.FormatInt(actualSize, 10)
	tagging, _ := tags.NewTags(tagMap, true)
	client.PutObjectTagging(ctx, minioCtx.bucket, objectName, tagging, minio.PutObjectTaggingOptions{})
}

func GetMinioActualSizeFromTags(ctx context.Context, client *minio.Client, minioCtx *minioContext, objKey string) int64 {
	hpfsUpload := false
	var size int64 = -1
	tagResult, err := client.GetObjectTagging(ctx, minioCtx.bucket, objKey, minio.GetObjectTaggingOptions{})
	if err != nil {
		return size
	}
	if tagResult.ToMap() != nil && len(tagResult.ToMap()) > 2 {
		sizeStr := ""
		for key, value := range tagResult.ToMap() {
			if key == "uploader" && value == "hpfs" {
				hpfsUpload = true
			}
			if key == "size" {
				sizeStr = value
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

func (m *minioFs) DownloadFile(ctx context.Context, writer io.Writer, path string, auth, params map[string]string) (FileTask, error) {
	minioCtx, err := newMinioContext(ctx, auth, params)
	if err != nil {
		return nil, err
	}
	client, err := m.newClient(minioCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}
	ft := newFileTask(ctx)
	go func() {
		var bytesCount int64
		if minioCtx.writeLen {
			r, err := client.StatObject(ctx, minioCtx.bucket, path, minio.StatObjectOptions{})
			if err != nil {
				ft.complete(fmt.Errorf("failed to get object meta: %w", err))
				return
			}
			bytesCount = r.Size
			actualSize := GetMinioActualSizeFromTags(ctx, client, minioCtx, path)
			if actualSize != -1 {
				bytesCount = actualSize
			}
			polarxIo.WriteUint64(writer, uint64(bytesCount))
		}
		r, err := client.GetObject(ctx, minioCtx.bucket, path, minio.GetObjectOptions{})
		if err != nil {
			ft.complete(fmt.Errorf("failed to get object: %w", err))
			return
		}
		if minioCtx.writeLen {
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

func (m *minioFs) ListFiles(ctx context.Context, writer io.Writer, path string, auth, params map[string]string) (FileTask, error) {
	minioCtx, err := newMinioContext(ctx, auth, params)
	if err != nil {
		return nil, err
	}
	client, err := m.newCore(minioCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	ft := newFileTask(ctx)
	go func() {
		entryNames := make([]string, 0)
		marker := ""
		prefix := path
		delimiter := "/"

		for {
			lsRes, err := client.ListObjects(minioCtx.bucket, prefix, marker, delimiter, 1000)
			if err != nil {
				ft.complete(fmt.Errorf("failed to list oss objects in path %s: %w", path, err))
				return
			}
			for _, object := range lsRes.Contents { // file
				entryNames = append(entryNames, polarxPath.GetBaseNameFromPath(object.Key))
			}
			for _, dir := range lsRes.CommonPrefixes { // subdirectory
				entryNames = append(entryNames, polarxPath.GetBaseNameFromPath(dir.Prefix))
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
		if minioCtx.writeLen {
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

func (m *minioFs) ListAllFiles(ctx context.Context, path string, auth, params map[string]string) (FileTask, error) {
	minioCtx, err := newMinioContext(ctx, auth, params)
	if err != nil {
		return nil, err
	}
	client, err := m.newCore(minioCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}
	ft := newFileTask(ctx)
	go func() {
		err := m.ListMinioFileWithDeadline(client, minioCtx, path, func(objs []string) error {
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

func (m *minioFs) ListMinioFileWithDeadline(client *minio.Core, minioCtx *minioContext, path string, callback func([]string) error) error {
	marker := ""
	delimiter := "/"
	fileQueue := queue.New()
	path = path + "/"
	fileQueue.Add([]string{path, marker})
	for fileQueue.Length() != 0 {
		element := fileQueue.Remove().([]string)
		lsRes, err := client.ListObjects(minioCtx.bucket, element[0], marker, delimiter, 1000)
		if err != nil {
			return err
		}
		if lsRes.IsTruncated {
			fileQueue.Add([]string{element[0], lsRes.NextMarker})
		}
		for _, commonPrefix := range lsRes.CommonPrefixes {
			fileQueue.Add([]string{commonPrefix.Prefix, ""})
		}
		objs := make([]string, 0)

		for _, obj := range lsRes.Contents {
			if obj.LastModified.Unix() < minioCtx.deadline {
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
