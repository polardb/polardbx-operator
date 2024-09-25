package importbackupset

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/filestream"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms/security"
	"github.com/alibaba/polardbx-operator/pkg/util/json"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"io"
	_ "modernc.org/sqlite"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	SQL_CREATE_UPLOAD_TASK_TABLE = `
CREATE TABLE IF NOT EXISTS upload_task(
    filepath varchar(4000) not null,
    content_hash varchar(4000) not null,
    status varchar(20) not null,
    created_at datetime default CURRENT_TIMESTAMP,
	updated_at datetime default CURRENT_TIMESTAMP,
    unique(filepath,content_hash)
)
`
	SQL_DELETE_NOT_FINISHED_TASK = "delete from upload_task where `status` != 'FINISHED'"
	SQL_INSERT                   = "insert or ignore into upload_task(filepath,content_hash,status) values(?,?,?)"
	SQL_UPDATE_STATUS            = "update upload_task set status = ? where filepath = ? and content_hash = ? "

	UPLOAD_TASK_STATUS_INIT     = "INIT"
	UPLOAD_TASK_STATUS_FINISHED = "FINISHED"
)

var (
	httpClient = &http.Client{}
)

type UploadObj struct {
	// Either DownloadURL or Content is not empty
	DownloadURL string `json:"downloadURL,omitempty"`
	Content     []byte `json:"content,omitempty"`
	Filepath    string `json:"filepath,omitempty"`
}

func (uo *UploadObj) getReader() (io.Reader, int64, func()) {
	if len(uo.Content) > 0 {
		return bytes.NewReader(uo.Content), int64(len(uo.Content)), nil
	}
	resp, err := httpClient.Get(uo.DownloadURL)
	if err != nil {
		panic(err)
	}
	return resp.Body, resp.ContentLength, func() {
		resp.Body.Close()
	}
}

func (uo *UploadObj) getSourceHash() string {
	hash := security.Hash(string(uo.Content) + uo.DownloadURL)
	return strconv.FormatInt(int64(hash), 10)
}

type Uploader interface {
	StartUploadServer() error
	StartUpload() error
	RecordUploadObj() error
}

type uploader struct {
	logger       logr.Logger
	inputParam   InputParam
	uploadObjs   []UploadObj
	toUploadObjs []UploadObj
	db           *sql.DB
}

func NewUploader(logger logr.Logger, inputParam InputParam, uploadObjs []UploadObj) Uploader {
	return &uploader{
		logger:     logger,
		inputParam: inputParam,
		uploadObjs: uploadObjs,
	}
}

func (u *uploader) StartUploadServer() error {
	filestreamConfig := u.inputParam.GetFileStreamConfig()
	// create flow control
	flowControl := filestream.NewFlowControl(filestream.FlowControlConfig{
		MaxFlow:    filestreamConfig.FlowControlMaxFlow,
		TotalFlow:  filestreamConfig.FlowControlTotalFLow,
		MinFlow:    filestreamConfig.FlowControlMinFlow,
		BufferSize: filestreamConfig.FlowControlBufferSize,
	})
	flowControl.Start()
	filestream.GlobalFlowControl = flowControl
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		u.logger.Info("Start filestream server", "listen port", filestreamConfig.FilestreamServerPort, "root path", filestreamConfig.FilestreamRootPath)
		config.SetConfig(config.Config{
			Sinks: []config.Sink{*(u.inputParam.GetSink())},
		})
		fileServer := filestream.NewFileServer("", filestreamConfig.FilestreamServerPort, filestreamConfig.FilestreamRootPath, filestream.GlobalFlowControl)
		wg.Done()
		err := fileServer.Start()
		if err != nil {
			u.logger.Error(err, "Failed to start file server")
			panic(err)
		}
	}()
	wg.Wait()
	u.logger.Info("wait 0.5 second for file stream server to start")
	time.Sleep(500 * time.Millisecond)
	return nil
}

func (u *uploader) StartUpload() error {
	u.logger.Info(fmt.Sprintf("toUploadObjs size is %d", len(u.toUploadObjs)))
	if len(u.toUploadObjs) == 0 {
		u.logger.Info("do not need to upload file")
		return nil
	}
	parallelism := u.inputParam.GetFileStreamConfig().Parallelism
	u.logger.Info("upload parallelism is " + strconv.FormatInt(int64(parallelism), 10))
	var wg sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		startIndex := i
		wg.Add(1)
		go func() {
			u.logger.Info("begin upload worker", "number", startIndex)
			for j := startIndex; j < len(u.toUploadObjs); j += parallelism {
				uploadObj := u.toUploadObjs[j]
				u.logger.Info("begin to upload obj ", "number", startIndex, "filepath", uploadObj.Filepath)
				u.upload(uploadObj)
				u.markUploadObjFinished(uploadObj)
				u.logger.Info("finish uploading obj ", "number", startIndex, "filepath", uploadObj.Filepath)
			}
			u.logger.Info("end upload worker", "number", startIndex)
			wg.Done()
		}()
	}
	u.logger.Info("wait for the work to finish")
	wg.Wait()
	time.Sleep(5 * time.Second)
	return nil
}

func (u *uploader) markUploadObjFinished(obj UploadObj) {
	u.getDB().Exec(SQL_UPDATE_STATUS, UPLOAD_TASK_STATUS_FINISHED, obj.Filepath, obj.getSourceHash())
}

func (u *uploader) upload(uploadObj UploadObj) {
	logger := u.logger.WithValues("action", "upload", "filepath", uploadObj.Filepath)
	fileStreamClient := filestream.NewFileClient("127.0.0.1", u.inputParam.GetFileStreamConfig().FilestreamServerPort, nil)
	uploadTypeMap := map[string]filestream.Action{
		config.SinkTypeNone:  filestream.UploadLocal,
		config.SinkTypeSftp:  filestream.UploadSsh,
		config.SinkTypeMinio: filestream.UploadMinio,
		config.SinkTypeOss:   filestream.UploadOss,
	}
	sinkType := u.inputParam.GetSink().Type
	action, ok := uploadTypeMap[sinkType]
	if !ok {
		panic("invalid sink type " + sinkType)
	}
	reader, size, callback := uploadObj.getReader()
	defer func() {
		if callback != nil {
			callback()
		}
	}()
	metadata := filestream.ActionMetadata{
		Action:    action,
		Filename:  uploadObj.Filepath,
		RequestId: uuid.New().String(),
		Sink:      u.inputParam.GetSink().Name,
	}
	if size < int64(200)*(1<<20) {
		metadata.MinioBufferSize = strconv.FormatInt(size, 10)
		metadata.OssBufferSize = strconv.FormatInt(size, 10)
	}
	logger.Info("filestream client request :  " + json.Convert2JsonString(metadata))

	logger.Info("get reader", "size", size)
	pReader, pWriter := io.Pipe()
	defer pReader.Close()
	go func() {
		var sent int64
		var lastSent int64
		lasterReportTime := time.Now()
		logger.Info("report upload process", "sent", sent, "totalSize", size, "process", fmt.Sprintf("%d%%", int(float64(sent)/float64(size)*100)))
		defer pWriter.Close()
		for {
			written, err := io.CopyN(pWriter, reader, 1<<20)
			sent += written
			now := time.Now()
			if now.Unix()-lasterReportTime.Unix() >= 30 {
				logger.Info("report upload process", "sent", sent, "totalSize", size, "process", fmt.Sprintf("%d%%", int(float64(sent)/float64(size)*100)))
				lasterReportTime = now
				if lastSent != 0 {
					logger.Info("report upload process", "speed(MB/s)", fmt.Sprintf("%.2f", float64(lastSent-sent)/float64(1<<20)))
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					logger.Info("report upload process", "sent", sent, "totalSize", size, "process", fmt.Sprintf("%d%%", int(float64(sent)/float64(size)*100)))
					break
				}
				logger.Error(err, "failed to copy from download url")
				panic(err)
			}
		}
	}()
	totalSent, err := fileStreamClient.Upload(pReader, metadata)
	if err != nil {
		logger.Error(err, "failed to upload", "totalSent", totalSent, "size", size)
		panic(err)
	}
	err = fileStreamClient.Check(metadata)
	if err != nil {
		logger.Error(err, "failed to upload check err", "totalSent", totalSent, "size", size)
		panic(err)
	}
	if totalSent != size {
		logger.Info("failed to upload", "totalSent", totalSent, "size", size)
		panic("failed to upload")
	}
}

func (u *uploader) getDB() *sql.DB {
	if u.db == nil {
		db, err := sql.Open("sqlite", u.inputParam.GetFileStreamConfig().RecordDBFilepath)
		if err != nil {
			u.logger.Error(err, "failed to open sqlite db file", "filepath", u.inputParam.GetFileStreamConfig().RecordDBFilepath)
			panic(err)
		}
		u.db = db
	}
	return u.db
}

func (u *uploader) RecordUploadObj() error {
	if len(u.uploadObjs) == 0 {
		u.logger.Info("no upload obj should be recorded")
		return nil
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*60)
	defer cancelFunc()
	conn, err := u.getDB().Conn(ctx)
	if err != nil {
		u.logger.Error(err, "failed to get db conn")
		panic(err)
	}
	defer conn.Close()
	// try to create table
	_, err = conn.ExecContext(ctx, SQL_CREATE_UPLOAD_TASK_TABLE)
	if err != nil {
		u.logger.Error(err, "failed to create table for record")
		panic(err)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		u.logger.Error(err, "failed to begin transaction")
		panic(err)
	}
	//delete all not finished task
	_, err = tx.ExecContext(ctx, SQL_DELETE_NOT_FINISHED_TASK)
	if err != nil {
		tx.Rollback()
		u.logger.Error(err, "failed exec sql : "+SQL_DELETE_NOT_FINISHED_TASK)
		panic(err)
	}
	//insert ignore record
	newUploadObjs := make([]UploadObj, 0)
	for _, uploadObj := range u.uploadObjs {
		u.logger.Info("to record upload obj", "filepath", uploadObj.Filepath, "content", string(uploadObj.Content), "downloadURL", uploadObj.DownloadURL, "hashCode", uploadObj.getSourceHash())
		result, err := tx.ExecContext(ctx, SQL_INSERT, uploadObj.Filepath, uploadObj.getSourceHash(), UPLOAD_TASK_STATUS_INIT)
		if err != nil {
			tx.Rollback()
			panic(err)
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			tx.Rollback()
			panic(err)
		}
		u.logger.Info("record upload obj, insert or ignore", "rowsAffected", rowsAffected, "filepath", uploadObj.Filepath, "hashCode", uploadObj.getSourceHash())
		if rowsAffected > 0 {
			newUploadObjs = append(newUploadObjs, uploadObj)
		}
	}
	tx.Commit()
	u.toUploadObjs = newUploadObjs
	return nil
}
