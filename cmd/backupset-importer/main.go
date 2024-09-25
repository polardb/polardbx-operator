package main

import (
	"flag"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/pitr/importbackupset"
	"github.com/alibaba/polardbx-operator/pkg/util/json"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

/**
  a config directory path should be input. The directory contains three files whose content is a json object:
   - sink.json
   - backupset_info.json
   - filestream.json
*/

var (
	configDirPath string // the path of the config directory
)

const AppName = "backupset-importer"

func init() {
	flag.StringVar(&configDirPath, "conf", "/Users/busu/tmp/openbackup", "the path of the config directory")
	flag.Parse()
}

func main() {
	logger := zap.New(zap.UseDevMode(true)).WithName(AppName)
	logger.Info(fmt.Sprintf(" conf dir is %s", configDirPath))
	inputParam := importbackupset.NewInputParam(logger, configDirPath)
	if err := inputParam.LoadConfig(); err != nil {
		logger.Error(err, "failed to load config")
		panic(err)
	}
	backupSetUploadObjs := importbackupset.GenerateBackupSetUploadObjs(inputParam.GetBackupSet())
	logger.Info("generate backupset upload objs" + json.Convert2JsonString(backupSetUploadObjs))
	binlogUploadObjs := importbackupset.GenerateBinlogUploadObjs(inputParam.GetBackupSet())
	logger.Info("generate binlog upload objs" + json.Convert2JsonString(binlogUploadObjs))
	uploadObjs := make([]importbackupset.UploadObj, 0, len(backupSetUploadObjs)+len(binlogUploadObjs))
	uploadObjs = append(uploadObjs, backupSetUploadObjs...)
	uploadObjs = append(uploadObjs, binlogUploadObjs...)
	logger.Info("to upload objs ", "backupset upload obj size", len(backupSetUploadObjs), "binlog upload obj size", len(binlogUploadObjs))
	uploader := importbackupset.NewUploader(logger, inputParam, uploadObjs)
	logger.Info("record upload obj")
	uploader.RecordUploadObj()
	logger.Info("start upload server")
	uploader.StartUploadServer()
	logger.Info("succeeded to start upload server")
	logger.Info("start upload ...")
	uploader.StartUpload()
	logger.Info("finish uploading...")
}
