package importbackupset

import (
	"encoding/json"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	"github.com/go-logr/logr"
	"os"
	"path/filepath"
)

const SINK_FILENAME = "sink.json"
const BACKUPSET_INFO_FILENAME = "backupset_info.json"
const FILESTREAM_CONFIG_FILENAME = "filestream.json"

type DnBinlog struct {
	Filename              string `json:"filename,omitempty"`
	FileSize              string `json:"fileSize,omitempty"`
	InnerDownloadUrl      string `json:"innerDownloadUrl,omitempty"`
	PubDownloadUrl        string `json:"pubDownloadUrl,omitempty"`
	BackupLinkExpiredTime string `json:"backupLinkExpiredTime,omitempty"`
	CheckType             string `json:"checkType,omitempty"`
	CheckSum              string `json:"checkSum,omitempty"`
	EndTime               string `json:"endTime,omitempty"`
	BeginTime             string `json:"beginTime,omitempty"`
}

type DnBackupSet struct {
	InnerFullDownloadUrl  string     `json:"innerFullDownloadUrl,omitempty"`
	PubFullDownloadUrl    string     `json:"pubFullDownloadUrl,omitempty"`
	DnName                string     `json:"dnName,omitempty"`
	XStoreUid             string     `json:"xstoreUid,omitempty"`
	TargetPod             string     `json:"targetPod,omitempty"`
	HostInstanceId        int        `json:"hostInstanceId,omitempty"`
	BackupLinkExpiredTime string     `json:"backupLinkExpiredTime,omitempty"`
	BackupEndTime         string     `json:"backupEndTime,omitempty"`
	Binlogs               []DnBinlog `json:"binlogs,omitempty"`
	BackupSetSize         int64      `json:"backupSetSize,omitempty"`
	CommitIndex           string     `json:"commitIndex,omitempty"`
}

type BackupSet struct {
	InsName                   string        `json:"insName,omitempty"`
	PxcUid                    string        `json:"pxcUid,omitempty"`
	BackupSetId               string        `json:"backupSetId,omitempty"`
	DnBackupSets              []DnBackupSet `json:"dnBackupSets,omitempty"`
	GmsBackupSet              DnBackupSet   `json:"gmsBackupSet,omitempty"`
	BackupRootPath            string        `json:"backupRootPath,omitempty"`
	CanBinlogRecoverToTime    int64         `json:"canBinlogRecoverToTime,omitempty"`
	CanBackupMinRecoverToTime int64         `json:"canBackupMinRecoverToTime,omitempty"`
	PitrInvalid               bool          `json:"pitrInvalid,omitempty"`
	BackupStartTime           string        `json:"backupStartTime,omitempty"`
	BackupEndTime             string        `json:"backupEndTime,omitempty"`
	CNImage                   string        `json:"cnImage,omitempty"`
	DNImage                   string        `json:"dnImage,omitempty"`
	CDCImage                  string        `json:"CDCImage,omitempty"`
}

type FileStreamConfig struct {
	FilestreamServerPort  int     `json:"filestreamServerPort,omitempty"`
	FilestreamRootPath    string  `json:"filestreamRootPath,omitempty"`
	FlowControlMinFlow    float64 `json:"flowControlMinFlow,omitempty"`
	FlowControlMaxFlow    float64 `json:"flowControlMaxFlow,omitempty"`
	FlowControlTotalFLow  float64 `json:"flowControlTotalFLow,omitempty"`
	FlowControlBufferSize int     `json:"flowControlBufferSize,omitempty"`
	// Parallelism defines the count of concurrent upload tasks
	Parallelism int `json:"parallelism,omitempty"`
	// RecordDBFilepath defines the sqlite db filepath to record upload task
	RecordDBFilepath string `json:"recordDBFilepath,omitempty"`
}

type InputParam interface {
	GetSink() *config.Sink
	GetBackupSet() *BackupSet
	GetFileStreamConfig() *FileStreamConfig
	LoadConfig() error
}

type inputParam struct {
	sink             *config.Sink
	backupSet        *BackupSet
	logger           logr.Logger
	configPath       string
	filestreamConfig *FileStreamConfig
}

func NewInputParam(logger logr.Logger, configPath string) InputParam {
	return &inputParam{
		logger:     logger,
		configPath: configPath,
	}
}

func (i *inputParam) GetSink() *config.Sink {
	return i.sink
}

func (i *inputParam) GetBackupSet() *BackupSet {
	return i.backupSet
}

func (i *inputParam) GetFileStreamConfig() *FileStreamConfig {
	return i.filestreamConfig
}

func (i *inputParam) LoadConfig() error {
	// check filepath for sink and backupset
	if err := i.checkFilepath(i.configPath); err != nil {
		return err
	}
	err := i.loadSink()
	if err != nil {
		i.logger.Error(err, "failed to load sink")
		return err
	}
	err = i.loadBackupSet()
	if err != nil {
		i.logger.Error(err, "failed to load backup set info")
		return err
	}
	err = i.loadFileStreamConfig()
	if err != nil {
		i.logger.Error(err, "failed to load filestream config")
		return err
	}
	return nil
}

func (i *inputParam) loadSink() error {
	sinkFilepath := filepath.Join(i.configPath, SINK_FILENAME)
	i.logger.Info("load sink... ", "filepath", sinkFilepath)
	bytes, err := os.ReadFile(sinkFilepath)
	if err != nil {
		i.logger.Error(err, "failed to read file", "filepath", sinkFilepath)
		return err
	}
	var sink config.Sink
	if err = json.Unmarshal(bytes, &sink); err != nil {
		i.logger.Error(err, "failed to parse json", "filepath", sinkFilepath)
		return err
	}
	i.sink = &sink
	i.logger.Info("finish loading sink...", "filepath", sinkFilepath)
	return nil
}

func (i *inputParam) loadBackupSet() error {
	backupSetInfoFilepath := filepath.Join(i.configPath, BACKUPSET_INFO_FILENAME)
	i.logger.Info("load backupset...", "filepath", backupSetInfoFilepath)
	bytes, err := os.ReadFile(backupSetInfoFilepath)
	if err != nil {
		i.logger.Error(err, "failed to read file", "filepath", backupSetInfoFilepath)
		return err
	}
	var backupSet BackupSet
	if err = json.Unmarshal(bytes, &backupSet); err != nil {
		i.logger.Error(err, "failed to parse json", "filepath", backupSetInfoFilepath)
		return err
	}
	i.backupSet = &backupSet
	i.logger.Info("finish loading backupset...", "filepath", backupSetInfoFilepath)
	return nil
}

func (i *inputParam) getDefaultFileStreamConfig() FileStreamConfig {
	return FileStreamConfig{
		FilestreamServerPort:  6543,
		FilestreamRootPath:    "/filestream",
		FlowControlBufferSize: (1 << 20) * 2,
		FlowControlMinFlow:    float64(1 << 20),
		FlowControlMaxFlow:    float64((1 << 20) * 20),
		FlowControlTotalFLow:  float64((1 << 20) * 50),
		Parallelism:           5,
		RecordDBFilepath:      filepath.Join(i.configPath, "upload.db"),
	}
}

func (i *inputParam) loadFileStreamConfig() error {
	filestreamConfigFilepath := filepath.Join(i.configPath, FILESTREAM_CONFIG_FILENAME)
	i.logger.Info("load filestream config...", "filepath", filestreamConfigFilepath)
	filestreamConfig := i.getDefaultFileStreamConfig()
	i.filestreamConfig = &filestreamConfig
	bytes, err := os.ReadFile(filestreamConfigFilepath)
	if err != nil {
		if os.IsNotExist(err) {
			i.logger.Info("the filestream config doest not exist, the default config will be used.")
			return nil
		}
		i.logger.Error(err, "failed to read file", "filepath", filestreamConfigFilepath)
		return err
	}
	if err = json.Unmarshal(bytes, &filestreamConfig); err != nil {
		i.logger.Error(err, "failed to parse json", "filepath", filestreamConfigFilepath)
		return err
	}
	i.logger.Info("finish loading filestream config...", "filepath", filestreamConfigFilepath)
	return nil
}

func (i *inputParam) checkFilepath(configPath string) error {
	checkFilepathExist := func(filename string) error {
		// check sink filepath
		sinkFilepath := filepath.Join(configPath, filename)
		_, err := os.Stat(sinkFilepath)
		if err != nil {
			i.logger.Error(err, fmt.Sprintf("failed to check filepath existence %s", sinkFilepath))
			return err
		}
		return nil
	}
	if err := checkFilepathExist(BACKUPSET_INFO_FILENAME); err != nil {
		return err
	}
	if err := checkFilepathExist(SINK_FILENAME); err != nil {
		return err
	}
	return nil
}
