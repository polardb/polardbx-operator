package backupbinlog

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog"
	binlogEvent "github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
	. "github.com/alibaba/polardbx-operator/pkg/hpfs/common"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/filestream"
	"github.com/google/uuid"
	"io"
	"k8s.io/apimachinery/pkg/util/net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	BufferSizeBytes          = 8 << 10 // 8KB
	FilestreamIp             = "127.0.0.1"
	BinlogFilepathFormat     = "%s/%s/%s/%s/%s/%s/%s/%s/%s/binlog-file/%s"
	BinlogMetaFilepathFormat = "%s/%s/%s/%s/%s/%s/%s/%s/%s/binlog-meta/%s"
	BatchSize                = 1000
)

type Callback func(watcher *Watcher, file *BinlogFile) bool

var filestreamPort int

func SetLocalFilestreamSeverPort(port int) {
	filestreamPort = port
}

// BeforeUpload print some info. set RequestId
func BeforeUpload(w *Watcher, binlogFile *BinlogFile) bool {
	w.uploadLogger = w.logger.WithValues("trace", uuid.New().String(), "filepath", binlogFile.Filepath)
	infoJson, _ := json.Marshal(binlogFile)
	fileInfo, err := os.Stat(binlogFile.Filepath)
	if err != nil {
		w.uploadLogger.Error(err, "failed to stat file")
		return false
	}
	binlogFile.FileLastModifiedAt = fileInfo.ModTime()
	binlogFile.Size = fileInfo.Size()
	w.uploadLogger.Info("BeforeUpload", "binlogFile", string(infoJson))
	return true
}

// AfterUpload print some info. set RequestId
func AfterUpload(w *Watcher, binlogFile *BinlogFile) bool {
	return true
}

// FetchStartIndex parse start index from the beginning of the binlog file
func FetchStartIndex(w *Watcher, binlogFile *BinlogFile) bool {
	logger := w.uploadLogger.WithValues("action", "FetchStartIndex")
	logger.Info("begin")
	f, err := os.OpenFile(binlogFile.Filepath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		logger.Error(err, "failed to open file")
		return false
	}
	defer f.Close()
	headBytes, err := ReadBytes(f, BufferSizeBytes)
	if len(headBytes) > 0 {
		binlogFile.StartIndex, binlogFile.EventTimestamp, err = GetBinlogFileBeginInfo(headBytes, binlogFile.Filename, binlogFile.BinlogChecksum)
		if err != nil {
			logger.Error(err, "failed to get binlog file begin info")
			return false
		}
		logger.Info("success", "startIndex", binlogFile.StartIndex)
		return true
	}
	logger.Error(err, "failed")
	return false
}

func GetBinlogFileBeginInfo(beginBytes []byte, filename string, binlogChecksum string) (uint64, uint64, error) {
	opts := []binlog.LogEventScannerOption{
		binlog.WithBinlogFile(filename),
		binlog.WithChecksumAlgorithm(binlogChecksum),
		binlog.WithLogEventHeaderFilter(func(header binlogEvent.LogEventHeader) bool {
			return header.EventTypeCode() == spec.PREVIOUS_CONSENSUS_INDEX_LOG_EVENT
		}),
	}
	scanner, err := binlog.NewLogEventScanner(bufio.NewReader(bytes.NewReader(beginBytes)), opts...)
	if err != nil {
		return 0, 0, err
	}
	_, ev, err := scanner.Next()
	if err != nil {
		return 0, 0, err
	}
	consensusEvent, ok := ev.EventData().(*binlogEvent.PreviousConsensusIndexEvent)
	if !ok {
		return 0, 0, err
	}
	return consensusEvent.Index, uint64(ev.EventHeader().EventTimestamp()), nil
}

// Upload read the file only once. do 1.compute sha256 2.get start consensus log index 3. upload the file finally.
func Upload(w *Watcher, binlogFile *BinlogFile) bool {
	logger := w.uploadLogger.WithValues("action", "Upload")
	if binlogFile.SinkType == config.SinkTypeNone {
		w.uploadLogger.Info("skip")
		return true
	}
	logger.Info("begin")
	f, err := os.OpenFile(binlogFile.Filepath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		logger.Error(err, "failed to open file")
		return false
	}
	defer f.Close()
	buf := make([]byte, BufferSizeBytes)
	hash := sha256.New()
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()

	var waitGroup sync.WaitGroup
	var errValue atomic.Value
	binlogFile.CreatedAt = time.Now()
	binlogFile.UpdatedAt = binlogFile.CreatedAt
	go func() {
		defer reader.Close()
		waitGroup.Add(1)
		defer waitGroup.Done()
		// upload mock
		err := uploadBinlogFile(reader, binlogFile)
		if err != nil {
			logger.Error(err, "failed to upload remote")
			errValue.Store(err)
		}
	}()

	for {
		cnt, err := f.Read(buf)
		if err != nil {
			if net.IsProbableEOF(err) {
				writer.Close()
				break
			}
			logger.Error(err, "failed to read file")
			return false
		}
		if cnt > 0 {
			writer.Write(buf[:cnt])
			hash.Write(buf[:cnt])
		}
	}
	waitGroup.Wait()
	if errValue.Load() != nil {
		return false
	}
	hashBytes := hash.Sum(nil)
	binlogFile.Sha256 = hex.EncodeToString(hashBytes)
	err = uploadBinlogFileMeta(binlogFile)
	if err != nil {
		logger.Error(err, "failed to upload binlog meta")
		return false
	}
	logger.Info("success", "sha256", binlogFile.Sha256, "size", binlogFile.Size)
	return true
}

// UploadRemote by filestream client
func uploadBinlogFile(reader io.Reader, binlogFile *BinlogFile) error {
	//upload binlogfile meta
	client := filestream.NewFileClient(FilestreamIp, filestreamPort, nil)
	action := filestream.UploadOss
	if binlogFile.SinkType == config.SinkTypeSftp {
		action = filestream.UploadSsh
	} else if binlogFile.SinkType == config.SinkTypeMinio {
		action = filestream.UploadMinio
	}
	//upload binlogfile
	binlogFileMetadata := filestream.ActionMetadata{
		Action:          filestream.Action(action),
		Filepath:        getBinlogFilepath(binlogFile),
		RequestId:       uuid.New().String(),
		Sink:            binlogFile.SinkName,
		OssBufferSize:   strconv.FormatInt(binlogFile.Size, 10),
		MinioBufferSize: strconv.FormatInt(binlogFile.Size, 10),
	}
	uploadedLen, err := client.Upload(reader, binlogFileMetadata)
	if err != nil {
		return err
	}
	if uploadedLen != binlogFile.Size {
		return fmt.Errorf("not the same len contentSize=%d, uploadSize=%d", binlogFile.Size, uploadedLen)
	}
	return nil
}

func uploadBinlogFileMeta(binlogFile *BinlogFile) error {
	//upload binlogfile meta
	client := filestream.NewFileClient(FilestreamIp, filestreamPort, nil)
	action := filestream.UploadOss
	if binlogFile.SinkType == config.SinkTypeSftp {
		action = filestream.UploadSsh
	} else if binlogFile.SinkType == config.SinkTypeMinio {
		action = filestream.UploadMinio
	}
	binlogMetaJsonBytes, _ := json.Marshal(binlogFile)
	binlogMetaFileMetadata := filestream.ActionMetadata{
		Action:          filestream.Action(action),
		Filepath:        getBinlogMetaFilepath(binlogFile),
		RequestId:       uuid.New().String(),
		Sink:            binlogFile.SinkName,
		OssBufferSize:   fmt.Sprintf("%d", len(binlogMetaJsonBytes)),
		MinioBufferSize: fmt.Sprintf("%d", len(binlogMetaJsonBytes)),
	}
	reader := bytes.NewReader(binlogMetaJsonBytes)
	uploadedLen, err := client.Upload(reader, binlogMetaFileMetadata)
	if err != nil {
		return err
	}
	jsonByteLen := int64(len(binlogMetaJsonBytes))
	if uploadedLen != jsonByteLen {
		return fmt.Errorf("not the same len contentSize=%d, uploadSize=%d", jsonByteLen, uploadedLen)
	}
	return nil
}

func getBatchName(num int64) string {
	times := num / BatchSize
	return fmt.Sprintf("%d_%d", times*BatchSize, (times+1)*BatchSize)
}

func getBinlogFilepath(binlogFile *BinlogFile) string {
	batchName := getBatchName(binlogFile.Num)
	return fmt.Sprintf(BinlogFilepathFormat, config.GetBinlogStoragePathPrefix(), binlogFile.Namespace, binlogFile.PxcName, binlogFile.PxcUid, binlogFile.XStoreName, binlogFile.XStoreUid, binlogFile.PodName, binlogFile.Version, batchName, binlogFile.Filename)
}

func getBinlogMetaFilepath(binlogFile *BinlogFile) string {
	batchName := getBatchName(binlogFile.Num)
	filename := binlogFile.Filename + ".txt"
	return fmt.Sprintf(BinlogMetaFilepathFormat, config.GetBinlogStoragePathPrefix(), binlogFile.Namespace, binlogFile.PxcName, binlogFile.PxcUid, binlogFile.XStoreName, binlogFile.XStoreUid, binlogFile.PodName, binlogFile.Version, batchName, filename)
}

// RecordUpload add an upload record into db
func RecordUpload(w *Watcher, binlogFile *BinlogFile) bool {
	logger := w.uploadLogger.WithValues("action", "RecordUpload")
	logger.Info("begin")
	err := AddRecord(w.db, *binlogFile)
	if err != nil {
		logger.Error(err, "failed to record upload file")
		//ignore the error
	}
	logger.Info("success")
	return true
}
