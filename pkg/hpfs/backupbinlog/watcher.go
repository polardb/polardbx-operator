package backupbinlog

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	PERIOD              = 5 * time.Second
	PurgePeriod         = 1 * time.Minute
	InfoFilename        = "polarx_backupbinloginfo.txt"
	InfoVersionFilename = "polarx_backupbinloginfo_version.txt"
	IndexFilename       = "mysql_bin.index"
	UploadRecordsDbFile = "polarx_uploadrecord.db"
)

var (
	lock               = sync.Mutex{}
	registeredWatchers = map[string]*Watcher{}
)

func UploadLatestBinlogFile(workDir string) bool {
	setWatcherUploadLatest(workDir)
	return checkIfFinishUploadLatest(workDir)
}

func GetWatcherInfoHash(workDir string) string {
	lock.Lock()
	defer lock.Unlock()
	watcher, ok := registeredWatchers[workDir]
	if ok {
		return watcher.GetHash()
	}
	return ""
}

func setWatcherUploadLatest(workDir string) {
	lock.Lock()
	defer lock.Unlock()
	watcher, ok := registeredWatchers[workDir]
	if ok {
		watcher.SetUploadLatest(true)
	}
}

func checkIfFinishUploadLatest(workDir string) bool {
	indexFilepath := path.Join(workDir, IndexFilename)
	_, err := os.Stat(indexFilepath)
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	watcher := NewWatcher(workDir, nil)
	binlogNums, err := watcher.readLogIndex(indexFilepath)
	if err != nil {
		fmt.Printf("err : %+v\n", err)
		return true
	}
	if len(binlogNums) > 0 {
		maxBinlogNum := binlogNums[len(binlogNums)-1]
		dbFilepath := path.Join(workDir, UploadRecordsDbFile)
		db, err := GetDb(dbFilepath)
		if err != nil {
			fmt.Printf("err : %+v\n", err)
			return true
		}
		defer db.Close()
		record, err := FindRecord(db, maxBinlogNum.Num)
		if err != nil {
			fmt.Printf("err : %+v\n", err)
			return true
		}
		if record == nil || record.Status != 0 {
			return false
		}
	}
	return true
}

func register(watcher *Watcher) bool {
	lock.Lock()
	defer lock.Unlock()
	_, ok := registeredWatchers[watcher.workDir]
	if !ok {
		registeredWatchers[watcher.workDir] = watcher
		return true
	}
	return false
}

func unregister(watcher *Watcher) {
	lock.Lock()
	defer lock.Unlock()
	delete(registeredWatchers, watcher.workDir)
}

type Watcher struct {
	workDir               string
	actions               []Callback
	ctx                   context.Context
	cancelFunc            context.CancelFunc
	logger                logr.Logger
	uploadLogger          logr.Logger
	db                    *sql.DB
	mysqlDb               *sql.DB
	lastUploadedBinlogNum int64
	uploadLatest          bool
	uploadLatestCount     uint64
	lock                  sync.Mutex
	lastPurgeTime         time.Time
	infoHash              string
}

func NewWatcher(workDir string, actions ...Callback) *Watcher {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &Watcher{
		workDir:    workDir,
		actions:    actions,
		ctx:        ctx,
		cancelFunc: cancelFunc,
		logger:     zap.New(zap.UseDevMode(true)).WithName("MysqlBinlogFileWatcher").WithValues("WorkDirectory", workDir),
		lock:       sync.Mutex{},
	}
}

func (w *Watcher) SetHash(hash string) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.infoHash = hash
}

func (w *Watcher) GetHash() string {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.infoHash
}

func (w *Watcher) SetUploadLatest(uploadLatest bool) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.uploadLatest = uploadLatest
	w.uploadLatestCount = 0
}

func (w *Watcher) IsUploadLatest() bool {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.uploadLatest
}

func (w *Watcher) UploadLatestCount() uint64 {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.uploadLatestCount
}

func (w *Watcher) TryIncrementUploadLatestCount() bool {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.uploadLatest {
		w.uploadLatestCount += 1
	}
	return w.uploadLatest
}

func (w *Watcher) checkFiles(filepaths ...string) bool {
	for _, filepath := range filepaths {
		_, err := os.Stat(filepath)
		if err != nil {
			w.logger.Error(err, "failed to check file", "filepath", filepath)
			return false
		}
	}
	return true
}

func (w *Watcher) readFileLines(filepath string) ([]string, error) {
	bytes, err := os.ReadFile(filepath)
	if err != nil {
		w.logger.Error(err, "Failed to readfile", "filepath", filepath)
		return nil, err
	}
	content := string(bytes)
	result := strings.Split(content, "\n")
	return result, nil
}

func (w *Watcher) readLogIndex(indexFilepath string) ([]BinlogNum, error) {
	lines, err := w.readFileLines(indexFilepath)
	if err != nil {
		return nil, err
	}
	reg := regexp.MustCompile(`^mysql_bin.(\w+)$`)
	binlogNums := make([]BinlogNum, 0, len(lines))
	for _, line := range lines {
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}
		filename := filepath.Base(line)
		matchedResult := reg.FindStringSubmatch(filename)
		if len(matchedResult) == 2 {
			num, err := strconv.ParseInt(matchedResult[1], 10, 63)
			if err != nil {
				w.logger.Error(err, "filename suffix is not a number", "filename", filename)
				return nil, err
			}
			binlogNums = append(binlogNums, BinlogNum{
				Num:      num,
				Filename: filename,
				Filepath: filepath.Join(filepath.Dir(indexFilepath), filename),
			})
		}
	}
	sort.Slice(binlogNums, func(i, j int) bool {
		return binlogNums[i].Num < binlogNums[j].Num
	})
	return binlogNums, nil
}

func (w *Watcher) readInfo(filepath string) (*Info, error) {
	lines, err := w.readFileLines(filepath)
	if err != nil {
		return nil, err
	}
	infoMap := map[string]string{}
	for _, line := range lines {
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}
		kv := strings.Split(line, "=")
		if len(kv) != 2 {
			err := fmt.Errorf("invalid field, line %s", line)
			w.logger.Error(err, "failed to parse field line", "filepath", filepath)
			return nil, err
		}
		infoMap[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
	}
	namespace, namespaceOk := infoMap[InfoNamespace]
	xstoreName, xstoreNameOk := infoMap[InfoXStoreName]
	podName, podNameOk := infoMap[InfoPodName]
	version, versionOk := infoMap[InfoVersion]
	binlogChecksum, binlogChecksumOk := infoMap[InfoBinlogChecksum]
	sinkName, sinkNameOk := infoMap[InfoSinkName]
	sinkType, sinkTypeOk := infoMap[InfoSinkType]
	xstoreUid, xstoreUidOk := infoMap[InfoXStoreUid]
	pxcName, pxcNameOk := infoMap[InfoPxcName]
	pxcUid, pxcUidOk := infoMap[InfoPxcUid]
	uploadLatest, uploadLatestOk := infoMap[InfoUploadLatest]
	//InfoExpireLogDays InfoMaxLocalBinlogCount
	expireLogSeconds, expireLogSecondsOk := infoMap[InfoLocalExpireLogSeconds]
	maxLocalBinlogCount, maxLocalBinlogCountOk := infoMap[InfoMaxLocalBinlogCount]
	forbidPurgeStr, forbidPurgeOk := infoMap[InfoForbidPurge]
	if !(namespaceOk && xstoreNameOk && podNameOk && versionOk && binlogChecksumOk && sinkNameOk && sinkTypeOk && xstoreUidOk && pxcNameOk && pxcUidOk && forbidPurgeOk) {
		err := fmt.Errorf("invalid info")
		w.logger.Error(err, InfoNamespace, namespace, InfoXStoreName, xstoreName, InfoPodName, podName, InfoVersion, version, InfoBinlogChecksum, binlogChecksum, InfoSinkName, sinkName, InfoSinkType, sinkType, InfoXStoreUid, xstoreUid, InfoPxcName, pxcName, InfoPxcUid, pxcUid)
		return nil, err
	}
	forbidPurge, err := strconv.ParseBool(forbidPurgeStr)
	if err != nil {
		w.logger.Error(err, fmt.Sprintf("failed to parse forbid purge, str value = %s", forbidPurgeStr))
		return nil, err
	}
	var infoUploadLatest *bool
	if uploadLatestOk {
		uploadLatest = strings.TrimSpace(uploadLatest)
		val := strings.EqualFold(uploadLatest, "true") || strings.EqualFold(uploadLatest, "1")
		infoUploadLatest = &val
	}

	var infoExpireLogSeconds int64 = int64(config.GetConfig().BackupBinlogConfig.GetExpireLogHours() * 3600)
	if expireLogSecondsOk {
		val, err := strconv.ParseInt(expireLogSeconds, 10, 64)
		if err != nil {
			w.logger.Error(err, fmt.Sprintf("failed to parse expireLogSeconds , strValue=%s", expireLogSeconds))
		} else {
			infoExpireLogSeconds = val
		}
	}

	var infoMaxLocalBinlogCount int64 = config.GetConfig().BackupBinlogConfig.GetMaxLocalBinlogCount()
	if maxLocalBinlogCountOk {
		val, err := strconv.ParseInt(maxLocalBinlogCount, 10, 63)
		if err != nil {
			w.logger.Error(err, fmt.Sprintf("failed to parse maxLocalBinlogCount , strValue=%s", maxLocalBinlogCount))
		} else {
			infoMaxLocalBinlogCount = val
		}
	}
	info := &Info{
		Namespace:             namespace,
		XStoreName:            xstoreName,
		SinkName:              sinkName,
		SinkType:              sinkType,
		BinlogChecksum:        binlogChecksum,
		XStoreUid:             xstoreUid,
		PxcName:               pxcName,
		PxcUid:                pxcUid,
		LocalExpireLogSeconds: infoExpireLogSeconds,
		MaxLocalBinlogCount:   infoMaxLocalBinlogCount,
		ForbidPurge:           forbidPurge,
	}
	infoJson, _ := json.Marshal(info)
	w.SetHash(fmt.Sprintf("%x", md5.Sum(infoJson)))
	info.Version = version
	info.PodName = podName
	if info.UploadLatest != infoUploadLatest {
		info.UploadLatest = infoUploadLatest
		infoJson, _ = json.Marshal(info)
	}
	w.logger.Info("readInfo: " + string(infoJson))
	return info, nil
}

func (w *Watcher) Start() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				w.logger.Info("Skip panic", "err", err, "stack", string(debug.Stack()))
			}
		}()
		if !register(w) {
			return
		}
		defer unregister(w)
		infoFilepath := path.Join(w.workDir, InfoFilename)
		indexFilepath := path.Join(w.workDir, IndexFilename)
		if !w.checkFiles(infoFilepath, indexFilepath) {
			return
		}
		dbFilepath := path.Join(w.workDir, UploadRecordsDbFile)
		db, err := GetDb(dbFilepath)
		if err != nil {
			w.logger.Error(err, "failed to GetDb", "DbFilepath", dbFilepath)
			return
		}
		defer db.Close()
		err = TryCreateTableOfUploadRecord(db)
		if err != nil {
			w.logger.Error(err, "TryCreateTableOfUploadRecord")
			return
		}
		w.db = db

		// find mysql socket filepath
		mysqlSockPath, err := FindMysqlSockByLogDir(w.workDir, config.GetConfig().BackupBinlogConfig.RootDirectories)
		if err != nil {
			w.logger.Error(err, "failed to find mysql sock by log dir", "logDir", w.workDir, "rootDirs", config.GetConfig().BackupBinlogConfig.RootDirectories)
			return
		}
		mysqlDb, err := GetMysqlDb(mysqlSockPath)
		if err != nil {
			w.logger.Error(err, fmt.Sprintf("failed to get mysql db mysqlSockFilepath=%s", mysqlSockPath))
			return
		}
		w.mysqlDb = mysqlDb

		for {
			binlogNums, err := w.readLogIndex(indexFilepath)
			if err != nil {
				return
			}
			info, err := w.readInfo(infoFilepath)
			if err != nil {
				return
			}
			if info.UploadLatest != nil {
				w.SetUploadLatest(*info.UploadLatest)
			}

			// purge consensus logs
			if w.lastPurgeTime.Add(PurgePeriod).Before(time.Now()) {
				go func() {
					defer func() {
						if err := recover(); err != nil {
							w.logger.Info("Skip panic", "err", err, "stack", string(debug.Stack()))
						}
					}()
					if len(binlogNums) > 1 {
						err := w.tryPurge(w.db, w.mysqlDb, *info, binlogNums[:len(binlogNums)-1])
						if err != nil {
							w.logger.Error(err, "failed to purge")
						}
					}

				}()
				w.lastPurgeTime = time.Now()
			}

			// upload consensus logs
		binlogNumOut:
			for binlogNumArrayIndex, binlogNum := range binlogNums {
				if binlogNum.Num <= w.lastUploadedBinlogNum {
					continue
				}
				var thisLatestBinlogUploaded bool
				if binlogNumArrayIndex == len(binlogNums)-1 {
					if w.IsUploadLatest() {
						thisLatestBinlogUploaded = true
					} else {
						//not upload the latest binlog file
						continue
					}
				}
				binlogFile, err := FindRecord(db, binlogNum.Num)
				if err != nil {
					w.logger.Error(err, "failed to find record", "num", binlogNum.Num)
					return
				}
				if binlogFile == nil || binlogFile.Status != 0 {
					newBinlogFile := BinlogFile{
						Info:      *info,
						BinlogNum: binlogNum,
					}
					if w.actions != nil {
						for _, action := range w.actions {
							if !action(w, &newBinlogFile) {
								// breaking exec action if one action fails
								w.logger.Info("action executing interrupt", "binlog filepath", newBinlogFile.Filepath)
								break binlogNumOut
							}
						}
					}
				}
				w.lastUploadedBinlogNum = binlogNum.Num
				if thisLatestBinlogUploaded {
					w.TryIncrementUploadLatestCount()
				}
			}
			select {
			case <-w.ctx.Done():
				return
			case <-time.After(PERIOD):
				break
			}
		}
	}()
}

func (w *Watcher) tryPurge(sqliteDb *sql.DB, mysqlDb *sql.DB, info Info, binlogNums []BinlogNum) error {
	w.logger.Info("begin tryPurge consensus los")
	defer w.logger.Info("end tryPurge consensus logs")
	if info.ForbidPurge {
		w.logger.Info("forbid purge. ignore this trigger")
		return nil
	}
	var purgeBinlogNum *BinlogNum
	countPurgeCnt := len(binlogNums) - int(info.MaxLocalBinlogCount)
	if countPurgeCnt > 0 {
		// need purge because of count
		purgeBinlogNum = &binlogNums[countPurgeCnt-1]
	}
	expireTime := time.Now().Add(time.Duration(-info.LocalExpireLogSeconds) * time.Second)
	for _, binlogNum := range binlogNums {
		fileInfo, err := os.Stat(binlogNum.Filepath)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if fileInfo.ModTime().Before(expireTime) {
			if purgeBinlogNum == nil {
				purgeBinlogNum = &binlogNum
			} else if binlogNum.Num > purgeBinlogNum.Num {
				purgeBinlogNum = &binlogNum
			}
		}
	}
	if purgeBinlogNum != nil {
		//check if the binlog has been uploaded
		w.logger.Info(fmt.Sprintf("check the uploaded status of %s", purgeBinlogNum.Filename))
		uploadedRecord, err := FindRecord(sqliteDb, purgeBinlogNum.Num)
		if err != nil {
			w.logger.Error(err, "failed to find the binlog", "binlog filename", purgeBinlogNum.Filename)
			return err
		}
		if uploadedRecord != nil && uploadedRecord.Status == 0 {
			//do purge
			consensusLogRows, err := ShowConsensusLogs(w.mysqlDb)
			if err != nil {
				w.logger.Error(err, "failed to show consensus logs")
				return err
			}
			sort.Slice(consensusLogRows, func(i, j int) bool {
				return consensusLogRows[i].StartLogIndex < consensusLogRows[j].StartLogIndex
			})
			for i, consensusLogRow := range consensusLogRows {
				if consensusLogRow.LogName == purgeBinlogNum.Filename {
					if len(consensusLogRows) > i+1 {
						purgeToIndex := consensusLogRows[i+1].StartLogIndex
						w.logger.Info(fmt.Sprintf("exec purge local consensus_log before %d", purgeToIndex))
						err := Purge(w.mysqlDb, purgeToIndex)
						if err != nil {
							w.logger.Error(err, "fail to purge local consensus_log")
							return err
						}
						w.logger.Info("succeeded to purge local consensus_log")
					}
					break
				}
			}
		}
	}
	return nil
}

func (w *Watcher) Stop() {
	if w.cancelFunc != nil {
		w.cancelFunc()
	}
}
