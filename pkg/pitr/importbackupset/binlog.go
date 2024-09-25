package importbackupset

import (
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/backupbinlog"
	"github.com/alibaba/polardbx-operator/pkg/util/json"
	"net/http"
	"strconv"
	"strings"
)

func GenerateBinlogUploadObjs(backupSet *BackupSet) []UploadObj {
	uploadObjs := make([]UploadObj, 0)
	pxcPrefix := "polardbx-binlogbackup/default/" + backupSet.InsName + "/" + backupSet.PxcUid
	dnBackupSets := make([]DnBackupSet, 0, len(backupSet.DnBackupSets)+1)
	dnBackupSets = append(dnBackupSets, backupSet.DnBackupSets...)
	dnBackupSets = append(dnBackupSets, backupSet.GmsBackupSet)
	for _, dnBackupSet := range dnBackupSets {
		binlogStartIndex := getBinlogIndex(dnBackupSet.Binlogs[0].Filename)
		binlogEndIndex := getBinlogIndex(dnBackupSet.Binlogs[len(dnBackupSet.Binlogs)-1].Filename)
		version := dnBackupSet.HostInstanceId
		dnPrefix := pxcPrefix + "/" + dnBackupSet.DnName + "/" + dnBackupSet.XStoreUid + "/" + dnBackupSet.TargetPod + "/" + strconv.FormatInt(int64(version), 10) + "/" + fmt.Sprintf("%d_%d", binlogStartIndex, binlogEndIndex)
		for _, bg := range dnBackupSet.Binlogs {
			bg.Filename = strings.ReplaceAll(bg.Filename, "-", "_")
			binlogFilepath := dnPrefix + "/" + "binlog-file" + "/" + bg.Filename
			binlogFileUploadObj := UploadObj{
				DownloadURL: GetDownloadUrl(bg.InnerDownloadUrl, bg.PubDownloadUrl),
				Filepath:    binlogFilepath,
			}
			resp, err := http.Get(binlogFileUploadObj.DownloadURL)
			if err != nil {
				panic(err)
			}
			buff := make([]byte, backupbinlog.BufferSizeBytes)
			len, err := resp.Body.Read(buff)
			if err != nil {
				panic(err)
			}
			buff = buff[:len]
			startIndex, eventTimestamp, err := backupbinlog.GetBinlogFileBeginInfo(buff, bg.Filename, "crc32")
			if err != nil {
				panic(err)
			}
			binlogFile := backupbinlog.BinlogFile{
				Info: backupbinlog.Info{
					Version: strconv.FormatInt(int64(version), 10),
				},
				StartIndex:     startIndex,
				EventTimestamp: eventTimestamp,
			}
			binlogFileMetaUploadObj := UploadObj{
				Content:  []byte(json.Convert2JsonString(binlogFile)),
				Filepath: dnPrefix + "/" + "binlog-meta" + "/" + bg.Filename + ".txt",
			}
			uploadObjs = append(uploadObjs, binlogFileUploadObj)
			uploadObjs = append(uploadObjs, binlogFileMetaUploadObj)
		}
	}
	return uploadObjs
}

func getBinlogIndex(binlogFileName string) int64 {
	val, err := strconv.ParseInt(strings.Split(binlogFileName, ".")[1], 10, 64)
	if err != nil {
		panic("failed to parse int " + binlogFileName)
	}
	return val
}
