package pitr

import (
	"context"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/common"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/filestream"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/remote"
	"strconv"
	"testing"
	"time"
)

func startFileServer() *filestream.FileServer {
	config.ConfigFilepath = "/Users/busu/tmp/filestream/config.yaml"
	config.InitConfig()
	flowControl := filestream.NewFlowControl(filestream.FlowControlConfig{
		MaxFlow:    1 << 40, // 1 byte/s
		TotalFlow:  (1 << 40) * 10,
		MinFlow:    1 << 40,
		BufferSize: 1 << 10,
	})
	flowControl.Start()
	fileServer := filestream.NewFileServer("", 9999, ".", flowControl)
	go func() {
		fileServer.Start()
	}()
	time.Sleep(1 * time.Second)
	return fileServer
}

func TestSearchByTimestamp(t *testing.T) {
	startFileServer()
	pxcName := "polardb-x-2"
	pxcUid := "16bd261e-ac47-42d7-bb2f-f2ff940d6780"
	xStoreName := "polardb-x-2-wt9x-dn-1"
	xStoreUid := "f38bea9c-2cac-4a27-ae21-997a7e30d737"
	podName := "polardb-x-2-wt9x-dn-1-cand-1"
	var startIndex uint64 = 1983339
	var timestamp uint64 = 1678193950
	var version uint64 = 1678182799
	heartbeatSname := "pitr_sname"
	//1678193948
	rb := newRestoreBinlog(pxcName, pxcUid, xStoreName, xStoreUid, podName, startIndex, timestamp, version, heartbeatSname, []BinlogSource{
		{
			BinlogChecksum: "crc32",
			RSource: &RemoteSource{
				FsIp:   "127.0.0.1",
				FsPort: 9999,
				Sink: &config.Sink{
					Name: "default",
					Type: "oss",
				},
				MetaFilepath: "binlogbackup/default/polardb-x-2/16bd261e-ac47-42d7-bb2f-f2ff940d6780/polardb-x-2-wt9x-dn-1/f38bea9c-2cac-4a27-ae21-997a7e30d737/polardb-x-2-wt9x-dn-1-cand-1/1678182799/0_1000/binlog-meta/mysql_bin.000005.txt",
				DataFilepath: "binlogbackup/default/polardb-x-2/16bd261e-ac47-42d7-bb2f-f2ff940d6780/polardb-x-2-wt9x-dn-1/f38bea9c-2cac-4a27-ae21-997a7e30d737/polardb-x-2-wt9x-dn-1-cand-1/1678182799/0_1000/binlog-file/mysql_bin.000005",
			},
		},
	})
	rb.SearchByTimestamp()
	rb.SetSpillFilepath("./test.json")
	rb.SpillResultSources()
	rb.LoadResultSources()
	fmt.Println("test")
}

func TestOssAliyunTest(t *testing.T) {
	config.ConfigFilepath = "/Users/busu/tmp/filestream/config.yaml"
	config.InitConfig()
	_, params, auth, fileServiceName, _ := hpfs.GetFileServiceParam("default", "oss")
	fileService, _ := remote.GetFileService(fileServiceName)
	resultFiles := make([]string, 0)
	resultFilesPtr := &resultFiles
	ctx := context.WithValue(context.Background(), common.AffectedFiles, resultFilesPtr)
	//xstoreBinlogDir := config.GetXStorePodBinlogStorageDirectory("default", "", request.GetPxcUid(), request.GetXStoreName(), request.GetXStoreUid(), request.GetPodName())
	params["deadline"] = strconv.FormatInt(time.Now().Unix(), 10)
	ft, err := fileService.ListAllFiles(ctx, "binlogbackup/default/polardb-x-2/16bd261e-ac47-42d7-bb2f-f2ff940d6780/polardb-x-2-wt9x-dn-1/f38bea9c-2cac-4a27-ae21-997a7e30d737/polardb-x-2-wt9x-dn-1-cand-0", auth, params)
	if err == nil {
		err = ft.Wait()
	}
}

func TestTimeZone(t *testing.T) {
	location, _ := time.LoadLocation("UTC")

	val, err := time.ParseInLocation("2006-01-02T15:04:05Z", "2023-03-14T14:50:27Z", location)
	fmt.Println(val.Unix())
	fmt.Println(time.Time{}.Unix())
	fmt.Println(err)
}
