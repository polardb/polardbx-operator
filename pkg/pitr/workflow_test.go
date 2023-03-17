package pitr

import (
	"github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/filestream"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"testing"
	"time"
)

func startFileServer1() *filestream.FileServer {
	config.ConfigFilepath = "/Users/busu/tmp/filestream/config.yaml"
	config.InitConfig()
	flowControl := filestream.NewFlowControl(filestream.FlowControlConfig{
		MaxFlow:    1 << 40, // 1 byte/s
		TotalFlow:  (1 << 40) * 10,
		MinFlow:    1 << 40,
		BufferSize: 1 << 10,
	})
	flowControl.Start()
	fileServer := filestream.NewFileServer("0.0.0.0", 9999, ".", flowControl)
	go func() {
		fileServer.Start()
	}()
	time.Sleep(1 * time.Second)
	return fileServer
}

func TestFinishAndStartHttpServer(t *testing.T) {
	startFileServer1()
	pCtx := &Context{
		TaskConfig: &TaskConfig{
			HttpServerPort: 10000,
		},
		Logger: zap.New(zap.UseDevMode(true)).WithName("pitr"),
		RestoreBinlogs: []RestoreBinlog{
			{
				XStoreName: "busu",
				ResultSources: []BinlogSource{
					{
						Filename: "mysql_bin.000010",
						RSource: &RemoteSource{
							FsIp:   "127.0.0.1",
							FsPort: 9999,
							Sink: &config.Sink{
								Name: "default",
								Type: "oss",
							},
							MetaFilepath: "binlogbackup/default/polardb-x-2/16bd261e-ac47-42d7-bb2f-f2ff940d6780/polardb-x-2-wt9x-dn-0/326e21cb-7796-4fa1-b573-2fce08a872f9/polardb-x-2-wt9x-dn-0-cand-1/1678182799/0_1000/binlog-meta/mysql_bin.000010.txt",
							DataFilepath: "binlogbackup/default/polardb-x-2/16bd261e-ac47-42d7-bb2f-f2ff940d6780/polardb-x-2-wt9x-dn-0/326e21cb-7796-4fa1-b573-2fce08a872f9/polardb-x-2-wt9x-dn-0-cand-1/1678182799/0_1000/binlog-file/mysql_bin.000010",
						},
					},
				},
			},
		},
	}
	FinishAndStartHttpServer(pCtx)
}

func PrepareConfig() {
	config := TaskConfig{
		Namespace:    "default",
		PxcName:      "polardb-x-2",
		PxcUid:       "16bd261e-ac47-42d7-bb2f-f2ff940d6780",
		SinkName:     "default",
		SinkType:     "oss",
		HpfsEndpoint: "127.0.0.1:6543",
		FsEndpoint:   "127.0.0.1:6643",
		XStores: map[string]*XStoreConfig{
			"polardb-x-2-wt9x-dn-0": {
				GlobalConsistent:    true,
				XStoreName:          "polardb-x-2-wt9x-dn-0",
				XStoreUid:           "326e21cb-7796-4fa1-b573-2fce08a872f9",
				BackupSetStartIndex: 2643291,
				HeartbeatSname:      "pitr_sname",
				Pods: map[string]*PodConfig{
					"polardb-x-2-wt9x-dn-0-cand-0": {
						PodName: "polardb-x-2-wt9x-dn-0-cand-0",
						Host:    "cn-beijing.172.16.2.204",
						LogDir:  "/data/xstore/default/polardb-x-2-wt9x-dn-0-cand-0/log",
					},
					"polardb-x-2-wt9x-dn-0-cand-1": {
						PodName: "polardb-x-2-wt9x-dn-0-cand-1",
						Host:    "cn-beijing.172.16.2.53",
						LogDir:  "/data/xstore/default/polardb-x-2-wt9x-dn-0-cand-1/log",
					},
					"polardb-x-2-wt9x-dn-0-log-0": {
						PodName: "polardb-x-2-wt9x-dn-0-log-0",
						Host:    "cn-beijing.172.16.2.118",
						LogDir:  "/data/xstore/default/polardb-x-2-wt9x-dn-0-log-0/log",
					},
				},
			},
			"polardb-x-2-wt9x-dn-1": {
				GlobalConsistent:    true,
				XStoreName:          "polardb-x-2-wt9x-dn-1",
				XStoreUid:           "f38bea9c-2cac-4a27-ae21-997a7e30d737",
				BackupSetStartIndex: 2643291,
				HeartbeatSname:      "pitr_sname",
				Pods: map[string]*PodConfig{
					"polardb-x-2-wt9x-dn-1-cand-0": {
						PodName: "polardb-x-2-wt9x-dn-1-cand-0",
						Host:    "cn-beijing.172.16.2.205",
						LogDir:  "/data/xstore/default/polardb-x-2-wt9x-dn-1-cand-0/log",
					},
					"polardb-x-2-wt9x-dn-1-cand-1": {
						PodName: "polardb-x-2-wt9x-dn-1-cand-1",
						Host:    "cn-beijing.172.16.2.119",
						LogDir:  "/data/xstore/default/polardb-x-2-wt9x-dn-1-cand-1/log",
					},
					"polardb-x-2-wt9x-dn-1-log-0": {
						PodName: "polardb-x-2-wt9x-dn-1-log-0",
						Host:    "cn-beijing.172.16.2.54",
						LogDir:  "/data/xstore/default/polardb-x-2-wt9x-dn-1-log-0/log",
					},
				},
			},
			"polardb-x-2-wt9x-gms": {
				GlobalConsistent:    false,
				XStoreName:          "polardb-x-2-wt9x-gms-cand-0",
				XStoreUid:           "a949057a-6b8d-42f0-b3a8-4c6f41350496",
				BackupSetStartIndex: 100,
				HeartbeatSname:      "pitr_sname",
				Pods: map[string]*PodConfig{
					"polardb-x-2-wt9x-gms-cand-0": {
						PodName: "polardb-x-2-wt9x-gms-cand-0",
						Host:    "cn-beijing.172.16.2.54",
						LogDir:  "/data/xstore/default/polardb-x-2-wt9x-gms-cand-0/log",
					},
					"polardb-x-2-wt9x-gms-cand-1": {
						PodName: "polardb-x-2-wt9x-gms-cand-1",
						Host:    "cn-beijing.172.16.2.118",
						LogDir:  "/data/xstore/default/polardb-x-2-wt9x-gms-cand-1/log",
					},
					"polardb-x-2-wt9x-gms-log-0": {
						PodName: "polardb-x-2-wt9x-gms-log-0",
						Host:    "cn-beijing.172.16.2.205",
						LogDir:  "/data/xstore/default/polardb-x-2-wt9x-gms-log-0/log",
					},
				},
			},
		},
		Timestamp:      1678193950,
		BinlogChecksum: "crc32",
	}
	configContent := MustMarshalJSON(config)
	spillDir := "/Users/busu/tmp/pitr/spill"
	os.Setenv(EnvSpillOutDirectory, spillDir)
	configFilepath := "/Users/busu/tmp/pitr/conf/config.json"
	os.Setenv(EnvConfigFilepath, configFilepath)
	os.WriteFile(configFilepath, []byte(configContent), 0644)
}

func TestDo(t *testing.T) {
	PrepareConfig()
	Run()
}
