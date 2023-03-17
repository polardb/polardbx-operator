package pitr

type TaskConfig struct {
	Namespace      string                   `json:"namespace,omitempty"`
	PxcName        string                   `json:"pxc_name,omitempty"`
	PxcUid         string                   `json:"pxc_uid,omitempty"`
	SinkName       string                   `json:"sinkName,omitempty"`
	SinkType       string                   `json:"sinkType,omitempty"`
	SpillDirectory string                   `json:"spill_directory,omitempty"`
	HpfsEndpoint   string                   `json:"hpfs_endpoint,omitempty"`
	FsEndpoint     string                   `json:"fs_endpoint,omitempty"`
	XStores        map[string]*XStoreConfig `json:"xstores,omitempty"`
	Timestamp      uint64                   `json:"timestamp,omitempty"`
	BinlogChecksum string                   `json:"binlog_checksum,omitempty"`
	HttpServerPort int                      `json:"http_server_port,omitempty"`
}

type XStoreConfig struct {
	GlobalConsistent    bool                  `json:"global_consistent,omitempty"`
	XStoreName          string                `json:"xstore_name,omitempty"`
	XStoreUid           string                `json:"xstore_uid,omitempty"`
	BackupSetStartIndex uint64                `json:"backupset_start_index,omitempty"`
	HeartbeatSname      string                `json:"heartbeat_sname,omitempty"`
	Pods                map[string]*PodConfig `json:"pods,omitempty"`
}

type PodConfig struct {
	PodName string `json:"pod_name,omitempty"`
	Host    string `json:"host,omitempty"`
	LogDir  string `json:"log_dir,omitempty"`
}

type HttpBinlogFileInfo struct {
	Filename string  `json:"filename,omitempty"`
	Length   *uint64 `json:"length,omitempty"`
}
