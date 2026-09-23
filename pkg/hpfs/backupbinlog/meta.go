/*
Copyright 2021 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package backupbinlog

import "time"

const (
	InfoNamespace             = "namespace"
	InfoPxcName               = "pxc_name"
	InfoPxcUid                = "pxc_uid"
	InfoXStoreName            = "xstore_name"
	InfoPodName               = "pod_name"
	InfoVersion               = "version"
	InfoBinlogChecksum        = "binlog_checksum"
	InfoSinkType              = "sink_type"
	InfoSinkName              = "sink_name"
	InfoXStoreUid             = "xstore_uid"
	InfoUploadLatest          = "upload_latest"
	InfoLocalExpireLogSeconds = "local_expire_log_seconds"
	InfoMaxLocalBinlogCount   = "max_local_binlog_count"
	InfoForbidPurge           = "forbid_purge"
)

type Info struct {
	Namespace             string `json:"namespace,omitempty"`
	XStoreName            string `json:"xstore_name,omitempty"`
	PodName               string `json:"pod_name,omitempty"`
	Version               string `json:"version,omitempty"`
	SinkName              string `json:"sinkName,omitempty"`
	SinkType              string `json:"sinkType,omitempty"`
	BinlogChecksum        string `json:"binlogChecksum,omitempty"`
	XStoreUid             string `json:"xstoreUid,omitempty"`
	PxcName               string `json:"pxcName,omitempty"`
	PxcUid                string `json:"pxcUid,omitempty"`
	UploadLatest          *bool  `json:"uploadLatest,omitempty"`
	LocalExpireLogSeconds int64  `json:"localExpireLogSeconds,omitempty"`
	MaxLocalBinlogCount   int64  `json:"maxLocalBinlogCount,omitempty"`
	ForbidPurge           bool   `json:"forbidPurge,omitempty"`
}

type BinlogFile struct {
	Info           `json:"info,omitempty"`
	BinlogNum      `json:"binlog_num,omitempty"`
	StartIndex     uint64 `json:"start_index,omitempty"`
	EventTimestamp uint64 `json:"event_timestamp,omitempty"`
	Sha256         string `json:"sha256,omitempty"`
	Size           int64  `json:"size,omitempty"`
	//Status 0：success
	Status             int       `json:"status,omitempty"`
	ErrMsg             string    `json:"err_msg,omitempty"`
	FileLastModifiedAt time.Time `json:"file_last_modified_at,omitempty"`
	CreatedAt          time.Time `json:"created_at,omitempty"`
	UpdatedAt          time.Time `json:"updated_at,omitempty"`
}

type BinlogNum struct {
	Num      int64  `json:"num,omitempty"`
	Filename string `json:"filename,omitempty"`
	Filepath string `json:"filepath,omitempty"`
	Latest   bool   `json:"latest,omitempty"`
}
