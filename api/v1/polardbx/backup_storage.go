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

package polardbx

import (
	"errors"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/filestream"
)

// BackupStorageProvider defines the configuration of storage for storing backup files.
type BackupStorageProvider struct {
	// StorageName defines the storage medium used to perform backup
	StorageName BackupStorage `json:"storageName,omitempty"`

	// Sink defines the storage configuration choose to perform backup
	Sink string `json:"sink,omitempty"`
	// TODO: Add Nas Provider
}

// BackupStorage defines the storage of backup
type BackupStorage string

const (
	OSS  BackupStorage = "oss"
	SFTP BackupStorage = "sftp"
)

// BackupStorageFilestreamAction records filestream actions related to specified backup storage
type BackupStorageFilestreamAction struct {
	Download filestream.Action
	Upload   filestream.Action
	List     filestream.Action
}

func NewBackupStorageFilestreamAction(storage BackupStorage) (*BackupStorageFilestreamAction, error) {
	switch storage {
	case OSS:
		return &BackupStorageFilestreamAction{
			Download: filestream.DownloadOss,
			Upload:   filestream.UploadOss,
			List:     filestream.ListOss,
		}, nil
	case SFTP:
		return &BackupStorageFilestreamAction{
			Download: filestream.DownloadSsh,
			Upload:   filestream.UploadSsh,
			List:     filestream.ListSsh,
		}, nil
	default:
		return nil, errors.New("invalid storage: " + string(storage))
	}
}
