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

import "strings"

type FileStorageInfo struct {
	// +kubebuilder:validation:Pattern=`^(?i)innodb|mrg_myisam|blackhole|myisam|csv|archive|performance_schema|federated|local_disk|external_disk|s3|oss$`
	// +kubebuilder:validation:Required

	// Engine describes the engine type of file storage
	Engine string `json:"engine,omitempty"`
}

type EngineType string

const (
	EngineTypeInnodb            EngineType = "INNODB"
	EngineTypeMrgMyisam         EngineType = "MRG_MYISAM"
	EngineTypeBlackhole         EngineType = "BLACKHOLE"
	EngineTypeMyisam            EngineType = "MYISAM"
	EngineTypeCsv               EngineType = "CSV"
	EngineTypeArchive           EngineType = "ARCHIVE"
	EngineTypePerformanceSchema EngineType = "PERFORMANCE_SCHEMA"
	EngineTypeFederated         EngineType = "FEDERATED"
	EngineTypeLocalDisk         EngineType = "LOCAL_DISK"
	EngineTypeExternalDisk      EngineType = "EXTERNAL_DISK"
	EngineTypeS3                EngineType = "S3"
	EngineTypeOss               EngineType = "OSS"
)

func (info *FileStorageInfo) GetEngineType() EngineType {
	return EngineType(strings.ToUpper(info.Engine))
}

func (info *FileStorageInfo) CheckEngineExists(existedFileStorages []FileStorageInfo) bool {
	for _, existedFileStorage := range existedFileStorages {
		if info.GetEngineType() == existedFileStorage.GetEngineType() {
			return true
		}
	}
	return false
}
