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

package config

import "time"

type Config interface {
	Images() ImagesConfig
	Cluster() ClusterConfig
	Backup() BackupConfig
	Store() StoreConfig
	Scheduler() SchedulerConfig
	Security() SecurityConfig
	Oss() OssConfig
	Nfs() NfsConfig
}

type SecurityConfig interface {
	DefaultEncodeKey() string
}

type SchedulerConfig interface {
	AllowScheduleToMasterNode() bool
}

type ImagesConfig interface {
	DefaultImageRepo() string
	DefaultJobImage() string
	DefaultImageForCluster(role string, container string, version string) string
	DefaultImageForStore(engine string, container string, version string) string
}

type ClusterConfig interface {
	EnableExporters() bool
	EnableAliyunAckResourceController() bool
	EnableDebugModeForComputeNodes() bool
	EnableRunModeCheck() bool
	ContainerPrivileged() bool
	ForceCGroup() bool
}

type StoreConfig interface {
	ContainerPrivileged() bool

	HostPathTools() string
	HostPathDataVolumeRoot() string
	HostPathLogVolumeRoot() string
	HostPathFilestreamVolumeRoot() string

	HostPathFileServiceEndpoint() string
	FilestreamServiceEndpoint() string
	GetMaxAutoRebuildingCount() int
}

type OssConfig interface {
	Endpoint() string
	Bucket() string
	AccessKey() string
	AccessSecret() string
}

type NfsConfig interface {
	Path() string
	Server() string
}

type BackupConfig interface {
	CheckBinlogExpiredFileInterval() (time.Duration, error)
	GetHeartbeatJobNamePrefix() string
	GetHeartbeatInterval() (time.Duration, error)
	GetRestorePodSuffix() string
}
