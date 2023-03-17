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

import (
	"strconv"
	"strings"
	"time"

	"github.com/distribution/distribution/reference"

	"github.com/alibaba/polardbx-operator/pkg/util/defaults"
)

type config struct {
	ImagesConfig    imagesConfig    `json:"images,omitempty"`
	SchedulerConfig schedulerConfig `json:"scheduler,omitempty"`
	ClusterConfig   clusterConfig   `json:"cluster,omitempty"`
	BackupConfig    backupConfig    `json:"backup,omitempty"`
	StoreConfig     storeConfig     `json:"store,omitempty"`
	SecurityConfig  securityConfig  `json:"security,omitempty"`
	OssConfig       ossConfig       `json:"oss,omitempty"`
	NfsConfig       nfsConfig       `json:"nfs,omitempty"`
}

func (c *config) Security() SecurityConfig {
	return &c.SecurityConfig
}

func (c *config) Images() ImagesConfig {
	return &c.ImagesConfig
}

func (c *config) Cluster() ClusterConfig {
	return &c.ClusterConfig
}

func (c *config) Backup() BackupConfig {
	return &c.BackupConfig
}

func (c *config) Store() StoreConfig {
	return &c.StoreConfig
}

func (c *config) Scheduler() SchedulerConfig {
	return &c.SchedulerConfig
}

func (c *config) Oss() OssConfig {
	return &c.OssConfig
}

func (c *config) Nfs() NfsConfig {
	return &c.NfsConfig
}

type imagesConfig struct {
	Repo          string                       `json:"repo,omitempty"`
	Common        map[string]string            `json:"common,omitempty"`
	ComputeImages map[string]string            `json:"compute,omitempty"`
	CdcImages     map[string]string            `json:"cdc,omitempty"`
	StoreImages   map[string]map[string]string `json:"store,omitempty"`
}

func newImage(image string, defaultRepo, defaultTag string) string {
	if len(image) == 0 {
		return image
	}

	matches := reference.ReferenceRegexp.FindStringSubmatch(image)
	// Not match
	if matches == nil {
		return image
	}

	name, tag, digest := matches[1], matches[2], matches[3]

	// Fill the default.
	if tag == "" {
		tag = defaultTag
	}

	// Must parse.
	named, _ := reference.WithName(name)
	registry, paths := reference.Domain(named), strings.Split(reference.Path(named), "/")
	registryAndRepo := strings.Join(paths[:len(paths)-1], "/")
	if registry != "" {
		if registryAndRepo != "" {
			registryAndRepo = registry + "/" + registryAndRepo
		} else {
			registryAndRepo = registry
		}
	}
	if len(registryAndRepo) == 0 {
		registryAndRepo = defaultRepo
	}

	// Return
	r := paths[len(paths)-1]
	if len(registryAndRepo) > 0 {
		r = registryAndRepo + "/" + r
	}
	if len(tag) > 0 {
		r = r + ":" + tag
	}
	if len(digest) > 0 {
		r = r + "@" + digest
	}

	// Use "latest" if tag and digest are not found.
	if len(tag) == 0 && len(digest) == 0 {
		r = r + ":latest"
	}

	return r
}

func (c *imagesConfig) DefaultImageRepo() string {
	return c.Repo
}

func (c *imagesConfig) DefaultImageForCluster(role string, container string, version string) string {
	var image string
	switch strings.ToLower(role) {
	case "cn":
		image = defaults.NonEmptyStrOrDefault(c.ComputeImages[container], c.Common[container])
	case "cdc":
		image = defaults.NonEmptyStrOrDefault(c.CdcImages[container], c.Common[container])
	default:
		panic("invalid role: " + role)
	}

	return newImage(image, c.Repo, version)
}

func (c *imagesConfig) DefaultImageForStore(engine, container string, version string) string {
	image := defaults.NonEmptyStrOrDefault(c.StoreImages[engine][container], c.Common[container])
	return newImage(image, c.Repo, version)
}

func (c *imagesConfig) DefaultJobImage() string {
	image := c.Common["job"]
	return newImage(image, c.Repo, "")
}

type schedulerConfig struct {
	EnableMaster bool `json:"enable_master,omitempty"`
}

func (c *schedulerConfig) AllowScheduleToMasterNode() bool {
	return c.EnableMaster
}

type clusterConfig struct {
	OptionEnableExporters                   bool `json:"enable_exporters,omitempty"`
	OptionEnableAliyunAckResourceController bool `json:"enable_aliyun_ack_resource_controller,omitempty"`
	OptionEnableDebugModeForComputeNodes    bool `json:"enable_debug_mode_for_compute_nodes,omitempty"`
	OptionEnableRunModeCheck                bool `json:"enable_run_mode_check,omitempty"`
	OptionEnablePrivilegedContainer         bool `json:"enable_privileged_container,omitempty"`
	OptionForceCGroup                       bool `json:"force_cgroup,omitempty"`
}

func (c *clusterConfig) EnableExporters() bool {
	return c.OptionEnableExporters
}

func (c *clusterConfig) EnableAliyunAckResourceController() bool {
	return c.OptionEnableAliyunAckResourceController
}

func (c *clusterConfig) EnableDebugModeForComputeNodes() bool {
	return c.OptionEnableDebugModeForComputeNodes
}

func (c *clusterConfig) EnableRunModeCheck() bool {
	return c.OptionEnableRunModeCheck
}

func (c *clusterConfig) ContainerPrivileged() bool {
	return c.OptionEnablePrivilegedContainer
}

func (c *clusterConfig) ForceCGroup() bool {
	return c.OptionForceCGroup
}

type storeConfig struct {
	EnablePrivilegedContainer bool              `json:"enable_privileged_container,omitempty"`
	HostPaths                 map[string]string `json:"host_paths,omitempty"`
	HpfsEndpoint              string            `json:"hpfs_endpoint,omitempty"`
	FsEndpoint                string            `json:"fs_endpoint,omitempty"`
	MaxAutoRebuildingCount    string            `json:"max_auto_rebuilding_count,omitempty"`
}

func (c *storeConfig) GetMaxAutoRebuildingCount() int {
	val, err := strconv.Atoi(defaults.NonEmptyStrOrDefault(c.MaxAutoRebuildingCount, "1"))
	if err != nil {
		panic(err)
	}
	return val
}

func (c *storeConfig) ContainerPrivileged() bool {
	return c.EnablePrivilegedContainer
}

func (c *storeConfig) HostPathTools() string {
	return defaults.NonEmptyStrOrDefault(c.HostPaths["tools"], "/data/cache/tools/xstore")
}

func (c *storeConfig) HostPathDataVolumeRoot() string {
	return defaults.NonEmptyStrOrDefault(c.HostPaths["volume_data"], "/data/xstore")
}

func (c *storeConfig) HostPathLogVolumeRoot() string {
	return defaults.NonEmptyStrOrDefault(c.HostPaths["volume_log"], "/data-log/xstore")
}

func (c *storeConfig) HostPathFilestreamVolumeRoot() string {
	return defaults.NonEmptyStrOrDefault(c.HostPaths["volume_filestream"], "/filestream")
}

func (c *storeConfig) HostPathFileServiceEndpoint() string {
	return c.HpfsEndpoint
}

func (c *storeConfig) FilestreamServiceEndpoint() string {
	return c.FsEndpoint
}

type securityConfig struct {
	EncodeKey string `json:"encode_key,omitempty"`
}

func (c *securityConfig) DefaultEncodeKey() string {
	return c.EncodeKey
}

type ossConfig struct {
	OssEndpoint     string `json:"oss_endpoint,omitempty"`
	OssBucket       string `json:"oss_bucket,omitempty"`
	OssAccessKey    string `json:"oss_access_key,omitempty"`
	OssAccessSecret string `json:"oss_access_secret,omitempty"`
}

func (c *ossConfig) Endpoint() string {
	return c.OssEndpoint
}

func (c *ossConfig) Bucket() string {
	return c.OssBucket
}

func (c *ossConfig) AccessKey() string {
	return c.OssAccessKey
}

func (c *ossConfig) AccessSecret() string {
	return c.OssAccessSecret
}

type nfsConfig struct {
	NfsPath   string `json:"nfs_path,omitempty"`
	NfsServer string `json:"nfs_server,omitempty"`
}

func (c *nfsConfig) Path() string {
	return c.NfsPath
}

func (c *nfsConfig) Server() string {
	return c.NfsServer
}

type backupConfig struct {
	CheckBinlogExpiredInterval string `json:"check_binlog_expired_interval,omitempty"`
	HeartbeatJobNamePrefix     string `json:"heartbeat_job_name_prefix,omitempty"`
	HeartbeatInterval          string `json:"heartbeat_interval,omitempty"`
}

func (b *backupConfig) CheckBinlogExpiredFileInterval() (time.Duration, error) {
	interval := defaults.NonEmptyStrOrDefault(b.CheckBinlogExpiredInterval, "3600s")
	return time.ParseDuration(interval)
}

func (b *backupConfig) GetHeartbeatJobNamePrefix() string {
	return defaults.NonEmptyStrOrDefault(b.HeartbeatJobNamePrefix, "heartbeat-")
}

func (b *backupConfig) GetHeartbeatInterval() (time.Duration, error) {
	interval := defaults.NonEmptyStrOrDefault(b.HeartbeatInterval, "1s")
	return time.ParseDuration(interval)
}
