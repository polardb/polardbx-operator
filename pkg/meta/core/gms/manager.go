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

package gms

import (
	"errors"
	"regexp"
	"strconv"

	"k8s.io/apimachinery/pkg/types"

	"github.com/alibaba/polardbx-operator/pkg/featuregate"
)

// ClusterKind defines the PolarDBX cluster's kinds
type ClusterKind int32

// These are valid cluster kinds
const (
	MasterCluster       ClusterKind = 0
	ReadOnlyCluster     ClusterKind = 1
	HtapReadOnlyCluster ClusterKind = 2
)

func (k ClusterKind) String() string {
	return strconv.Itoa(int(k))
}

// GrantPrivilegeType for
type GrantPrivilegeType string

// These are valid grant privilege types.
const (
	GrantSuperPrivilege     GrantPrivilegeType = "super"
	GrantAllPrivilege       GrantPrivilegeType = "all"
	GrantReadWritePrivilege GrantPrivilegeType = "read-write"
	GrantReadOnlyPrivilege  GrantPrivilegeType = "read-only"
	GrantDdlPrivilege       GrantPrivilegeType = "ddl"
	GrantCustomPrivilege    GrantPrivilegeType = "custom"
)

type AccountType string

const (
	AccountTypeUser    AccountType = "0"
	AccountTypeRole    AccountType = "1"
	AccountTypeDBA     AccountType = "2"
	AccountTypeSSO     AccountType = "3"
	AccountTypeAuditor AccountType = "4"
	AccountTypeGod     AccountType = "5"
)

// GrantOption which
type GrantOption struct {
	// Type of grant.
	Type GrantPrivilegeType `json:"type"`

	// Databases to grant, when type is GrantSuperPrivilege, the values are ignored.
	Databases []string `json:"databases"`

	// User to grant.
	User string `json:"user"`

	// Host to grant, default is "%" stands for all.
	Host string `json:"host,omitempty"`

	// Customized grant statement, only be used when GrantCustomPrivilege. Refer to
	// PolarDB-X privileges document.
	Privileges []string `json:"privileges,omitempty"`
}

// PCNodeStatus is the abbrev. for PolarDB-X Compute Node Status
type PCNodeStatus int32

// These are valid PCNodeStatus
const (
	PCNodeEnabled  PCNodeStatus = 0
	PCNodeDisabled PCNodeStatus = 1
)

// PSNodeStatus is the abbrev. for PolarDB-X Storage Node Status
type PSNodeStatus int32

// These are valid PSNodeStatus
const (
	PSNodeEnabled   PSNodeStatus = 0
	PSNodeDisabling PSNodeStatus = 1
	PSNodeDisabled  PSNodeStatus = 2
)

// ComputeNodeInfo defines the necessary information for compute nodes in PolarDBX cluster.
type ComputeNodeInfo struct {
	// Host is the host or ip of server node.
	Host string `json:"host"`

	// Port is the service port of server node.
	Port int32 `json:"port"`

	// HtapPort is the port for HTAP of server node.
	HtapPort int32 `json:"htapPort"`

	// MgrPort is the port for management of server node.
	MgrPort int32 `json:"mgrPort"`

	// MppPort is the port for MPP of server node.
	MppPort int32 `json:"mppPort"`

	// Status stands
	Status PCNodeStatus `json:"status"`

	CpuCore int32 `json:"cpuCore"`

	MemSize int64 `json:"memSize"`

	Extra string `json:"extra,omitempty"`
}

// StorageType indicates the type of the underlying storage node.
type StorageType int32

// These are valid storage types.
const (
	StorageTypeXCluster57    StorageType = 0
	StorageTypeMySQL         StorageType = 1
	StorageTypePolarDB       StorageType = 2
	StorageTypeXCluster80    StorageType = 3
	StorageTypeGalaxySingle  StorageType = 4
	StorageTypeGalaxyCluster StorageType = 5
)

// Conventions for storage type.

var (
	mysqlVersionPattern      = regexp.MustCompile("((\\d+)\\.(\\d+)\\.(\\d+))")
	xcluster57VersionPattern = regexp.MustCompile("5\\.7\\.\\S+-X-Cluster-\\S+")
	xcluster80VersionPattern = regexp.MustCompile("8\\.0\\.\\S+-X-Cluster-\\S+")
)

func GetStorageType(engine string, version string, annotationStorageType string) (StorageType, error) {
	if annotationStorageType != "" {
		storageType, err := strconv.ParseInt(annotationStorageType, 10, 63)
		if err != nil {
			return 0, err
		}
		return StorageType(int32(storageType)), nil
	}
	switch engine {
	case "xcluster":
		if xcluster80VersionPattern.MatchString(version) {
			return StorageTypeXCluster80, nil
		} else if xcluster57VersionPattern.MatchString(version) {
			return StorageTypeXCluster57, nil
		} else {
			return 0, errors.New("unrecognized storage engine version: " + version)
		}
	case "galaxy":
		if featuregate.EnableGalaxyClusterMode.Enabled() {
			return StorageTypeGalaxyCluster, nil
		} else {
			return StorageTypeGalaxySingle, nil
		}
	case "polardb":
		return StorageTypePolarDB, nil
	case "mysql":
		return StorageTypeMySQL, nil
	}

	panic("unsupported engine: " + engine)
}

// StorageKind indicates the role of the storage node.
type StorageKind int32

// These are valid storage kinds.
const (
	StorageKindMaster StorageKind = 0
	StorageKindSlave  StorageKind = 1
	StorageKindMetaDB StorageKind = 2
)

type VipType int32

const (
	IsNotVip VipType = 0
	IsVip    VipType = 1
)

// StorageNodeInfo defines the basic information for storage node.
type StorageNodeInfo struct {
	// Id is the unique identity for storage node.
	Id string `json:"id"`

	// MasterId is the unique identity for master storage node
	MasterId string `json:"masterId"`

	// ClusterId is the unique identity for the PolarDBX instance
	ClusterId string `json:"clusterId"`

	// Host is host or ip of the storage node.
	Host string `json:"host"`

	// Port is the service port of the storage node.
	Port int32 `json:"port"`

	// XProtocolPort is the x-protocol port of the storage node.
	XProtocolPort int32 `json:"xport"`

	// User is the user of the storage node with super privilege.
	User string `json:"user"`

	// Passwd is the password encrypted of the user above.
	Passwd string `json:"passwd"`

	// Type indicates the storage type.
	Type StorageType `json:"type"`

	// Kind indicates the storage kind.
	Kind StorageKind `json:"kind"`

	// Status indicates that the storage's status.
	Status PSNodeStatus `json:"status"`

	// MaxConn indicates the max connection size the storage node supports.
	MaxConn int32 `json:"maxConn"`

	// CpuCore indicates the CPU core the storage node uses.
	CpuCore int32 `json:"cpuCore"`

	// MemSize indicates the memory size the storage node uses.
	MemSize int64 `json:"memSize"`

	// IsVip indicates the storage is vip or not, 1 means vip, 0 means not vip
	IsVip VipType `json:"isVip"`

	// Extra define the extra messages.
	Extra string `json:"extra,omitempty"`
}

// CdcNodeInfo
type CdcNodeInfo struct {
	ContainerId string `json:"containerId"`
	Host        string `json:"host"`
	DaemonPort  int32  `json:"daemonPort"`
}

// Manager defines a set of methods for manage the PolarDBX cluster.
type Manager interface {
	IsMetaDBExisted() (bool, error)

	IsMetaDBInitialized(clusterId string) (bool, error)

	// InitializeMetaDBSchema initializes the metadb. It's essential for creating a PolarDBX cluster.
	InitializeMetaDBSchema() error

	// InitializeMetaDBInfo initialize the config listener and quarantine config in metadb
	InitializeMetaDBInfo(readonly bool) error

	// IsGmsSchemaRestored return the status if the schema is restored.
	IsGmsSchemaRestored() (bool, error)

	// RestoreSchemas restores the schemas in metadb.
	RestoreSchemas(fromPxcCluster, fromPxcHash, PxcHash string) error

	// EnableComputeNodes enables the specified compute server nodes by setting
	// the metadb.
	EnableComputeNodes(computeNodes ...ComputeNodeInfo) error

	// DisableComputeNodes disables the specified compute server nodes by setting
	// the metadb.
	DisableComputeNodes(computeNodes ...ComputeNodeInfo) error

	// DisableAllComputeNodes disables all storage nodes that belongs to the cluster
	DisableAllComputeNodes(primaryPolardbxName string) error

	// DeleteComputeNodes deletes the specified compute server nodes by setting
	// the metadb.
	DeleteComputeNodes(computeNodes ...ComputeNodeInfo) error

	// ListComputeNodes lists all enabled compute server nodes.
	ListComputeNodes() ([]ComputeNodeInfo, error)

	// SyncComputeNodes deletes all nodes not specified and insert those not exists
	// in metadb
	SyncComputeNodes(computeNodes ...ComputeNodeInfo) error

	// EnableStorageNodes adds the specified storage nodes by adding the corresponding
	// records into metadb.
	EnableStorageNodes(storageNodes ...StorageNodeInfo) error

	//UpdateRoStorageNodes update the ro not_vip storage node info
	UpdateRoStorageNodes(storageNodes ...StorageNodeInfo) error

	// DisableStorageNodes disables the specified storage nodes by removing the corresponding
	// records from metadb.
	DisableStorageNodes(storageNodes ...StorageNodeInfo) error

	// DisableAllStorageNodes disables all storage nodes that belongs to the cluster
	DisableAllStorageNodes(primaryPolardbxName string) error

	// ListStorageNodes lists all storage nodes.
	ListStorageNodes(kind StorageKind) ([]StorageNodeInfo, error)

	// ListCdcNodes lists all enabled CDC nodes.
	ListCdcNodes() ([]CdcNodeInfo, error)

	// DeleteCdcNodes deletes the specified CDC nodes by setting the metadb.
	DeleteCdcNodes(cdcNodes ...CdcNodeInfo) error

	// ListDynamicParams list all dynamic parameters in the cluster.
	ListDynamicParams() (map[string]string, error)

	// SyncDynamicParams syncs all specified parameters to cluster by setting the parameter table in metadb.
	SyncDynamicParams(params map[string]string) error

	// SyncSecurityIPs syncs all specified security ips to cluster.
	SyncSecurityIPs([]string) error

	// CreateDBAccount creates an account in cluster by directly creating records in metadb.
	CreateDBAccount(user, passwd string, grantOptions ...*GrantOption) error

	// DeleteDBAccount deletes the specified account in cluster by directly deleting records in metadb.
	DeleteDBAccount(user string) error

	// ModifyDBAccountType modify DB account type in metadb user_priv table if account_type fields exists.
	ModifyDBAccountType(user string, accountType AccountType) error

	// SyncAccountPasswd syncs the password of specified user in cluster by directly modifying records
	// in metadb. If the user not exists, it returns an error.
	SyncAccountPasswd(user, passwd string) error

	// SyncK8sTopology syncs the k8s topology into GMS.
	SyncK8sTopology(topology *K8sTopology) error

	// GetK8sTopology gets the k8s topology from GMS.
	GetK8sTopology() (*K8sTopology, error)

	// SetGlobalVariables sets the global variables for GMS.
	SetGlobalVariables(key, value string) error

	// Lock locks the cluster, idempotent
	Lock() error

	// Unlock unlocks the cluster, idempotent
	Unlock() error

	// Close closes the manager
	Close() error
}

type K8sObject struct {
	Uid  types.UID
	Name string
}

type K8sTopology struct {
	GMS K8sObject
	DN  map[string]K8sObject
}
