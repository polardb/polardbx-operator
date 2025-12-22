// Package xstores provides DTO types for Swagger documentation.
// These types mirror the Kubernetes CRD structures but are defined locally
// to enable proper Swagger schema generation without external dependencies.
package xstores

import "time"

// =============================================================================
// Common / Shared DTOs
// =============================================================================

// ObjectMetaDTO represents Kubernetes ObjectMeta for Swagger documentation.
// @Description Kubernetes resource metadata including name, namespace, labels and annotations.
type ObjectMetaDTO struct {
	// Resource name, unique within namespace
	Name string `json:"name" example:"my-xstore"`
	// Kubernetes namespace
	Namespace string `json:"namespace" example:"default"`
	// Unique identifier for the resource
	UID string `json:"uid,omitempty" example:"a1b2c3d4-e5f6-7890-abcd-ef1234567890"`
	// Resource version for optimistic concurrency
	ResourceVersion string `json:"resourceVersion,omitempty" example:"12345"`
	// Generation number, incremented on spec changes
	Generation int64 `json:"generation,omitempty" example:"1"`
	// Creation timestamp
	CreationTimestamp *time.Time `json:"creationTimestamp,omitempty"`
	// Labels attached to the resource
	Labels map[string]string `json:"labels,omitempty"`
	// Annotations attached to the resource
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ConditionDTO represents a Kubernetes-style condition.
// @Description Resource condition indicating current state.
type ConditionDTO struct {
	// Type of the condition
	Type string `json:"type" example:"Ready"`
	// Status of the condition (True, False, Unknown)
	Status string `json:"status" example:"True"`
	// Last time the condition transitioned
	LastTransitionTime *time.Time `json:"lastTransitionTime,omitempty"`
	// Reason for the condition's last transition
	Reason string `json:"reason,omitempty" example:"PodReady"`
	// Human-readable message
	Message string `json:"message,omitempty" example:"All pods are ready"`
}

// =============================================================================
// XStore DTOs
// =============================================================================

// XStoreDTO represents an XStore resource for Swagger documentation.
// @Description XStore is a distributed MySQL-compatible storage engine instance.
type XStoreDTO struct {
	// API version
	APIVersion string `json:"apiVersion,omitempty" example:"polardbx.aliyun.com/v1"`
	// Resource kind
	Kind string `json:"kind,omitempty" example:"XStore"`
	// Resource metadata
	Metadata ObjectMetaDTO `json:"metadata"`
	// Desired state specification
	Spec XStoreSpecDTO `json:"spec"`
	// Observed state status
	Status XStoreStatusDTO `json:"status,omitempty"`
}

// XStoreSpecDTO represents the specification of an XStore.
// @Description Desired state specification for an XStore instance.
type XStoreSpecDTO struct {
	// Storage engine type (default: galaxy)
	Engine string `json:"engine,omitempty" example:"galaxy"`
	// Service name override
	ServiceName string `json:"serviceName,omitempty" example:"my-xstore-svc"`
	// Kubernetes service type (ClusterIP, NodePort, LoadBalancer)
	ServiceType string `json:"serviceType,omitempty" example:"NodePort"`
	// Extra labels for the service
	ServiceLabels map[string]string `json:"serviceLabels,omitempty"`
	// Database accounts/privileges configuration
	Privileges []XStorePrivilegeDTO `json:"privileges,omitempty"`
	// Topology specification (nodes, replicas, resources)
	Topology XStoreTopologyDTO `json:"topology,omitempty"`
	// MySQL configuration overlay
	Config XStoreConfigDTO `json:"config,omitempty"`
	// Upgrade strategy (Force, BestEffort)
	UpgradeStrategy string `json:"upgradeStrategy,omitempty" example:"BestEffort"`
	// Parameter template reference
	ParameterTemplate XStoreParameterTemplateDTO `json:"parameterTemplate,omitempty"`
	// Whether the instance is read-only
	Readonly bool `json:"readonly,omitempty" example:"false"`
	// Primary cluster name for read-only replicas
	PrimaryCluster string `json:"primaryCluster,omitempty"`
	// Primary XStore name for read-only replicas
	PrimaryXStore string `json:"primaryXStore,omitempty"`
	// Restore specification for point-in-time recovery
	Restore *XStoreRestoreSpecDTO `json:"restore,omitempty"`
	// Transparent Data Encryption configuration
	TDE XSTORETDEDTO `json:"tde,omitempty"`
	// Resource isolation mode
	Exclusive bool `json:"exclusive,omitempty" example:"false"`
}

// XStorePrivilegeDTO represents a database account privilege.
// @Description Database account configuration for XStore.
type XStorePrivilegeDTO struct {
	// Username for the account
	Username string `json:"username" example:"admin"`
	// Password (may be referenced from secret)
	Password string `json:"password,omitempty"`
	// Privilege type (Super, ReadWrite, ReadOnly, DDLOnly, DMLOnly)
	Type string `json:"type" example:"Super"`
}

// XStoreTopologyDTO represents XStore topology configuration.
// @Description Topology configuration defining node layout and resources.
type XStoreTopologyDTO struct {
	// Node sets configuration
	NodeSets []XStoreNodeSetDTO `json:"nodeSets,omitempty"`
}

// XStoreNodeSetDTO represents a node set in XStore topology.
// @Description Configuration for a group of XStore nodes.
type XStoreNodeSetDTO struct {
	// Node set name
	Name string `json:"name" example:"cand"`
	// Role of nodes in this set (Candidate, Voter, Learner)
	Role string `json:"role" example:"Candidate"`
	// Number of replicas
	Replicas int32 `json:"replicas" example:"3"`
	// Resource requirements
	Template XStorePodTemplateDTO `json:"template,omitempty"`
}

// XStorePodTemplateDTO represents pod template for XStore nodes.
// @Description Pod template specification for XStore node resources.
type XStorePodTemplateDTO struct {
	// Resource requirements
	Resources ResourceRequirementsDTO `json:"resources,omitempty"`
	// Host network mode
	HostNetwork bool `json:"hostNetwork,omitempty"`
}

// ResourceRequirementsDTO represents Kubernetes resource requirements.
// @Description CPU and memory resource requirements.
type ResourceRequirementsDTO struct {
	// Resource limits
	Limits ResourceListDTO `json:"limits,omitempty"`
	// Resource requests
	Requests ResourceListDTO `json:"requests,omitempty"`
}

// ResourceListDTO represents a list of resources (CPU, memory).
// @Description Resource quantities for CPU and memory.
type ResourceListDTO struct {
	// CPU resource (e.g., "2", "500m")
	CPU string `json:"cpu,omitempty" example:"2"`
	// Memory resource (e.g., "4Gi", "512Mi")
	Memory string `json:"memory,omitempty" example:"4Gi"`
}

// XStoreConfigDTO represents XStore MySQL configuration.
// @Description MySQL configuration overlay for XStore.
type XStoreConfigDTO struct {
	// MySQL dynamic configuration parameters
	Dynamic map[string]any `json:"dynamic,omitempty"`
	// Engine-specific configuration
	Engine map[string]any `json:"engine,omitempty"`
}

// XStoreParameterTemplateDTO represents parameter template reference.
// @Description Reference to a parameter template.
type XStoreParameterTemplateDTO struct {
	// Template name
	Name string `json:"name,omitempty" example:"default-params"`
	// Namespace of the template
	Namespace string `json:"namespace,omitempty" example:"default"`
}

// XStoreRestoreSpecDTO represents restore specification.
// @Description Specification for restoring XStore from backup.
type XStoreRestoreSpecDTO struct {
	// Backup set name to restore from
	BackupSet string `json:"backupSet,omitempty" example:"my-backup"`
	// Point-in-time to restore to (RFC3339 format)
	Time string `json:"time,omitempty" example:"2024-01-15T10:30:00Z"`
}

// XSTORETDEDTO represents Transparent Data Encryption config.
// @Description TDE (Transparent Data Encryption) configuration.
type XSTORETDEDTO struct {
	// Enable TDE
	Enable bool `json:"enable,omitempty" example:"false"`
}

// XStoreStatusDTO represents the observed status of an XStore.
// @Description Current observed state of the XStore instance.
type XStoreStatusDTO struct {
	// Current lifecycle phase
	Phase string `json:"phase,omitempty" example:"Running"`
	// Current stage within phase
	Stage string `json:"stage,omitempty" example:"Idle"`
	// Status conditions
	Conditions []ConditionDTO `json:"conditions,omitempty"`
	// Last observed generation
	ObservedGeneration int64 `json:"observedGeneration,omitempty" example:"1"`
	// Name of the leader pod
	LeaderPod string `json:"leaderPod,omitempty" example:"my-xstore-0"`
	// Number of ready pods
	ReadyPods int32 `json:"readyPods,omitempty" example:"3"`
	// Total number of pods
	TotalPods int32 `json:"totalPods,omitempty" example:"3"`
	// Ready status string (e.g., "3/3")
	ReadyStatus string `json:"readyStatus,omitempty" example:"3/3"`
	// Total data directory size
	TotalDataDirSize string `json:"totalDataDirSize,omitempty" example:"10Gi"`
	// Engine version
	EngineVersion string `json:"engineVersion,omitempty" example:"8.0.18"`
	// Associated parameter template name
	ParameterName string `json:"parameterName,omitempty"`
	// Whether pods need restart
	Restarting bool `json:"restarting,omitempty" example:"false"`
}

// =============================================================================
// XStore Backup DTOs
// =============================================================================

// XStoreBackupDTO represents an XStore backup resource for Swagger documentation.
// @Description XStoreBackup represents a point-in-time backup of an XStore instance.
type XStoreBackupDTO struct {
	// API version
	APIVersion string `json:"apiVersion,omitempty" example:"polardbx.aliyun.com/v1"`
	// Resource kind
	Kind string `json:"kind,omitempty" example:"XStoreBackup"`
	// Resource metadata
	Metadata ObjectMetaDTO `json:"metadata"`
	// Desired state specification
	Spec XStoreBackupSpecDTO `json:"spec"`
	// Observed state status
	Status XStoreBackupStatusDTO `json:"status,omitempty"`
}

// XStoreBackupSpecDTO represents the specification of an XStore backup.
// @Description Desired state specification for an XStore backup.
type XStoreBackupSpecDTO struct {
	// Storage engine type
	Engine string `json:"engine,omitempty" example:"galaxy"`
	// Reference to the source XStore
	XStore XStoreReferenceDTO `json:"xstore"`
	// Timezone for backup timestamps
	Timezone string `json:"timezone,omitempty" example:"Asia/Shanghai"`
	// How long to retain this backup
	RetentionTime string `json:"retentionTime,omitempty" example:"168h"`
	// Storage provider configuration
	StorageProvider BackupStorageProviderDTO `json:"storageProvider,omitempty"`
	// Preferred node role for backup (leader, follower)
	PreferredBackupRole string `json:"preferredBackupRole,omitempty" example:"follower"`
	// Cleanup policy when backup is deleted (Retain, Delete, OnFailure)
	CleanPolicy string `json:"cleanPolicy,omitempty" example:"Retain"`
}

// XStoreReferenceDTO represents a reference to an XStore.
// @Description Reference to the source XStore for backup.
type XStoreReferenceDTO struct {
	// Name of the XStore
	Name string `json:"name" example:"my-xstore"`
	// UID of the XStore
	UID string `json:"uid,omitempty" example:"a1b2c3d4-e5f6-7890-abcd-ef1234567890"`
}

// BackupStorageProviderDTO represents backup storage configuration.
// @Description Configuration for backup storage destination.
type BackupStorageProviderDTO struct {
	// Storage type (S3, OSS, NFS, etc.)
	StorageName string `json:"storageName,omitempty" example:"s3"`
	// Sink name reference
	Sink string `json:"sink,omitempty" example:"my-s3-sink"`
}

// XStoreBackupStatusDTO represents the observed status of an XStore backup.
// @Description Current observed state of the XStore backup.
type XStoreBackupStatusDTO struct {
	// Current backup phase (New, Running, Completed, Failed, Deleting)
	Phase string `json:"phase,omitempty" example:"Completed"`
	// Backup start time
	StartTime *time.Time `json:"startTime,omitempty"`
	// Backup end time
	EndTime *time.Time `json:"endTime,omitempty"`
	// Pod where backup was executed
	TargetPod string `json:"targetPod,omitempty" example:"my-xstore-1"`
	// Commit index at backup time
	CommitIndex int64 `json:"commitIndex,omitempty" example:"12345"`
	// Storage type used
	StorageName string `json:"storageName,omitempty" example:"s3"`
	// Backup set path/name
	BackupSetPath string `json:"backupSetPath,omitempty" example:"/backups/my-xstore/20240115"`
	// Total backup size
	BackupSize string `json:"backupSize,omitempty" example:"1.5Gi"`
}

// =============================================================================
// XStore Backup Binlog DTOs
// =============================================================================

// XStoreBackupBinlogDTO represents an XStore binlog backup resource.
// @Description XStoreBackupBinlog represents continuous binlog backup for an XStore.
type XStoreBackupBinlogDTO struct {
	// API version
	APIVersion string `json:"apiVersion,omitempty" example:"polardbx.aliyun.com/v1"`
	// Resource kind
	Kind string `json:"kind,omitempty" example:"XStoreBackupBinlog"`
	// Resource metadata
	Metadata ObjectMetaDTO `json:"metadata"`
	// Desired state specification
	Spec XStoreBackupBinlogSpecDTO `json:"spec"`
	// Observed state status
	Status XStoreBackupBinlogStatusDTO `json:"status,omitempty"`
}

// XStoreBackupBinlogSpecDTO represents binlog backup specification.
// @Description Desired state specification for XStore binlog backup.
type XStoreBackupBinlogSpecDTO struct {
	// Reference to the source XStore
	XStore XStoreReferenceDTO `json:"xstore"`
	// Storage provider configuration
	StorageProvider BackupStorageProviderDTO `json:"storageProvider,omitempty"`
	// Local expire time for binlogs
	LocalExpireLogSeconds int64 `json:"localExpireLogSeconds,omitempty" example:"604800"`
	// Maximum local binlog usage
	MaxLocalBinlogCount int32 `json:"maxLocalBinlogCount,omitempty" example:"10"`
}

// XStoreBackupBinlogStatusDTO represents binlog backup status.
// @Description Current observed state of XStore binlog backup.
type XStoreBackupBinlogStatusDTO struct {
	// Current phase
	Phase string `json:"phase,omitempty" example:"Running"`
	// Checkpoint timestamp
	CheckpointTime *time.Time `json:"checkpointTime,omitempty"`
	// Last uploaded binlog file
	LastUploadedFile string `json:"lastUploadedFile,omitempty" example:"mysql-bin.000015"`
}

// =============================================================================
// XStore Follower DTOs
// =============================================================================

// XStoreFollowerDTO represents an XStore follower resource.
// @Description XStoreFollower represents a read-only follower of an XStore.
type XStoreFollowerDTO struct {
	// API version
	APIVersion string `json:"apiVersion,omitempty" example:"polardbx.aliyun.com/v1"`
	// Resource kind
	Kind string `json:"kind,omitempty" example:"XStoreFollower"`
	// Resource metadata
	Metadata ObjectMetaDTO `json:"metadata"`
	// Desired state specification
	Spec XStoreFollowerSpecDTO `json:"spec"`
	// Observed state status
	Status XStoreFollowerStatusDTO `json:"status,omitempty"`
}

// XStoreFollowerSpecDTO represents follower specification.
// @Description Desired state specification for XStore follower.
type XStoreFollowerSpecDTO struct {
	// Reference to the source XStore
	XStore XStoreReferenceDTO `json:"xstore"`
	// Local mode (no remote data center)
	Local bool `json:"local,omitempty" example:"true"`
	// Role of the follower node
	Role string `json:"role,omitempty" example:"Learner"`
	// Resource requirements
	Resources ResourceRequirementsDTO `json:"resources,omitempty"`
}

// XStoreFollowerStatusDTO represents follower status.
// @Description Current observed state of XStore follower.
type XStoreFollowerStatusDTO struct {
	// Current phase
	Phase string `json:"phase,omitempty" example:"Running"`
	// Ready status
	Ready bool `json:"ready,omitempty" example:"true"`
	// Replication lag in seconds
	LagSeconds int64 `json:"lagSeconds,omitempty" example:"0"`
	// Target pod name
	PodName string `json:"podName,omitempty" example:"my-xstore-follower-0"`
}

// =============================================================================
// XStore Pod DTO
// =============================================================================

// XStorePodDTO represents a pod belonging to an XStore.
// @Description Pod information for XStore instance.
type XStorePodDTO struct {
	// Pod name
	Name string `json:"name" example:"my-xstore-0"`
	// Pod namespace
	Namespace string `json:"namespace" example:"default"`
	// Pod phase (Pending, Running, Succeeded, Failed, Unknown)
	Phase string `json:"phase" example:"Running"`
	// Host IP where pod is running
	HostIP string `json:"hostIP,omitempty" example:"192.168.1.100"`
	// Pod IP address
	PodIP string `json:"podIP,omitempty" example:"10.244.0.15"`
	// Node name where pod is scheduled
	NodeName string `json:"nodeName,omitempty" example:"worker-node-1"`
	// Pod role (Leader, Follower, Learner)
	Role string `json:"role,omitempty" example:"Leader"`
	// Container statuses
	ContainerStatuses []ContainerStatusDTO `json:"containerStatuses,omitempty"`
	// Pod creation timestamp
	CreationTimestamp *time.Time `json:"creationTimestamp,omitempty"`
}

// ContainerStatusDTO represents container status within a pod.
// @Description Status of a container within a pod.
type ContainerStatusDTO struct {
	// Container name
	Name string `json:"name" example:"engine"`
	// Whether container is ready
	Ready bool `json:"ready" example:"true"`
	// Restart count
	RestartCount int32 `json:"restartCount" example:"0"`
	// Container image
	Image string `json:"image,omitempty" example:"polardbx/polardbx-engine:latest"`
}

// =============================================================================
// Response DTOs
// =============================================================================

// MessageResponseDTO represents a simple message response.
// @Description Simple response with a message field.
type MessageResponseDTO struct {
	// Response message
	Message string `json:"message" example:"Operation completed successfully"`
}

// BackupRemoteInfoDTO represents remote backup storage information.
// @Description Remote storage information for a backup.
type BackupRemoteInfoDTO struct {
	// Storage type
	StorageType string `json:"storageType" example:"s3"`
	// Remote path or URL
	Path string `json:"path" example:"s3://my-bucket/backups/my-xstore/20240115"`
	// Sink name
	Sink string `json:"sink,omitempty" example:"my-s3-sink"`
	// Backup size
	Size string `json:"size,omitempty" example:"1.5Gi"`
}

// =============================================================================
// Rebuild DTOs
// =============================================================================

// RebuildStatusDTO represents XStore rebuild operation status.
// @Description Status of an XStore rebuild operation.
type RebuildStatusDTO struct {
	// Whether rebuild is in progress
	Rebuilding bool `json:"rebuilding" example:"false"`
	// Current rebuild phase
	Phase string `json:"phase,omitempty" example:"Completed"`
	// Pod being rebuilt
	TargetPod string `json:"targetPod,omitempty" example:"my-xstore-1"`
	// Progress percentage (0-100)
	Progress int32 `json:"progress,omitempty" example:"100"`
	// Start time of rebuild
	StartTime *time.Time `json:"startTime,omitempty"`
	// End time of rebuild
	EndTime *time.Time `json:"endTime,omitempty"`
	// Error message if failed
	Error string `json:"error,omitempty"`
}

// RebuildProgressDTO represents detailed rebuild progress.
// @Description Detailed progress of an XStore rebuild operation.
type RebuildProgressDTO struct {
	// Target pod being rebuilt
	TargetPod string `json:"targetPod" example:"my-xstore-1"`
	// Current step name
	CurrentStep string `json:"currentStep" example:"CopyingData"`
	// Step number
	StepNumber int32 `json:"stepNumber" example:"3"`
	// Total steps
	TotalSteps int32 `json:"totalSteps" example:"5"`
	// Progress percentage (0-100)
	Percentage int32 `json:"percentage" example:"60"`
	// Estimated time remaining
	ETA string `json:"eta,omitempty" example:"10m"`
	// Status message
	Message string `json:"message,omitempty" example:"Copying data from leader"`
}
