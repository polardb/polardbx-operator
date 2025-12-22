package services

// DTOs for cluster operations (kept minimal to preserve API contract)

type LogConfigRequest struct {
	EnableAuditLog   *bool  `json:"enableAuditLog,omitempty"`
	LogLevel         string `json:"logLevel,omitempty"`
	AuditLogFilter   string `json:"auditLogFilter,omitempty"`
	SlowLogThreshold *int   `json:"slowLogThreshold,omitempty"`
}

type ClusterScalingRequest struct {
	CNReplicas  *int32 `json:"cnReplicas,omitempty"`
	DNReplicas  *int32 `json:"dnReplicas,omitempty"`
	GMSReplicas *int32 `json:"gmsReplicas,omitempty"`
	CDCReplicas *int32 `json:"cdcReplicas,omitempty"`
}

type ClusterUpgradeRequest struct {
	TargetVersion  string `json:"targetVersion" binding:"required"`
	Strategy       string `json:"strategy,omitempty"`
	MaxUnavailable *int32 `json:"maxUnavailable,omitempty"`
}

// NodeResources is the node resource configuration
type NodeResources struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

// ClusterNodeConfig is the cluster node configuration
type ClusterNodeConfig struct {
	Replicas  int           `json:"replicas"`
	Resources NodeResources `json:"resources,omitempty"`
}

// ClusterTopologyConfig is the cluster topology configuration
type ClusterTopologyConfig struct {
	CN  ClusterNodeConfig  `json:"cn"`
	DN  ClusterNodeConfig  `json:"dn"`
	GMS ClusterNodeConfig  `json:"gms"`
	CDC *ClusterNodeConfig `json:"cdc,omitempty"`
}

// StorageConfig is the storage configuration
type StorageConfig struct {
	// StorageClassName is ignored by PolarDB-X Operator default HostPath storage model.
	// Kept for backward compatibility with older dashboard payloads.
	StorageClassName string `json:"storageClassName,omitempty"`

	// Size is mapped to DN/GMS diskQuota (soft quota) in PolarDBXCluster spec.
	Size             string   `json:"size,omitempty"`

	// AccessMode/AccessModes are ignored (no PVC semantics in PolarDBXCluster CRD).
	// Kept for backward compatibility with older dashboard payloads.
	AccessMode  string   `json:"accessMode,omitempty"`
	AccessModes []string `json:"accessModes,omitempty"`
}

// NetworkConfig is the network configuration
type NetworkConfig struct {
	ServiceType       string `json:"serviceType,omitempty"`
	LoadBalancerClass string `json:"loadBalancerClass,omitempty"`
	HostNetwork       bool   `json:"hostNetwork,omitempty"` // Use host network mode
}

// SecurityConfig is the security configuration
type SecurityConfig struct {
	EnableTLS  bool   `json:"enableTLS,omitempty"`
	SecretName string `json:"secretName,omitempty"`
}

// AdvancedConfig is the advanced configuration
type AdvancedConfig struct {
	EnableMonitoring    bool              `json:"enableMonitoring,omitempty"`
	EnableBackup        bool              `json:"enableBackup,omitempty"`
	EnableLogCollection bool              `json:"enableLogCollection,omitempty"`
	CustomLabels        map[string]string `json:"customLabels,omitempty"`
	CustomAnnotations   map[string]string `json:"customAnnotations,omitempty"`
	NodeSelector        map[string]string `json:"nodeSelector,omitempty"` // Node selector
	ShareGMS            bool              `json:"shareGMS,omitempty"`     // GMS shared minimal mode
}

// ImageConfig is the image configuration
type ImageConfig struct {
	Repository string `json:"repository,omitempty"` // Image repository
	Tag        string `json:"tag,omitempty"`        // Image tag
	PullPolicy string `json:"pullPolicy,omitempty"` // Pull policy: Always, IfNotPresent, Never
}

// ClusterCreationConfig is the cluster creation configuration (user-friendly format)
type ClusterCreationConfig struct {
	Name        string                `json:"name" binding:"required"`
	Namespace   string                `json:"namespace,omitempty"`
	Description string                `json:"description,omitempty"`
	Version     string                `json:"version,omitempty"`
	Image       *ImageConfig          `json:"image,omitempty"` // Image configuration
	Topology    ClusterTopologyConfig `json:"topology" binding:"required"`
	Storage     StorageConfig         `json:"storage"`
	Network     *NetworkConfig        `json:"network,omitempty"`
	Security    *SecurityConfig       `json:"security,omitempty"`
	Advanced    *AdvancedConfig       `json:"advanced,omitempty"`
}
