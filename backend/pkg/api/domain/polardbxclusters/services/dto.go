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
