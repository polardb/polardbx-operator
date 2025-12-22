package services

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	svcerr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
	"polardbx-dashboard-backend/pkg/logger"
)

// patchClusterJSON is a test-hookable wrapper around util.K8sPatchClusterJSON.
var patchClusterJSON = util.K8sPatchClusterJSON

// ClusterService aggregates cluster-related orchestration operations.
type ClusterService struct{}

func NewClusterService() *ClusterService { return &ClusterService{} }

// UpdateLogConfig constructs and applies a JSON patch to update log config for a node type.
func (s *ClusterService) UpdateLogConfig(ctx context.Context, cli client.Client, namespace, name, nodeType string, req *LogConfigRequest) error {
	if cli == nil {
		return svcerr.InternalServiceError("kubernetes client not initialized", nil)
	}
	switch nodeType {
	case "cn", "dn", "gms", "cdc":
	default:
		return svcerr.ValidationError("invalid nodeType: "+nodeType, nil)
	}
	if req == nil {
		return svcerr.ValidationError("log config payload is required", nil)
	}
	patchData := map[string]any{"spec": map[string]any{"config": map[string]any{nodeType: map[string]any{}}}}
	nodeConfig := patchData["spec"].(map[string]any)["config"].(map[string]any)[nodeType].(map[string]any)
	if req.EnableAuditLog != nil {
		nodeConfig["enableAuditLog"] = *req.EnableAuditLog
	}
	if req.LogLevel != "" {
		nodeConfig["logLevel"] = req.LogLevel
	}
	if req.AuditLogFilter != "" {
		nodeConfig["auditLogFilter"] = req.AuditLogFilter
	}
	if req.SlowLogThreshold != nil {
		nodeConfig["slowLogThreshold"] = *req.SlowLogThreshold
	}
	b, _ := json.Marshal(patchData)
	if _, err := patchClusterJSON(ctx, cli, namespace, name, b); err != nil {
		logger.Error("ops UpdateLogConfig failed", "namespace", namespace, "name", name, "nodeType", nodeType, "error", err)
		return fmt.Errorf("update log config for %s/%s (%s): %w", namespace, name, nodeType, err)
	}
	logger.Info("ops UpdateLogConfig ok", "namespace", namespace, "name", name, "nodeType", nodeType)
	return nil
}

// Scale constructs and applies a JSON patch to adjust replicas.
func (s *ClusterService) Scale(ctx context.Context, cli client.Client, namespace, name string, req *ClusterScalingRequest) error {
	if cli == nil {
		return svcerr.InternalServiceError("kubernetes client not initialized", nil)
	}
	if req == nil {
		return svcerr.ValidationError("scaling payload is required", nil)
	}
	// NOTE: GMS replicas are not controlled via spec.topology.nodes.gms.replicas (it doesn't exist in CRD).
	// If we accept this field silently, the request appears successful but has no effect.
	if req.GMSReplicas != nil {
		return svcerr.ValidationError("gmsReplicas is not supported; adjust GMS via topology.rules.components.gms instead", nil)
	}
	patch := map[string]any{"spec": map[string]any{"topology": map[string]any{"nodes": map[string]any{}}}}
	nodes := patch["spec"].(map[string]any)["topology"].(map[string]any)["nodes"].(map[string]any)
	if req.CNReplicas != nil {
		nodes["cn"] = map[string]any{"replicas": *req.CNReplicas}
	}
	if req.DNReplicas != nil {
		nodes["dn"] = map[string]any{"replicas": *req.DNReplicas}
	}
	if req.CDCReplicas != nil {
		nodes["cdc"] = map[string]any{"replicas": *req.CDCReplicas}
	}
	if len(nodes) == 0 {
		return svcerr.ValidationError("no replica changes specified", nil)
	}
	start := time.Now()
	b, _ := json.Marshal(patch)
	if _, err := patchClusterJSON(ctx, cli, namespace, name, b); err != nil {
		logger.Error("ops Scale failed", "namespace", namespace, "name", name, "duration", time.Since(start), "error", err)
		return fmt.Errorf("scale cluster %s/%s: %w", namespace, name, err)
	}
	logger.Info("ops Scale ok", "namespace", namespace, "name", name, "duration", time.Since(start))
	return nil
}

// Upgrade sets target version and optional strategy via JSON patch.
func (s *ClusterService) Upgrade(ctx context.Context, cli client.Client, namespace, name string, req *ClusterUpgradeRequest) error {
	if cli == nil {
		return svcerr.InternalServiceError("kubernetes client not initialized", nil)
	}
	if req == nil {
		return svcerr.ValidationError("upgrade payload is required", nil)
	}
	if req.TargetVersion == "" {
		return svcerr.ValidationError("targetVersion is required", nil)
	}
	patch := map[string]any{"spec": map[string]any{"topology": map[string]any{"version": req.TargetVersion}}}
	if req.Strategy != "" {
		patch["spec"].(map[string]any)["upgradeStrategy"] = req.Strategy
	}
	start := time.Now()
	b, _ := json.Marshal(patch)
	if _, err := patchClusterJSON(ctx, cli, namespace, name, b); err != nil {
		logger.Error("ops Upgrade failed", "namespace", namespace, "name", name, "duration", time.Since(start), "error", err)
		return fmt.Errorf("upgrade cluster %s/%s: %w", namespace, name, err)
	}
	logger.Info("ops Upgrade ok", "namespace", namespace, "name", name, "duration", time.Since(start))
	return nil
}
