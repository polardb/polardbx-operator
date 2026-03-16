package service

import (
	"context"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"polardbx-dashboard-backend/pkg/api/domain/platform/settings/repository"
	"polardbx-dashboard-backend/pkg/config"
)

const (
	SettingsNamespace = "polardbx-operator-system"
	SettingsConfigMap = "polardbx-dashboard-backend-config"
)

// BackupDashboardSettings backup dashboard settings
type BackupDashboardSettings struct {
	RPOThresholdSeconds                int     `json:"rpoThresholdSeconds"`
	ThroughputLowerBoundMBps           float64 `json:"throughputLowerBoundMBps"`
	DiagnosisRetentionDays             int     `json:"diagnosisRetentionDays"`
	AutoRebuildLagThresholdSeconds     int     `json:"autoRebuildLagThresholdSeconds"`
	AutoRebuildPreferredNodeLabelKey   string  `json:"autoRebuildPreferredNodeLabelKey"`
	AutoRebuildPreferredNodeLabelValue string  `json:"autoRebuildPreferredNodeLabelValue"`
}

// SettingsService defines settings business logic layer
type SettingsService struct {
	repo repository.SettingsRepository
}

// NewSettingsService creates a new SettingsService
func NewSettingsService(repo repository.SettingsRepository) *SettingsService {
	return &SettingsService{repo: repo}
}

// Get gets all settings
func (s *SettingsService) Get(ctx context.Context) (map[string]string, error) {
	cm, _ := s.repo.GetConfigMap(ctx, SettingsNamespace, SettingsConfigMap)
	if cm == nil {
		return map[string]string{}, nil
	}
	return cm.Data, nil
}

// Update updates settings
func (s *SettingsService) Update(ctx context.Context, body map[string]any) error {
	cm, err := s.repo.GetConfigMap(ctx, SettingsNamespace, SettingsConfigMap)
	if err != nil {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: SettingsNamespace, Name: SettingsConfigMap},
			Data:       map[string]string{},
		}
		if err2 := s.repo.CreateConfigMap(ctx, cm); err2 != nil {
			return err2
		}
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	for k, v := range body {
		cm.Data[k] = fmt.Sprintf("%v", v)
	}
	return s.repo.UpdateConfigMap(ctx, cm)
}

// GetDashboardSettings gets backup dashboard settings
func (s *SettingsService) GetDashboardSettings(ctx context.Context) BackupDashboardSettings {
	settings := defaultSettings()
	cm, err := s.repo.GetConfigMap(ctx, SettingsNamespace, SettingsConfigMap)
	if err != nil || cm == nil || cm.Data == nil {
		return settings
	}
	settings.RPOThresholdSeconds = parseInt(cm.Data["rpoThresholdSeconds"], settings.RPOThresholdSeconds)
	settings.ThroughputLowerBoundMBps = parseFloat(cm.Data["throughputLowerBoundMBps"], settings.ThroughputLowerBoundMBps)
	settings.DiagnosisRetentionDays = parseInt(cm.Data["diagnosisRetentionDays"], settings.DiagnosisRetentionDays)
	settings.AutoRebuildLagThresholdSeconds = parseInt(cm.Data["autoRebuildLagThresholdSeconds"], settings.AutoRebuildLagThresholdSeconds)
	if v, ok := cm.Data["autoRebuildPreferredNodeLabelKey"]; ok {
		settings.AutoRebuildPreferredNodeLabelKey = v
	}
	if v, ok := cm.Data["autoRebuildPreferredNodeLabelValue"]; ok {
		settings.AutoRebuildPreferredNodeLabelValue = v
	}
	return settings
}

// GetImageRegistryConfig gets image registry configuration
func (s *SettingsService) GetImageRegistryConfig() map[string]any {
	cfg := config.GetGlobalConfig()
	return cfg.ToMap()
}

// UpdateImageRegistryConfig updates image registry configuration
func (s *SettingsService) UpdateImageRegistryConfig(registry, customRegistry, defaultRegistry string) (map[string]any, error) {
	cfg := config.GetGlobalConfig()

	reg := registry
	if reg == "" {
		reg = defaultRegistry
	}
	if reg == "custom" && customRegistry != "" {
		reg = customRegistry
	}
	if reg == "" {
		return nil, ErrEmptyRegistry
	}

	cfg.SetDefaultRegistry(reg)
	return cfg.ToMap(), nil
}

// GetAvailableRegistries gets available image registry presets
func (s *SettingsService) GetAvailableRegistries() []map[string]interface{} {
	return []map[string]interface{}{
		{
			"name":        "DaoCloud Mirror (Recommended)",
			"registry":    "docker.m.daocloud.io",
			"description": "DaoCloud public image acceleration service, fully proxies Docker Hub",
			"region":      "China",
			"status":      "verified",
		},
		{
			"name":        "Docker Hub (Official)",
			"registry":    "docker.io",
			"description": "Official Docker Hub image registry (docker.io)",
			"region":      "Global",
			"status":      "slow",
		},
		{
			"name":        "Custom Image Registry",
			"registry":    "custom",
			"description": "Use enterprise private image registry (such as Harbor), need to sync alpine/helm:3.12.3 image in advance",
			"region":      "Custom",
			"status":      "custom",
		},
	}
}

func defaultSettings() BackupDashboardSettings {
	return BackupDashboardSettings{
		RPOThresholdSeconds:                3600,
		ThroughputLowerBoundMBps:           1.0,
		DiagnosisRetentionDays:             7,
		AutoRebuildLagThresholdSeconds:     0,
		AutoRebuildPreferredNodeLabelKey:   "",
		AutoRebuildPreferredNodeLabelValue: "",
	}
}

func parseInt(s string, def int) int {
	if v, err := strconv.Atoi(s); err == nil {
		return v
	}
	return def
}

func parseFloat(s string, def float64) float64 {
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return v
	}
	return def
}

// Error definitions
type SettingsError string

func (e SettingsError) Error() string { return string(e) }

const (
	ErrEmptyRegistry SettingsError = "registry cannot be empty"
)
