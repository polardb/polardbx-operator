package repository

import (
	"context"

	corev1 "k8s.io/api/core/v1"
)

// SettingsRepository defines the settings storage layer interface
type SettingsRepository interface {
	// GetConfigMap retrieves the specified ConfigMap
	GetConfigMap(ctx context.Context, namespace, name string) (*corev1.ConfigMap, error)

	// CreateConfigMap creates a ConfigMap
	CreateConfigMap(ctx context.Context, cm *corev1.ConfigMap) error

	// UpdateConfigMap updates a ConfigMap
	UpdateConfigMap(ctx context.Context, cm *corev1.ConfigMap) error
}
