package repository

import (
	"context"

	corev1 "k8s.io/api/core/v1"
)

// AlertsRepository defines alert storage layer interface
type AlertsRepository interface {
	// GetConfigMap retrieves the specified ConfigMap
	GetConfigMap(ctx context.Context, namespace, name string) (*corev1.ConfigMap, error)

	// CreateConfigMap creates a ConfigMap
	CreateConfigMap(ctx context.Context, cm *corev1.ConfigMap) error

	// UpdateConfigMap updates a ConfigMap
	UpdateConfigMap(ctx context.Context, cm *corev1.ConfigMap) error

	// ListEvents lists events
	ListEvents(ctx context.Context, namespace string) ([]corev1.Event, error)
}
