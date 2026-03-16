package repository

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// K8sSettingsRepository implements SettingsRepository using Kubernetes client
type K8sSettingsRepository struct {
	client client.Client
}

// NewK8sSettingsRepository creates a new K8s implementation
func NewK8sSettingsRepository(cli client.Client) *K8sSettingsRepository {
	return &K8sSettingsRepository{client: cli}
}

// GetConfigMap retrieves the specified ConfigMap
func (r *K8sSettingsRepository) GetConfigMap(ctx context.Context, namespace, name string) (*corev1.ConfigMap, error) {
	cm := &corev1.ConfigMap{}
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cm); err != nil {
		return nil, err
	}
	return cm, nil
}

// CreateConfigMap creates a ConfigMap
func (r *K8sSettingsRepository) CreateConfigMap(ctx context.Context, cm *corev1.ConfigMap) error {
	return r.client.Create(ctx, cm)
}

// UpdateConfigMap updates a ConfigMap
func (r *K8sSettingsRepository) UpdateConfigMap(ctx context.Context, cm *corev1.ConfigMap) error {
	return r.client.Update(ctx, cm)
}
