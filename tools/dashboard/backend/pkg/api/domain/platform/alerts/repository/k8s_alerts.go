package repository

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// K8sAlertsRepository implements AlertsRepository using Kubernetes client
type K8sAlertsRepository struct {
	client client.Client
}

// NewK8sAlertsRepository creates new K8s implementation
func NewK8sAlertsRepository(cli client.Client) *K8sAlertsRepository {
	return &K8sAlertsRepository{client: cli}
}

// GetConfigMap retrieves the specified ConfigMap
func (r *K8sAlertsRepository) GetConfigMap(ctx context.Context, namespace, name string) (*corev1.ConfigMap, error) {
	cm := &corev1.ConfigMap{}
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cm); err != nil {
		return nil, err
	}
	return cm, nil
}

// CreateConfigMap creates a ConfigMap
func (r *K8sAlertsRepository) CreateConfigMap(ctx context.Context, cm *corev1.ConfigMap) error {
	return r.client.Create(ctx, cm)
}

// UpdateConfigMap updates a ConfigMap
func (r *K8sAlertsRepository) UpdateConfigMap(ctx context.Context, cm *corev1.ConfigMap) error {
	return r.client.Update(ctx, cm)
}

// ListEvents lists events
func (r *K8sAlertsRepository) ListEvents(ctx context.Context, namespace string) ([]corev1.Event, error) {
	var evList corev1.EventList
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := r.client.List(ctx, &evList, opts...); err != nil {
		return nil, err
	}
	return evList.Items, nil
}
