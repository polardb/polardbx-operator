package repository

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// K8sSystemRepository implements SystemRepository using Kubernetes client
type K8sSystemRepository struct {
	client client.Client
}

// NewK8sSystemRepository creates new K8s implementation
func NewK8sSystemRepository(cli client.Client) *K8sSystemRepository {
	return &K8sSystemRepository{client: cli}
}

// ListNamespaces lists all namespaces
func (r *K8sSystemRepository) ListNamespaces(ctx context.Context) ([]corev1.Namespace, error) {
	var nsList corev1.NamespaceList
	if err := r.client.List(ctx, &nsList, &client.ListOptions{}); err != nil {
		return nil, err
	}
	return nsList.Items, nil
}

// ListStorageClasses lists all storage classes
func (r *K8sSystemRepository) ListStorageClasses(ctx context.Context) ([]storagev1.StorageClass, error) {
	var scList storagev1.StorageClassList
	if err := r.client.List(ctx, &scList, &client.ListOptions{}); err != nil {
		return nil, err
	}
	return scList.Items, nil
}
