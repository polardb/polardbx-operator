package repository

import (
	"context"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RestoreRepository defines the storage layer interface for restore operations
type RestoreRepository interface {
	// GetCluster gets cluster
	GetCluster(ctx context.Context, namespace, name string) (*polardbxv1.PolarDBXCluster, error)

	// CreateCluster creates cluster
	CreateCluster(ctx context.Context, cluster *polardbxv1.PolarDBXCluster) error

	// DeleteCluster deletes cluster
	DeleteCluster(ctx context.Context, namespace, name string) error

	// ListClusters lists clusters
	ListClusters(ctx context.Context, namespace string) ([]polardbxv1.PolarDBXCluster, error)

	// GetBackup gets backup
	GetBackup(ctx context.Context, namespace, name string) (*polardbxv1.PolarDBXBackup, error)
}

// K8sRestoreRepository implements RestoreRepository using Kubernetes client
type K8sRestoreRepository struct {
	client client.Client
}

// NewK8sRestoreRepository creates new K8s implementation
func NewK8sRestoreRepository(cli client.Client) *K8sRestoreRepository {
	return &K8sRestoreRepository{client: cli}
}

// GetCluster gets cluster
func (r *K8sRestoreRepository) GetCluster(ctx context.Context, namespace, name string) (*polardbxv1.PolarDBXCluster, error) {
	var cluster polardbxv1.PolarDBXCluster
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &cluster); err != nil {
		return nil, err
	}
	return &cluster, nil
}

// CreateCluster creates cluster
func (r *K8sRestoreRepository) CreateCluster(ctx context.Context, cluster *polardbxv1.PolarDBXCluster) error {
	return r.client.Create(ctx, cluster)
}

func (r *K8sRestoreRepository) DeleteCluster(ctx context.Context, namespace, name string) error {
	obj := &polardbxv1.PolarDBXCluster{}
	obj.Namespace = namespace
	obj.Name = name
	return r.client.Delete(ctx, obj)
}

// ListClusters lists clusters
func (r *K8sRestoreRepository) ListClusters(ctx context.Context, namespace string) ([]polardbxv1.PolarDBXCluster, error) {
	var clusterList polardbxv1.PolarDBXClusterList
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := r.client.List(ctx, &clusterList, opts...); err != nil {
		return nil, err
	}
	return clusterList.Items, nil
}

// GetBackup gets backup
func (r *K8sRestoreRepository) GetBackup(ctx context.Context, namespace, name string) (*polardbxv1.PolarDBXBackup, error) {
	var backup polardbxv1.PolarDBXBackup
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &backup); err != nil {
		return nil, err
	}
	return &backup, nil
}
