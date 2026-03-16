package repository

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
)

// SystemRepository defines the storage layer interface for system information queries
type SystemRepository interface {
	// ListNamespaces lists all namespaces
	ListNamespaces(ctx context.Context) ([]corev1.Namespace, error)
	// ListStorageClasses lists all storage classes
	ListStorageClasses(ctx context.Context) ([]storagev1.StorageClass, error)
}
