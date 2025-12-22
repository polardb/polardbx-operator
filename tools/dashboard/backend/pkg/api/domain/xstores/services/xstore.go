package services

import (
	"context"
	"fmt"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-dashboard-backend/pkg/api/domain/xstores/k8srepo"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
)

// XStoreService encapsulates XStore basic CRUD and Pod listing
// Refactored to be independent of HTTP framework for better testability
type XStoreService struct {
	repo k8srepo.XStoreRepository
}

// NewXStoreService creates a new XStoreService with the given repository
func NewXStoreService(repo k8srepo.XStoreRepository) *XStoreService {
	if repo == nil {
		repo = k8srepo.NewXStoreRepository()
	}
	return &XStoreService{repo: repo}
}

// List lists XStores in the specified namespace
func (s *XStoreService) List(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.XStore, error) {
	items, err := s.repo.List(ctx, cli, namespace)
	if err != nil {
		return nil, fmt.Errorf("list xstores in namespace %s: %w", namespace, err)
	}
	return items, nil
}

// Get gets the specified XStore
func (s *XStoreService) Get(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStore, error) {
	item, err := s.repo.Get(ctx, cli, namespace, name)
	if err != nil {
		// When the underlying error is a Kubernetes NotFound, surface it as a
		// service-level NotFoundError so that handlers can return a consistent
		// RES_4001 error code instead of a lower-level K8S_* code.
		if k8serrors.IsNotFound(err) {
			return nil, apierr.NotFoundError("xstore", fmt.Sprintf("%s/%s", namespace, name))
		}
		return nil, fmt.Errorf("get xstore %s/%s: %w", namespace, name, err)
	}
	return item, nil
}

// Create creates a new XStore
func (s *XStoreService) Create(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStore) (*polardbxv1.XStore, error) {
	created, err := s.repo.Create(ctx, cli, namespace, obj)
	if err != nil {
		return nil, fmt.Errorf("create xstore in namespace %s: %w", namespace, err)
	}
	return created, nil
}

// Update updates an existing XStore
func (s *XStoreService) Update(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStore) (*polardbxv1.XStore, error) {
	updated, err := s.repo.Update(ctx, cli, namespace, obj)
	if err != nil {
		return nil, fmt.Errorf("update xstore %s/%s: %w", namespace, obj.Name, err)
	}
	return updated, nil
}

// Delete deletes the specified XStore
func (s *XStoreService) Delete(ctx context.Context, cli client.Client, namespace, name string) error {
	if err := s.repo.Delete(ctx, cli, namespace, name); err != nil {
		return fmt.Errorf("delete xstore %s/%s: %w", namespace, name, err)
	}
	return nil
}

// ListPods lists Pods that belong to the specified XStore
func (s *XStoreService) ListPods(ctx context.Context, cli client.Client, namespace, xstoreName string) ([]corev1.Pod, error) {
	items, err := s.repo.ListPods(ctx, cli, namespace, xstoreName)
	if err != nil {
		return nil, fmt.Errorf("list pods for xstore %s/%s: %w", namespace, xstoreName, err)
	}
	return items, nil
}
