package services

import (
	"context"
	"fmt"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-dashboard-backend/pkg/api/domain/xstores/k8srepo"
)

// BackupsService encapsulates XStore backup related orchestration
// Refactored to be independent of HTTP framework for better testability
type BackupsService struct {
	repo k8srepo.XStoreRepository
}

// NewBackupsService creates a new BackupsService with the given repository
func NewBackupsService(repo k8srepo.XStoreRepository) *BackupsService {
	if repo == nil {
		repo = k8srepo.NewXStoreRepository()
	}
	return &BackupsService{repo: repo}
}

// List lists XStore backups in the specified namespace
func (s *BackupsService) List(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.XStoreBackup, error) {
	items, err := s.repo.ListBackups(ctx, cli, namespace)
	if err != nil {
		return nil, fmt.Errorf("list xstore backups in namespace %s: %w", namespace, err)
	}
	return items, nil
}

// Get gets the specified XStore backup
func (s *BackupsService) Get(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStoreBackup, error) {
	item, err := s.repo.GetBackup(ctx, cli, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("get xstore backup %s/%s: %w", namespace, name, err)
	}
	return item, nil
}

// Create creates a new XStore backup
func (s *BackupsService) Create(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error) {
	created, err := s.repo.CreateBackup(ctx, cli, namespace, obj)
	if err != nil {
		return nil, fmt.Errorf("create xstore backup in namespace %s: %w", namespace, err)
	}
	return created, nil
}

// Update updates an existing XStore backup
func (s *BackupsService) Update(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error) {
	updated, err := s.repo.UpdateBackup(ctx, cli, namespace, obj)
	if err != nil {
		return nil, fmt.Errorf("update xstore backup %s/%s: %w", namespace, obj.Name, err)
	}
	return updated, nil
}

// Delete deletes the specified XStore backup
func (s *BackupsService) Delete(ctx context.Context, cli client.Client, namespace, name string) error {
	if err := s.repo.DeleteBackup(ctx, cli, namespace, name); err != nil {
		return fmt.Errorf("delete xstore backup %s/%s: %w", namespace, name, err)
	}
	return nil
}

// ForceDelete forcefully deletes an XStore backup by removing finalizers
func (s *BackupsService) ForceDelete(ctx context.Context, cli client.Client, namespace, name string) error {
	bk, err := s.repo.GetBackup(ctx, cli, namespace, name)
	if err != nil {
		return fmt.Errorf("get xstore backup %s/%s for force delete: %w", namespace, name, err)
	}
	bk.SetFinalizers([]string{})
	if _, err := s.repo.UpdateBackup(ctx, cli, namespace, bk); err != nil {
		return fmt.Errorf("remove finalizers from xstore backup %s/%s: %w", namespace, name, err)
	}
	return nil
}

// RemoteInfo gets remote storage information for an XStore backup
func (s *BackupsService) RemoteInfo(ctx context.Context, cli client.Client, namespace, name string) (map[string]interface{}, error) {
	bk, err := s.repo.GetBackup(ctx, cli, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("get xstore backup %s/%s for remote info: %w", namespace, name, err)
	}
	return map[string]interface{}{
		"namespace": namespace,
		"name":      name,
		"phase":     bk.Status.Phase,
	}, nil
}
