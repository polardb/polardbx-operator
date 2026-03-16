package services

import (
	"context"
	"fmt"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// BackupBinlogService encapsulates minimal CRUD for XStore standard edition incremental log backup (XStoreBackupBinlog)
// Refactored to be independent of HTTP framework for better testability
type BackupBinlogService struct{}

// NewBackupBinlogService creates a new BackupBinlogService
func NewBackupBinlogService() *BackupBinlogService {
	return &BackupBinlogService{}
}

// List lists XStore backup binlogs, optionally filtered by namespace
func (s *BackupBinlogService) List(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.XStoreBackupBinlog, error) {
	var list polardbxv1.XStoreBackupBinlogList
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := cli.List(ctx, &list, opts...); err != nil {
		return nil, fmt.Errorf("list xstore backup binlogs: %w", err)
	}
	return list.Items, nil
}

// Get gets the specified XStore backup binlog
func (s *BackupBinlogService) Get(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStoreBackupBinlog, error) {
	var obj polardbxv1.XStoreBackupBinlog
	if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &obj); err != nil {
		return nil, fmt.Errorf("get xstore backup binlog %s/%s: %w", namespace, name, err)
	}
	return &obj, nil
}

// Create creates a new XStore backup binlog
func (s *BackupBinlogService) Create(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreBackupBinlog) (*polardbxv1.XStoreBackupBinlog, error) {
	if obj.Namespace == "" {
		obj.Namespace = namespace
	}
	if err := cli.Create(ctx, obj); err != nil {
		return nil, fmt.Errorf("create xstore backup binlog in namespace %s: %w", namespace, err)
	}
	return obj, nil
}

// Update updates an existing XStore backup binlog
func (s *BackupBinlogService) Update(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreBackupBinlog) (*polardbxv1.XStoreBackupBinlog, error) {
	obj.Namespace = namespace
	if err := cli.Update(ctx, obj); err != nil {
		return nil, fmt.Errorf("update xstore backup binlog %s/%s: %w", namespace, obj.Name, err)
	}
	return obj, nil
}

// Delete deletes the specified XStore backup binlog
func (s *BackupBinlogService) Delete(ctx context.Context, cli client.Client, namespace, name string) error {
	obj := &polardbxv1.XStoreBackupBinlog{}
	obj.Namespace = namespace
	obj.Name = name
	if err := cli.Delete(ctx, obj); err != nil {
		return fmt.Errorf("delete xstore backup binlog %s/%s: %w", namespace, name, err)
	}
	return nil
}
