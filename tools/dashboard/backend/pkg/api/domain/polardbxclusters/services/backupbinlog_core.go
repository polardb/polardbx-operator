package services

import (
	"context"
	"fmt"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	svcerr "polardbx-dashboard-backend/pkg/api/errors"
)

// Since backupbinlog CRUD is implemented in existing modules, here we use a minimal implementation
// that directly connects to K8sClient. This can be extracted to pkg/k8s later if convenient functions exist.

type BackupBinlogService struct{}

func NewBackupBinlogService() *BackupBinlogService { return &BackupBinlogService{} }

// ListBinlogs lists backup binlogs for a namespace using pure parameters.
func (s *BackupBinlogService) ListBinlogs(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.PolarDBXBackupBinlog, error) {
	var list polardbxv1.PolarDBXBackupBinlogList
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := cli.List(ctx, &list, opts...); err != nil {
		return nil, fmt.Errorf("list backup binlogs in namespace %s: %w", namespace, err)
	}
	return list.Items, nil
}

// CreateBinlog creates a new backup binlog using pure parameters.
func (s *BackupBinlogService) CreateBinlog(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.PolarDBXBackupBinlog) (*polardbxv1.PolarDBXBackupBinlog, error) {
	if obj == nil {
		return nil, svcerr.ValidationError("backup binlog payload is required", nil)
	}
	if obj.Name == "" {
		return nil, svcerr.ValidationError("name is required", nil)
	}
	if obj.Namespace == "" {
		if namespace == "" {
			return nil, svcerr.ValidationError("namespace is required", nil)
		}
		obj.Namespace = namespace
	}
	if err := cli.Create(ctx, obj); err != nil {
		return nil, fmt.Errorf("create backup binlog %s/%s: %w", obj.Namespace, obj.Name, err)
	}
	return obj, nil
}

// GetBinlog gets a backup binlog using pure parameters.
func (s *BackupBinlogService) GetBinlog(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.PolarDBXBackupBinlog, error) {
	var obj polardbxv1.PolarDBXBackupBinlog
	if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &obj); err != nil {
		return nil, fmt.Errorf("get backup binlog %s/%s: %w", namespace, name, err)
	}
	return &obj, nil
}

// UpdateBinlog updates a backup binlog using pure parameters.
func (s *BackupBinlogService) UpdateBinlog(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.PolarDBXBackupBinlog) (*polardbxv1.PolarDBXBackupBinlog, error) {
	if obj == nil {
		return nil, svcerr.ValidationError("backup binlog payload is required", nil)
	}
	if obj.Name == "" {
		return nil, svcerr.ValidationError("name is required", nil)
	}
	if namespace == "" && obj.Namespace == "" {
		return nil, svcerr.ValidationError("namespace is required", nil)
	}
	if obj.Namespace == "" {
		obj.Namespace = namespace
	}
	if err := cli.Update(ctx, obj); err != nil {
		return nil, fmt.Errorf("update backup binlog %s/%s: %w", obj.Namespace, obj.Name, err)
	}
	return obj, nil
}

// DeleteBinlog deletes a backup binlog using pure parameters.
func (s *BackupBinlogService) DeleteBinlog(ctx context.Context, cli client.Client, namespace, name string) error {
	if name == "" {
		return svcerr.ValidationError("name is required", nil)
	}
	if namespace == "" {
		return svcerr.ValidationError("namespace is required", nil)
	}
	var obj polardbxv1.PolarDBXBackupBinlog
	obj.Namespace = namespace
	obj.Name = name
	if err := cli.Delete(ctx, &obj); err != nil {
		return fmt.Errorf("delete backup binlog %s/%s: %w", namespace, name, err)
	}
	return nil
}
