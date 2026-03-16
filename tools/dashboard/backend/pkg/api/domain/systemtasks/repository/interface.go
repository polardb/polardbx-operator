package repository

import (
	"context"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SystemTaskRepository defines the data access interface for SystemTask resources
type SystemTaskRepository interface {
	List(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.SystemTask, error)
	Get(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.SystemTask, error)
	Create(ctx context.Context, cli client.Client, namespace string, task *polardbxv1.SystemTask) (*polardbxv1.SystemTask, error)
	Update(ctx context.Context, cli client.Client, namespace string, task *polardbxv1.SystemTask) (*polardbxv1.SystemTask, error)
	Delete(ctx context.Context, cli client.Client, namespace, name string) error
}
