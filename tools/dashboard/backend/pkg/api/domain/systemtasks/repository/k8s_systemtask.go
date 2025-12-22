package repository

import (
	"context"

	"polardbx-dashboard-backend/pkg/k8s"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// K8sSystemTaskRepository is the K8s implementation of SystemTaskRepository
type K8sSystemTaskRepository struct{}

// NewK8sSystemTaskRepository creates a K8s implementation of Repository
func NewK8sSystemTaskRepository() *K8sSystemTaskRepository {
	return &K8sSystemTaskRepository{}
}

// Ensure interface implementation
var _ SystemTaskRepository = (*K8sSystemTaskRepository)(nil)

func (r *K8sSystemTaskRepository) List(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.SystemTask, error) {
	return k8s.ListSystemTasksWithContext(ctx, cli, namespace)
}

func (r *K8sSystemTaskRepository) Get(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.SystemTask, error) {
	return k8s.GetSystemTaskWithContext(ctx, cli, namespace, name)
}

func (r *K8sSystemTaskRepository) Create(ctx context.Context, cli client.Client, namespace string, task *polardbxv1.SystemTask) (*polardbxv1.SystemTask, error) {
	return k8s.CreateSystemTaskWithContext(ctx, cli, namespace, task)
}

func (r *K8sSystemTaskRepository) Update(ctx context.Context, cli client.Client, namespace string, task *polardbxv1.SystemTask) (*polardbxv1.SystemTask, error) {
	return k8s.UpdateSystemTaskWithContext(ctx, cli, namespace, task)
}

func (r *K8sSystemTaskRepository) Delete(ctx context.Context, cli client.Client, namespace, name string) error {
	return k8s.DeleteSystemTaskWithContext(ctx, cli, namespace, name)
}
