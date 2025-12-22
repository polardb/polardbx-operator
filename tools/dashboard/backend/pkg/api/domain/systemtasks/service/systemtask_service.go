package service

import (
	"context"
	"fmt"

	"polardbx-dashboard-backend/pkg/api/domain/systemtasks/repository"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SystemTaskService provides business logic for SystemTask
// Does not depend on HTTP, receives pure business parameters
type SystemTaskService struct {
	repo repository.SystemTaskRepository
}

// NewSystemTaskService creates a Service instance
func NewSystemTaskService(repo repository.SystemTaskRepository) *SystemTaskService {
	return &SystemTaskService{repo: repo}
}

// List lists all SystemTasks in the specified namespace
func (s *SystemTaskService) List(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.SystemTask, error) {
	tasks, err := s.repo.List(ctx, cli, namespace)
	if err != nil {
		return nil, fmt.Errorf("list system tasks: %w", err)
	}
	return tasks, nil
}

// Get gets the specified SystemTask
func (s *SystemTaskService) Get(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.SystemTask, error) {
	task, err := s.repo.Get(ctx, cli, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("get system task %s/%s: %w", namespace, name, err)
	}
	return task, nil
}

// Create creates a SystemTask
func (s *SystemTaskService) Create(ctx context.Context, cli client.Client, namespace string, task *polardbxv1.SystemTask) (*polardbxv1.SystemTask, error) {
	created, err := s.repo.Create(ctx, cli, namespace, task)
	if err != nil {
		return nil, fmt.Errorf("create system task: %w", err)
	}
	return created, nil
}

// Update updates a SystemTask
func (s *SystemTaskService) Update(ctx context.Context, cli client.Client, namespace string, task *polardbxv1.SystemTask) (*polardbxv1.SystemTask, error) {
	updated, err := s.repo.Update(ctx, cli, namespace, task)
	if err != nil {
		return nil, fmt.Errorf("update system task: %w", err)
	}
	return updated, nil
}

// Delete deletes a SystemTask
func (s *SystemTaskService) Delete(ctx context.Context, cli client.Client, namespace, name string) error {
	if err := s.repo.Delete(ctx, cli, namespace, name); err != nil {
		return fmt.Errorf("delete system task %s/%s: %w", namespace, name, err)
	}
	return nil
}
