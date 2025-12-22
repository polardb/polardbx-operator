package service

import (
	"context"

	corev1 "k8s.io/api/core/v1"

	"polardbx-dashboard-backend/pkg/api/domain/platform/pod/repository"
)

// PodService defines the Pod business logic layer
type PodService struct {
	repo repository.PodRepository
}

// NewPodService creates new PodService
func NewPodService(repo repository.PodRepository) *PodService {
	return &PodService{repo: repo}
}

// List lists all Pods in the specified namespace
func (s *PodService) List(ctx context.Context, namespace string) ([]corev1.Pod, error) {
	return s.repo.List(ctx, namespace)
}

// ListForCluster lists all Pods for a PolarDBX cluster
func (s *PodService) ListForCluster(ctx context.Context, namespace, clusterName string) ([]corev1.Pod, error) {
	return s.repo.ListForCluster(ctx, namespace, clusterName)
}

// Get retrieves the specified Pod
func (s *PodService) Get(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	return s.repo.Get(ctx, namespace, name)
}

// Delete removes the specified Pod
func (s *PodService) Delete(ctx context.Context, namespace, name string) error {
	return s.repo.Delete(ctx, namespace, name)
}

// GetLogs retrieves Pod logs
func (s *PodService) GetLogs(ctx context.Context, namespace, podName, container string, tailLines int64) (string, error) {
	return s.repo.GetLogs(ctx, namespace, podName, container, tailLines)
}
