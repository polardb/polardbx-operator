package services

import (
	"context"
	"fmt"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-dashboard-backend/pkg/api/domain/xstores/k8srepo"
	svcerr "polardbx-dashboard-backend/pkg/api/errors"
)

// FollowersService encapsulates XStoreFollower related orchestration (using k8srepo).
type FollowersService struct {
	repo k8srepo.XStoreRepository
}

func NewFollowersService() *FollowersService {
	return &FollowersService{repo: k8srepo.NewXStoreRepository()}
}

func (s *FollowersService) List(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.XStoreFollower, error) {
	items, err := s.repo.ListFollowers(ctx, cli, namespace)
	if err != nil {
		return nil, fmt.Errorf("list xstore followers in namespace %s: %w", namespace, err)
	}
	return items, nil
}

// Create creates a follower CR with basic validation.
func (s *FollowersService) Create(ctx context.Context, cli client.Client, obj *polardbxv1.XStoreFollower) (*polardbxv1.XStoreFollower, error) {
	if obj == nil {
		return nil, svcerr.ValidationError("follower payload is required", nil)
	}
	if obj.Name == "" {
		return nil, svcerr.ValidationError("name is required", nil)
	}
	if obj.Namespace == "" {
		return nil, svcerr.ValidationError("namespace is required", nil)
	}
	if obj.Spec.XStoreName == "" {
		return nil, svcerr.ValidationError("xStoreName is required", nil)
	}
	created, err := s.repo.CreateFollower(ctx, cli, obj.Namespace, obj)
	if err != nil {
		return nil, fmt.Errorf("create xstore follower %s/%s: %w", obj.Namespace, obj.Name, err)
	}
	return created, nil
}

func (s *FollowersService) Get(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStoreFollower, error) {
	item, err := s.repo.GetFollower(ctx, cli, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("get xstore follower %s/%s: %w", namespace, name, err)
	}
	return item, nil
}

func (s *FollowersService) Update(ctx context.Context, cli client.Client, obj *polardbxv1.XStoreFollower) (*polardbxv1.XStoreFollower, error) {
	if obj == nil {
		return nil, svcerr.ValidationError("follower payload is required", nil)
	}
	if obj.Namespace == "" {
		return nil, svcerr.ValidationError("namespace is required", nil)
	}
	if obj.Name == "" {
		return nil, svcerr.ValidationError("name is required", nil)
	}
	updated, err := s.repo.UpdateFollower(ctx, cli, obj.Namespace, obj)
	if err != nil {
		return nil, fmt.Errorf("update xstore follower %s/%s: %w", obj.Namespace, obj.Name, err)
	}
	return updated, nil
}

func (s *FollowersService) Delete(ctx context.Context, cli client.Client, namespace, name string) error {
	if err := s.repo.DeleteFollower(ctx, cli, namespace, name); err != nil {
		return fmt.Errorf("delete xstore follower %s/%s: %w", namespace, name, err)
	}
	return nil
}

// Retry recreates a failed follower task.
func (s *FollowersService) Retry(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStoreFollower, error) {
	original, err := s.repo.GetFollower(ctx, cli, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("get xstore follower %s/%s: %w", namespace, name, err)
	}

	if original.Status.Phase != polardbxv1xstore.FollowerPhaseFailed {
		return nil, svcerr.ValidationError("only failed tasks can be retried", map[string]any{"phase": original.Status.Phase})
	}

	if err := s.repo.DeleteFollower(ctx, cli, namespace, name); err != nil {
		return nil, fmt.Errorf("delete follower %s/%s for retry: %w", namespace, name, err)
	}

	newFollower := &polardbxv1.XStoreFollower{
		ObjectMeta: original.ObjectMeta,
		Spec:       original.Spec,
	}
	newFollower.Namespace = namespace
	newFollower.Status = polardbxv1.XStoreFollowerStatus{}
	newFollower.ResourceVersion = ""
	newFollower.Generation = 0

	created, err := s.repo.CreateFollower(ctx, cli, namespace, newFollower)
	if err != nil {
		return nil, fmt.Errorf("recreate follower %s/%s: %w", namespace, name, err)
	}

	return created, nil
}

// Cancel deletes a follower task if it is not in a terminal phase.
func (s *FollowersService) Cancel(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStoreFollower, error) {
	follower, err := s.repo.GetFollower(ctx, cli, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("get xstore follower %s/%s: %w", namespace, name, err)
	}

	if follower.Status.Phase == polardbxv1xstore.FollowerPhaseSuccess ||
		follower.Status.Phase == polardbxv1xstore.FollowerPhaseFailed ||
		follower.Status.Phase == polardbxv1xstore.FollowerPhaseDeleting {
		return nil, svcerr.ValidationError("cannot cancel completed or already deleting task", map[string]any{"phase": follower.Status.Phase})
	}

	if err := s.repo.DeleteFollower(ctx, cli, namespace, name); err != nil {
		return nil, fmt.Errorf("delete follower %s/%s: %w", namespace, name, err)
	}

	return follower, nil
}
