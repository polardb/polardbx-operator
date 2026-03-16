package services

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"

	"polardbx-dashboard-backend/pkg/api/domain/xstores/k8srepo"
	svcerr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/logger"
)

// RebuildService: Migrate Status first, other entries to be orchestrated later
type RebuildService struct {
	repo k8srepo.XStoreRepository
}

func NewRebuildService() *RebuildService { return &RebuildService{repo: k8srepo.NewXStoreRepository()} }

// RebuildStatus summarizes active follower tasks.
type RebuildStatus struct {
	Name      string `json:"name"`
	Phase     string `json:"phase"`
	Message   string `json:"message"`
	TargetPod string `json:"targetPod"`
}

// CreateFollower creates an XStoreFollower with the given role and best-effort target pod selection.
func (s *RebuildService) CreateFollower(ctx context.Context, cli client.Client, namespace, xstoreName, name string, role polardbxv1xstore.FollowerRole) (*polardbxv1.XStoreFollower, error) {
	if name == "" {
		return nil, svcerr.ValidationError("name is required", nil)
	}
	if xstoreName == "" {
		return nil, svcerr.ValidationError("xStoreName is required", nil)
	}
	obj := &polardbxv1.XStoreFollower{}
	obj.Namespace = namespace
	obj.Name = name
	obj.Spec.XStoreName = xstoreName
	obj.Spec.Role = role
	obj.Spec.Local = false

	// Auto-select target Pod (prefer follower role and Running pods)
	if obj.Spec.TargetPodName == "" || obj.Spec.FromPodName == "" {
		var pods corev1.PodList
		if err := cli.List(ctx, &pods, client.InNamespace(namespace)); err == nil {
			var candidate string
			for _, p := range pods.Items {
				if p.Labels["xstore/name"] != xstoreName {
					continue
				}
				if p.Status.Phase != corev1.PodRunning {
					continue
				}
				if p.Labels["xstore/role"] == "follower" {
					candidate = p.Name
					break
				}
				if candidate == "" {
					candidate = p.Name
				}
			}
			if candidate != "" {
				if obj.Spec.TargetPodName == "" {
					obj.Spec.TargetPodName = candidate
				}
				if obj.Spec.FromPodName == "" {
					obj.Spec.FromPodName = candidate
				}
			}
		}
	}
	if obj.Labels == nil {
		obj.Labels = map[string]string{}
	}
	obj.Labels["xstore/rebuild-type"] = string(role)
	if err := cli.Create(ctx, obj); err != nil {
		return nil, fmt.Errorf("create rebuild follower %s/%s: %w", namespace, name, err)
	}
	return obj, nil
}

// Status lists non-terminal followers for an XStore.
func (s *RebuildService) Status(ctx context.Context, cli client.Client, namespace, xstoreName string) ([]RebuildStatus, error) {
	var list polardbxv1.XStoreFollowerList
	if err := cli.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list followers in %s: %w", namespace, err)
	}
	items := make([]RebuildStatus, 0)
	for _, f := range list.Items {
		if f.Spec.XStoreName == xstoreName && !polardbxv1xstore.IsEndPhase(f.Status.Phase) {
			items = append(items, RebuildStatus{
				Name:      f.Name,
				Phase:     string(f.Status.Phase),
				Message:   f.Status.Message,
				TargetPod: f.Status.TargetPodName,
			})
		}
	}
	return items, nil
}

// Wait polls until follower reaches terminal phase.
func (s *RebuildService) Wait(ctx context.Context, cli client.Client, namespace, name string, timeout, interval time.Duration) (*polardbxv1.XStoreFollower, error) {
	if name == "" {
		return nil, svcerr.ValidationError("follower is required", nil)
	}
	deadline := time.Now().Add(timeout)
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled while waiting follower %s/%s: %w", namespace, name, ctx.Err())
		default:
		}

		var f polardbxv1.XStoreFollower
		if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &f); err != nil {
			return nil, fmt.Errorf("get follower %s/%s: %w", namespace, name, err)
		}
		if polardbxv1xstore.IsEndPhase(f.Status.Phase) {
			return &f, nil
		}
		if time.Now().After(deadline) {
			return nil, svcerr.NewServiceError(svcerr.CategoryInternal, svcerr.ErrTimeout, fmt.Sprintf("timeout waiting for follower %s (phase: %s)", f.Name, f.Status.Phase), nil)
		}
		logger.Info("rebuild wait",
			"namespace", namespace,
			"follower", name,
			"phase", f.Status.Phase)
		time.Sleep(interval)
	}
}

// Progress returns current follower status.
func (s *RebuildService) Progress(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStoreFollower, error) {
	if name == "" {
		return nil, svcerr.ValidationError("follower is required", nil)
	}
	var f polardbxv1.XStoreFollower
	if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &f); err != nil {
		return nil, fmt.Errorf("get follower %s/%s: %w", namespace, name, err)
	}
	return &f, nil
}

// Cancel deletes the active rebuild follower for a given XStore.
func (s *RebuildService) Cancel(ctx context.Context, cli client.Client, namespace, xstoreName string) (*polardbxv1.XStoreFollower, error) {
	followers, err := s.repo.ListFollowers(ctx, cli, namespace)
	if err != nil {
		return nil, fmt.Errorf("list followers in %s: %w", namespace, err)
	}

	var targetFollower *polardbxv1.XStoreFollower
	for _, f := range followers {
		if f.Spec.XStoreName == xstoreName {
			if f.Status.Phase != polardbxv1xstore.FollowerPhaseSuccess &&
				f.Status.Phase != polardbxv1xstore.FollowerPhaseFailed &&
				f.Status.Phase != polardbxv1xstore.FollowerPhaseDeleting {
				targetFollower = &f
				break
			}
		}
	}

	if targetFollower == nil {
		return nil, svcerr.NotFoundError("rebuild task", xstoreName)
	}

	if err := s.repo.DeleteFollower(ctx, cli, namespace, targetFollower.Name); err != nil {
		return nil, fmt.Errorf("delete follower %s/%s: %w", namespace, targetFollower.Name, err)
	}

	return targetFollower, nil
}
