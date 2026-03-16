package services

import (
	"context"
	"testing"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// ==================== Followers Retry/Cancel Tests ====================

// mockFollowerRepo is a mock repository for testing Retry and Cancel
type mockFollowerRepo struct {
	getFollowerErr    error
	deleteFollowerErr error
	createFollowerErr error
	updateFollowerErr error

	follower *polardbxv1.XStoreFollower
}

func (m *mockFollowerRepo) List(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.XStore, error) {
	return nil, nil
}
func (m *mockFollowerRepo) Create(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStore) (*polardbxv1.XStore, error) {
	return nil, nil
}
func (m *mockFollowerRepo) Get(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStore, error) {
	return nil, nil
}
func (m *mockFollowerRepo) Update(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStore) (*polardbxv1.XStore, error) {
	return nil, nil
}
func (m *mockFollowerRepo) Delete(ctx context.Context, cli client.Client, namespace, name string) error {
	return nil
}
func (m *mockFollowerRepo) ListPods(ctx context.Context, cli client.Client, namespace, xstoreName string) ([]corev1.Pod, error) {
	return nil, nil
}
func (m *mockFollowerRepo) ListBackups(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.XStoreBackup, error) {
	return nil, nil
}
func (m *mockFollowerRepo) CreateBackup(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error) {
	return nil, nil
}
func (m *mockFollowerRepo) GetBackup(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStoreBackup, error) {
	return nil, nil
}
func (m *mockFollowerRepo) UpdateBackup(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error) {
	return nil, nil
}
func (m *mockFollowerRepo) DeleteBackup(ctx context.Context, cli client.Client, namespace, name string) error {
	return nil
}
func (m *mockFollowerRepo) ListFollowers(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.XStoreFollower, error) {
	return nil, nil
}
func (m *mockFollowerRepo) CreateFollower(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreFollower) (*polardbxv1.XStoreFollower, error) {
	if m.createFollowerErr != nil {
		return nil, m.createFollowerErr
	}
	return obj, nil
}
func (m *mockFollowerRepo) GetFollower(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStoreFollower, error) {
	if m.getFollowerErr != nil {
		return nil, m.getFollowerErr
	}
	if m.follower != nil {
		return m.follower, nil
	}
	return &polardbxv1.XStoreFollower{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Status: polardbxv1.XStoreFollowerStatus{
			Phase: polardbxv1xstore.FollowerPhaseFailed,
		},
	}, nil
}
func (m *mockFollowerRepo) UpdateFollower(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreFollower) (*polardbxv1.XStoreFollower, error) {
	if m.updateFollowerErr != nil {
		return nil, m.updateFollowerErr
	}
	return obj, nil
}
func (m *mockFollowerRepo) DeleteFollower(ctx context.Context, cli client.Client, namespace, name string) error {
	return m.deleteFollowerErr
}

func TestFollowersService_Retry_Success(t *testing.T) {
	repo := &mockFollowerRepo{
		follower: &polardbxv1.XStoreFollower{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-follower",
				Namespace: "default",
			},
			Status: polardbxv1.XStoreFollowerStatus{
				Phase: polardbxv1xstore.FollowerPhaseFailed,
			},
		},
	}
	svc := &FollowersService{repo: repo}
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)
	cli := crfake.NewClientBuilder().WithScheme(scheme).Build()

	created, err := svc.Retry(ctx, cli, "default", "test-follower")
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

// NOTE: Followers Retry/Cancel HTTP behavior is exercised in xstores handlers tests.
