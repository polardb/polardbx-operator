package services

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
)

type stubXRepo struct {
	listErr   error
	createErr error
	getErr    error
	updateErr error
	deleteErr error
	podErr    error

	backupListErr   error
	backupCreateErr error
	backupGetErr    error
	backupUpdateErr error
	backupDeleteErr error

	followerListErr   error
	followerCreateErr error
	followerGetErr    error
	followerUpdateErr error
	followerDeleteErr error
}

func (s *stubXRepo) List(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.XStore, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return []polardbxv1.XStore{{}}, nil
}

func (s *stubXRepo) Create(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStore) (*polardbxv1.XStore, error) {
	if s.createErr != nil {
		return nil, s.createErr
	}
	return obj, nil
}

func (s *stubXRepo) Get(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStore, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return &polardbxv1.XStore{}, nil
}

func (s *stubXRepo) Update(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStore) (*polardbxv1.XStore, error) {
	if s.updateErr != nil {
		return nil, s.updateErr
	}
	return obj, nil
}

func (s *stubXRepo) Delete(ctx context.Context, cli client.Client, namespace, name string) error {
	return s.deleteErr
}

func (s *stubXRepo) ListPods(ctx context.Context, cli client.Client, namespace, xstoreName string) ([]corev1.Pod, error) {
	if s.podErr != nil {
		return nil, s.podErr
	}
	return []corev1.Pod{{}}, nil
}

// Unused methods for backups/followers to satisfy interface (noop).
func (s *stubXRepo) ListBackups(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.XStoreBackup, error) {
	if s.backupListErr != nil {
		return nil, s.backupListErr
	}
	return []polardbxv1.XStoreBackup{{}}, nil
}
func (s *stubXRepo) CreateBackup(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error) {
	if s.backupCreateErr != nil {
		return nil, s.backupCreateErr
	}
	return obj, nil
}
func (s *stubXRepo) GetBackup(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStoreBackup, error) {
	if s.backupGetErr != nil {
		return nil, s.backupGetErr
	}
	return &polardbxv1.XStoreBackup{}, nil
}
func (s *stubXRepo) UpdateBackup(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error) {
	if s.backupUpdateErr != nil {
		return nil, s.backupUpdateErr
	}
	return obj, nil
}
func (s *stubXRepo) DeleteBackup(ctx context.Context, cli client.Client, namespace, name string) error {
	return s.backupDeleteErr
}
func (s *stubXRepo) ListFollowers(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.XStoreFollower, error) {
	if s.followerListErr != nil {
		return nil, s.followerListErr
	}
	return []polardbxv1.XStoreFollower{{}}, nil
}
func (s *stubXRepo) CreateFollower(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreFollower) (*polardbxv1.XStoreFollower, error) {
	if s.followerCreateErr != nil {
		return nil, s.followerCreateErr
	}
	return obj, nil
}
func (s *stubXRepo) GetFollower(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStoreFollower, error) {
	if s.followerGetErr != nil {
		return nil, s.followerGetErr
	}
	return &polardbxv1.XStoreFollower{}, nil
}
func (s *stubXRepo) UpdateFollower(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreFollower) (*polardbxv1.XStoreFollower, error) {
	if s.followerUpdateErr != nil {
		return nil, s.followerUpdateErr
	}
	return obj, nil
}
func (s *stubXRepo) DeleteFollower(ctx context.Context, cli client.Client, namespace, name string) error {
	return s.followerDeleteErr
}

func fakeClient() client.Client {
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)
	return fake.NewClientBuilder().WithScheme(scheme).Build()
}

func TestXStoreService_List_ErrorAndSuccess(t *testing.T) {
	ctx := context.Background()
	cli := fakeClient()
	ns := "test-ns"

	// Test error case
	svc := &XStoreService{repo: &stubXRepo{listErr: assert.AnError}}
	_, err := svc.List(ctx, cli, ns)
	assert.Error(t, err)

	// Test success case
	svc.repo = &stubXRepo{}
	items, err := svc.List(ctx, cli, ns)
	assert.NoError(t, err)
	assert.Len(t, items, 1)
}

func TestXStoreService_Create_ErrorAndSuccess(t *testing.T) {
	ctx := context.Background()
	cli := fakeClient()
	ns := "test-ns"
	obj := &polardbxv1.XStore{}

	// Test error case
	svc := &XStoreService{repo: &stubXRepo{createErr: assert.AnError}}
	_, err := svc.Create(ctx, cli, ns, obj)
	assert.Error(t, err)

	// Test success case
	svc.repo = &stubXRepo{}
	created, err := svc.Create(ctx, cli, ns, obj)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestXStoreService_Get_ErrorAndSuccess(t *testing.T) {
	ctx := context.Background()
	cli := fakeClient()
	ns := "test-ns"
	name := "x1"

	// Test error case
	svc := &XStoreService{repo: &stubXRepo{getErr: assert.AnError}}
	_, err := svc.Get(ctx, cli, ns, name)
	assert.Error(t, err)

	// Test success case
	svc.repo = &stubXRepo{}
	item, err := svc.Get(ctx, cli, ns, name)
	assert.NoError(t, err)
	assert.NotNil(t, item)
}

func TestXStoreService_Update_ErrorAndSuccess(t *testing.T) {
	ctx := context.Background()
	cli := fakeClient()
	ns := "test-ns"
	obj := &polardbxv1.XStore{}

	// Test error case
	svc := &XStoreService{repo: &stubXRepo{updateErr: assert.AnError}}
	_, err := svc.Update(ctx, cli, ns, obj)
	assert.Error(t, err)

	// Test success case
	svc.repo = &stubXRepo{}
	updated, err := svc.Update(ctx, cli, ns, obj)
	assert.NoError(t, err)
	assert.NotNil(t, updated)
}

func TestXStoreService_Delete_ErrorAndSuccess(t *testing.T) {
	ctx := context.Background()
	cli := fakeClient()
	ns := "test-ns"
	name := "x1"

	// Test error case
	svc := &XStoreService{repo: &stubXRepo{deleteErr: assert.AnError}}
	err := svc.Delete(ctx, cli, ns, name)
	assert.Error(t, err)

	// Test success case
	svc.repo = &stubXRepo{}
	err = svc.Delete(ctx, cli, ns, name)
	assert.NoError(t, err)
}

func TestXStoreService_ListPods_ErrorAndSuccess(t *testing.T) {
	ctx := context.Background()
	cli := fakeClient()
	ns := "test-ns"
	name := "x1"

	// Test error case
	svc := &XStoreService{repo: &stubXRepo{podErr: assert.AnError}}
	_, err := svc.ListPods(ctx, cli, ns, name)
	assert.Error(t, err)

	// Test success case
	svc.repo = &stubXRepo{}
	pods, err := svc.ListPods(ctx, cli, ns, name)
	assert.NoError(t, err)
	assert.Len(t, pods, 1)
}

// ---- Backups (service level) ----

func TestBackupsService_List_ErrorAndSuccess(t *testing.T) {
	ctx := context.Background()
	cli := fakeClient()
	ns := "test-ns"

	// Test error case
	svc := &BackupsService{repo: &stubXRepo{backupListErr: assert.AnError}}
	_, err := svc.List(ctx, cli, ns)
	assert.Error(t, err)

	// Test success case
	svc.repo = &stubXRepo{}
	items, err := svc.List(ctx, cli, ns)
	assert.NoError(t, err)
	assert.Len(t, items, 1)
}

func TestBackupsService_Create_ErrorAndSuccess(t *testing.T) {
	ctx := context.Background()
	cli := fakeClient()
	ns := "test-ns"
	obj := &polardbxv1.XStoreBackup{}

	// Test error case
	svc := &BackupsService{repo: &stubXRepo{backupCreateErr: assert.AnError}}
	_, err := svc.Create(ctx, cli, ns, obj)
	assert.Error(t, err)

	// Test success case
	svc.repo = &stubXRepo{}
	created, err := svc.Create(ctx, cli, ns, obj)
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

func TestBackupsService_Get_ErrorAndSuccess(t *testing.T) {
	ctx := context.Background()
	cli := fakeClient()
	ns := "test-ns"
	name := "b1"

	// Test error case
	svc := &BackupsService{repo: &stubXRepo{backupGetErr: assert.AnError}}
	_, err := svc.Get(ctx, cli, ns, name)
	assert.Error(t, err)

	// Test success case
	svc.repo = &stubXRepo{}
	item, err := svc.Get(ctx, cli, ns, name)
	assert.NoError(t, err)
	assert.NotNil(t, item)
}

func TestBackupsService_Update_ErrorAndSuccess(t *testing.T) {
	ctx := context.Background()
	cli := fakeClient()
	ns := "test-ns"
	obj := &polardbxv1.XStoreBackup{}

	// Test error case
	svc := &BackupsService{repo: &stubXRepo{backupUpdateErr: assert.AnError}}
	_, err := svc.Update(ctx, cli, ns, obj)
	assert.Error(t, err)

	// Test success case
	svc.repo = &stubXRepo{}
	updated, err := svc.Update(ctx, cli, ns, obj)
	assert.NoError(t, err)
	assert.NotNil(t, updated)
}

func TestBackupsService_Delete_ErrorAndSuccess(t *testing.T) {
	ctx := context.Background()
	cli := fakeClient()
	ns := "test-ns"
	name := "b1"

	// Test error case
	svc := &BackupsService{repo: &stubXRepo{backupDeleteErr: assert.AnError}}
	err := svc.Delete(ctx, cli, ns, name)
	assert.Error(t, err)

	// Test success case
	svc.repo = &stubXRepo{}
	err = svc.Delete(ctx, cli, ns, name)
	assert.NoError(t, err)
}

func TestBackupsService_ForceDelete_ErrorAndSuccess(t *testing.T) {
	ctx := context.Background()
	cli := fakeClient()
	ns := "test-ns"
	name := "b1"

	// Test get error case
	svc := &BackupsService{repo: &stubXRepo{backupGetErr: assert.AnError}}
	err := svc.ForceDelete(ctx, cli, ns, name)
	assert.Error(t, err)

	// Test update error case
	svc.repo = &stubXRepo{backupUpdateErr: assert.AnError}
	err = svc.ForceDelete(ctx, cli, ns, name)
	assert.Error(t, err)

	// Test success case
	svc.repo = &stubXRepo{}
	err = svc.ForceDelete(ctx, cli, ns, name)
	assert.NoError(t, err)
}

func TestBackupsService_RemoteInfo_ErrorAndSuccess(t *testing.T) {
	ctx := context.Background()
	cli := fakeClient()
	ns := "test-ns"
	name := "b1"

	// Test error case
	svc := &BackupsService{repo: &stubXRepo{backupGetErr: assert.AnError}}
	_, err := svc.RemoteInfo(ctx, cli, ns, name)
	assert.Error(t, err)

	// Test success case
	svc.repo = &stubXRepo{}
	info, err := svc.RemoteInfo(ctx, cli, ns, name)
	assert.NoError(t, err)
	assert.NotNil(t, info)
}

// ---- Followers (service level) ----

// NOTE: FollowersService behavior is tested in followers_test.go and handlers tests.
