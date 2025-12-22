package k8srepo

import (
	"context"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-dashboard-backend/pkg/k8s"
)

// XStoreRepository abstracts XStore-related K8s access for easy mocking and testing.
type XStoreRepository interface {
	// XStore core
	List(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.XStore, error)
	Create(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStore) (*polardbxv1.XStore, error)
	Get(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStore, error)
	Update(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStore) (*polardbxv1.XStore, error)
	Delete(ctx context.Context, cli client.Client, namespace, name string) error

	// Pods under XStore
	ListPods(ctx context.Context, cli client.Client, namespace, xstoreName string) ([]corev1.Pod, error)

	// XStoreBackup
	ListBackups(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.XStoreBackup, error)
	CreateBackup(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error)
	GetBackup(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStoreBackup, error)
	UpdateBackup(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error)
	DeleteBackup(ctx context.Context, cli client.Client, namespace, name string) error

	// XStoreFollower
	ListFollowers(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.XStoreFollower, error)
	CreateFollower(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreFollower) (*polardbxv1.XStoreFollower, error)
	GetFollower(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStoreFollower, error)
	UpdateFollower(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreFollower) (*polardbxv1.XStoreFollower, error)
	DeleteFollower(ctx context.Context, cli client.Client, namespace, name string) error
}

type DefaultXStoreRepository struct{}

func NewXStoreRepository() XStoreRepository { return &DefaultXStoreRepository{} }

// XStore core
func (r *DefaultXStoreRepository) List(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.XStore, error) {
	return k8s.ListXStoresWithContext(ctx, cli, namespace)
}

func (r *DefaultXStoreRepository) Create(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStore) (*polardbxv1.XStore, error) {
	return k8s.CreateXStoreWithContext(ctx, cli, namespace, obj)
}

func (r *DefaultXStoreRepository) Get(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStore, error) {
	return k8s.GetXStoreWithContext(ctx, cli, namespace, name)
}

func (r *DefaultXStoreRepository) Update(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStore) (*polardbxv1.XStore, error) {
	return k8s.UpdateXStoreWithContext(ctx, cli, namespace, obj)
}

func (r *DefaultXStoreRepository) Delete(ctx context.Context, cli client.Client, namespace, name string) error {
	return k8s.DeleteXStoreWithContext(ctx, cli, namespace, name)
}

func (r *DefaultXStoreRepository) ListPods(ctx context.Context, cli client.Client, namespace, xstoreName string) ([]corev1.Pod, error) {
	// pods list function is non-ctx version, keep current implementation
	return k8s.ListPodsForPolarDBXCluster(cli, namespace, xstoreName)
}

// XStoreBackup
func (r *DefaultXStoreRepository) ListBackups(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.XStoreBackup, error) {
	return k8s.ListXStoreBackupsWithContext(ctx, cli, namespace)
}

func (r *DefaultXStoreRepository) CreateBackup(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error) {
	return k8s.CreateXStoreBackupWithContext(ctx, cli, namespace, obj)
}

func (r *DefaultXStoreRepository) GetBackup(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStoreBackup, error) {
	return k8s.GetXStoreBackupWithContext(ctx, cli, namespace, name)
}

func (r *DefaultXStoreRepository) UpdateBackup(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error) {
	return k8s.UpdateXStoreBackupWithContext(ctx, cli, namespace, obj)
}

func (r *DefaultXStoreRepository) DeleteBackup(ctx context.Context, cli client.Client, namespace, name string) error {
	return k8s.DeleteXStoreBackupWithContext(ctx, cli, namespace, name)
}

// XStoreFollower
func (r *DefaultXStoreRepository) ListFollowers(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.XStoreFollower, error) {
	return k8s.ListXStoreFollowers(cli, namespace)
}

func (r *DefaultXStoreRepository) CreateFollower(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreFollower) (*polardbxv1.XStoreFollower, error) {
	return k8s.CreateXStoreFollower(cli, namespace, obj)
}

func (r *DefaultXStoreRepository) GetFollower(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.XStoreFollower, error) {
	return k8s.GetXStoreFollower(cli, namespace, name)
}

func (r *DefaultXStoreRepository) UpdateFollower(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.XStoreFollower) (*polardbxv1.XStoreFollower, error) {
	return k8s.UpdateXStoreFollower(cli, namespace, obj)
}

func (r *DefaultXStoreRepository) DeleteFollower(ctx context.Context, cli client.Client, namespace, name string) error {
	return k8s.DeleteXStoreFollower(cli, namespace, name)
}

// Alias type helper for services
type XStoreBackupAlias polardbxv1.XStoreBackup

func (a *XStoreBackupAlias) As() *polardbxv1.XStoreBackup { return (*polardbxv1.XStoreBackup)(a) }
