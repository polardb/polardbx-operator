package k8srepo

import (
	"context"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-ui-backend/pkg/k8s"
)

// ClusterRepository 抽象集群相关的 K8s 访问。
type ClusterRepository interface {
	List(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.PolarDBXCluster, error)
	Create(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.PolarDBXCluster) (*polardbxv1.PolarDBXCluster, error)
	Get(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.PolarDBXCluster, error)
	Update(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.PolarDBXCluster) (*polardbxv1.PolarDBXCluster, error)
	Delete(ctx context.Context, cli client.Client, namespace, name string) error

	ListPods(ctx context.Context, cli client.Client, namespace, clusterName string) ([]corev1.Pod, error)
}

type DefaultClusterRepository struct{}

func NewClusterRepository() ClusterRepository { return &DefaultClusterRepository{} }

func (r *DefaultClusterRepository) List(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.PolarDBXCluster, error) {
	return k8s.ListPolarDBXClustersWithContext(ctx, cli, namespace)
}

func (r *DefaultClusterRepository) Create(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.PolarDBXCluster) (*polardbxv1.PolarDBXCluster, error) {
	return k8s.CreatePolarDBXClusterWithContext(ctx, cli, namespace, obj)
}

func (r *DefaultClusterRepository) Get(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.PolarDBXCluster, error) {
	return k8s.GetPolarDBXClusterWithContext(ctx, cli, namespace, name)
}

func (r *DefaultClusterRepository) Update(ctx context.Context, cli client.Client, namespace string, obj *polardbxv1.PolarDBXCluster) (*polardbxv1.PolarDBXCluster, error) {
	return k8s.UpdatePolarDBXClusterWithContext(ctx, cli, namespace, obj)
}

func (r *DefaultClusterRepository) Delete(ctx context.Context, cli client.Client, namespace, name string) error {
	return k8s.DeletePolarDBXClusterWithContext(ctx, cli, namespace, name)
}

func (r *DefaultClusterRepository) ListPods(ctx context.Context, cli client.Client, namespace, clusterName string) ([]corev1.Pod, error) {
	return k8s.ListPodsForPolarDBXCluster(cli, namespace, clusterName)
}
