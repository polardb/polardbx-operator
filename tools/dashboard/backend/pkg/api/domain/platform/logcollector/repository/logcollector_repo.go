package repository

import (
	"context"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LogCollectorRepository defines the data access interface for log collectors
type LogCollectorRepository interface {
	// List lists log collectors in specified namespace
	List(ctx context.Context, namespace string) (*polardbxv1.PolarDBXLogCollectorList, error)
	// Get gets specified log collector
	Get(ctx context.Context, namespace, name string) (*polardbxv1.PolarDBXLogCollector, error)
	// Create creates log collector
	Create(ctx context.Context, collector *polardbxv1.PolarDBXLogCollector) (*polardbxv1.PolarDBXLogCollector, error)
	// Update updates log collector
	Update(ctx context.Context, collector *polardbxv1.PolarDBXLogCollector) (*polardbxv1.PolarDBXLogCollector, error)
	// Delete deletes log collector
	Delete(ctx context.Context, namespace, name string) error
}

// K8sLogCollectorRepository implements LogCollectorRepository using Kubernetes client
type K8sLogCollectorRepository struct {
	client client.Client
}

// NewK8sLogCollectorRepository creates new K8sLogCollectorRepository
func NewK8sLogCollectorRepository(cli client.Client) *K8sLogCollectorRepository {
	return &K8sLogCollectorRepository{client: cli}
}

// List lists log collectors in specified namespace
func (r *K8sLogCollectorRepository) List(ctx context.Context, namespace string) (*polardbxv1.PolarDBXLogCollectorList, error) {
	list := &polardbxv1.PolarDBXLogCollectorList{}
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := r.client.List(ctx, list, opts...); err != nil {
		return nil, err
	}
	return list, nil
}

// Get gets specified log collector
func (r *K8sLogCollectorRepository) Get(ctx context.Context, namespace, name string) (*polardbxv1.PolarDBXLogCollector, error) {
	collector := &polardbxv1.PolarDBXLogCollector{}
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, collector); err != nil {
		return nil, err
	}
	return collector, nil
}

// Create creates log collector
func (r *K8sLogCollectorRepository) Create(ctx context.Context, collector *polardbxv1.PolarDBXLogCollector) (*polardbxv1.PolarDBXLogCollector, error) {
	if err := r.client.Create(ctx, collector); err != nil {
		return nil, err
	}
	return collector, nil
}

// Update updates log collector
func (r *K8sLogCollectorRepository) Update(ctx context.Context, collector *polardbxv1.PolarDBXLogCollector) (*polardbxv1.PolarDBXLogCollector, error) {
	if err := r.client.Update(ctx, collector); err != nil {
		return nil, err
	}
	return collector, nil
}

// Delete deletes log collector
func (r *K8sLogCollectorRepository) Delete(ctx context.Context, namespace, name string) error {
	collector := &polardbxv1.PolarDBXLogCollector{}
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, collector); err != nil {
		return err
	}
	return r.client.Delete(ctx, collector)
}
