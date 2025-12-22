package service

import (
	"context"
	"errors"
	"testing"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type stubRepo struct {
	listErr   error
	getErr    error
	createErr error
	updateErr error
	deleteErr error
}

func (s *stubRepo) List(ctx context.Context, cli client.Client, namespace string) ([]polardbxv1.SystemTask, error) {
	return nil, s.listErr
}

func (s *stubRepo) Get(ctx context.Context, cli client.Client, namespace, name string) (*polardbxv1.SystemTask, error) {
	return nil, s.getErr
}

func (s *stubRepo) Create(ctx context.Context, cli client.Client, namespace string, task *polardbxv1.SystemTask) (*polardbxv1.SystemTask, error) {
	return nil, s.createErr
}

func (s *stubRepo) Update(ctx context.Context, cli client.Client, namespace string, task *polardbxv1.SystemTask) (*polardbxv1.SystemTask, error) {
	return nil, s.updateErr
}

func (s *stubRepo) Delete(ctx context.Context, cli client.Client, namespace, name string) error {
	return s.deleteErr
}

func TestSystemTaskService_ErrorsPropagate(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	svc := NewSystemTaskService(&stubRepo{
		listErr:   errors.New("list err"),
		getErr:    errors.New("get err"),
		createErr: errors.New("create err"),
		updateErr: errors.New("update err"),
		deleteErr: errors.New("delete err"),
	})

	_, err := svc.List(context.Background(), cli, "ns")
	assert.Error(t, err)
	_, err = svc.Get(context.Background(), cli, "ns", "name")
	assert.Error(t, err)
	_, err = svc.Create(context.Background(), cli, "ns", &polardbxv1.SystemTask{})
	assert.Error(t, err)
	_, err = svc.Update(context.Background(), cli, "ns", &polardbxv1.SystemTask{})
	assert.Error(t, err)
	err = svc.Delete(context.Background(), cli, "ns", "name")
	assert.Error(t, err)
}

func TestSystemTaskService_Success(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	task := &polardbxv1.SystemTask{}
	svc := NewSystemTaskService(&stubRepo{
		listErr:   nil,
		getErr:    nil,
		createErr: nil,
		updateErr: nil,
		deleteErr: nil,
	})

	_, err := svc.List(context.Background(), cli, "ns")
	assert.NoError(t, err)
	_, err = svc.Get(context.Background(), cli, "ns", "name")
	assert.NoError(t, err)
	_, err = svc.Create(context.Background(), cli, "ns", task)
	assert.NoError(t, err)
	_, err = svc.Update(context.Background(), cli, "ns", task)
	assert.NoError(t, err)
	err = svc.Delete(context.Background(), cli, "ns", "name")
	assert.NoError(t, err)
}
