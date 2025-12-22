package services

import (
	"context"
	"testing"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// ==================== BackupBinlogService Tests ====================

func fakeBackupBinlogClient(objs ...runtime.Object) client.Client {
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)
	return crfake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
}

func TestBackupBinlogService_List_Empty(t *testing.T) {
	ctx := context.Background()
	cli := fakeBackupBinlogClient()
	svc := NewBackupBinlogService()

	items, err := svc.List(ctx, cli, "default")
	assert.NoError(t, err)
	assert.Empty(t, items)
}

func TestBackupBinlogService_List_WithData(t *testing.T) {
	ctx := context.Background()
	binlog := &polardbxv1.XStoreBackupBinlog{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-binlog",
			Namespace: "default",
		},
	}
	cli := fakeBackupBinlogClient(binlog)
	svc := NewBackupBinlogService()

	items, err := svc.List(ctx, cli, "default")
	assert.NoError(t, err)
	assert.Len(t, items, 1)
	assert.Equal(t, "test-binlog", items[0].Name)
}

func TestBackupBinlogService_List_AllNamespaces(t *testing.T) {
	ctx := context.Background()
	cli := fakeBackupBinlogClient()
	svc := NewBackupBinlogService()

	items, err := svc.List(ctx, cli, "")
	assert.NoError(t, err)
	assert.Empty(t, items)
}

func TestBackupBinlogService_Create_Success(t *testing.T) {
	ctx := context.Background()
	cli := fakeBackupBinlogClient()
	svc := NewBackupBinlogService()

	obj := &polardbxv1.XStoreBackupBinlog{
		ObjectMeta: metav1.ObjectMeta{
			Name: "new-binlog",
		},
	}

	created, err := svc.Create(ctx, cli, "default", obj)
	assert.NoError(t, err)
	assert.NotNil(t, created)
	assert.Equal(t, "new-binlog", created.Name)
	assert.Equal(t, "default", created.Namespace)
}

func TestBackupBinlogService_Get_Success(t *testing.T) {
	ctx := context.Background()
	binlog := &polardbxv1.XStoreBackupBinlog{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-binlog",
			Namespace: "default",
		},
	}
	cli := fakeBackupBinlogClient(binlog)
	svc := NewBackupBinlogService()

	item, err := svc.Get(ctx, cli, "default", "test-binlog")
	assert.NoError(t, err)
	assert.NotNil(t, item)
	assert.Equal(t, "test-binlog", item.Name)
}

func TestBackupBinlogService_Get_NotFound(t *testing.T) {
	ctx := context.Background()
	cli := fakeBackupBinlogClient()
	svc := NewBackupBinlogService()

	_, err := svc.Get(ctx, cli, "default", "nonexistent")
	assert.Error(t, err)
}

func TestBackupBinlogService_Update_Success(t *testing.T) {
	ctx := context.Background()
	binlog := &polardbxv1.XStoreBackupBinlog{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-binlog",
			Namespace:       "default",
			ResourceVersion: "1",
		},
	}
	cli := fakeBackupBinlogClient(binlog)
	svc := NewBackupBinlogService()

	obj := &polardbxv1.XStoreBackupBinlog{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-binlog",
			ResourceVersion: "1",
		},
	}

	updated, err := svc.Update(ctx, cli, "default", obj)
	assert.NoError(t, err)
	assert.NotNil(t, updated)
}

func TestBackupBinlogService_Delete_Success(t *testing.T) {
	ctx := context.Background()
	binlog := &polardbxv1.XStoreBackupBinlog{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-binlog",
			Namespace: "default",
		},
	}
	cli := fakeBackupBinlogClient(binlog)
	svc := NewBackupBinlogService()

	err := svc.Delete(ctx, cli, "default", "test-binlog")
	assert.NoError(t, err)
}

func TestNewBackupBinlogService(t *testing.T) {
	svc := NewBackupBinlogService()
	require.NotNil(t, svc)
}
