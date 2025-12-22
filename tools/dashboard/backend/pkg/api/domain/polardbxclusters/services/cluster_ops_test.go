package services

import (
	"context"
	"testing"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func fakeClientWithObjs(objs ...runtime.Object) client.Client {
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)
	return fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
}

func TestUpgrade_ValidAndInvalid(t *testing.T) {
	svc := NewClusterService()
	ctx := context.Background()
	cli := fakeClientWithObjs()

	orig := patchClusterJSON
	defer func() { patchClusterJSON = orig }()
	patchClusterJSON = func(ctx context.Context, cli client.Client, namespace, name string, patch []byte) (*polardbxv1.PolarDBXCluster, error) {
		return &polardbxv1.PolarDBXCluster{}, nil
	}

	// valid
	err := svc.Upgrade(ctx, cli, "ns", "c1", &ClusterUpgradeRequest{TargetVersion: "8.0.20"})
	assert.NoError(t, err)

	// invalid (missing targetVersion)
	err = svc.Upgrade(ctx, cli, "ns", "c1", &ClusterUpgradeRequest{})
	assert.Error(t, err)
}

func TestScale_ValidAndNoChanges(t *testing.T) {
	svc := NewClusterService()
	ctx := context.Background()
	cli := fakeClientWithObjs()

	orig := patchClusterJSON
	defer func() { patchClusterJSON = orig }()
	patchClusterJSON = func(ctx context.Context, cli client.Client, namespace, name string, patch []byte) (*polardbxv1.PolarDBXCluster, error) {
		return &polardbxv1.PolarDBXCluster{}, nil
	}

	// valid
	cn := int32(2)
	err := svc.Scale(ctx, cli, "ns", "c1", &ClusterScalingRequest{CNReplicas: &cn})
	assert.NoError(t, err)

	// no changes
	err = svc.Scale(ctx, cli, "ns", "c1", &ClusterScalingRequest{})
	assert.Error(t, err)

	// unsupported field (gmsReplicas)
	gms := int32(2)
	err = svc.Scale(ctx, cli, "ns", "c1", &ClusterScalingRequest{GMSReplicas: &gms})
	assert.Error(t, err)
}

func TestUpdateLogConfig_ValidAndInvalidNodeType(t *testing.T) {
	svc := NewClusterService()
	ctx := context.Background()
	cli := fakeClientWithObjs()

	orig := patchClusterJSON
	defer func() { patchClusterJSON = orig }()
	patchClusterJSON = func(ctx context.Context, cli client.Client, namespace, name string, patch []byte) (*polardbxv1.PolarDBXCluster, error) {
		return &polardbxv1.PolarDBXCluster{}, nil
	}

	// valid for cn
	err := svc.UpdateLogConfig(ctx, cli, "ns", "c1", "cn", &LogConfigRequest{LogLevel: "INFO"})
	assert.NoError(t, err)

	// invalid node type
	err = svc.UpdateLogConfig(ctx, cli, "ns", "c1", "bad", &LogConfigRequest{})
	assert.Error(t, err)
}
