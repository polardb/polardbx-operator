package services

import (
	"context"
	"testing"
	"time"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// helper to build a fake client with schemes registered
func newRebuildTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	builder := fake.NewClientBuilder().WithScheme(scheme)
	if len(objs) > 0 {
		builder = builder.WithObjects(objs...)
	}
	return builder.Build()
}

func TestRebuild_CreateFollower_MissingName_ReturnsValidationError(t *testing.T) {
	cli := newRebuildTestClient(t)
	svc := NewRebuildService()

	_, err := svc.CreateFollower(context.Background(), cli, "default", "x1", "", polardbxv1xstore.FollowerRole("logger"))
	assert.Error(t, err)
}

func TestRebuild_CreateFollower_MissingXStoreName_ReturnsValidationError(t *testing.T) {
	cli := newRebuildTestClient(t)
	svc := NewRebuildService()

	_, err := svc.CreateFollower(context.Background(), cli, "default", "", "rb1", polardbxv1xstore.FollowerRole("logger"))
	assert.Error(t, err)
}

func TestRebuild_NoRunningPods_StillCreatesWithoutTargetPod(t *testing.T) {
	// pod exists but not running -> CreateFollower should still succeed,
	// but not auto-fill TargetPodName/FromPodName.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "x1-dn-0",
			Namespace: "default",
			Labels: map[string]string{
				"xstore/name": "x1",
				"xstore/role": "follower",
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	cli := newRebuildTestClient(t, pod)
	svc := NewRebuildService()

	obj, err := svc.CreateFollower(context.Background(), cli, "default", "x1", "rb-no-running", polardbxv1xstore.FollowerRole("follower"))
	assert.NoError(t, err)
	assert.Equal(t, "rb-no-running", obj.Name)
	assert.Equal(t, "default", obj.Namespace)
	assert.Equal(t, "", obj.Spec.TargetPodName)
	assert.Equal(t, "", obj.Spec.FromPodName)
}

func TestRebuildWait_SucceedsImmediately(t *testing.T) {
	f := &polardbxv1.XStoreFollower{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rb1",
			Namespace: "default",
		},
		Status: polardbxv1.XStoreFollowerStatus{
			Phase: polardbxv1xstore.FollowerPhaseSuccess,
		},
	}
	cli := newRebuildTestClient(t, f)
	svc := NewRebuildService()

	ctx := context.Background()
	out, err := svc.Wait(ctx, cli, "default", "rb1", 1*time.Second, 10*time.Millisecond)
	assert.NoError(t, err)
	assert.Equal(t, "rb1", out.Name)
}

func TestRebuildWait_Timeout(t *testing.T) {
	f := &polardbxv1.XStoreFollower{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rb2",
			Namespace: "default",
		},
		Status: polardbxv1.XStoreFollowerStatus{
			Phase: polardbxv1xstore.FollowerPhaseBackup, // non-terminal
		},
	}
	cli := newRebuildTestClient(t, f)
	svc := NewRebuildService()

	ctx := context.Background()
	out, err := svc.Wait(ctx, cli, "default", "rb2", 200*time.Millisecond, 50*time.Millisecond)
	assert.Error(t, err)
	assert.Nil(t, out)
}
