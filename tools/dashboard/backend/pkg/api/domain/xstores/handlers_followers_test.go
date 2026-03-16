package xstores

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1xstore "github.com/alibaba/polardbx-operator/api/v1/xstore"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// setupFollowersTestClient creates a fake k8s client with the polardbxv1 scheme registered.
func setupFollowersTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := polardbxv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add polardbxv1 to scheme: %v", err)
	}
	builder := crfake.NewClientBuilder().WithScheme(scheme)
	if len(objs) > 0 {
		builder = builder.WithObjects(objs...)
	}
	return builder.Build()
}

// setupFollowersTestRouter wires a single route to the given handler, injecting the fake k8s client.
func setupFollowersTestRouter(method, path string, cli client.Client, handler gin.HandlerFunc) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	switch method {
	case http.MethodPost:
		r.POST(path, func(c *gin.Context) {
			c.Set("k8sClient", cli)
			handler(c)
		})
	case http.MethodDelete:
		r.DELETE(path, func(c *gin.Context) {
			c.Set("k8sClient", cli)
			handler(c)
		})
	default:
		r.Handle(method, path, func(c *gin.Context) {
			c.Set("k8sClient", cli)
			handler(c)
		})
	}
	return r
}

func TestRetryFollower_Success(t *testing.T) {
	// Prepare a failed follower task that can be retried.
	follower := &polardbxv1.XStoreFollower{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "f1",
			Namespace: "default",
		},
		Status: polardbxv1.XStoreFollowerStatus{
			Phase: polardbxv1xstore.FollowerPhaseFailed,
		},
	}
	cli := setupFollowersTestClient(t, follower)

	r := setupFollowersTestRouter(http.MethodPost, "/xstores/followers/:namespace/:name/retry", cli, RetryFollower)

	req, _ := http.NewRequest(http.MethodPost, "/xstores/followers/default/f1/retry", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "task retried successfully", resp["message"])
	assert.Equal(t, "f1", resp["original_task"])
	// After retry, the new task is recreated with the same name.
	assert.Equal(t, "f1", resp["new_task"])
}

func TestRetryFollower_ValidationError_WhenNotFailed(t *testing.T) {
	// Follower in non-failed phase should trigger validation error.
	follower := &polardbxv1.XStoreFollower{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "f2",
			Namespace: "default",
		},
		Status: polardbxv1.XStoreFollowerStatus{
			Phase: polardbxv1xstore.FollowerPhaseBackup,
		},
	}
	cli := setupFollowersTestClient(t, follower)

	r := setupFollowersTestRouter(http.MethodPost, "/xstores/followers/:namespace/:name/retry", cli, RetryFollower)

	req, _ := http.NewRequest(http.MethodPost, "/xstores/followers/default/f2/retry", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// Service layer returns ValidationError -> HTTP 400.
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCancelFollower_Success(t *testing.T) {
	// Prepare an active follower task that can be cancelled.
	follower := &polardbxv1.XStoreFollower{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "c1",
			Namespace: "default",
		},
		Spec: polardbxv1.XStoreFollowerSpec{
			XStoreName: "x1",
		},
		Status: polardbxv1.XStoreFollowerStatus{
			Phase: polardbxv1xstore.FollowerPhaseBackup,
		},
	}
	cli := setupFollowersTestClient(t, follower)

	r := setupFollowersTestRouter(http.MethodDelete, "/xstores/followers/:namespace/:name/cancel", cli, CancelFollower)

	req, _ := http.NewRequest(http.MethodDelete, "/xstores/followers/default/c1/cancel", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "task cancelled successfully", resp["message"])
	assert.Equal(t, "c1", resp["task"])
	assert.Equal(t, "x1", resp["xstore"])
}

func TestCancelFollower_ValidationError_WhenTerminalPhase(t *testing.T) {
	// Success phase should not be cancellable.
	follower := &polardbxv1.XStoreFollower{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "c2",
			Namespace: "default",
		},
		Status: polardbxv1.XStoreFollowerStatus{
			Phase: polardbxv1xstore.FollowerPhaseSuccess,
		},
	}
	cli := setupFollowersTestClient(t, follower)

	r := setupFollowersTestRouter(http.MethodDelete, "/xstores/followers/:namespace/:name/cancel", cli, CancelFollower)

	req, _ := http.NewRequest(http.MethodDelete, "/xstores/followers/default/c2/cancel", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// Service layer returns ValidationError -> HTTP 400.
	assert.Equal(t, http.StatusBadRequest, w.Code)
}
