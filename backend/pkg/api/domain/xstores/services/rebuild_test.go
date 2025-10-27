package services

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRebuild_MissingName_Returns404(t *testing.T) {
	gin.SetMode(gin.TestMode)
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("k8sClient", cli)
	c.Params = gin.Params{{Key: "namespace", Value: "default"}, {Key: "name", Value: "x1"}}
	body, _ := json.Marshal(map[string]any{"xStoreName": "x1"})
	c.Request = httptest.NewRequest(http.MethodPost, "/xstore-rebuild/default/x1/auto", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	NewRebuildService().Auto(c)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestRebuild_MissingXStoreName_Returns400(t *testing.T) {
	gin.SetMode(gin.TestMode)
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("k8sClient", cli)
	c.Params = gin.Params{{Key: "namespace", Value: "default"}} // no path xstore name
	body, _ := json.Marshal(map[string]any{"name": "rb1"})
	c.Request = httptest.NewRequest(http.MethodPost, "/xstore-rebuild/default//auto", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	NewRebuildService().Auto(c)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestRebuild_NoRunningPods_StillCreatesWithoutTargetPod(t *testing.T) {
	gin.SetMode(gin.TestMode)
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	// pod exists but not running
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "x1-dn-0", Namespace: "default", Labels: map[string]string{"xstore/name": "x1", "xstore/role": "follower"}}, Status: corev1.PodStatus{Phase: corev1.PodPending}}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(pod).Build()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("k8sClient", cli)
	c.Params = gin.Params{{Key: "namespace", Value: "default"}, {Key: "name", Value: "x1"}}
	body, _ := json.Marshal(map[string]any{"name": "rb-no-running", "xStoreName": "x1"})
	c.Request = httptest.NewRequest(http.MethodPost, "/xstore-rebuild/default/x1/auto", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	NewRebuildService().Auto(c)
	assert.Equal(t, http.StatusCreated, w.Code)

	created := &polardbxv1.XStoreFollower{}
	err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: "default", Name: "rb-no-running"}, created)
	assert.NoError(t, err)
	assert.Equal(t, "", created.Spec.TargetPodName)
}

func TestRebuildWait_SucceedsImmediately(t *testing.T) {
	gin.SetMode(gin.TestMode)
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	f := &polardbxv1.XStoreFollower{ObjectMeta: metav1.ObjectMeta{Name: "rb1", Namespace: "default"}}
	f.Status.Phase = "FollowerPhaseSuccess"
	cli := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(f).Build()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("k8sClient", cli)
	c.Params = gin.Params{{Key: "namespace", Value: "default"}, {Key: "name", Value: "x1"}}
	c.Request = httptest.NewRequest(http.MethodGet, "/xstores/default/x1/rebuild/wait?follower=rb1&timeoutSec=1&intervalSec=1", nil)

	NewRebuildService().Wait(c)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRebuildWait_Timeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	f := &polardbxv1.XStoreFollower{ObjectMeta: metav1.ObjectMeta{Name: "rb2", Namespace: "default"}}
	// non-end phase
	f.Status.Phase = "FollowerPhaseBackup"
	cli := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(f).Build()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("k8sClient", cli)
	c.Params = gin.Params{{Key: "namespace", Value: "default"}, {Key: "name", Value: "x1"}}
	c.Request = httptest.NewRequest(http.MethodGet, "/xstores/default/x1/rebuild/wait?follower=rb2&timeoutSec=1&intervalSec=1", nil)

	NewRebuildService().Wait(c)
	assert.Equal(t, http.StatusGatewayTimeout, w.Code)
}
