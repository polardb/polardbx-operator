package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	domain_xs "polardbx-ui-backend/pkg/api/domain/xstores"
)

func setupXStoreFollowerTestRouter(fakeClient client.Client) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.Default()
	router.Use(func(c *gin.Context) {
		c.Set("k8sClient", fakeClient)
	})

	// Register XStoreFollower routes
	router.GET("/xstore-followers", domain_xs.ListFollowers)
	router.POST("/xstore-followers", domain_xs.CreateFollower)
	router.GET("/xstore-followers/:namespace/:name", domain_xs.GetFollower)
	router.PUT("/xstore-followers/:namespace/:name", domain_xs.UpdateFollower)
	router.DELETE("/xstore-followers/:namespace/:name", domain_xs.DeleteFollower)

	// Rebuild wrappers
	router.POST("/xstore-rebuild/:namespace/:name/logger", domain_xs.RebuildLogger)
	router.POST("/xstore-rebuild/:namespace/:name/learner", domain_xs.RebuildLearner)
	router.POST("/xstore-rebuild/:namespace/:name/auto", domain_xs.AutoRebuild)

	return router
}

func TestXStoreFollowerEndpoints(t *testing.T) {
	scheme := runtime.NewScheme()
	polardbxv1.AddToScheme(scheme)
	corev1.AddToScheme(scheme)

	// --- Test Data ---
	sampleFollower := &polardbxv1.XStoreFollower{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "follower-for-xstore1",
			Namespace: "default",
		},
		Spec: polardbxv1.XStoreFollowerSpec{
			XStoreName: "xstore1",
			Local:      false,
			Role:       "follower",
		},
	}

	// --- Test Cases ---
	t.Run("CreateXStoreFollower", func(t *testing.T) {
		// seed a running non-leader target pod for xstore1 and xstore CR
		targetPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "xstore1-dn-0",
				Namespace: "default",
				Labels: map[string]string{
					"xstore/name": "xstore1",
					"xstore/role": "follower",
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		xstore := &polardbxv1.XStore{ObjectMeta: metav1.ObjectMeta{Name: "xstore1", Namespace: "default"}}
		scheme := runtime.NewScheme()
		polardbxv1.AddToScheme(scheme)
		corev1.AddToScheme(scheme)
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(targetPod, xstore).Build()
		router := setupXStoreFollowerTestRouter(fakeClient)
		w := httptest.NewRecorder()

		// send simplified request expected by handler
		reqBody := map[string]any{
			"name":       "follower-for-xstore1",
			"xStoreName": "xstore1",
		}
		body, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest(http.MethodPost, "/xstore-followers?namespace=default", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusCreated, w.Code)
		var createdFollower polardbxv1.XStoreFollower
		err := fakeClient.Get(context.TODO(), client.ObjectKey{Namespace: "default", Name: "follower-for-xstore1"}, &createdFollower)
		assert.NoError(t, err)
		assert.Equal(t, "xstore1", createdFollower.Spec.XStoreName)
	})

	t.Run("ListXStoreFollowers", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sampleFollower).Build()
		router := setupXStoreFollowerTestRouter(fakeClient)
		w := httptest.NewRecorder()

		req, _ := http.NewRequest(http.MethodGet, "/xstore-followers?namespace=default", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var followers []polardbxv1.XStoreFollower
		err := json.Unmarshal(w.Body.Bytes(), &followers)
		assert.NoError(t, err)
		assert.Len(t, followers, 1)
		assert.Equal(t, "follower-for-xstore1", followers[0].Name)
	})

	t.Run("GetXStoreFollower", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sampleFollower).Build()
		router := setupXStoreFollowerTestRouter(fakeClient)
		w := httptest.NewRecorder()

		req, _ := http.NewRequest(http.MethodGet, "/xstore-followers/default/follower-for-xstore1", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var follower polardbxv1.XStoreFollower
		err := json.Unmarshal(w.Body.Bytes(), &follower)
		assert.NoError(t, err)
		assert.Equal(t, "follower-for-xstore1", follower.Name)
	})

	t.Run("UpdateXStoreFollower", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sampleFollower.DeepCopy()).Build()
		router := setupXStoreFollowerTestRouter(fakeClient)
		w := httptest.NewRecorder()

		updatedFollower := sampleFollower.DeepCopy()
		// In a real scenario, spec might be immutable, but we test the endpoint's ability to handle an update call.
		// Let's assume we can add a label or annotation.
		updatedFollower.Labels = map[string]string{"updated": "true"}
		body, _ := json.Marshal(updatedFollower)
		req, _ := http.NewRequest(http.MethodPut, "/xstore-followers/default/follower-for-xstore1", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var followerInClient polardbxv1.XStoreFollower
		err := fakeClient.Get(context.TODO(), client.ObjectKeyFromObject(sampleFollower), &followerInClient)
		assert.NoError(t, err)
		assert.Equal(t, "true", followerInClient.Labels["updated"])
	})

	t.Run("DeleteXStoreFollower", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sampleFollower.DeepCopy()).Build()
		router := setupXStoreFollowerTestRouter(fakeClient)
		w := httptest.NewRecorder()

		req, _ := http.NewRequest(http.MethodDelete, "/xstore-followers/default/follower-for-xstore1", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var deletedFollower polardbxv1.XStoreFollower
		err := fakeClient.Get(context.TODO(), client.ObjectKeyFromObject(sampleFollower), &deletedFollower)
		assert.True(t, k8serrors.IsNotFound(err))
	})

	t.Run("CreateXStoreFollower_InvalidJSON", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		router := setupXStoreFollowerTestRouter(fakeClient)
		w := httptest.NewRecorder()

		req, _ := http.NewRequest(http.MethodPost, "/xstore-followers?namespace=default", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")

		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("GetXStoreFollower_NotFound", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		router := setupXStoreFollowerTestRouter(fakeClient)
		w := httptest.NewRecorder()

		req, _ := http.NewRequest(http.MethodGet, "/xstore-followers/default/non-existent", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("UpdateXStoreFollower_InvalidJSON", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sampleFollower.DeepCopy()).Build()
		router := setupXStoreFollowerTestRouter(fakeClient)
		w := httptest.NewRecorder()

		req, _ := http.NewRequest(http.MethodPut, "/xstore-followers/default/follower-for-xstore1", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")

		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("DeleteXStoreFollower_NotFound", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		router := setupXStoreFollowerTestRouter(fakeClient)
		w := httptest.NewRecorder()

		req, _ := http.NewRequest(http.MethodDelete, "/xstore-followers/default/non-existent", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("RebuildLogger_UsesLoggerRoleAndPathParams", func(t *testing.T) {
		// seed a running non-leader target pod for xstore1
		targetPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "xstore1-dn-0",
				Namespace: "default",
				Labels: map[string]string{
					"xstore/name": "xstore1",
					"xstore/role": "follower",
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		xstore := &polardbxv1.XStore{ObjectMeta: metav1.ObjectMeta{Name: "xstore1", Namespace: "default"}}
		scheme := runtime.NewScheme()
		polardbxv1.AddToScheme(scheme)
		corev1.AddToScheme(scheme)
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(targetPod, xstore).Build()
		router := setupXStoreFollowerTestRouter(fakeClient)
		w := httptest.NewRecorder()

		reqBody := map[string]any{
			"name":       "rebuild-logger-x1",
			"xStoreName": "xstore1",
		}
		body, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest(http.MethodPost, "/xstore-rebuild/default/xstore1/logger", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusCreated, w.Code)
		var created polardbxv1.XStoreFollower
		err := fakeClient.Get(context.TODO(), client.ObjectKey{Namespace: "default", Name: "rebuild-logger-x1"}, &created)
		assert.NoError(t, err)
		assert.Equal(t, "logger", string(created.Spec.Role))
		assert.Equal(t, "logger", created.Labels["xstore/rebuild-type"])
	})

	t.Run("RebuildLearner_UsesLearnerRoleAndPathParams", func(t *testing.T) {
		targetPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "xstore2-dn-0",
				Namespace: "default",
				Labels: map[string]string{
					"xstore/name": "xstore2",
					"xstore/role": "follower",
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		xstore := &polardbxv1.XStore{ObjectMeta: metav1.ObjectMeta{Name: "xstore2", Namespace: "default"}}
		scheme := runtime.NewScheme()
		polardbxv1.AddToScheme(scheme)
		corev1.AddToScheme(scheme)
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(targetPod, xstore).Build()
		router := setupXStoreFollowerTestRouter(fakeClient)
		w := httptest.NewRecorder()

		reqBody := map[string]any{
			"name":       "rebuild-learner-x2",
			"xStoreName": "xstore2",
		}
		body, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest(http.MethodPost, "/xstore-rebuild/default/xstore2/learner", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusCreated, w.Code)
		var created polardbxv1.XStoreFollower
		err := fakeClient.Get(context.TODO(), client.ObjectKey{Namespace: "default", Name: "rebuild-learner-x2"}, &created)
		assert.NoError(t, err)
		assert.Equal(t, "learner", string(created.Spec.Role))
		assert.Equal(t, "learner", created.Labels["xstore/rebuild-type"])
	})

	t.Run("AutoRebuild_DefaultFollowerRole", func(t *testing.T) {
		targetPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "xstore3-dn-0",
				Namespace: "default",
				Labels: map[string]string{
					"xstore/name": "xstore3",
					"xstore/role": "follower",
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		xstore := &polardbxv1.XStore{ObjectMeta: metav1.ObjectMeta{Name: "xstore3", Namespace: "default"}}
		scheme := runtime.NewScheme()
		polardbxv1.AddToScheme(scheme)
		corev1.AddToScheme(scheme)
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(targetPod, xstore).Build()
		router := setupXStoreFollowerTestRouter(fakeClient)
		w := httptest.NewRecorder()

		reqBody := map[string]any{
			"name":       "rebuild-auto-x3",
			"xStoreName": "xstore3",
		}
		body, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest(http.MethodPost, "/xstore-rebuild/default/xstore3/auto", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusCreated, w.Code)
		var created polardbxv1.XStoreFollower
		err := fakeClient.Get(context.TODO(), client.ObjectKey{Namespace: "default", Name: "rebuild-auto-x3"}, &created)
		assert.NoError(t, err)
		assert.Equal(t, "follower", string(created.Spec.Role))
		assert.Equal(t, "follower", created.Labels["xstore/rebuild-type"])
	})
}
