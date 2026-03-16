package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func setupSystemTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	return r
}

func TestSystem_ListNamespaces_ErrorHandling(t *testing.T) {
	r := setupSystemTestRouter()
	r.GET("/namespaces", func(c *gin.Context) {
		// Simulate missing k8s client (should return 500, not 200 with warning)
		ListNamespaces(c)
	})

	req, _ := http.NewRequest(http.MethodGet, "/namespaces", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// K8sClientFromContext returns 401 (Unauthorized) when client is not initialized, which is correct behavior
	// The handler then returns 500 after checking the client, so we expect either 401 or 500
	assert.True(t, w.Code == http.StatusUnauthorized || w.Code == http.StatusInternalServerError,
		"should return 401 or 500 when k8s client is not initialized, got %d", w.Code)
}

func TestSystem_ListNamespaces_Success(t *testing.T) {
	r := setupSystemTestRouter()
	scheme := runtime.NewScheme()
	corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	r.GET("/namespaces", func(c *gin.Context) {
		c.Set("k8sClient", fakeClient)
		ListNamespaces(c)
	})

	req, _ := http.NewRequest(http.MethodGet, "/namespaces", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestSystem_ListStorageClasses_ErrorHandling(t *testing.T) {
	r := setupSystemTestRouter()
	r.GET("/storage-classes", func(c *gin.Context) {
		// Simulate missing k8s client (should return 500, not 200 with warning)
		ListStorageClasses(c)
	})

	req, _ := http.NewRequest(http.MethodGet, "/storage-classes", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// K8sClientFromContext returns 401 (Unauthorized) when client is not initialized, which is correct behavior
	// The handler then returns 500 after checking the client, so we expect either 401 or 500
	assert.True(t, w.Code == http.StatusUnauthorized || w.Code == http.StatusInternalServerError,
		"should return 401 or 500 when k8s client is not initialized, got %d", w.Code)
}

func TestSystem_ListStorageClasses_Success(t *testing.T) {
	r := setupSystemTestRouter()
	scheme := runtime.NewScheme()
	corev1.AddToScheme(scheme)
	storagev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	r.GET("/storage-classes", func(c *gin.Context) {
		c.Set("k8sClient", fakeClient)
		ListStorageClasses(c)
	})

	req, _ := http.NewRequest(http.MethodGet, "/storage-classes", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}
