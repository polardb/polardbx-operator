package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/kubernetes/fake"
)

func setupLogStrategyTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	return r
}

func TestLogStrategy_List_ErrorHandling(t *testing.T) {
	r := setupLogStrategyTestRouter()
	r.GET("/log-strategies", func(c *gin.Context) {
		// Simulate missing clientset (should return 500, not 200 with warning)
		List(c)
	})

	req, _ := http.NewRequest(http.MethodGet, "/log-strategies", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// ClientsetFromContext may return 401 (Unauthorized) or the handler may return 500
	// Both are acceptable - the important thing is we don't return 200 with warning
	assert.True(t, w.Code == http.StatusUnauthorized || w.Code == http.StatusInternalServerError, 
		"should return 401 or 500 when clientset is not available, got %d", w.Code)
}

func TestLogStrategy_List_Success(t *testing.T) {
	r := setupLogStrategyTestRouter()
	r.GET("/log-strategies", func(c *gin.Context) {
		// Setup fake clientset
		clientset := fake.NewSimpleClientset()
		c.Set("clientset", clientset)
		List(c)
	})

	req, _ := http.NewRequest(http.MethodGet, "/log-strategies", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// Should return 200 with empty list when ConfigMap doesn't exist (will be created)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestLogStrategy_ListApplyRecords_ErrorHandling(t *testing.T) {
	r := setupLogStrategyTestRouter()
	r.GET("/log-strategies/apply-records", func(c *gin.Context) {
		// Simulate missing clientset (should return 500, not 200 with empty list)
		ListApplyRecords(c)
	})

	req, _ := http.NewRequest(http.MethodGet, "/log-strategies/apply-records", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// Should return 500 Internal Server Error, not 200 with empty list
	assert.Equal(t, http.StatusInternalServerError, w.Code, "should return 500 when clientset is not available")
}

func TestLogStrategy_Get_ErrorHandling(t *testing.T) {
	r := setupLogStrategyTestRouter()
	r.GET("/log-strategies/:name", func(c *gin.Context) {
		// Simulate missing clientset
		Get(c)
	})

	req, _ := http.NewRequest(http.MethodGet, "/log-strategies/test-strategy", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// Should return 500 when store cannot be loaded
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestLogStrategy_Create_ErrorHandling(t *testing.T) {
	r := setupLogStrategyTestRouter()
	r.POST("/log-strategies", func(c *gin.Context) {
		// Simulate missing clientset
		Create(c)
	})

	req, _ := http.NewRequest(http.MethodPost, "/log-strategies", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// Should return 500 when store cannot be loaded
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

