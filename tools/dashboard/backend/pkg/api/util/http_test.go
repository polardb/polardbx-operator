package util

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestDefaultNamespace(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/?namespace=query-ns", nil)
	assert.Equal(t, "query-ns", DefaultNamespace(c, "fallback"))

	c = &gin.Context{Request: httptest.NewRequest(http.MethodGet, "/", nil)}
	c.Set("k8sDefaultNamespace", "injected")
	assert.Equal(t, "injected", DefaultNamespace(c, "fallback"))
}

func TestGetNamespacePrefersPathThenQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Params = gin.Params{{Key: "namespace", Value: "path-ns"}}
	c.Request = httptest.NewRequest(http.MethodGet, "/?namespace=query-ns", nil)
	assert.Equal(t, "path-ns", GetNamespace(c, "fallback"))
}

func TestExtractKubeconfigB64_FromHeaderQueryAndBody(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// header
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	c.Request.Header.Set("X-Kubeconfig-B64", "header-val")
	val, ok := ExtractKubeconfigB64(c)
	assert.True(t, ok)
	assert.Equal(t, "header-val", val)

	// query
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/?kubeconfig=query-val", nil)
	val, ok = ExtractKubeconfigB64(c)
	assert.True(t, ok)
	assert.Equal(t, "query-val", val)

	// short query key
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/?k=short", nil)
	val, ok = ExtractKubeconfigB64(c)
	assert.True(t, ok)
	assert.Equal(t, "short", val)

	// body
	body := map[string]string{"kubeconfig": base64.StdEncoding.EncodeToString([]byte("config"))}
	raw, _ := json.Marshal(body)
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/", bytes.NewBuffer(raw))
	c.Request.Header.Set("Content-Type", "application/json")
	val, ok = ExtractKubeconfigB64(c)
	assert.True(t, ok)
	assert.Equal(t, body["kubeconfig"], val)
}

func TestHandleK8sErrorLogsAndAborts(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	HandleK8sError(c, "list", assert.AnError)
	assert.Equal(t, http.StatusBadGateway, w.Code)
}

func TestK8sClientFromContext_MissingReturnsUnauthorized(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)

	cli, ok := K8sClientFromContext(c)
	assert.False(t, ok)
	assert.Nil(t, cli)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestBindValidateAndCtx(t *testing.T) {
	gin.SetMode(gin.TestMode)

	type payload struct {
		Name string `json:"name"`
	}

	// invalid json
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString("{"))
	c.Request.Header.Set("Content-Type", "application/json")
	_, _, ok := BindValidateAndCtx(c, &payload{}, time.Second)
	assert.False(t, ok)
	assert.Equal(t, http.StatusBadRequest, w.Code)

	// validator failure
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	body, _ := json.Marshal(payload{Name: "ok"})
	c.Request = httptest.NewRequest(http.MethodPost, "/", bytes.NewBuffer(body))
	c.Request.Header.Set("Content-Type", "application/json")
	_, _, ok = BindValidateAndCtx(c, &payload{}, time.Second, func(i interface{}) error {
		return assert.AnError
	})
	assert.False(t, ok)
	assert.Equal(t, http.StatusBadRequest, w.Code)

	// success path returns context with timeout
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	body, _ = json.Marshal(payload{Name: "ok"})
	c.Request = httptest.NewRequest(http.MethodPost, "/", bytes.NewBuffer(body))
	c.Request.Header.Set("Content-Type", "application/json")
	ctx, cancel, ok := BindValidateAndCtx(c, &payload{}, time.Second)
	assert.True(t, ok)
	defer cancel()
	assert.NotNil(t, ctx)
	assert.Equal(t, http.StatusOK, w.Code)
}
