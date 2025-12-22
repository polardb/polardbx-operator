package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func stubProbes(t *testing.T, cfg, cache, kube, build func(context.Context) ComponentStatus) {
	origConfig := configProbe
	origCache := cacheProbe
	origKube := kubeProbe
	origBuild := buildProbe

	if cfg != nil {
		configProbe = cfg
	}
	if cache != nil {
		cacheProbe = cache
	}
	if kube != nil {
		kubeProbe = kube
	}
	if build != nil {
		buildProbe = build
	}

	t.Cleanup(func() {
		configProbe = origConfig
		cacheProbe = origCache
		kubeProbe = origKube
		buildProbe = origBuild
	})
}

func TestReadyHandler_AllOK(t *testing.T) {
	gin.SetMode(gin.TestMode)
	stubProbes(t,
		func(context.Context) ComponentStatus { return ComponentStatus{Status: "ok"} },
		func(context.Context) ComponentStatus { return ComponentStatus{Status: "ok"} },
		func(context.Context) ComponentStatus { return ComponentStatus{Status: "ok"} },
		func(context.Context) ComponentStatus { return ComponentStatus{Status: "ok"} },
	)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/ready", nil)

	readyHandler(c)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp ReadyResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "ready", resp.Status)
	assert.Equal(t, "ok", resp.Components["kubernetes"].Status)
}

func TestReadyHandler_ErrorBubblesToNotReady(t *testing.T) {
	gin.SetMode(gin.TestMode)
	stubProbes(t,
		nil,
		nil,
		func(context.Context) ComponentStatus { return ComponentStatus{Status: "error", Detail: "boom"} },
		nil,
	)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/ready", nil)

	readyHandler(c)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	var resp ReadyResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "not_ready", resp.Status)
	assert.Equal(t, "error", resp.Components["kubernetes"].Status)
	assert.Equal(t, "boom", resp.Components["kubernetes"].Detail)
}

func TestReadyHandler_WarnKeepsReadyButDegraded(t *testing.T) {
	gin.SetMode(gin.TestMode)
	stubProbes(t,
		func(context.Context) ComponentStatus { return ComponentStatus{Status: "ok"} },
		func(context.Context) ComponentStatus { return ComponentStatus{Status: "ok"} },
		func(context.Context) ComponentStatus { return ComponentStatus{Status: "ok"} },
		func(context.Context) ComponentStatus {
			return ComponentStatus{Status: "warn", Detail: "missing build info"}
		},
	)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/ready", nil)

	readyHandler(c)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp ReadyResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "degraded", resp.Status)
	assert.Equal(t, "warn", resp.Components["build"].Status)
}

func TestBuildInfoStatus(t *testing.T) {
	origVersion, origCommit, origBuildDate, origGoVersion := Version, Commit, BuildDate, GoVersion
	Version, Commit, BuildDate, GoVersion = "dev", "unknown", "unknown", "unknown"
	t.Cleanup(func() {
		Version, Commit, BuildDate, GoVersion = origVersion, origCommit, origBuildDate, origGoVersion
	})

	cs := BuildInfoStatus()
	assert.Equal(t, "warn", cs.Status)

	Version, Commit, BuildDate, GoVersion = "1.0.0", "abc123", "2024-01-01T00:00:00Z", "go1.21"
	cs = BuildInfoStatus()
	assert.Equal(t, "ok", cs.Status)
}

func TestHealthHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/health", nil)

	healthHandler(c)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp HealthResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "healthy", resp.Status)
	assert.NotEmpty(t, resp.Timestamp)
}

func TestVersionHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Save original values
	origVersion, origCommit, origBuildDate, origGoVersion := Version, Commit, BuildDate, GoVersion
	Version, Commit, BuildDate, GoVersion = "1.0.0", "abc123", "2024-01-01T00:00:00Z", "go1.21"
	t.Cleanup(func() {
		Version, Commit, BuildDate, GoVersion = origVersion, origCommit, origBuildDate, origGoVersion
	})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/version", nil)

	versionHandler(c)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp VersionInfo
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "1.0.0", resp.Version)
	assert.Equal(t, "abc123", resp.Commit)
	assert.Equal(t, "2024-01-01T00:00:00Z", resp.BuildDate)
	assert.Equal(t, "go1.21", resp.GoVersion)
	assert.NotEmpty(t, resp.Uptime)
}
