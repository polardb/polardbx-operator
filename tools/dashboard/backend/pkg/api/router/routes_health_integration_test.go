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

// This test exercises readyHandler end-to-end with probe overrides.
func TestReadyHandler_WarnAndErrorBranches(t *testing.T) {
	gin.SetMode(gin.TestMode)

	origConfig := configProbe
	origCache := cacheProbe
	origKube := kubeProbe
	origBuild := buildProbe
	t.Cleanup(func() {
		configProbe = origConfig
		cacheProbe = origCache
		kubeProbe = origKube
		buildProbe = origBuild
	})

	configProbe = func(ctx context.Context) ComponentStatus { return ComponentStatus{Status: "ok"} }
	cacheProbe = func(ctx context.Context) ComponentStatus { return ComponentStatus{Status: "warn", Detail: "slow"} }
	kubeProbe = func(ctx context.Context) ComponentStatus {
		return ComponentStatus{Status: "error", Detail: "unreachable"}
	}
	buildProbe = func(ctx context.Context) ComponentStatus {
		return ComponentStatus{Status: "warn", Detail: "missing metadata"}
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/ready", nil)

	readyHandler(c)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	var resp ReadyResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "not_ready", resp.Status)
	assert.Equal(t, "error", resp.Components["kubernetes"].Status)
}

func TestReadyHandler_AllOK_Integration(t *testing.T) {
	gin.SetMode(gin.TestMode)

	origConfig := configProbe
	origCache := cacheProbe
	origKube := kubeProbe
	origBuild := buildProbe
	t.Cleanup(func() {
		configProbe = origConfig
		cacheProbe = origCache
		kubeProbe = origKube
		buildProbe = origBuild
	})

	configProbe = func(ctx context.Context) ComponentStatus { return ComponentStatus{Status: "ok"} }
	cacheProbe = func(ctx context.Context) ComponentStatus { return ComponentStatus{Status: "ok"} }
	kubeProbe = func(ctx context.Context) ComponentStatus { return ComponentStatus{Status: "ok"} }
	buildProbe = func(ctx context.Context) ComponentStatus { return ComponentStatus{Status: "ok"} }

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/ready", nil)

	readyHandler(c)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp ReadyResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "ready", resp.Status)
}

func TestReadyHandler_WarnButNoError_Degraded_Integration(t *testing.T) {
	gin.SetMode(gin.TestMode)

	origConfig := configProbe
	origCache := cacheProbe
	origKube := kubeProbe
	origBuild := buildProbe
	t.Cleanup(func() {
		configProbe = origConfig
		cacheProbe = origCache
		kubeProbe = origKube
		buildProbe = origBuild
	})

	configProbe = func(ctx context.Context) ComponentStatus { return ComponentStatus{Status: "ok"} }
	cacheProbe = func(ctx context.Context) ComponentStatus {
		return ComponentStatus{Status: "warn", Detail: "warming up"}
	}
	kubeProbe = func(ctx context.Context) ComponentStatus { return ComponentStatus{Status: "ok"} }
	buildProbe = func(ctx context.Context) ComponentStatus { return ComponentStatus{Status: "ok"} }

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/ready", nil)

	readyHandler(c)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp ReadyResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "degraded", resp.Status)
	assert.Equal(t, "warn", resp.Components["cache"].Status)
}
