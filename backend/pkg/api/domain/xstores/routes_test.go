package xstores

import (
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRegisterRoutes_XStores(t *testing.T) {
	gin.SetMode(gin.TestMode)
	e := gin.New()
	v1 := e.Group("/api/v1")
	RegisterRoutes(v1)

	hasList := false
	hasRebuild := false
	for _, r := range e.Routes() {
		if r.Method == "GET" && r.Path == "/api/v1/xstores" {
			hasList = true
		}
		if r.Method == "POST" && r.Path == "/api/v1/xstores/:namespace/:name/rebuild/logger" {
			hasRebuild = true
		}
	}
	if !hasList {
		t.Fatalf("expected GET /api/v1/xstores to be registered")
	}
	if !hasRebuild {
		t.Fatalf("expected POST /api/v1/xstores/:namespace/:name/rebuild/logger to be registered")
	}
}
