package platform

import (
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRegisterRoutes_Platform(t *testing.T) {
	gin.SetMode(gin.TestMode)
	e := gin.New()
	v1 := e.Group("/api/v1")
	RegisterRoutes(v1)

	hasSystem := false
	hasGrafana := false
	hasLogs := false
	for _, r := range e.Routes() {
		if r.Method == "GET" && r.Path == "/api/v1/platform/system/context" {
			hasSystem = true
		}
		if r.Method == "GET" && r.Path == "/api/v1/platform/grafana/config" {
			hasGrafana = true
		}
		if r.Method == "POST" && r.Path == "/api/v1/platform/logs/query" {
			hasLogs = true
		}
	}
	if !hasSystem {
		t.Fatalf("expected system route registered")
	}
	if !hasGrafana {
		t.Fatalf("expected grafana route registered")
	}
	if !hasLogs {
		t.Fatalf("expected logs route registered")
	}
}
