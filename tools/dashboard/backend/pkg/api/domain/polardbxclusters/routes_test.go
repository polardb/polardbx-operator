package polardbxclusters

import (
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRegisterRoutes_PolarDBXClusters(t *testing.T) {
	gin.SetMode(gin.TestMode)
	e := gin.New()
	v1 := e.Group("/api/v1")
	RegisterRoutes(v1)

	hasList := false
	hasScale := false
	for _, r := range e.Routes() {
		if r.Method == "GET" && r.Path == "/api/v1/polardbxclusters" {
			hasList = true
		}
		if r.Method == "PATCH" && r.Path == "/api/v1/polardbxclusters/:namespace/:name/scale" {
			hasScale = true
		}
	}
	if !hasList {
		t.Fatalf("expected GET /api/v1/polardbxclusters to be registered")
	}
	if !hasScale {
		t.Fatalf("expected PATCH /api/v1/polardbxclusters/:namespace/:name/scale to be registered")
	}
}
