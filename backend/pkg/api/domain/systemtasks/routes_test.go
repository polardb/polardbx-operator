package systemtasks

import (
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRegisterRoutes_SystemTasks(t *testing.T) {
	gin.SetMode(gin.TestMode)
	e := gin.New()
	v1 := e.Group("/api/v1")
	RegisterRoutes(v1)

	hasList := false
	hasItem := false
	for _, r := range e.Routes() {
		if r.Method == "GET" && r.Path == "/api/v1/systemtasks" {
			hasList = true
		}
		if r.Method == "GET" && r.Path == "/api/v1/systemtasks/:namespace/:name" {
			hasItem = true
		}
	}
	if !hasList {
		t.Fatalf("expected GET /api/v1/systemtasks to be registered")
	}
	if !hasItem {
		t.Fatalf("expected GET /api/v1/systemtasks/:namespace/:name to be registered")
	}
}
