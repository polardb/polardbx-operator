package routerutil

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestRegisterCRUD_DefaultPattern(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()

	var hit = struct {
		list, create, get, update, delete bool
	}{}

	RegisterCRUD(engine.Group("/api"), "/items", CRUDHandlers{
		List: func(c *gin.Context) { hit.list = true },
		Create: func(c *gin.Context) {
			hit.create = true
		},
		Get: func(c *gin.Context) { hit.get = true },
		Update: func(c *gin.Context) {
			hit.update = true
		},
		Delete: func(c *gin.Context) { hit.delete = true },
	})

	assertRoute(t, engine, http.MethodGet, "/api/items")
	assertRoute(t, engine, http.MethodPost, "/api/items")
	assertRoute(t, engine, http.MethodGet, "/api/items/name1")
	assertRoute(t, engine, http.MethodPut, "/api/items/name1")
	assertRoute(t, engine, http.MethodDelete, "/api/items/name1")
}

func TestRegisterCRUD_CustomPattern(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()

	RegisterCRUDWithItemPattern(engine.Group("/api"), "/ns-items", "/ns-items/:namespace/:name", CRUDHandlers{
		List:   func(c *gin.Context) {},
		Create: func(c *gin.Context) {},
		Get:    func(c *gin.Context) {},
		Update: func(c *gin.Context) {},
		Delete: func(c *gin.Context) {},
	})

	assertRoute(t, engine, http.MethodGet, "/api/ns-items")
	assertRoute(t, engine, http.MethodPost, "/api/ns-items")
	assertRoute(t, engine, http.MethodGet, "/api/ns-items/ns/name1")
	assertRoute(t, engine, http.MethodPut, "/api/ns-items/ns/name1")
	assertRoute(t, engine, http.MethodDelete, "/api/ns-items/ns/name1")
}

func TestRegisterLGD(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()

	RegisterLGD(engine.Group("/api"), "/lgd-items", "/lgd-items/:namespace/:name", LGDHandlers{
		List:   func(c *gin.Context) {},
		Get:    func(c *gin.Context) {},
		Delete: func(c *gin.Context) {},
	})

	assertRoute(t, engine, http.MethodGet, "/api/lgd-items")
	assertRoute(t, engine, http.MethodGet, "/api/lgd-items/ns/name1")
	assertRoute(t, engine, http.MethodDelete, "/api/lgd-items/ns/name1")
}

func assertRoute(t *testing.T, engine *gin.Engine, method, path string) {
	t.Helper()
	req, _ := http.NewRequest(method, path, nil)
	w := performRequest(engine, req)
	// 404 means route missing; any status other than 404 is acceptable for route existence
	assert.NotEqual(t, http.StatusNotFound, w.Code, "route should be registered: %s %s", method, path)
}

// performRequest is a tiny helper to exercise gin router in tests.
func performRequest(r http.Handler, req *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}
