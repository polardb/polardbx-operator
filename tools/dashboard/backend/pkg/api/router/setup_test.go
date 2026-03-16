package router

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"polardbx-dashboard-backend/pkg/config"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestCORSMiddleware_WildcardOrigin(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	cfg := &config.ServerConfig{
		CORSAllowOrigins:     []string{"*"},
		CORSAllowMethods:     []string{"GET", "POST", "PUT", "DELETE"},
		CORSAllowHeaders:     []string{"Content-Type", "Authorization"},
		CORSExposeHeaders:    []string{"X-Request-Id"},
		CORSMaxAge:           86400,
		CORSAllowCredentials: false,
	}

	r.Use(CORSMiddleware(cfg))
	r.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Set("Origin", "http://example.com")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "GET, POST, PUT, DELETE", w.Header().Get("Access-Control-Allow-Methods"))
}

func TestCORSMiddleware_SpecificOrigin(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	cfg := &config.ServerConfig{
		CORSAllowOrigins:     []string{"http://allowed.com", "http://another.com"},
		CORSAllowMethods:     []string{"GET", "POST"},
		CORSAllowHeaders:     []string{"Content-Type"},
		CORSExposeHeaders:    []string{},
		CORSMaxAge:           3600,
		CORSAllowCredentials: true,
	}

	r.Use(CORSMiddleware(cfg))
	r.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	// Test allowed origin
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Set("Origin", "http://allowed.com")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "http://allowed.com", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "true", w.Header().Get("Access-Control-Allow-Credentials"))

	// Test disallowed origin
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest("GET", "/test", nil)
	req2.Header.Set("Origin", "http://disallowed.com")
	r.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, "", w2.Header().Get("Access-Control-Allow-Origin"))
}

func TestCORSMiddleware_PreflightRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	cfg := &config.ServerConfig{
		CORSAllowOrigins:     []string{"*"},
		CORSAllowMethods:     []string{"GET", "POST", "PUT", "DELETE"},
		CORSAllowHeaders:     []string{"Content-Type", "Authorization"},
		CORSExposeHeaders:    []string{},
		CORSMaxAge:           86400,
		CORSAllowCredentials: false,
	}

	r.Use(CORSMiddleware(cfg))
	r.POST("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("OPTIONS", "/test", nil)
	req.Header.Set("Origin", "http://example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
	assert.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))
}

func TestCORSMiddleware_NoOriginHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	cfg := &config.ServerConfig{
		CORSAllowOrigins:     []string{"http://allowed.com"},
		CORSAllowMethods:     []string{"GET"},
		CORSAllowHeaders:     []string{"Content-Type"},
		CORSExposeHeaders:    []string{},
		CORSMaxAge:           3600,
		CORSAllowCredentials: false,
	}

	r.Use(CORSMiddleware(cfg))
	r.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	// Request without Origin header
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/test", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	// No CORS headers should be set when no origin
	assert.Equal(t, "", w.Header().Get("Access-Control-Allow-Origin"))
}

func TestLogRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/test1", func(c *gin.Context) {})
	r.POST("/test2", func(c *gin.Context) {})

	// LogRoutes should not panic
	assert.NotPanics(t, func() {
		LogRoutes(r)
	})
}
