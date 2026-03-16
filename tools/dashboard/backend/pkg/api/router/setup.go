package router

import (
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"polardbx-dashboard-backend/pkg/api"
	domain_auth "polardbx-dashboard-backend/pkg/api/domain/platform/auth/handler"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/middleware"
	"polardbx-dashboard-backend/pkg/api/provider"
	"polardbx-dashboard-backend/pkg/config"
	"polardbx-dashboard-backend/pkg/logger"
)

// SetupRouter creates and configures the main Gin router
func SetupRouter() *gin.Engine {
	cfg := config.GetServerConfig()

	// Set Gin mode from config
	gin.SetMode(cfg.Mode)

	r := gin.New()

	// Setup middleware (must be before routes)
	setupMiddleware(r, cfg)

	// Setup routes (API routes must be registered before static file serving)
	setupRoutes(r)

	// Serve static frontend assets if UI_STATIC_DIR is set (for all-in-one deployment)
	// This must be AFTER API routes to avoid conflicts with /api/* and /swagger/*
	if uiDir := strings.TrimSpace(os.Getenv("UI_STATIC_DIR")); uiDir != "" {
		logger.Info("Serving static UI assets", "dir", uiDir)
		// Use NoRoute to handle unmatched paths (for SPA routing)
		// API routes are already registered, so they take precedence
		r.NoRoute(func(c *gin.Context) {
			path := c.Request.URL.Path
			// API paths should return 404
			if strings.HasPrefix(path, "/api/") {
				c.JSON(http.StatusNotFound, gin.H{
					"error":   "API route not found",
					"path":    path,
					"message": "The requested API endpoint does not exist",
				})
				return
			}
			// For SPA: try to serve the requested file, fallback to index.html
			filePath := strings.TrimPrefix(path, "/")
			if filePath == "" {
				filePath = "index.html"
			}
			// Try to serve the file, if not found, serve index.html for SPA routing
			fs := http.Dir(uiDir)
			file, err := fs.Open(filePath)
			if err != nil {
				// File not found, serve index.html for SPA routing
				filePath = "index.html"
				file, err = fs.Open(filePath)
				if err != nil {
					c.String(http.StatusNotFound, "index.html not found")
					return
				}
			}
			defer file.Close()
			stat, err := file.Stat()
			if err != nil {
				c.String(http.StatusInternalServerError, "Failed to stat file")
				return
			}
			// Serve the file directly without redirect
			http.ServeContent(c.Writer, c.Request, filePath, stat.ModTime(), file)
		})
	}

	return r
}

// setupMiddleware configures all middleware
func setupMiddleware(r *gin.Engine, cfg *config.ServerConfig) {
	// Request ID middleware (first, for tracing)
	r.Use(apierr.RequestIDMiddleware())

	// Recovery middleware - use our enhanced version that returns APIError
	r.Use(apierr.RecoveryHandler())

	// Global error handler - catches any unhandled errors and formats them
	r.Use(apierr.Handler())

	// Request logging middleware
	r.Use(middleware.RequestLogger(middleware.RequestLogConfig{
		LogRequestBody:  cfg.LogRequestBody,
		LogResponseBody: cfg.LogResponseBody,
		MaxBodyLogSize:  cfg.LogMaxBodySize,
		SkipPaths:       cfg.LogSkipPaths,
		SensitiveFields: cfg.LogSensitiveFields,
	}))

	// CORS middleware
	r.Use(CORSMiddleware(cfg))
}

// setupRoutes registers all route groups
func setupRoutes(r *gin.Engine) {
	// Swagger/OpenAPI documentation (only in debug mode or when enabled)
	RegisterSwaggerRoutes(r)

	// Health check endpoints (no auth required)
	RegisterHealthRoutes(r)

	// Ping endpoint
	r.GET("/ping", func(c *gin.Context) {
		apierr.OK(c, gin.H{"message": "pong"})
	})

	// API v1 group
	v1 := r.Group("/api/v1")
	v1.Use(
		provider.Inject(provider.NewDefaultProvider()),
		VersionMiddleware("v1"),
	)

	// Public routes (no kubeconfig required)
	RegisterPublicRoutes(v1)

	// Auth routes
	RegisterAuthRoutes(v1)

	// Protected routes (require kubeconfig; optional JWT layer)
	protected := v1.Group("")
	protected.Use(
		api.KubeconfigAuthMiddleware(),
		domain_auth.JWTAuthMiddleware(),
	)
	{
		reg := NewRouteRegistry()
		RegisterClusterRoutesRegistry(reg)
		RegisterAlertsRoutesRegistry(reg)
		RegisterBackupRoutesRegistry(reg)
		RegisterXStoreRoutesRegistry(reg)
		RegisterMonitoringRoutesRegistry(reg)
		RegisterLogsRoutesRegistry(reg)
		RegisterSystemRoutesRegistry(reg)
		RegisterDiagnosticsRoutesRegistry(reg)
		RegisterRestoreRoutesRegistry(reg)
		reg.Apply(protected)
	}

	// Register CRD-aligned alias routes
	RegisterCRDAliasRoutes(protected)
	RegisterDomainRoutes(protected)
}

// CORSMiddleware creates CORS middleware with config
func CORSMiddleware(cfg *config.ServerConfig) gin.HandlerFunc {
	// Pre-compute headers
	allowOrigin := "*"
	if len(cfg.CORSAllowOrigins) > 0 {
		allowOrigin = cfg.CORSAllowOrigins[0]
	}
	allowMethods := strings.Join(cfg.CORSAllowMethods, ", ")
	allowHeaders := strings.Join(cfg.CORSAllowHeaders, ", ")
	exposeHeaders := strings.Join(cfg.CORSExposeHeaders, ", ")
	maxAge := strconv.Itoa(cfg.CORSMaxAge)
	allowCredentials := "false"
	if cfg.CORSAllowCredentials {
		allowCredentials = "true"
	}

	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")

		// Check if origin is allowed
		originAllowed := allowOrigin == "*"
		if !originAllowed && origin != "" {
			for _, allowed := range cfg.CORSAllowOrigins {
				if allowed == origin || allowed == "*" {
					originAllowed = true
					break
				}
			}
		}

		if originAllowed {
			if allowOrigin == "*" {
				c.Header("Access-Control-Allow-Origin", "*")
			} else if origin != "" {
				c.Header("Access-Control-Allow-Origin", origin)
			}
			c.Header("Access-Control-Allow-Credentials", allowCredentials)
			c.Header("Access-Control-Allow-Headers", allowHeaders)
			c.Header("Access-Control-Allow-Methods", allowMethods)
			c.Header("Access-Control-Expose-Headers", exposeHeaders)
			c.Header("Access-Control-Max-Age", maxAge)
		}

		// Handle preflight
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

// LogRoutes logs all registered routes
func LogRoutes(r *gin.Engine) {
	logger.Info("=== Registered Routes ===")
	for _, route := range r.Routes() {
		logger.Info("Route registered", "method", route.Method, "path", route.Path)
	}
	logger.Info("=== Total routes ===", "count", len(r.Routes()))
}
