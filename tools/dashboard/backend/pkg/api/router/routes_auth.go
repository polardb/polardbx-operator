package router

import (
	"polardbx-dashboard-backend/pkg/api"
	domain_auth "polardbx-dashboard-backend/pkg/api/domain/platform/auth/handler"
	domain_settings "polardbx-dashboard-backend/pkg/api/domain/platform/settings/handler"

	"github.com/gin-gonic/gin"
)

// RegisterPublicRoutes registers routes that don't require authentication
func RegisterPublicRoutes(v1 *gin.RouterGroup) {
	// Health endpoints (compatibility aliases under /api/v1/*).
	// Canonical endpoints are /health, /ready, /version.
	// Keeping these aliases avoids breaking existing manifests/scripts.
	v1.GET("/health", healthHandler)
	v1.GET("/ready", readyHandler)
	v1.GET("/version", versionHandler)

	// Image registry configuration (public for UI initialization)
	v1.GET("/image-registry/config", domain_settings.GetImageRegistryConfig)
	v1.PUT("/image-registry/config", domain_settings.UpdateImageRegistryConfig)
	v1.GET("/image-registry/presets", domain_settings.GetAvailableRegistries)
	v1.POST("/image-registry/test", domain_settings.TestImageRegistry)

	// Connect endpoint - establishes client for subsequent requests
	v1.POST("/connect", api.KubeconfigAuthMiddleware(), api.Connect)
}

// RegisterAuthRoutes registers authentication routes
func RegisterAuthRoutes(v1 *gin.RouterGroup) {
	// JWT login endpoints (optional)
	v1.POST("/auth/login", domain_auth.Login)
	v1.GET("/auth/me", domain_auth.Me)
}
