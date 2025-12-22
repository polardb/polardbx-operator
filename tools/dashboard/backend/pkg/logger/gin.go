package logger

import (
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// GinLogger returns a gin middleware that logs requests using zap
func GinLogger() gin.HandlerFunc {
	return GinLoggerWithConfig(GinLoggerConfig{})
}

// GinLoggerConfig holds configuration for the Gin logger middleware
type GinLoggerConfig struct {
	// SkipPaths are paths that should not be logged
	SkipPaths []string
	// SkipHealthCheck skips /health, /ready, /ping paths
	SkipHealthCheck bool
}

// GinLoggerWithConfig returns a gin middleware with custom configuration
func GinLoggerWithConfig(cfg GinLoggerConfig) gin.HandlerFunc {
	skipPaths := make(map[string]bool)
	for _, path := range cfg.SkipPaths {
		skipPaths[path] = true
	}
	if cfg.SkipHealthCheck {
		skipPaths["/health"] = true
		skipPaths["/healthz"] = true
		skipPaths["/ready"] = true
		skipPaths["/readyz"] = true
		skipPaths["/ping"] = true
	}

	return func(c *gin.Context) {
		// Skip logging for configured paths
		if skipPaths[c.Request.URL.Path] {
			c.Next()
			return
		}

		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		// Process request
		c.Next()

		// Calculate latency
		latency := time.Since(start)

		// Get request ID if available
		requestID := c.GetString("X-Request-ID")

		// Build log fields
		fields := []zap.Field{
			zap.Int("status", c.Writer.Status()),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.String("ip", c.ClientIP()),
			zap.String("user-agent", c.Request.UserAgent()),
			zap.Duration("latency", latency),
			zap.Int("body-size", c.Writer.Size()),
		}

		if requestID != "" {
			fields = append(fields, zap.String("request-id", requestID))
		}

		// Add error if any
		if len(c.Errors) > 0 {
			fields = append(fields, zap.String("errors", c.Errors.String()))
		}

		// Log based on status code
		status := c.Writer.Status()
		switch {
		case status >= 500:
			L().Error("Server error", fields...)
		case status >= 400:
			L().Warn("Client error", fields...)
		default:
			L().Info("Request completed", fields...)
		}
	}
}

// GinRecovery returns a gin middleware that recovers from panics and logs them
func GinRecovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				// Log the panic
				L().Error("Panic recovered",
					zap.Any("error", err),
					zap.String("path", c.Request.URL.Path),
					zap.String("method", c.Request.Method),
					zap.String("ip", c.ClientIP()),
				)

				// Return 500 error
				c.AbortWithStatusJSON(500, gin.H{
					"error": gin.H{
						"code":    "SYS_1001",
						"message": "Internal server error",
					},
				})
			}
		}()
		c.Next()
	}
}
