package router

import (
	"net/http"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	"polardbx-dashboard-backend/pkg/config"
)

// RegisterSwaggerRoutes registers Swagger/OpenAPI documentation routes.
// Swagger UI will be available at /swagger/index.html
func RegisterSwaggerRoutes(r *gin.Engine) {
	cfg := config.GetServerConfig()

	// Only enable Swagger in debug mode or when explicitly enabled
	if !cfg.IsDebug() && !cfg.EnableSwagger {
		return
	}

	// OpenAPI JSON spec endpoint (served by our handler)
	r.GET("/swagger/doc.json", func(c *gin.Context) {
		c.JSON(http.StatusOK, getSwaggerSpec())
	})

	// Swagger UI endpoint – use a separate prefix to avoid wildcard conflicts
	r.GET("/swagger/ui/*any", ginSwagger.WrapHandler(swaggerFiles.Handler, ginSwagger.URL("/swagger/doc.json")))

	// Redirect root swagger to UI index
	r.GET("/swagger", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/swagger/ui/index.html")
	})
}

// getSwaggerSpec returns the OpenAPI specification.
// This can be generated from code annotations or loaded from a file.
func getSwaggerSpec() map[string]interface{} {
	return map[string]interface{}{
		"openapi": "3.0.0",
		"info": map[string]interface{}{
			"title":       "PolarDB-X UI Backend API",
			"description": "RESTful API for PolarDB-X cluster management and operations",
			"version":     "1.0.0",
			"contact": map[string]interface{}{
				"name": "PolarDB-X Team",
			},
		},
		"servers": []map[string]interface{}{
			{
				"url":         "/api/v1",
				"description": "API v1",
			},
		},
		"tags": []map[string]interface{}{
			{"name": "Health", "description": "Health check endpoints"},
			{"name": "Clusters", "description": "PolarDB-X cluster management"},
			{"name": "Backups", "description": "Backup operations"},
			{"name": "XStores", "description": "XStore management"},
			{"name": "Monitoring", "description": "Monitoring and diagnostics"},
			{"name": "Logs", "description": "Log collection and query"},
			{"name": "System", "description": "System tasks and operations"},
		},
		"paths": map[string]interface{}{
			"/health": map[string]interface{}{
				"get": map[string]interface{}{
					"tags":        []string{"Health"},
					"summary":     "Health check",
					"description": "Returns the health status of the service",
					"responses": map[string]interface{}{
						"200": map[string]interface{}{
							"description": "Service is healthy",
							"content": map[string]interface{}{
								"application/json": map[string]interface{}{
									"schema": map[string]interface{}{
										"$ref": "#/components/schemas/HealthResponse",
									},
								},
							},
						},
					},
				},
			},
			"/ready": map[string]interface{}{
				"get": map[string]interface{}{
					"tags":        []string{"Health"},
					"summary":     "Readiness check",
					"description": "Returns the readiness status of the service and its dependencies",
					"responses": map[string]interface{}{
						"200": map[string]interface{}{
							"description": "Service is ready",
							"content": map[string]interface{}{
								"application/json": map[string]interface{}{
									"schema": map[string]interface{}{
										"$ref": "#/components/schemas/ReadyResponse",
									},
								},
							},
						},
						"503": map[string]interface{}{
							"description": "Service is not ready",
						},
					},
				},
			},
			"/version": map[string]interface{}{
				"get": map[string]interface{}{
					"tags":        []string{"Health"},
					"summary":     "Version information",
					"description": "Returns build version information",
					"responses": map[string]interface{}{
						"200": map[string]interface{}{
							"description": "Version information",
							"content": map[string]interface{}{
								"application/json": map[string]interface{}{
									"schema": map[string]interface{}{
										"$ref": "#/components/schemas/VersionInfo",
									},
								},
							},
						},
					},
				},
			},
		},
		"components": map[string]interface{}{
			"schemas": map[string]interface{}{
				"HealthResponse": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"status": map[string]interface{}{
							"type":        "string",
							"description": "Health status",
							"example":     "healthy",
						},
						"timestamp": map[string]interface{}{
							"type":        "string",
							"format":      "date-time",
							"description": "Current timestamp",
						},
					},
				},
				"ReadyResponse": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"status": map[string]interface{}{
							"type":        "string",
							"description": "Readiness status: ready, degraded, or not_ready",
							"enum":        []string{"ready", "degraded", "not_ready"},
						},
						"timestamp": map[string]interface{}{
							"type":        "string",
							"format":      "date-time",
							"description": "Current timestamp",
						},
						"components": map[string]interface{}{
							"type":        "object",
							"description": "Component status map",
							"additionalProperties": map[string]interface{}{
								"$ref": "#/components/schemas/ComponentStatus",
							},
						},
					},
				},
				"ComponentStatus": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"status": map[string]interface{}{
							"type":        "string",
							"description": "Component status: ok, warn, or error",
							"enum":        []string{"ok", "warn", "error"},
						},
						"detail": map[string]interface{}{
							"type":        "string",
							"description": "Optional detail message",
						},
					},
				},
				"VersionInfo": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"version": map[string]interface{}{
							"type":        "string",
							"description": "Application version",
						},
						"commit": map[string]interface{}{
							"type":        "string",
							"description": "Git commit hash",
						},
						"buildDate": map[string]interface{}{
							"type":        "string",
							"description": "Build date",
						},
						"goVersion": map[string]interface{}{
							"type":        "string",
							"description": "Go version used for build",
						},
						"uptime": map[string]interface{}{
							"type":        "string",
							"description": "Service uptime",
						},
					},
				},
				"ErrorResponse": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"error": map[string]interface{}{
							"type":        "string",
							"description": "Error code",
						},
						"message": map[string]interface{}{
							"type":        "string",
							"description": "Error message",
						},
						"requestId": map[string]interface{}{
							"type":        "string",
							"description": "Request ID for tracing",
						},
					},
				},
			},
			"securitySchemes": map[string]interface{}{
				"KubeconfigAuth": map[string]interface{}{
					"type":        "apiKey",
					"in":          "header",
					"name":        "X-Kubeconfig",
					"description": "Base64-encoded kubeconfig file",
				},
			},
		},
		"security": []map[string]interface{}{
			{
				"KubeconfigAuth": []string{},
			},
		},
	}
}
