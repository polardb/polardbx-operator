package router

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/getkin/kin-openapi/openapi2"
	"github.com/getkin/kin-openapi/openapi2conv"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"github.com/swaggo/swag"

	docs "polardbx-dashboard-backend/docs"
	"polardbx-dashboard-backend/pkg/config"
)

// RegisterSwaggerRoutes registers Swagger/OpenAPI documentation routes.
// Swagger UI will be available at /swagger/ui/index.html
func RegisterSwaggerRoutes(r *gin.Engine) {
	cfg := config.GetServerConfig()

	// Only enable Swagger in debug mode or when explicitly enabled
	if !cfg.IsDebug() && !cfg.EnableSwagger {
		return
	}

	// OpenAPI JSON spec endpoint (served by our handler)
	r.GET("/swagger/doc.json", func(c *gin.Context) {
		spec, err := getSwaggerSpec()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":   "swagger_spec_error",
				"message": err.Error(),
			})
			return
		}
		c.Data(http.StatusOK, "application/json; charset=utf-8", spec)
	})

	// Swagger UI endpoint – use a separate prefix to avoid wildcard conflicts
	r.GET("/swagger/ui/*any", ginSwagger.WrapHandler(swaggerFiles.Handler, ginSwagger.URL("/swagger/doc.json")))

	// Redirect root swagger to UI index
	r.GET("/swagger", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/swagger/ui/index.html")
	})
}

var (
	swaggerSpecOnce sync.Once
	swaggerSpecJSON []byte
	swaggerSpecErr  error
)

// getSwaggerSpec returns the OpenAPI v3 specification (JSON).
// Source of truth is the swaggo-generated v2 doc (tools/dashboard/backend/docs),
// which we convert to OpenAPI v3 and then augment with health endpoints + auth schemes.
func getSwaggerSpec() ([]byte, error) {
	swaggerSpecOnce.Do(func() {
		swaggerSpecJSON, swaggerSpecErr = buildOpenAPISpec()
	})
	return swaggerSpecJSON, swaggerSpecErr
}

func buildOpenAPISpec() ([]byte, error) {
	// Fill base metadata for swaggo template rendering.
	docs.SwaggerInfo.Title = "PolarDB-X UI Backend API"
	docs.SwaggerInfo.Description = "RESTful API for PolarDB-X cluster management and operations"
	if Version != "" {
		docs.SwaggerInfo.Version = Version
	} else {
		docs.SwaggerInfo.Version = "dev"
	}
	docs.SwaggerInfo.BasePath = "/"

	raw, err := swag.ReadDoc()
	if err != nil {
		return nil, err
	}

	var v2 openapi2.T
	if err := json.Unmarshal([]byte(raw), &v2); err != nil {
		return nil, err
	}

	v3, err := openapi2conv.ToV3(&v2)
	if err != nil {
		return nil, err
	}

	ensureOpenAPIMetadata(v3)
	ensureAuthSchemes(v3)
	addHealthEndpoints(v3)

	return json.Marshal(v3)
}

func ensureOpenAPIMetadata(doc *openapi3.T) {
	if doc.Info == nil {
		doc.Info = &openapi3.Info{}
	}
	if doc.Info.Title == "" {
		doc.Info.Title = docs.SwaggerInfo.Title
	}
	if doc.Info.Description == "" {
		doc.Info.Description = docs.SwaggerInfo.Description
	}
	if doc.Info.Version == "" {
		doc.Info.Version = docs.SwaggerInfo.Version
	}
	if doc.Info.Contact == nil {
		doc.Info.Contact = &openapi3.Contact{Name: "PolarDB-X Team"}
	} else if doc.Info.Contact.Name == "" {
		doc.Info.Contact.Name = "PolarDB-X Team"
	}

	// Keep servers relative so the spec works behind proxies and at different hosts.
	if len(doc.Servers) == 0 {
		doc.Servers = openapi3.Servers{&openapi3.Server{URL: "/"}}
	}
}

func ensureAuthSchemes(doc *openapi3.T) {
	if doc.Components == nil {
		doc.Components = &openapi3.Components{}
	}
	if doc.Components.SecuritySchemes == nil {
		doc.Components.SecuritySchemes = openapi3.SecuritySchemes{}
	}

	doc.Components.SecuritySchemes["KubeconfigAuth"] = &openapi3.SecuritySchemeRef{
		Value: &openapi3.SecurityScheme{
			Type:        "apiKey",
			In:          "header",
			Name:        "X-Kubeconfig-B64",
			Description: "Base64-encoded kubeconfig file",
		},
	}
	doc.Components.SecuritySchemes["BearerAuth"] = &openapi3.SecuritySchemeRef{
		Value: openapi3.NewJWTSecurityScheme(),
	}

	// Most API endpoints require kubeconfig; we expose it globally for Swagger UI "Authorize".
	if len(doc.Security) == 0 {
		doc.Security = openapi3.SecurityRequirements{
			openapi3.NewSecurityRequirement().Authenticate("KubeconfigAuth"),
		}
	}
}

func addHealthEndpoints(doc *openapi3.T) {
	if doc.Paths == nil {
		doc.Paths = openapi3.Paths{}
	}
	if doc.Components == nil {
		doc.Components = &openapi3.Components{}
	}
	if doc.Components.Schemas == nil {
		doc.Components.Schemas = openapi3.Schemas{}
	}

	// Schemas
	doc.Components.Schemas["HealthResponse"] = &openapi3.SchemaRef{Value: &openapi3.Schema{
		Type: "object",
		Properties: openapi3.Schemas{
			"status":    &openapi3.SchemaRef{Value: &openapi3.Schema{Type: "string", Description: "Health status", Example: "healthy"}},
			"timestamp": &openapi3.SchemaRef{Value: &openapi3.Schema{Type: "string", Format: "date-time", Description: "Current timestamp"}},
		},
	}}
	doc.Components.Schemas["ComponentStatus"] = &openapi3.SchemaRef{Value: &openapi3.Schema{
		Type: "object",
		Properties: openapi3.Schemas{
			"status": &openapi3.SchemaRef{Value: &openapi3.Schema{
				Type:        "string",
				Description: "Component status: ok, warn, or error",
				Enum:        []interface{}{"ok", "warn", "error"},
			}},
			"detail": &openapi3.SchemaRef{Value: &openapi3.Schema{Type: "string", Description: "Optional detail message"}},
		},
	}}
	doc.Components.Schemas["ReadyResponse"] = &openapi3.SchemaRef{Value: &openapi3.Schema{
		Type: "object",
		Properties: openapi3.Schemas{
			"status": &openapi3.SchemaRef{Value: &openapi3.Schema{
				Type:        "string",
				Description: "Readiness status: ready, degraded, or not_ready",
				Enum:        []interface{}{"ready", "degraded", "not_ready"},
			}},
			"timestamp": &openapi3.SchemaRef{Value: &openapi3.Schema{Type: "string", Format: "date-time", Description: "Current timestamp"}},
			"components": &openapi3.SchemaRef{Value: &openapi3.Schema{
				Type:                 "object",
				Description:          "Component status map",
				AdditionalProperties: openapi3.AdditionalProperties{Schema: &openapi3.SchemaRef{Ref: "#/components/schemas/ComponentStatus"}},
			}},
		},
	}}
	doc.Components.Schemas["VersionInfo"] = &openapi3.SchemaRef{Value: &openapi3.Schema{
		Type: "object",
		Properties: openapi3.Schemas{
			"version":   &openapi3.SchemaRef{Value: &openapi3.Schema{Type: "string", Description: "Application version"}},
			"commit":    &openapi3.SchemaRef{Value: &openapi3.Schema{Type: "string", Description: "Git commit hash"}},
			"buildDate": &openapi3.SchemaRef{Value: &openapi3.Schema{Type: "string", Description: "Build date"}},
			"goVersion": &openapi3.SchemaRef{Value: &openapi3.Schema{Type: "string", Description: "Go version used for build"}},
			"uptime":    &openapi3.SchemaRef{Value: &openapi3.Schema{Type: "string", Description: "Service uptime"}},
		},
	}}

	noSecurity := &openapi3.SecurityRequirements{}

	// Paths
	doc.Paths["/health"] = &openapi3.PathItem{
		Get: &openapi3.Operation{
			Tags:        []string{"Health"},
			Summary:     "Health check",
			Description: "Returns the health status of the service",
			Security:    noSecurity,
			Responses: openapi3.Responses{
				"200": {Value: openapi3.NewResponse().WithDescription("Service is healthy").WithContent(
					openapi3.NewContentWithJSONSchemaRef(&openapi3.SchemaRef{Ref: "#/components/schemas/HealthResponse"}),
				)},
			},
		},
	}
	doc.Paths["/ready"] = &openapi3.PathItem{
		Get: &openapi3.Operation{
			Tags:        []string{"Health"},
			Summary:     "Readiness check",
			Description: "Returns the readiness status of the service and its dependencies",
			Security:    noSecurity,
			Responses: openapi3.Responses{
				"200": {Value: openapi3.NewResponse().WithDescription("Service is ready").WithContent(
					openapi3.NewContentWithJSONSchemaRef(&openapi3.SchemaRef{Ref: "#/components/schemas/ReadyResponse"}),
				)},
				"503": {Value: openapi3.NewResponse().WithDescription("Service is not ready")},
			},
		},
	}
	doc.Paths["/version"] = &openapi3.PathItem{
		Get: &openapi3.Operation{
			Tags:        []string{"Health"},
			Summary:     "Version information",
			Description: "Returns build version information",
			Security:    noSecurity,
			Responses: openapi3.Responses{
				"200": {Value: openapi3.NewResponse().WithDescription("Version information").WithContent(
					openapi3.NewContentWithJSONSchemaRef(&openapi3.SchemaRef{Ref: "#/components/schemas/VersionInfo"}),
				)},
			},
		},
	}
}
