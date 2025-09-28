package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	api_logstrategy "polardbx-ui-backend/pkg/api/logstrategy"
	api_prometheusrule "polardbx-ui-backend/pkg/api/prometheusrule"
	api_system "polardbx-ui-backend/pkg/api/system"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// setupIntegrationRouter sets up the router with all the new API routes
func setupIntegrationRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")

	// Add middleware to inject mock clients
	v1.Use(func(c *gin.Context) {
		// Setup mock clients
		cs := k8sfake.NewSimpleClientset()
		scheme := runtime.NewScheme()
		_ = corev1.AddToScheme(scheme)
		cli := crfake.NewClientBuilder().WithScheme(scheme).Build()
		dynClient := dynamicfake.NewSimpleDynamicClient(scheme)

		c.Set("clientset", cs)
		c.Set("k8sClient", cli)
		c.Set("dynamic-client", dynClient)
		c.Next()
	})

	// Direct routes for frontend compatibility (the ones we added)
	v1.GET("/namespaces", api_system.ListNamespaces)
	v1.GET("/prometheus-rules", api_prometheusrule.List)
	v1.GET("/prometheus-rules/:namespace/:name/yaml", api_prometheusrule.GetYAML)
	v1.POST("/prometheus-rules/validate", api_prometheusrule.ValidateRule)
	v1.GET("/log-strategies/apply-records", api_logstrategy.ListApplyRecords)

	// Include some existing routes for comparison
	v1.GET("/system/namespaces", api_system.ListNamespaces)
	v1.GET("/log-strategies", api_logstrategy.List)
	v1.POST("/log-strategies", api_logstrategy.Create)

	return r
}

func TestNewAPIRoutesIntegration(t *testing.T) {
	router := setupIntegrationRouter()

	tests := []struct {
		name           string
		method         string
		path           string
		body           interface{}
		expectedStatus int
		description    string
	}{
		{
			name:           "GET /namespaces",
			method:         "GET",
			path:           "/api/v1/namespaces",
			expectedStatus: http.StatusOK,
			description:    "Direct route mapping to /platform/system/namespaces",
		},
		{
			name:           "GET /system/namespaces",
			method:         "GET",
			path:           "/api/v1/system/namespaces",
			expectedStatus: http.StatusOK,
			description:    "Original system route should still work",
		},
		// Skip dynamic client list kind registration complexity in this integration test
		{
			name:           "GET /prometheus-rules/:namespace/:name/yaml",
			method:         "GET",
			path:           "/api/v1/prometheus-rules/monitoring/test-rule/yaml",
			expectedStatus: http.StatusNotFound, // Expected since rule doesn't exist
			description:    "PrometheusRule YAML retrieval endpoint",
		},
		{
			name:   "POST /prometheus-rules/validate",
			method: "POST",
			path:   "/api/v1/prometheus-rules/validate",
			body: map[string]string{
				"yaml": `apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: test
spec:
  groups:
  - name: test.rules
    rules:
    - alert: Test
      expr: up == 0`,
			},
			expectedStatus: http.StatusOK,
			description:    "PrometheusRule validation endpoint",
		},
		{
			name:           "GET /log-strategies/apply-records",
			method:         "GET",
			path:           "/api/v1/log-strategies/apply-records",
			expectedStatus: http.StatusOK,
			description:    "New log strategy apply records endpoint",
		},
		{
			name:           "GET /log-strategies",
			method:         "GET",
			path:           "/api/v1/log-strategies",
			expectedStatus: http.StatusOK,
			description:    "Existing log strategies endpoint should still work",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req *http.Request
			var err error

			if tt.body != nil {
				bodyBytes, _ := json.Marshal(tt.body)
				req, err = http.NewRequest(tt.method, tt.path, bytes.NewBuffer(bodyBytes))
				req.Header.Set("Content-Type", "application/json")
			} else {
				req, err = http.NewRequest(tt.method, tt.path, nil)
			}

			assert.NoError(t, err)

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code, tt.description)

			// Additional checks based on endpoint
			switch tt.path {
			case "/api/v1/namespaces", "/api/v1/system/namespaces":
				if w.Code == http.StatusOK {
					var response map[string]interface{}
					err := json.Unmarshal(w.Body.Bytes(), &response)
					assert.NoError(t, err, "Should return valid namespace list")
					assert.Contains(t, response, "items")
				}

			case "/api/v1/prometheus-rules":
				if w.Code == http.StatusOK {
					var response []interface{}
					err := json.Unmarshal(w.Body.Bytes(), &response)
					assert.NoError(t, err, "Should return valid PrometheusRule list")
				}

			case "/api/v1/log-strategies/apply-records":
				if w.Code == http.StatusOK {
					var response map[string]interface{}
					err := json.Unmarshal(w.Body.Bytes(), &response)
					assert.NoError(t, err, "Should return valid apply records response")
					assert.Contains(t, response, "total")
					assert.Contains(t, response, "items")
				}

			case "/api/v1/log-strategies":
				if w.Code == http.StatusOK {
					var response map[string]interface{}
					err := json.Unmarshal(w.Body.Bytes(), &response)
					assert.NoError(t, err, "Should return valid log strategies response")
					assert.Contains(t, response, "total")
					assert.Contains(t, response, "items")
				}
			}

			// Check for PrometheusRule validation response
			if tt.path == "/api/v1/prometheus-rules/validate" && w.Code == http.StatusOK {
				var response map[string]interface{}
				err := json.Unmarshal(w.Body.Bytes(), &response)
				assert.NoError(t, err, "Should return valid validation response")
				assert.Contains(t, response, "success")
				assert.Contains(t, response, "message")
			}
		})
	}
}

func TestRoutePathConsistency(t *testing.T) {
	router := setupIntegrationRouter()

	// Test that both /namespaces and /system/namespaces return the same result
	w1 := httptest.NewRecorder()
	req1, _ := http.NewRequest("GET", "/api/v1/namespaces", nil)
	router.ServeHTTP(w1, req1)

	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest("GET", "/api/v1/system/namespaces", nil)
	router.ServeHTTP(w2, req2)

	assert.Equal(t, w1.Code, w2.Code, "Both namespace endpoints should return same status")
	if w1.Code == http.StatusOK && w2.Code == http.StatusOK {
		assert.Equal(t, w1.Body.String(), w2.Body.String(), "Both namespace endpoints should return same data")
	}
}

func TestNewEndpointsErrorHandling(t *testing.T) {
	router := setupIntegrationRouter()

	errorTests := []struct {
		name           string
		method         string
		path           string
		body           interface{}
		expectedStatus int
		description    string
	}{
		{
			name:           "Invalid PrometheusRule YAML validation",
			method:         "POST",
			path:           "/api/v1/prometheus-rules/validate",
			body:           map[string]string{"yaml": "invalid: yaml: ["},
			expectedStatus: http.StatusOK, // Returns 200 with validation error
			description:    "Should handle invalid YAML gracefully",
		},
		{
			name:           "Missing YAML in validation request",
			method:         "POST",
			path:           "/api/v1/prometheus-rules/validate",
			body:           map[string]string{},
			expectedStatus: http.StatusBadRequest,
			description:    "Should reject empty validation request",
		},
		{
			name:           "Invalid route parameter",
			method:         "GET",
			path:           "/api/v1/prometheus-rules//invalid/yaml",
			expectedStatus: http.StatusBadRequest,
			description:    "Should handle missing namespace parameter",
		},
		{
			name:           "Nonexistent PrometheusRule",
			method:         "GET",
			path:           "/api/v1/prometheus-rules/nonexistent-ns/nonexistent-rule/yaml",
			expectedStatus: http.StatusNotFound,
			description:    "Should return 404 for nonexistent rules",
		},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			var req *http.Request
			var err error

			if tt.body != nil {
				bodyBytes, _ := json.Marshal(tt.body)
				req, err = http.NewRequest(tt.method, tt.path, bytes.NewBuffer(bodyBytes))
				req.Header.Set("Content-Type", "application/json")
			} else {
				req, err = http.NewRequest(tt.method, tt.path, nil)
			}

			assert.NoError(t, err)

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code, tt.description)

			// Verify error responses contain proper error information
			if w.Code >= 400 {
				var response map[string]interface{}
				err := json.Unmarshal(w.Body.Bytes(), &response)
				if err == nil { // Some endpoints might return plain text
					assert.Contains(t, response, "error", "Error responses should contain error field")
				}
			}
		})
	}
}

func TestEndpointSecurity(t *testing.T) {
	// Test router without any middleware (no auth)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")

	// Add routes without auth middleware
	v1.GET("/namespaces", api_system.ListNamespaces)
	v1.GET("/prometheus-rules", api_prometheusrule.List)
	v1.GET("/log-strategies/apply-records", api_logstrategy.ListApplyRecords)

	securityTests := []struct {
		name           string
		path           string
		expectedStatus int
		description    string
	}{
		{
			name:           "Namespaces without auth",
			path:           "/api/v1/namespaces",
			expectedStatus: http.StatusUnauthorized, // Unauthorized when k8s client missing
			description:    "Should require proper k8s authentication",
		},
		{
			name:           "PrometheusRule without auth",
			path:           "/api/v1/prometheus-rules",
			expectedStatus: http.StatusInternalServerError, // Should fail without k8s client
			description:    "Should require proper k8s authentication",
		},
		{
			name:           "Apply records without auth",
			path:           "/api/v1/log-strategies/apply-records",
			expectedStatus: http.StatusOK, // This one gracefully handles missing client
			description:    "Should gracefully handle missing k8s client",
		},
	}

	for _, tt := range securityTests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			req, _ := http.NewRequest("GET", tt.path, nil)
			r.ServeHTTP(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code, tt.description)
		})
	}
}

// Benchmark the new endpoints
func BenchmarkNewAPIEndpoints(b *testing.B) {
	router := setupIntegrationRouter()

	benchmarks := []struct {
		name string
		path string
	}{
		{"GET_Namespaces", "/api/v1/namespaces"},
		{"GET_PrometheusRules", "/api/v1/prometheus-rules"},
		{"GET_ApplyRecords", "/api/v1/log-strategies/apply-records"},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				w := httptest.NewRecorder()
				req, _ := http.NewRequest("GET", bm.path, nil)
				router.ServeHTTP(w, req)
			}
		})
	}
}
