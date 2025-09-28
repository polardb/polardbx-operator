package prometheusrule

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func setupTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")
	{
		v1.GET("/prometheus-rules", List)
		v1.GET("/prometheus-rules/:namespace/:name/yaml", GetYAML)
		v1.POST("/prometheus-rules/validate", ValidateRule)
	}
	return r
}

func setupTestRouterWithDyn(dynClient dynamic.Interface) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	// Ensure middleware is registered BEFORE routes so it takes effect
	r.Use(withMockDynamicClient(dynClient))
	v1 := r.Group("/api/v1")
	{
		v1.GET("/prometheus-rules", List)
		v1.GET("/prometheus-rules/:namespace/:name/yaml", GetYAML)
		v1.POST("/prometheus-rules/validate", ValidateRule)
	}
	return r
}

// Mock middleware to inject fake dynamic client
func withMockDynamicClient(dynClient dynamic.Interface) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("dynamic-client", dynClient)
		c.Next()
	}
}

func createMockPrometheusRule(name, namespace string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "monitoring.coreos.com/v1",
			"kind":       "PrometheusRule",
			"metadata": map[string]interface{}{
				"name":              name,
				"namespace":         namespace,
				"creationTimestamp": time.Now().Format(time.RFC3339),
				"labels": map[string]interface{}{
					"prometheus": "kube-prometheus",
					"role":       "alert-rules",
				},
			},
			"spec": map[string]interface{}{
				"groups": []interface{}{
					map[string]interface{}{
						"name":     "test.rules",
						"interval": "30s",
						"rules": []interface{}{
							map[string]interface{}{
								"alert": "HighErrorRate",
								"expr":  "rate(http_requests_total{status=~\"5..\"}[5m]) > 0.1",
								"for":   "5m",
								"labels": map[string]interface{}{
									"severity": "warning",
								},
								"annotations": map[string]interface{}{
									"summary":     "High error rate detected",
									"description": "Error rate is {{ $value | humanizePercentage }}",
								},
							},
							map[string]interface{}{
								"record": "job:http_requests:rate5m",
								"expr":   "rate(http_requests_total[5m])",
							},
						},
					},
				},
			},
		},
	}
}

func TestList_Success(t *testing.T) {
	// Create mock PrometheusRule
	rule1 := createMockPrometheusRule("test-rule-1", "polardbx-monitor")
	rule2 := createMockPrometheusRule("test-rule-2", "polardbx-monitor")

	scheme := runtime.NewScheme()
	dynClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		scheme,
		map[schema.GroupVersionResource]string{prometheusRuleGVR: "PrometheusRuleList"},
		rule1, rule2,
	)

	router := setupTestRouterWithDyn(dynClient)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/prometheus-rules?namespace=polardbx-monitor", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var rules []PrometheusRule
	err := json.Unmarshal(w.Body.Bytes(), &rules)
	assert.NoError(t, err)
	assert.Len(t, rules, 2)

	// Verify structure
	rule := rules[0]
	assert.Equal(t, "monitoring.coreos.com/v1", rule.APIVersion)
	assert.Equal(t, "PrometheusRule", rule.Kind)
	assert.NotEmpty(t, rule.Metadata.Name)
	assert.Equal(t, "polardbx-monitor", rule.Metadata.Namespace)
	assert.Len(t, rule.Spec.Groups, 1)

	group := rule.Spec.Groups[0]
	assert.Equal(t, "test.rules", group.Name)
	assert.Equal(t, "30s", group.Interval)
	assert.Len(t, group.Rules, 2)

	// Verify alert rule
	alertRule := group.Rules[0]
	assert.Equal(t, "HighErrorRate", alertRule.Alert)
	assert.Equal(t, "rate(http_requests_total{status=~\"5..\"}[5m]) > 0.1", alertRule.Expr)
	assert.Equal(t, "5m", alertRule.For)
	assert.Equal(t, "warning", alertRule.Labels["severity"])

	// Verify record rule
	recordRule := group.Rules[1]
	assert.Equal(t, "job:http_requests:rate5m", recordRule.Record)
	assert.Equal(t, "rate(http_requests_total[5m])", recordRule.Expr)
}

func TestList_EmptyNamespace(t *testing.T) {
	scheme := runtime.NewScheme()
	dynClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		scheme,
		map[schema.GroupVersionResource]string{prometheusRuleGVR: "PrometheusRuleList"},
	)

	router := setupTestRouterWithDyn(dynClient)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/prometheus-rules?namespace=empty-namespace", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var rules []PrometheusRule
	err := json.Unmarshal(w.Body.Bytes(), &rules)
	assert.NoError(t, err)
	assert.Empty(t, rules)
}

func TestGetYAML_Success(t *testing.T) {
	rule := createMockPrometheusRule("test-rule", "polardbx-monitor")

	scheme := runtime.NewScheme()
	dynClient := dynamicfake.NewSimpleDynamicClient(scheme, rule)

	router := setupTestRouterWithDyn(dynClient)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/prometheus-rules/polardbx-monitor/test-rule/yaml", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "text/plain", w.Header().Get("Content-Type"))

	yamlContent := w.Body.String()
	assert.Contains(t, yamlContent, "apiVersion: monitoring.coreos.com/v1")
	assert.Contains(t, yamlContent, "kind: PrometheusRule")
	assert.Contains(t, yamlContent, "name: test-rule")
	assert.Contains(t, yamlContent, "namespace: polardbx-monitor")
}

func TestGetYAML_NotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	dynClient := dynamicfake.NewSimpleDynamicClient(scheme)

	router := setupTestRouterWithDyn(dynClient)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/prometheus-rules/polardbx-monitor/nonexistent/yaml", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGetYAML_MissingParams(t *testing.T) {
	router := setupTestRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/prometheus-rules//test-rule/yaml", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Contains(t, response["error"], "namespace and name are required")
}

func TestValidateRule_ValidRule(t *testing.T) {
	router := setupTestRouter()

	validYAML := `
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: test-rule
  namespace: monitoring
spec:
  groups:
  - name: test.rules
    rules:
    - alert: HighCPU
      expr: cpu_usage > 0.8
      for: 5m
      labels:
        severity: warning
      annotations:
        summary: High CPU usage
    - record: job:cpu_usage:avg
      expr: avg(cpu_usage) by (job)
`

	payload := map[string]string{
		"yaml": validYAML,
	}

	body, _ := json.Marshal(payload)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/prometheus-rules/validate", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.True(t, response["success"].(bool))
	assert.Equal(t, "Validation passed", response["message"])
}

func TestValidateRule_InvalidYAML(t *testing.T) {
	router := setupTestRouter()

	invalidYAML := `
invalid: yaml: content: [
`

	payload := map[string]string{
		"yaml": invalidYAML,
	}

	body, _ := json.Marshal(payload)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/prometheus-rules/validate", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.False(t, response["success"].(bool))
	assert.Contains(t, response["message"], "Invalid YAML format")
}

func TestValidateRule_MissingRequiredFields(t *testing.T) {
	router := setupTestRouter()

	invalidRule := `
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: test-rule
spec:
  groups:
  - name: test.rules
    rules:
    - alert: MissingExpr
      # Missing expr field
      for: 5m
    - expr: some_metric > 0
      # Missing both alert and record
`

	payload := map[string]string{
		"yaml": invalidRule,
	}

	body, _ := json.Marshal(payload)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/prometheus-rules/validate", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.False(t, response["success"].(bool))
	assert.Equal(t, "Validation failed", response["message"])

	details := response["details"].([]interface{})
	assert.True(t, len(details) >= 2) // Should have multiple validation errors

	// Check specific errors
	errorMessages := make([]string, 0)
	for _, detail := range details {
		detailMap := detail.(map[string]interface{})
		if detailMap["level"] == "error" {
			errorMessages = append(errorMessages, detailMap["message"].(string))
		}
	}

	assert.Contains(t, errorMessages[0], "expr is required")
	assert.Contains(t, errorMessages[1], "either 'alert' or 'record' must be specified")
}

func TestValidateRule_WrongAPIVersion(t *testing.T) {
	router := setupTestRouter()

	wrongVersionRule := `
apiVersion: v1
kind: PrometheusRule
metadata:
  name: test-rule
spec:
  groups:
  - name: test.rules
    rules:
    - alert: TestAlert
      expr: up == 0
`

	payload := map[string]string{
		"yaml": wrongVersionRule,
	}

	body, _ := json.Marshal(payload)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/prometheus-rules/validate", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.False(t, response["success"].(bool))

	details := response["details"].([]interface{})
	found := false
	for _, detail := range details {
		detailMap := detail.(map[string]interface{})
		if detailMap["level"] == "error" {
			if detailMap["message"].(string) == "apiVersion should be 'monitoring.coreos.com/v1'" {
				found = true
				break
			}
		}
	}
	assert.True(t, found, "Should validate apiVersion")
}

func TestValidateRule_EmptyPayload(t *testing.T) {
	router := setupTestRouter()

	payload := map[string]string{
		"yaml": "",
	}

	body, _ := json.Marshal(payload)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/prometheus-rules/validate", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Contains(t, response["error"], "YAML content is required")
}

func TestIsValidPromQLBasic(t *testing.T) {
	tests := []struct {
		expr     string
		expected bool
	}{
		{"up", true},
		{"rate(http_requests_total[5m])", true},
		{"cpu_usage > 0.8", true},
		{"", false},
		{"   ", false},
		{"((invalid", false},
		{"invalid))", false},
		{"[[invalid", false},
		{"invalid]]", false},
		{"{{invalid", false},
		{"invalid}}", false},
	}

	for _, test := range tests {
		result := isValidPromQLBasic(test.expr)
		assert.Equal(t, test.expected, result, "Expression: %s", test.expr)
	}
}

// Debug test to inspect dynamic fake List error details
func Test_debug_ListDirect(t *testing.T) {
	rule1 := createMockPrometheusRule("dbg-rule-1", "polardbx-monitor")
	rule2 := createMockPrometheusRule("dbg-rule-2", "polardbx-monitor")

	scheme := runtime.NewScheme()
	dynClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		scheme,
		map[schema.GroupVersionResource]string{prometheusRuleGVR: "PrometheusRuleList"},
		rule1, rule2,
	)

	// Directly call List on the fake client to see if it works
	list, err := dynClient.Resource(prometheusRuleGVR).Namespace("polardbx-monitor").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("direct list failed: %v", err)
	}
	if len(list.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(list.Items))
	}
}
