package prometheusrule

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func setupTemplateRouter(dynClient dynamic.Interface) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	if dynClient != nil {
		r.Use(withMockDynamicClient(dynClient))
	}
	v1 := r.Group("/api/v1")
	{
		v1.GET("/prometheus-rules/templates", ListTemplates)
		v1.GET("/prometheus-rules/templates/:name", GetTemplate)
		v1.POST("/prometheus-rules/apply", ApplyTemplate)
	}
	return r
}

func writeTemplateFile(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write template file: %v", err)
	}
}

func TestListTemplatesEndpoint(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(alertTemplateDirEnv, dir)

	writeTemplateFile(t, dir, "sample.yaml", `apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: sample
  namespace: polardbx-monitor
  annotations:
    polardbx.com/template-title: Sample Alert
spec:
  groups:
  - name: sample.group
    rules:
    - alert: SampleAlert
      expr: up == 0
      labels:
        severity: warning
`)

	router := setupTemplateRouter(nil)

	recorder := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/prometheus-rules/templates", nil)
	router.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusOK, recorder.Code)

	var payload struct {
		Items []AlertRuleTemplateSummary `json:"items"`
	}
	assert.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &payload))
	assert.Len(t, payload.Items, 1)
	summary := payload.Items[0]
	assert.Equal(t, "sample", summary.Name)
	assert.Equal(t, "Sample Alert", summary.Title)
	assert.Equal(t, "warning", summary.PrimarySeverity)
}

func TestGetTemplateEndpoint(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(alertTemplateDirEnv, dir)

	yaml := `apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: sample
  namespace: polardbx-monitor
  annotations:
    polardbx.com/template-title: Sample Alert
spec:
  groups:
  - name: sample.group
    rules:
    - alert: SampleAlert
      expr: up == 0
      labels:
        severity: warning
`
	writeTemplateFile(t, dir, "sample.yaml", yaml)

	router := setupTemplateRouter(nil)

	recorder := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/prometheus-rules/templates/sample", nil)
	router.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusOK, recorder.Code)

	var detail AlertRuleTemplateDetail
	assert.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &detail))
	assert.Equal(t, "sample", detail.Name)
	assert.Equal(t, yaml, detail.Content)
}

func TestApplyTemplateCreatesResource(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(alertTemplateDirEnv, dir)

	templateYAML := `apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: sample
spec:
  groups:
  - name: sample.group
    rules:
    - alert: SampleAlert
      expr: up == 0
      labels:
        severity: warning
`
	writeTemplateFile(t, dir, "sample.yaml", templateYAML)

	scheme := runtime.NewScheme()
	dynClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		scheme,
		map[schema.GroupVersionResource]string{prometheusRuleGVR: "PrometheusRuleList"},
	)

	router := setupTemplateRouter(dynClient)

	payload := map[string]interface{}{
		"template":  "sample",
		"namespace": "custom-ns",
		"name":      "custom-alert",
	}
	body, _ := json.Marshal(payload)

	recorder := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/prometheus-rules/apply", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusOK, recorder.Code)

	created, err := dynClient.Resource(prometheusRuleGVR).Namespace("custom-ns").Get(
		context.TODO(), "custom-alert", metav1.GetOptions{},
	)
	assert.NoError(t, err)
	assert.Equal(t, "custom-alert", created.GetName())
}

func TestApplyTemplateDryRun(t *testing.T) {
	scheme := runtime.NewScheme()
	dynClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		scheme,
		map[schema.GroupVersionResource]string{prometheusRuleGVR: "PrometheusRuleList"},
	)

	router := setupTemplateRouter(dynClient)

	payload := map[string]interface{}{
		"content": `apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: inline
spec:
  groups:
  - name: inline.group
    rules:
    - alert: InlineAlert
      expr: up == 0
      labels:
        severity: warning
`,
		"namespace": "dryrun",
		"dryRun":    true,
	}
	body, _ := json.Marshal(payload)

	recorder := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/prometheus-rules/apply", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusOK, recorder.Code)

	var resp map[string]interface{}
	assert.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	validation := resp["validation"].(map[string]interface{})
	assert.Equal(t, true, validation["success"])
}

func TestApplyTemplateValidationFailure(t *testing.T) {
	router := setupTemplateRouter(nil)

	payload := map[string]interface{}{
		"content": `apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: invalid
spec:
  groups:
  - name: invalid.group
    rules:
    - alert: InvalidAlert
      # missing expr
`,
	}
	body, _ := json.Marshal(payload)
	recorder := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/prometheus-rules/apply", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestLoadTemplatesFromCharts(t *testing.T) {
	t.Setenv(alertTemplateDirEnv, "")

	templates, dir, err := loadChartTemplates()
	if err != nil {
		t.Fatalf("loadChartTemplates returned error: %v", err)
	}
	if dir == "" {
		t.Fatalf("expected non-empty directory path")
	}
	if len(templates) == 0 {
		t.Fatalf("expected templates from charts")
	}
	for name := range templates {
		t.Logf("template: %s", name)
	}
	if _, ok := templates["polardbx-alert-rules-polardbx-cn"]; !ok {
		t.Fatalf("expected polardbx-alert-rules-polardbx-cn template present")
	}
}
