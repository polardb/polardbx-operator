package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	api_logs "polardbx-ui-backend/pkg/api/logs"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

func setupPresetsRouterWithClientset(cs *k8sfake.Clientset) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")
	{
		v1.Use(func(c *gin.Context) { c.Set("clientset", cs) })
		v1.GET("/logs/presets", api_logs.Presets)
		v1.GET("/logs/presets/:pattern", api_logs.PresetByPattern)
	}
	return r
}

func TestLogsPresets_List_Defaults(t *testing.T) {
	// No ConfigMap provided -> should return defaults
	cs := k8sfake.NewSimpleClientset()
	r := setupPresetsRouterWithClientset(cs)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/logs/presets", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", w.Code, w.Body.String())
	}
	var resp struct {
		Total int `json:"total"`
		Items []struct {
			IndexPattern string   `json:"indexPattern"`
			Facets       []string `json:"facets"`
			Histogram    struct {
				Field     string   `json:"field"`
				Intervals []string `json:"intervals"`
			} `json:"histogram"`
		} `json:"items"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Total <= 0 || len(resp.Items) == 0 {
		t.Fatalf("expected non-empty presets, got: %+v", resp)
	}
}

func TestLogsPresets_Get_OverrideMergeDefaults(t *testing.T) {
	// Provide presets.json overriding one pattern; histogram fields missing should be defaulted
	presetsJSON := `{
		"custom-*": {
			"indexPattern": "custom-*",
			"facets": ["a.keyword", "b"],
			"histogram": { "field": "", "intervals": [] }
		}
	}`
	cs := k8sfake.NewSimpleClientset(
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "logs-config", Namespace: "polardbx-logcollector"}, Data: map[string]string{
			"presets.json": presetsJSON,
		}},
	)
	r := setupPresetsRouterWithClientset(cs)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/logs/presets/custom-*", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", w.Code, w.Body.String())
	}
	var p struct {
		IndexPattern string   `json:"indexPattern"`
		Facets       []string `json:"facets"`
		Histogram    struct {
			Field     string   `json:"field"`
			Intervals []string `json:"intervals"`
		} `json:"histogram"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &p)
	if p.IndexPattern != "custom-*" || len(p.Facets) != 2 {
		t.Fatalf("unexpected preset payload: %+v", p)
	}
	if p.Histogram.Field != "@timestamp" || len(p.Histogram.Intervals) == 0 {
		t.Fatalf("histogram defaults not applied: %+v", p.Histogram)
	}
}

func TestLogsPresets_Get_NotFound(t *testing.T) {
	cs := k8sfake.NewSimpleClientset(
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "logs-config", Namespace: "polardbx-logcollector"}, Data: map[string]string{
			"presets.json": `{"other-*": {"indexPattern":"other-*","facets":[],"histogram":{"field":"@timestamp","intervals":["1m"]}}}`,
		}},
	)
	r := setupPresetsRouterWithClientset(cs)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/logs/presets/not-exist-*", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d (%s)", w.Code, w.Body.String())
	}
}
