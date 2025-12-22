package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	domain_logs "polardbx-dashboard-backend/pkg/api/domain/platform/logs/handler"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

func setupLogsRouterWithClientset(cs *k8sfake.Clientset) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")
	{
		v1.Use(func(c *gin.Context) { c.Set("clientset", cs) })
		v1.POST("/logs/query", domain_logs.Query)
	}
	return r
}

func TestLogsQuery_ForbiddenWhenHostNotAllowed(t *testing.T) {
	cs := k8sfake.NewSimpleClientset(
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "logs-config", Namespace: "polardbx-logcollector"}, Data: map[string]string{
			"allowedHosts": "http://allowed.local",
			"defaultHost":  "",
		}},
	)
	r := setupLogsRouterWithClientset(cs)

	body := `{"host":"http://not-allowed.local","index":"logs-*","query":{"match_all":{}}}`
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/logs/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d (%s)", w.Code, w.Body.String())
	}
}

func TestLogsQuery_TimeRangeDSL_BuildsAndPosts(t *testing.T) {
	var captured struct {
		Path string
		Body map[string]any
	}
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured.Path = r.URL.Path
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		captured.Body = b
		_, _ = w.Write([]byte(`{"hits":{"total":{"value":1},"hits":[{"_source":{"msg":"ok"}}]},"aggregations":{}}`))
	}))
	defer es.Close()

	cs := k8sfake.NewSimpleClientset(
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "logs-config", Namespace: "polardbx-logcollector"}, Data: map[string]string{
			"allowedHosts": es.URL,
			"defaultHost":  "",
		}},
	)
	r := setupLogsRouterWithClientset(cs)

	body := `{"host":"` + es.URL + `","index":"logs-test","query":{"match_all":{}},"timeRange":{"field":"@timestamp","from":"2024-05-01T00:00:00Z","to":"2024-05-01T01:00:00Z"},"size":5}`
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/logs/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", w.Code, w.Body.String())
	}

	if captured.Path != "/logs-test/_search" {
		t.Fatalf("unexpected path to ES: %s", captured.Path)
	}
	// Verify range filter is present
	q := captured.Body["query"].(map[string]any)
	b, ok := q["bool"].(map[string]any)
	if !ok {
		t.Fatalf("expected bool query wrapper")
	}
	filters, ok := b["filter"].([]any)
	if !ok || len(filters) == 0 {
		t.Fatalf("expected at least one filter")
	}
	found := false
	for _, f := range filters {
		m, ok := f.(map[string]any)["range"].(map[string]any)
		if ok {
			if ts, ok2 := m["@timestamp"].(map[string]any); ok2 {
				if ts["gte"] == "2024-05-01T00:00:00Z" && ts["lte"] == "2024-05-01T01:00:00Z" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatalf("expected range filter on @timestamp with gte/lte")
	}
}

func TestLogsQuery_FacetsAndHistogram_AggsBuilt(t *testing.T) {
	var captured struct {
		Path string
		Body map[string]any
	}
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured.Path = r.URL.Path
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		captured.Body = b
		_, _ = w.Write([]byte(`{"hits":{"total":{"value":0},"hits":[]},"aggregations":{}}`))
	}))
	defer es.Close()

	cs := k8sfake.NewSimpleClientset(
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "logs-config", Namespace: "polardbx-logcollector"}, Data: map[string]string{
			"allowedHosts": es.URL,
		}},
	)
	r := setupLogsRouterWithClientset(cs)

	body := `{
		"host":"` + es.URL + `",
		"index":"logs-test",
		"query":{"match_all":{}},
		"facets":[{"name":"by_host","field":"host","size":5,"order":"count"}],
		"histogram":{"name":"by_min","field":"@timestamp","interval":"1m","minDocCount":0}
	}`
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/logs/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", w.Code, w.Body.String())
	}
	aggs, ok := captured.Body["aggs"].(map[string]any)
	if !ok {
		t.Fatalf("expected aggs in request body")
	}
	if _, ok := aggs["by_host"].(map[string]any)["terms"]; !ok {
		t.Fatalf("expected terms agg for by_host")
	}
	if _, ok := aggs["by_min"].(map[string]any)["date_histogram"]; !ok {
		t.Fatalf("expected date_histogram agg for by_min")
	}
}

func TestLogsQuery_NormalizeOutput(t *testing.T) {
	// mock ES with aggregations
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"hits": {"total": {"value": 2}, "hits": [
				{"_source": {"msg": "A"}},
				{"_source": {"msg": "B"}}
			]},
			"aggregations": {
				"by_host": {"buckets": [{"key":"h1","doc_count":1},{"key":"h2","doc_count":1}]},
				"by_min": {"buckets": [{"key_as_string":"2024-05-01T00:00:00Z","doc_count":1}]}
			}
		}`))
	}))
	defer es.Close()

	cs := k8sfake.NewSimpleClientset(
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "logs-config", Namespace: "polardbx-logcollector"}, Data: map[string]string{
			"allowedHosts": es.URL,
		}},
	)
	r := setupLogsRouterWithClientset(cs)

	body := `{
		"host":"` + es.URL + `",
		"index":"logs-test",
		"query":{"match_all":{}},
		"facets":[{"name":"by_host","field":"host","size":5,"order":"count"}],
		"histogram":{"name":"by_min","field":"@timestamp","interval":"1m","minDocCount":0},
		"normalize": true
	}`
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/logs/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", w.Code, w.Body.String())
	}
	var out struct {
		Total  int64            `json:"total"`
		Items  []map[string]any `json:"items"`
		Facets map[string][]struct {
			Key   string `json:"key"`
			Count int64  `json:"count"`
		} `json:"facets"`
		Histogram []struct {
			Key   string `json:"key"`
			Count int64  `json:"count"`
		} `json:"histogram"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	if out.Total != 2 || len(out.Items) != 2 {
		t.Fatalf("unexpected items/total: %+v", out)
	}
	if len(out.Facets["by_host"]) != 2 || len(out.Histogram) != 1 {
		t.Fatalf("unexpected facets/histogram: %+v", out)
	}
}
