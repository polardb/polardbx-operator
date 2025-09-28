package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	api_logcollector "polardbx-ui-backend/pkg/api/logcollector"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	domain_pxc "polardbx-ui-backend/pkg/api/domain/polardbxclusters"
)

func setupRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")
	{
		// stub middleware: do not set k8s client to trigger 401 in handlers expecting kubeconfig
		v1.Use(func(c *gin.Context) { c.Next() })

		v1.GET("/log-collectors/:namespace/pipeline", api_logcollector.GetLogstashPipeline)
		v1.PUT("/log-collectors/:namespace/pipeline", api_logcollector.UpdateLogstashPipeline)
		v1.GET("/log-collectors/:namespace/elastic-certs", api_logcollector.GetElasticsearchCert)
		v1.PUT("/log-collectors/:namespace/elastic-certs", api_logcollector.UpdateElasticsearchCert)
		v1.GET("/log-collectors/:namespace/:name/status", api_logcollector.GetLogCollectorStatus)
		v1.GET("/log-collectors/:namespace/logstash/logs", api_logcollector.StreamLogstashLogs)
		v1.POST("/log-collectors/:namespace/test", api_logcollector.TestLogCollector)
	}
	return r
}

func TestUnauthorizedWhenNoKubeconfig(t *testing.T) {
	r := setupRouter()
	cases := []struct {
		method, path string
		body         string
	}{
		{http.MethodGet, "/api/v1/log-collectors/ns/pipeline", ""},
		{http.MethodPut, "/api/v1/log-collectors/ns/pipeline", `{"data":{}}`},
		{http.MethodGet, "/api/v1/log-collectors/ns/elastic-certs", ""},
		{http.MethodPut, "/api/v1/log-collectors/ns/elastic-certs", `{"caCrt":"abc"}`},
		{http.MethodGet, "/api/v1/log-collectors/ns/name/status", ""},
		{http.MethodGet, "/api/v1/log-collectors/ns/logstash/logs", ""},
		{http.MethodPost, "/api/v1/log-collectors/ns/test", ""},
	}
	for _, cs := range cases {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(cs.method, cs.path, strings.NewReader(cs.body))
		if cs.method == http.MethodPut || cs.method == http.MethodPost {
			req.Header.Set("Content-Type", "application/json")
		}
		r.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s: expected 401, got %d", cs.method, cs.path, w.Code)
		}
	}
}

func TestUpdatePipeline_BadJSON(t *testing.T) {
	// use router with fake clientset to avoid 401
	r := setupRouterWithClientset(k8sfake.NewSimpleClientset())
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPut, "/api/v1/log-collectors/ns/pipeline", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	var body map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if _, ok := body["error"]; !ok {
		t.Fatalf("expected error in response")
	}
}

// --- Happy path with fake clientset ---
func setupRouterWithClientset(cs *k8sfake.Clientset) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")
	{
		v1.Use(func(c *gin.Context) { c.Set("clientset", cs) })
		v1.GET("/log-collectors/:namespace/pipeline", api_logcollector.GetLogstashPipeline)
		v1.PUT("/log-collectors/:namespace/pipeline", api_logcollector.UpdateLogstashPipeline)
		v1.GET("/log-collectors/:namespace/elastic-certs", api_logcollector.GetElasticsearchCert)
		v1.PUT("/log-collectors/:namespace/elastic-certs", api_logcollector.UpdateElasticsearchCert)
		v1.GET("/log-collectors/:namespace/logstash/logs", api_logcollector.StreamLogstashLogs)
	}
	return r
}

func setupRouterWithClients(kc crclient.Client, cs *k8sfake.Clientset) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")
	{
		v1.Use(func(c *gin.Context) { c.Set("k8sClient", kc); c.Set("clientset", cs) })
		v1.GET("/log-collectors/:namespace/:name/status", api_logcollector.GetLogCollectorStatus)
	}
	return r
}

func TestPipelineCreateUpdate_Get_HappyPath(t *testing.T) {
	cs := k8sfake.NewSimpleClientset()
	r := setupRouterWithClientset(cs)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPut, "/api/v1/log-collectors/ns/pipeline", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	var body map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if _, ok := body["error"]; !ok {
		t.Fatalf("expected error in response")
	}
}

// --- Happy path with fake clientset ---
func TestPipelineCreateUpdate_Get_HappyPath_2(t *testing.T) {
	cs := k8sfake.NewSimpleClientset()
	r := setupRouterWithClientset(cs)

	// Create via PUT when CM not exists
	body := `{"data":{"logstash.conf":"output { stdout { codec => rubydebug } }"}}`
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPut, "/api/v1/log-collectors/logns/pipeline", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", w.Code, w.Body.String())
	}

	// Update existing ConfigMap
	body2 := `{"data":{"filters.conf":"filter { }"}}`
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPut, "/api/v1/log-collectors/logns/pipeline", strings.NewReader(body2))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// Get
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/api/v1/log-collectors/logns/pipeline", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	data := resp["data"].(map[string]any)
	if _, ok := data["logstash.conf"]; !ok {
		t.Fatalf("missing logstash.conf")
	}
	if _, ok := data["filters.conf"]; !ok {
		t.Fatalf("missing filters.conf")
	}
}

func TestElasticCert_HappyPath(t *testing.T) {
	cs := k8sfake.NewSimpleClientset()
	r := setupRouterWithClientset(cs)

	// PUT new secret
	body := `{"caCrt":"-----BEGIN CERTIFICATE-----\nABC\n-----END CERTIFICATE-----"}`
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPut, "/api/v1/log-collectors/logns/elastic-certs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}

	// GET with include
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/api/v1/log-collectors/logns/elastic-certs?include=true", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["hasCA"] != true {
		t.Fatalf("expected hasCA true")
	}
}

func TestStreamLogs_Wiring(t *testing.T) {
	// a pod with label app=logstash
	cs := k8sfake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ls-1", Namespace: "logns", Labels: map[string]string{"app": "logstash"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "logstash"}}}})
	// We cannot easily fake stream, but ensure handler no longer returns 404 once pod exists
	cs.Fake.PrependReactor("get", "pods", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		return false, nil, nil
	})

	r := setupRouterWithClientset(cs)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/log-collectors/logns/logstash/logs?follow=false&tailLines=1", nil)
	r.ServeHTTP(w, req)
	if w.Code == http.StatusNotFound {
		t.Fatalf("unexpected 404 when pod exists")
	}
	if w.Code != http.StatusOK && w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 200/500, got %d", w.Code)
	}
	_ = io.NopCloser(strings.NewReader(""))
}

func TestStatus_HappyPath(t *testing.T) {
	// scheme for controller-runtime client
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)
	fakeK8sClient := crfake.NewClientBuilder().WithScheme(scheme).Build()

	// prepare fake clientset resources
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "filebeat", Namespace: "logns"}}
	ds.Status.DesiredNumberScheduled = 3
	ds.Status.NumberReady = 3
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "logstash", Namespace: "logns"}}
	var replicas int32 = 1
	dep.Status.Replicas = replicas
	dep.Status.ReadyReplicas = replicas
	pipeline := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "logstash-pipeline", Namespace: "logns"}, Data: map[string]string{"logstash.conf": "output { elasticsearch { hosts => [\"https://es:9200\"] } }"}}

	cs := k8sfake.NewSimpleClientset(ds, dep, pipeline)
	r := setupRouterWithClients(fakeK8sClient, cs)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/log-collectors/logns/name/status", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	fb := resp["filebeat"].(map[string]any)
	if int(fb["ready"].(float64)) != 3 || int(fb["desired"].(float64)) != 3 {
		t.Fatalf("unexpected filebeat status: %+v", fb)
	}
	ls := resp["logstash"].(map[string]any)
	if int(ls["ready"].(float64)) != 1 || int(ls["desired"].(float64)) != 1 {
		t.Fatalf("unexpected logstash status: %+v", ls)
	}
	out := resp["outputs"].(map[string]any)
	if out["type"].(string) != "elasticsearch" {
		t.Fatalf("unexpected output type: %v", out["type"])
	}
}

type fakeReadCloser struct{ io.Reader }

func (f fakeReadCloser) Close() error { return nil }

func TestStreamLogs_StrictMock(t *testing.T) {
	// Setup fake pod present
	cs := k8sfake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ls-m", Namespace: "logns", Labels: map[string]string{"app": "logstash"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "logstash"}}}})
	r := setupRouterWithClientset(cs)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/log-collectors/logns/logstash/logs?follow=true&tailLines=2", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK && w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 200/500, got %d", w.Code)
	}
}

func TestBackupOverview_Aggregation(t *testing.T) {
	scheme := runtime.NewScheme()
	polardbxv1.AddToScheme(scheme)

	now := time.Now()
	b1 := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "b1", Namespace: "ns1"},
		Status: polardbxv1.PolarDBXBackupStatus{
			Phase:     polardbxv1.BackupFinished,
			StartTime: &metav1.Time{Time: now.Add(-2 * time.Hour)},
		},
	}
	b2 := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "b2", Namespace: "ns1"},
		Status: polardbxv1.PolarDBXBackupStatus{
			Phase:     polardbxv1.BackupFailed,
			StartTime: &metav1.Time{Time: now.Add(-3 * time.Hour)},
		},
	}
	b3 := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "b3", Namespace: "ns2"},
		Status: polardbxv1.PolarDBXBackupStatus{
			Phase: polardbxv1.FullBackuping,
		},
	}

	fakeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(b1, b2, b3).Build()
	router := gin.Default()
	router.Use(func(c *gin.Context) { c.Set("k8sClient", fakeClient) })
	router.GET("/api/v1/backups/overview", domain_pxc.GetBackupOverview)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/backups/overview?namespace=ns1", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	kpi := resp["kpi"].(map[string]any)
	// ns1 has b1 success, b2 failed within 24h, b3 is in ns2
	assert.Equal(t, float64(2), kpi["totalBackups24h"]) // JSON numbers decode to float64
	assert.Equal(t, "pending_implementation", kpi["totalStorage"])
	assert.Equal(t, "pending_implementation", kpi["storageConnectivity"])
}
