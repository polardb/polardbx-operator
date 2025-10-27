package logstrategy

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func setupTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")
	{
		v1.GET("/log-strategies", List)
		v1.POST("/log-strategies", Create)
		v1.GET("/log-strategies/apply-records", ListApplyRecords)
		v1.POST("/log-strategies/:name/apply", Apply)
		v1.POST("/log-strategies/precheck", Precheck)
		v1.POST("/log-strategies/test-connection", TestConnection)
	}
	return r
}

// setupTestRouterWithClients attaches mock clients at the group level to ensure availability in handlers.
func setupTestRouterWithClients(cs *k8sfake.Clientset, cli client.Client) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")
	v1.Use(withMockK8sClients(cs, cli))
	{
		v1.GET("/log-strategies", List)
		v1.POST("/log-strategies", Create)
		v1.GET("/log-strategies/apply-records", ListApplyRecords)
		v1.POST("/log-strategies/:name/apply", Apply)
		v1.POST("/log-strategies/precheck", Precheck)
		v1.POST("/log-strategies/test-connection", TestConnection)
	}
	return r
}

// Mock middleware to inject fake Kubernetes clients
func withMockK8sClients(cs *k8sfake.Clientset, cli client.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("clientset", cs)
		c.Set("k8sClient", cli)
		c.Next()
	}
}

func TestListApplyRecords_EmptyConfigMap(t *testing.T) {
	// Setup fake clients with no ConfigMap
	cs := k8sfake.NewSimpleClientset()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	cli := crfake.NewClientBuilder().WithScheme(scheme).Build()

	router := setupTestRouterWithClients(cs, cli)

	// Test empty records
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/log-strategies/apply-records", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, float64(0), response["total"])
	assert.Empty(t, response["items"])
}

func TestListApplyRecords_WithData(t *testing.T) {
	// Setup fake clients with ConfigMap containing records
	records := []ApplyRecord{
		{
			ID:           "20240101-120000-001",
			StrategyID:   "test-strategy",
			StrategyName: "test-strategy",
			AppliedAt:    time.Now(),
			Status:       "success",
			Message:      "Applied successfully",
			Targets:      []string{"default/test-cluster"},
		},
		{
			ID:           "20240101-110000-002",
			StrategyID:   "another-strategy",
			StrategyName: "another-strategy",
			AppliedAt:    time.Now().Add(-time.Hour),
			Status:       "failed",
			Message:      "Connection failed",
			Targets:      []string{"prod/prod-cluster"},
		},
	}

	recordsJSON, _ := json.Marshal(records)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      recordsCMName,
			Namespace: cmNamespace,
		},
		Data: map[string]string{
			recordsKey: string(recordsJSON),
		},
	}

	cs := k8sfake.NewSimpleClientset(cm)
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	cli := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()

	router := setupTestRouterWithClients(cs, cli)

	// Test with data
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/log-strategies/apply-records", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, float64(2), response["total"])

	items := response["items"].([]interface{})
	assert.Len(t, items, 2)

	// Verify records are sorted by appliedAt descending (newest first)
	firstRecord := items[0].(map[string]interface{})
	secondRecord := items[1].(map[string]interface{})
	assert.Equal(t, "test-strategy", firstRecord["strategyName"])
	assert.Equal(t, "another-strategy", secondRecord["strategyName"])
}

func TestAddApplyRecord(t *testing.T) {
	// Setup fake clients
	cs := k8sfake.NewSimpleClientset()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	cli := crfake.NewClientBuilder().WithScheme(scheme).Build()

	// Setup gin context
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	c, _ := gin.CreateTestContext(w)
	c.Request = req
	c.Set("clientset", cs)
	c.Set("k8sClient", cli)

	// Call addApplyRecord
	addApplyRecord(c, "test-strategy", "success", "Test message", []string{"ns/cluster"})

	// Verify ConfigMap was created with the record
	cm, err := cs.CoreV1().ConfigMaps(cmNamespace).Get(c.Request.Context(), recordsCMName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotEmpty(t, cm.Data[recordsKey])

	var records []ApplyRecord
	err = json.Unmarshal([]byte(cm.Data[recordsKey]), &records)
	assert.NoError(t, err)
	assert.Len(t, records, 1)

	record := records[0]
	assert.Equal(t, "test-strategy", record.StrategyName)
	assert.Equal(t, "success", record.Status)
	assert.Equal(t, "Test message", record.Message)
	assert.Equal(t, []string{"ns/cluster"}, record.Targets)
	assert.NotEmpty(t, record.ID)
}

func TestGenerateRecordID(t *testing.T) {
	id1 := generateRecordID()
	time.Sleep(time.Millisecond) // Ensure different timestamp
	id2 := generateRecordID()

	assert.NotEqual(t, id1, id2)
	assert.Len(t, id1, 19) // Format: 20060102-150405-000 (19 chars with hyphens)
	assert.Len(t, id2, 19)
}

func TestTestConnection_Success(t *testing.T) {
	// Mock HTTP server for Elasticsearch
	mockES := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"name":"test-es","version":{"number":"7.10.0"}}`))
	}))
	defer mockES.Close()

	router := setupTestRouter()

	payload := map[string]interface{}{
		"hosts":    []string{mockES.URL},
		"username": "",
		"password": "",
	}

	body, _ := json.Marshal(payload)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/log-strategies/test-connection", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.True(t, response["ok"].(bool))
}

func TestTestConnection_InvalidPayload(t *testing.T) {
	router := setupTestRouter()

	// Test with empty hosts
	payload := map[string]interface{}{
		"hosts": []string{},
	}

	body, _ := json.Marshal(payload)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/log-strategies/test-connection", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Contains(t, response["error"], "hosts required")
}

func TestPrecheck_ValidStrategy(t *testing.T) {
	// Setup fake clients with a test cluster
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	// Create a mock PolarDBX cluster (simplified)
	cluster := &corev1.ConfigMap{ // Using ConfigMap as a placeholder since we don't have PolarDBX CRD in test
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	cs := k8sfake.NewSimpleClientset(cluster)
	cli := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	router := setupTestRouterWithClients(cs, cli)

	strategy := Strategy{
		Name:        "test-strategy",
		ClusterNS:   "default",
		ClusterName: "test-cluster",
		Output: struct {
			Type     string `json:"type"`
			Hosts    string `json:"hosts,omitempty"`
			AuthType string `json:"authType,omitempty"`
			Username string `json:"username,omitempty"`
			Password string `json:"password,omitempty"`
			UseTLS   bool   `json:"useTLS,omitempty"`
			CACrt    string `json:"caCrt,omitempty"`
		}{
			Type:  "elasticsearch",
			Hosts: "http://localhost:9200",
		},
	}

	body, _ := json.Marshal(strategy)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/log-strategies/precheck", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)

	checks := response["checks"].(map[string]interface{})
	assert.True(t, checks["esHostsValid"].(bool))
	assert.True(t, checks["authValid"].(bool))
	assert.True(t, checks["tlsCrtValid"].(bool))
}

func TestPrecheck_InvalidStrategy(t *testing.T) {
	router := setupTestRouter()

	// Test with invalid strategy (missing required fields)
	strategy := Strategy{
		Name: "test-strategy",
		// Missing ClusterName
	}

	body, _ := json.Marshal(strategy)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/log-strategies/precheck", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Contains(t, response["error"], "name and clusterName required")
}
