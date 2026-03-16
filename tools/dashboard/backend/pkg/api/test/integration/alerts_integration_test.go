package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	domain_pxc "polardbx-dashboard-backend/pkg/api/domain/polardbxclusters"
)

// setupAlertsRouter sets up a test router with alerts routes
func setupAlertsRouter(t *testing.T, objs ...runtime.Object) (*gin.Engine, client.Client) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")

	// Build scheme with all required types
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	ctrlClient := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(objs...).
		Build()

	v1.Use(func(c *gin.Context) {
		c.Set("k8sClient", ctrlClient)
		c.Set("k8sDefaultNamespace", "default")
		c.Next()
	})

	// Register alerts route (using the correct route pattern)
	v1.GET("/polardbxclusters/:namespace/:name/alerts-summary", domain_pxc.GetAlertsSummary)

	return r, ctrlClient
}

// ==================== GetAlertsSummary Integration Tests ====================

func TestIntegration_GetAlertsSummary_Empty(t *testing.T) {
	router, _ := setupAlertsRouter(t)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/polardbxclusters/default/test-cluster/alerts-summary", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Contains(t, resp, "namespace")
	assert.Contains(t, resp, "name")
	assert.Contains(t, resp, "critical")
	assert.Contains(t, resp, "warning")
	assert.Contains(t, resp, "info")
	assert.Contains(t, resp, "total")
	assert.Contains(t, resp, "source")

	// Should default to "none" or "events" source
	assert.Contains(t, []string{"none", "events"}, resp["source"].(string))
}

func TestIntegration_GetAlertsSummary_WithEvents(t *testing.T) {
	event1 := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "event-1",
			Namespace: "default",
		},
		InvolvedObject: corev1.ObjectReference{
			Name: "test-cluster-pxc",
			Kind: "PolarDBXCluster",
		},
		Type:    "Warning",
		Reason:  "TestWarning",
		Message: "Test warning message",
	}

	event2 := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "event-2",
			Namespace: "default",
		},
		InvolvedObject: corev1.ObjectReference{
			Name: "test-cluster-dn",
			Kind: "XStore",
		},
		Type:    "Warning",
		Reason:  "TestWarning",
		Message: "Test warning message",
	}

	router, _ := setupAlertsRouter(t, event1, event2)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/polardbxclusters/default/test-cluster/alerts-summary", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "events", resp["source"])
	assert.Greater(t, int(resp["warning"].(float64)), 0)
	assert.Greater(t, int(resp["total"].(float64)), 0)
}

func TestIntegration_GetAlertsSummary_WithAlertManager(t *testing.T) {
	router, _ := setupAlertsRouter(t)

	// Note: In test environment, AlertManager URL won't be reachable
	// but we can test the query parameter handling
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/polardbxclusters/default/test-cluster/alerts-summary?alertmanager=http://alertmanager:9093", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	// Should fallback to events if AlertManager is unreachable
	assert.Contains(t, []string{"none", "events"}, resp["source"].(string))
}

func TestIntegration_GetAlertsSummary_NoMatchingEvents(t *testing.T) {
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "event-1",
			Namespace: "default",
		},
		InvolvedObject: corev1.ObjectReference{
			Name: "other-cluster-pxc", // Different cluster
			Kind: "PolarDBXCluster",
		},
		Type:    "Warning",
		Reason:  "TestWarning",
		Message: "Test warning message",
	}

	router, _ := setupAlertsRouter(t, event)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/polardbxclusters/default/test-cluster/alerts-summary", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	// Should have zero warnings for non-matching events
	assert.Equal(t, float64(0), resp["warning"].(float64))
	assert.Equal(t, float64(0), resp["total"].(float64))
}

func TestIntegration_GetAlertsSummary_WithNormalEvents(t *testing.T) {
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "event-1",
			Namespace: "default",
		},
		InvolvedObject: corev1.ObjectReference{
			Name: "test-cluster-pxc",
			Kind: "PolarDBXCluster",
		},
		Type:    "Normal", // Normal events should not be counted
		Reason:  "TestNormal",
		Message: "Test normal message",
	}

	router, _ := setupAlertsRouter(t, event)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/polardbxclusters/default/test-cluster/alerts-summary", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	// Normal events should not be counted as warnings
	assert.Equal(t, float64(0), resp["warning"].(float64))
}

func TestIntegration_GetAlertsSummary_ResponseStructure(t *testing.T) {
	router, _ := setupAlertsRouter(t)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/polardbxclusters/default/test-cluster/alerts-summary", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)

	// Verify all required fields
	assert.Equal(t, "default", resp["namespace"])
	assert.Equal(t, "test-cluster", resp["name"])
	assert.NotNil(t, resp["critical"])
	assert.NotNil(t, resp["warning"])
	assert.NotNil(t, resp["info"])
	assert.NotNil(t, resp["total"])
	assert.NotNil(t, resp["source"])

	// Verify numeric types
	assert.IsType(t, float64(0), resp["critical"])
	assert.IsType(t, float64(0), resp["warning"])
	assert.IsType(t, float64(0), resp["info"])
	assert.IsType(t, float64(0), resp["total"])
}
