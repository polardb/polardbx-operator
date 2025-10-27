package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/api/v1/xstore"

	domain_xs "polardbx-ui-backend/pkg/api/domain/xstores"
	api_monitor "polardbx-ui-backend/pkg/api/monitor"
)

func TestXStoreEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Create scheme and add types
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)

	// Sample XStore for testing
	sampleXStore := &polardbxv1.XStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-xstore",
			Namespace: "default",
		},
		Spec: polardbxv1.XStoreSpec{
			Engine: "galaxy",
			Topology: xstore.Topology{
				NodeSets: []xstore.NodeSet{
					{
						Name:     "node1",
						Role:     "Leader",
						Replicas: 1,
					},
				},
			},
		},
	}

	// A client pre-populated with XStore
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sampleXStore).Build()

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("k8sClient", fakeClient)
		c.Next()
	})

	// Register XStore routes (domain)
	router.GET("/xstores", domain_xs.List)
	router.POST("/xstores", domain_xs.Create)
	router.GET("/xstores/:namespace/:name", domain_xs.Get)
	router.PUT("/xstores/:namespace/:name", domain_xs.Update)
	router.DELETE("/xstores/:namespace/:name", domain_xs.Delete)

	// --- Test ListXStores ---
	t.Run("ListXStores", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/xstores?namespace=default", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var xstores []polardbxv1.XStore
		err := json.Unmarshal(w.Body.Bytes(), &xstores)
		assert.NoError(t, err)
		assert.Len(t, xstores, 1)
		assert.Equal(t, "test-xstore", xstores[0].Name)
		assert.Equal(t, "galaxy", xstores[0].Spec.Engine)
	})

	// --- Test CreateXStore ---
	t.Run("CreateXStore", func(t *testing.T) {
		w := httptest.NewRecorder()
		newXStore := &polardbxv1.XStore{
			ObjectMeta: metav1.ObjectMeta{
				Name: "new-xstore",
			},
			Spec: polardbxv1.XStoreSpec{
				Engine: "galaxy",
				Topology: xstore.Topology{
					NodeSets: []xstore.NodeSet{
						{
							Name:     "node1",
							Role:     "Leader",
							Replicas: 1,
						},
					},
				},
			},
		}
		body, _ := json.Marshal(newXStore)
		req, _ := http.NewRequest(http.MethodPost, "/xstores?namespace=default", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusCreated, w.Code)

		// Verify it was created
		var createdXStore polardbxv1.XStore
		err := fakeClient.Get(context.TODO(), client.ObjectKey{Name: "new-xstore", Namespace: "default"}, &createdXStore)
		assert.NoError(t, err)
		assert.Equal(t, "new-xstore", createdXStore.Name)
		assert.Equal(t, "galaxy", createdXStore.Spec.Engine)
	})

	// --- Test GetXStore ---
	t.Run("GetXStore", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/xstores/default/test-xstore", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var xstore polardbxv1.XStore
		err := json.Unmarshal(w.Body.Bytes(), &xstore)
		assert.NoError(t, err)
		assert.Equal(t, "test-xstore", xstore.Name)
		assert.Equal(t, "galaxy", xstore.Spec.Engine)
	})

	// --- Test DeleteXStore ---
	t.Run("DeleteXStore", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodDelete, "/xstores/default/test-xstore", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// Verify deletion response
		var response map[string]string
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, "xstore deleted", response["message"])
	})
}

func TestMonitorEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Create scheme and add types
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)

	// Sample Monitor for testing
	sampleMonitor := &polardbxv1.PolarDBXMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-monitor",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXMonitorSpec{
			ClusterName: "test-cluster",
		},
	}

	// A client pre-populated with Monitor
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sampleMonitor).Build()

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("k8sClient", fakeClient)
		c.Next()
	})

	// Register Monitor routes
	router.GET("/monitors", api_monitor.List)
	router.POST("/monitors", api_monitor.Create)
	router.GET("/monitors/:namespace/:name", api_monitor.Get)
	router.PUT("/monitors/:namespace/:name", api_monitor.Update)
	router.DELETE("/monitors/:namespace/:name", api_monitor.Delete)

	// --- Test ListMonitors ---
	t.Run("ListMonitors", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/monitors?namespace=default", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var monitors []polardbxv1.PolarDBXMonitor
		err := json.Unmarshal(w.Body.Bytes(), &monitors)
		assert.NoError(t, err)
		assert.Len(t, monitors, 1)
		assert.Equal(t, "test-monitor", monitors[0].Name)
		assert.Equal(t, "test-cluster", monitors[0].Spec.ClusterName)
	})

	// --- Test CreateMonitor ---
	t.Run("CreateMonitor", func(t *testing.T) {
		w := httptest.NewRecorder()
		newMonitor := &polardbxv1.PolarDBXMonitor{
			ObjectMeta: metav1.ObjectMeta{
				Name: "new-monitor",
			},
			Spec: polardbxv1.PolarDBXMonitorSpec{
				ClusterName: "new-cluster",
			},
		}
		body, _ := json.Marshal(newMonitor)
		req, _ := http.NewRequest(http.MethodPost, "/monitors?namespace=default", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusCreated, w.Code)

		// Verify it was created
		var createdMonitor polardbxv1.PolarDBXMonitor
		err := fakeClient.Get(context.TODO(), client.ObjectKey{Name: "new-monitor", Namespace: "default"}, &createdMonitor)
		assert.NoError(t, err)
		assert.Equal(t, "new-monitor", createdMonitor.Name)
		assert.Equal(t, "new-cluster", createdMonitor.Spec.ClusterName)
	})

	// --- Test GetMonitor ---
	t.Run("GetMonitor", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/monitors/default/test-monitor", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var monitor polardbxv1.PolarDBXMonitor
		err := json.Unmarshal(w.Body.Bytes(), &monitor)
		assert.NoError(t, err)
		assert.Equal(t, "test-monitor", monitor.Name)
		assert.Equal(t, "test-cluster", monitor.Spec.ClusterName)
	})

	// --- Test DeleteMonitor ---
	t.Run("DeleteMonitor", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodDelete, "/monitors/default/test-monitor", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// Verify deletion response
		var response map[string]string
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, "monitor deleted successfully", response["message"])
	})
}
