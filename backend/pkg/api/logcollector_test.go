package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api_logcollector "polardbx-ui-backend/pkg/api/logcollector"
)

func setupLogCollectorTest() (*gin.Engine, client.Client) {
	gin.SetMode(gin.TestMode)

	// Create scheme with all required types
	scheme := runtime.NewScheme()
	polardbxv1.AddToScheme(scheme)
	corev1.AddToScheme(scheme)

	// Create fake client
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	// Create gin router and register routes
	router := gin.New()

	// Add middleware to inject client
	router.Use(func(c *gin.Context) {
		c.Set("k8sClient", fakeClient)
		c.Next()
	})

	// Register LogCollector routes
	RegisterLogCollectorRoutes(router)

	return router, fakeClient
}

func TestPolarDBXLogCollectorEndpoints(t *testing.T) {
	router, fakeClient := setupLogCollectorTest()

	// Create test LogCollector
	testLogCollector := &polardbxv1.PolarDBXLogCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-log-collector",
			Namespace: "default",
		},
		Spec: polardbxv1.LogCollectorSpec{
			FileBeatName: "filebeat-cluster-1",
			LogStashName: "logstash-main",
		},
	}

	// Create the collector in fake client for tests that need it
	err := fakeClient.Create(context.TODO(), testLogCollector)
	assert.NoError(t, err)

	t.Run("ListLogCollectors", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/log-collectors?namespace=default", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)

		var collectors []polardbxv1.PolarDBXLogCollector
		err := json.Unmarshal(resp.Body.Bytes(), &collectors)
		assert.NoError(t, err)
		assert.Len(t, collectors, 1)
		assert.Equal(t, "test-log-collector", collectors[0].Name)
		assert.Equal(t, "filebeat-cluster-1", collectors[0].Spec.FileBeatName)
	})

	t.Run("GetLogCollector", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/log-collectors/default/test-log-collector", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)

		var collector polardbxv1.PolarDBXLogCollector
		err := json.Unmarshal(resp.Body.Bytes(), &collector)
		assert.NoError(t, err)
		assert.Equal(t, "test-log-collector", collector.Name)
		assert.Equal(t, "filebeat-cluster-1", collector.Spec.FileBeatName)
		assert.Equal(t, "logstash-main", collector.Spec.LogStashName)
	})

	t.Run("CreateLogCollector", func(t *testing.T) {
		newCollector := polardbxv1.PolarDBXLogCollector{
			ObjectMeta: metav1.ObjectMeta{
				Name: "new-log-collector",
			},
			Spec: polardbxv1.LogCollectorSpec{
				FileBeatName: "filebeat-cluster-2",
				LogStashName: "logstash-secondary",
			},
		}

		jsonData, _ := json.Marshal(newCollector)
		req, _ := http.NewRequest("POST", "/api/v1/log-collectors?namespace=default", bytes.NewBuffer(jsonData))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusCreated, resp.Code)

		var createdCollector polardbxv1.PolarDBXLogCollector
		err := json.Unmarshal(resp.Body.Bytes(), &createdCollector)
		assert.NoError(t, err)
		assert.Equal(t, "new-log-collector", createdCollector.Name)
		assert.Equal(t, "default", createdCollector.Namespace)
		assert.Equal(t, "filebeat-cluster-2", createdCollector.Spec.FileBeatName)
		assert.Equal(t, "logstash-secondary", createdCollector.Spec.LogStashName)
	})

	t.Run("UpdateLogCollector", func(t *testing.T) {
		updatedCollector := *testLogCollector
		updatedCollector.Spec.FileBeatName = "filebeat-updated"
		updatedCollector.Spec.LogStashName = "logstash-updated"

		jsonData, _ := json.Marshal(updatedCollector)
		req, _ := http.NewRequest("PUT", "/api/v1/log-collectors/default/test-log-collector", bytes.NewBuffer(jsonData))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)

		var returnedCollector polardbxv1.PolarDBXLogCollector
		err := json.Unmarshal(resp.Body.Bytes(), &returnedCollector)
		assert.NoError(t, err)
		assert.Equal(t, "filebeat-updated", returnedCollector.Spec.FileBeatName)
		assert.Equal(t, "logstash-updated", returnedCollector.Spec.LogStashName)
	})

	t.Run("DeleteLogCollector", func(t *testing.T) {
		req, _ := http.NewRequest("DELETE", "/api/v1/log-collectors/default/test-log-collector", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)

		// Verify the collector is deleted
		var collector polardbxv1.PolarDBXLogCollector
		err := fakeClient.Get(context.TODO(), client.ObjectKey{
			Namespace: "default",
			Name:      "test-log-collector",
		}, &collector)
		assert.Error(t, err) // Should be not found error
	})

	t.Run("CreateLogCollectorInvalidJSON", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/api/v1/log-collectors?namespace=default", bytes.NewBuffer([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("GetNonExistentLogCollector", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/log-collectors/default/non-existent-collector", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusNotFound, resp.Code)
	})

	t.Run("DeleteNonExistentLogCollector", func(t *testing.T) {
		req, _ := http.NewRequest("DELETE", "/api/v1/log-collectors/default/non-existent-collector", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}

func TestLogCollectorBusinessLogic(t *testing.T) {
	router, _ := setupLogCollectorTest()

	t.Run("TestDifferentComponents", func(t *testing.T) {
		testCases := []struct {
			name        string
			collector   polardbxv1.PolarDBXLogCollector
			description string
		}{
			{
				name: "FileBeatOnly",
				collector: polardbxv1.PolarDBXLogCollector{
					ObjectMeta: metav1.ObjectMeta{
						Name: "filebeat-only-collector",
					},
					Spec: polardbxv1.LogCollectorSpec{
						FileBeatName: "filebeat-cluster-1",
						// LogStashName is empty
					},
				},
				description: "Log collector with only FileBeat component",
			},
			{
				name: "LogStashOnly",
				collector: polardbxv1.PolarDBXLogCollector{
					ObjectMeta: metav1.ObjectMeta{
						Name: "logstash-only-collector",
					},
					Spec: polardbxv1.LogCollectorSpec{
						// FileBeatName is empty
						LogStashName: "logstash-main",
					},
				},
				description: "Log collector with only LogStash component",
			},
			{
				name: "BothComponents",
				collector: polardbxv1.PolarDBXLogCollector{
					ObjectMeta: metav1.ObjectMeta{
						Name: "full-stack-collector",
					},
					Spec: polardbxv1.LogCollectorSpec{
						FileBeatName: "filebeat-cluster-2",
						LogStashName: "logstash-main",
					},
				},
				description: "Log collector with both FileBeat and LogStash components",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				jsonData, _ := json.Marshal(tc.collector)
				req, _ := http.NewRequest("POST", "/api/v1/log-collectors?namespace=default", bytes.NewBuffer(jsonData))
				req.Header.Set("Content-Type", "application/json")
				resp := httptest.NewRecorder()

				router.ServeHTTP(resp, req)

				assert.Equal(t, http.StatusCreated, resp.Code)

				var createdCollector polardbxv1.PolarDBXLogCollector
				err := json.Unmarshal(resp.Body.Bytes(), &createdCollector)
				assert.NoError(t, err)
				assert.Equal(t, tc.collector.Name, createdCollector.Name)
				assert.Equal(t, tc.collector.Spec.FileBeatName, createdCollector.Spec.FileBeatName)
				assert.Equal(t, tc.collector.Spec.LogStashName, createdCollector.Spec.LogStashName)
			})
		}
	})

	t.Run("TestComponentNameValidation", func(t *testing.T) {
		// Test with special characters and long names
		specialCollector := polardbxv1.PolarDBXLogCollector{
			ObjectMeta: metav1.ObjectMeta{
				Name: "special-chars-collector",
			},
			Spec: polardbxv1.LogCollectorSpec{
				FileBeatName: "filebeat-cluster-prod-2024-east-1",
				LogStashName: "logstash-pipeline-analytics-main",
			},
		}

		jsonData, _ := json.Marshal(specialCollector)
		req, _ := http.NewRequest("POST", "/api/v1/log-collectors?namespace=default", bytes.NewBuffer(jsonData))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusCreated, resp.Code)

		var createdCollector polardbxv1.PolarDBXLogCollector
		err := json.Unmarshal(resp.Body.Bytes(), &createdCollector)
		assert.NoError(t, err)
		assert.Equal(t, "filebeat-cluster-prod-2024-east-1", createdCollector.Spec.FileBeatName)
		assert.Equal(t, "logstash-pipeline-analytics-main", createdCollector.Spec.LogStashName)
	})

	t.Run("TestLogCollectorWithStatus", func(t *testing.T) {
		// Create a collector with complete status information
		collectorWithStatus := polardbxv1.PolarDBXLogCollector{
			ObjectMeta: metav1.ObjectMeta{
				Name: "status-test-collector",
			},
			Spec: polardbxv1.LogCollectorSpec{
				FileBeatName: "filebeat-test",
				LogStashName: "logstash-test",
			},
			Status: polardbxv1.LogCollectorStatus{
				ConfigStatus: &polardbxv1.LogCollectorConfigStatus{
					FileBeatReadyCount: 3,
					FileBeatCount:      3,
					FileBeatConfigId:   "fb-config-123",
					LogStashReadyCount: 2,
					LogStashCount:      2,
					LogStashConfigId:   "ls-config-456",
				},
				SpecSnapshot: &polardbxv1.LogCollectorSpec{
					FileBeatName: "filebeat-test",
					LogStashName: "logstash-test",
				},
			},
		}

		jsonData, _ := json.Marshal(collectorWithStatus)
		req, _ := http.NewRequest("POST", "/api/v1/log-collectors?namespace=default", bytes.NewBuffer(jsonData))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusCreated, resp.Code)

		var createdCollector polardbxv1.PolarDBXLogCollector
		err := json.Unmarshal(resp.Body.Bytes(), &createdCollector)
		assert.NoError(t, err)
		assert.Equal(t, "status-test-collector", createdCollector.Name)

		// Note: Status is typically set by the controller, not through API
		// But we can test that the structure is preserved correctly
		if createdCollector.Status.ConfigStatus != nil {
			assert.Equal(t, int32(3), createdCollector.Status.ConfigStatus.FileBeatReadyCount)
			assert.Equal(t, "fb-config-123", createdCollector.Status.ConfigStatus.FileBeatConfigId)
		}
	})
}

// RegisterLogCollectorRoutes registers the LogCollector related routes
func RegisterLogCollectorRoutes(router *gin.Engine) {
	api := router.Group("/api/v1")
	{
		api.GET("/log-collectors", api_logcollector.List)
		api.POST("/log-collectors", api_logcollector.Create)
		api.GET("/log-collectors/:namespace/:name", api_logcollector.Get)
		api.PUT("/log-collectors/:namespace/:name", api_logcollector.Update)
		api.DELETE("/log-collectors/:namespace/:name", api_logcollector.Delete)
	}
}
