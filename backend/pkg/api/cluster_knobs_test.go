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
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"

	api_clusterknobs "polardbx-ui-backend/pkg/api/clusterknobs"
)

func TestClusterKnobsEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Create scheme and add types
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)

	// Sample ClusterKnobs for testing
	sampleClusterKnobs := &polardbxv1.PolarDBXClusterKnobs{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-knobs",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXClusterKnobsSpec{
			ClusterName: "test-cluster",
			Knobs: map[string]intstr.IntOrString{
				"max_connections":         intstr.FromString("1000"),
				"innodb_buffer_pool_size": intstr.FromString("1G"),
				"query_cache_size":        intstr.FromString("128M"),
				"log_level":               intstr.FromString("INFO"),
			},
		},
		Status: polardbxv1.PolarDBXClusterKnobsStatus{
			Size:        4,
			Version:     1,
			LastUpdated: metav1.Now(),
		},
	}

	// A client pre-populated with ClusterKnobs
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sampleClusterKnobs).Build()

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("k8sClient", fakeClient)
		c.Next()
	})

	// Register ClusterKnobs routes
	router.GET("/cluster-knobs", api_clusterknobs.GetList)
	router.POST("/cluster-knobs", api_clusterknobs.Create)
	router.GET("/cluster-knobs/:namespace/:name", api_clusterknobs.Get)
	router.PUT("/cluster-knobs/:namespace/:name", api_clusterknobs.Update)
	router.DELETE("/cluster-knobs/:namespace/:name", api_clusterknobs.Delete)

	// --- Test GetClusterKnobsList ---
	t.Run("GetClusterKnobsList", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/cluster-knobs?namespace=default", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var knobsList polardbxv1.PolarDBXClusterKnobsList
		err := json.Unmarshal(w.Body.Bytes(), &knobsList)
		assert.NoError(t, err)
		assert.Len(t, knobsList.Items, 1)
		assert.Equal(t, "test-cluster-knobs", knobsList.Items[0].Name)
		assert.Equal(t, "test-cluster", knobsList.Items[0].Spec.ClusterName)
		assert.Len(t, knobsList.Items[0].Spec.Knobs, 4)
		assert.Equal(t, "1000", knobsList.Items[0].Spec.Knobs["max_connections"].StrVal)
		assert.Equal(t, "1G", knobsList.Items[0].Spec.Knobs["innodb_buffer_pool_size"].StrVal)
	})

	// --- Test CreateClusterKnobs ---
	t.Run("CreateClusterKnobs", func(t *testing.T) {
		w := httptest.NewRecorder()
		newClusterKnobs := &polardbxv1.PolarDBXClusterKnobs{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "new-cluster-knobs",
				Namespace: "default",
			},
			Spec: polardbxv1.PolarDBXClusterKnobsSpec{
				ClusterName: "new-cluster",
				Knobs: map[string]intstr.IntOrString{
					"slow_query_log":           intstr.FromString("ON"),
					"long_query_time":          intstr.FromString("2"),
					"innodb_lock_wait_timeout": intstr.FromString("30"),
				},
			},
		}
		body, _ := json.Marshal(newClusterKnobs)
		req, _ := http.NewRequest(http.MethodPost, "/cluster-knobs?namespace=default", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusCreated, w.Code)

		// Verify it was created
		var createdKnobs polardbxv1.PolarDBXClusterKnobs
		err := fakeClient.Get(context.TODO(), client.ObjectKey{Name: "new-cluster-knobs", Namespace: "default"}, &createdKnobs)
		assert.NoError(t, err)
		assert.Equal(t, "new-cluster-knobs", createdKnobs.Name)
		assert.Equal(t, "new-cluster", createdKnobs.Spec.ClusterName)
		assert.Equal(t, "ON", createdKnobs.Spec.Knobs["slow_query_log"].StrVal)
		assert.Equal(t, "2", createdKnobs.Spec.Knobs["long_query_time"].StrVal)
		assert.Equal(t, "30", createdKnobs.Spec.Knobs["innodb_lock_wait_timeout"].StrVal)
	})

	// --- Test GetClusterKnobs ---
	t.Run("GetClusterKnobs", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/cluster-knobs/default/test-cluster-knobs", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var knobs polardbxv1.PolarDBXClusterKnobs
		err := json.Unmarshal(w.Body.Bytes(), &knobs)
		assert.NoError(t, err)
		assert.Equal(t, "test-cluster-knobs", knobs.Name)
		assert.Equal(t, "test-cluster", knobs.Spec.ClusterName)

		// Verify knobs parameters
		assert.Equal(t, "1000", knobs.Spec.Knobs["max_connections"].StrVal)
		assert.Equal(t, "1G", knobs.Spec.Knobs["innodb_buffer_pool_size"].StrVal)
		assert.Equal(t, "128M", knobs.Spec.Knobs["query_cache_size"].StrVal)
		assert.Equal(t, "INFO", knobs.Spec.Knobs["log_level"].StrVal)

		// Verify status
		assert.Equal(t, int32(4), knobs.Status.Size)
		assert.Equal(t, int64(1), knobs.Status.Version)
	})

	// --- Test UpdateClusterKnobs ---
	t.Run("UpdateClusterKnobs", func(t *testing.T) {
		w := httptest.NewRecorder()
		updatedKnobs := sampleClusterKnobs.DeepCopy()
		updatedKnobs.Spec.Knobs["max_connections"] = intstr.FromString("2000")
		updatedKnobs.Spec.Knobs["innodb_buffer_pool_size"] = intstr.FromString("2G")
		updatedKnobs.Spec.Knobs["new_parameter"] = intstr.FromString("new_value")

		body, _ := json.Marshal(updatedKnobs)
		req, _ := http.NewRequest(http.MethodPut, "/cluster-knobs/default/test-cluster-knobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// Verify it was updated
		var knobsInClient polardbxv1.PolarDBXClusterKnobs
		err := fakeClient.Get(context.TODO(), client.ObjectKey{Name: "test-cluster-knobs", Namespace: "default"}, &knobsInClient)
		assert.NoError(t, err)
		assert.Equal(t, "2000", knobsInClient.Spec.Knobs["max_connections"].StrVal)
		assert.Equal(t, "2G", knobsInClient.Spec.Knobs["innodb_buffer_pool_size"].StrVal)
		assert.Equal(t, "new_value", knobsInClient.Spec.Knobs["new_parameter"].StrVal)
	})

	// --- Test DeleteClusterKnobs ---
	t.Run("DeleteClusterKnobs", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodDelete, "/cluster-knobs/default/test-cluster-knobs", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// Verify deletion response
		var response map[string]string
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, "cluster knobs deleted successfully", response["message"])
	})

	// --- Test CreateClusterKnobs with invalid JSON ---
	t.Run("CreateClusterKnobsInvalidJSON", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/cluster-knobs?namespace=default", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Contains(t, response["error"], "invalid cluster knobs data")
	})

	// --- Test GetClusterKnobs for non-existent knobs ---
	t.Run("GetNonExistentClusterKnobs", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/cluster-knobs/default/non-existent-knobs", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)

		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Contains(t, response["error"], "failed to get cluster knobs")
	})

	// --- Test DeleteClusterKnobs for non-existent knobs ---
	t.Run("DeleteNonExistentClusterKnobs", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodDelete, "/cluster-knobs/default/non-existent-knobs", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)

		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Contains(t, response["error"], "failed to delete cluster knobs")
	})
}

func TestClusterKnobsBusinessLogic(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Create scheme and add types
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)

	// Test different parameter types and categories
	t.Run("TestDifferentParameterTypes", func(t *testing.T) {
		testCases := []struct {
			name     string
			knobs    map[string]intstr.IntOrString
			expected bool
		}{
			{
				"Connection Parameters",
				map[string]intstr.IntOrString{
					"max_connections":      intstr.FromString("1000"),
					"max_user_connections": intstr.FromString("500"),
					"connect_timeout":      intstr.FromString("10"),
				},
				true,
			},
			{
				"Memory Parameters",
				map[string]intstr.IntOrString{
					"innodb_buffer_pool_size": intstr.FromString("1G"),
					"query_cache_size":        intstr.FromString("256M"),
					"sort_buffer_size":        intstr.FromString("2M"),
				},
				true,
			},
			{
				"Query Parameters",
				map[string]intstr.IntOrString{
					"slow_query_log":   intstr.FromString("ON"),
					"long_query_time":  intstr.FromString("2"),
					"query_cache_type": intstr.FromString("ON"),
				},
				true,
			},
			{
				"Logging Parameters",
				map[string]intstr.IntOrString{
					"log_level":                     intstr.FromString("INFO"),
					"general_log":                   intstr.FromString("OFF"),
					"log_queries_not_using_indexes": intstr.FromString("ON"),
				},
				true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

				router := gin.New()
				router.Use(func(c *gin.Context) {
					c.Set("k8sClient", fakeClient)
					c.Next()
				})
				router.POST("/cluster-knobs", api_clusterknobs.Create)

				clusterKnobs := &polardbxv1.PolarDBXClusterKnobs{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-" + tc.name,
					},
					Spec: polardbxv1.PolarDBXClusterKnobsSpec{
						ClusterName: "test-cluster",
						Knobs:       tc.knobs,
					},
				}

				body, _ := json.Marshal(clusterKnobs)
				w := httptest.NewRecorder()
				req, _ := http.NewRequest(http.MethodPost, "/cluster-knobs?namespace=default", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				router.ServeHTTP(w, req)

				if tc.expected {
					assert.Equal(t, http.StatusCreated, w.Code)
				}
			})
		}
	})

	// Test parameter validation scenarios
	t.Run("TestParameterValidation", func(t *testing.T) {
		testCases := []struct {
			name        string
			clusterName string
			knobs       map[string]intstr.IntOrString
			expected    bool
		}{
			{
				"Valid cluster name and parameters",
				"valid-cluster",
				map[string]intstr.IntOrString{
					"max_connections": intstr.FromString("1000"),
					"log_level":       intstr.FromString("INFO"),
				},
				true,
			},
			{
				"Empty cluster name",
				"",
				map[string]intstr.IntOrString{
					"max_connections": intstr.FromString("1000"),
				},
				true, // API should still accept empty cluster name
			},
			{
				"Empty knobs map",
				"test-cluster",
				map[string]intstr.IntOrString{},
				true,
			},
			{
				"Mixed parameter types",
				"test-cluster",
				map[string]intstr.IntOrString{
					"max_connections":         intstr.FromInt(1000),
					"innodb_buffer_pool_size": intstr.FromString("1G"),
					"slow_query_log":          intstr.FromString("ON"),
				},
				true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

				router := gin.New()
				router.Use(func(c *gin.Context) {
					c.Set("k8sClient", fakeClient)
					c.Next()
				})
				router.POST("/cluster-knobs", api_clusterknobs.Create)

				clusterKnobs := &polardbxv1.PolarDBXClusterKnobs{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-validation-" + tc.name,
					},
					Spec: polardbxv1.PolarDBXClusterKnobsSpec{
						ClusterName: tc.clusterName,
						Knobs:       tc.knobs,
					},
				}

				body, _ := json.Marshal(clusterKnobs)
				w := httptest.NewRecorder()
				req, _ := http.NewRequest(http.MethodPost, "/cluster-knobs?namespace=default", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				router.ServeHTTP(w, req)

				assert.Equal(t, http.StatusCreated, w.Code)
			})
		}
	})

	// Test complete cluster knobs with all categories
	t.Run("TestCompleteClusterKnobs", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

		router := gin.New()
		router.Use(func(c *gin.Context) {
			c.Set("k8sClient", fakeClient)
			c.Next()
		})
		router.POST("/cluster-knobs", api_clusterknobs.Create)

		clusterKnobs := &polardbxv1.PolarDBXClusterKnobs{
			ObjectMeta: metav1.ObjectMeta{
				Name: "complete-cluster-knobs",
			},
			Spec: polardbxv1.PolarDBXClusterKnobsSpec{
				ClusterName: "production-cluster",
				Knobs: map[string]intstr.IntOrString{
					// Connection parameters
					"max_connections":      intstr.FromString("2000"),
					"max_user_connections": intstr.FromString("1000"),
					"connect_timeout":      intstr.FromString("10"),

					// Memory parameters
					"innodb_buffer_pool_size": intstr.FromString("8G"),
					"query_cache_size":        intstr.FromString("512M"),
					"sort_buffer_size":        intstr.FromString("4M"),
					"read_buffer_size":        intstr.FromString("2M"),

					// Query parameters
					"slow_query_log":     intstr.FromString("ON"),
					"long_query_time":    intstr.FromString("1"),
					"query_cache_type":   intstr.FromString("ON"),
					"max_allowed_packet": intstr.FromString("64M"),

					// Logging parameters
					"log_level":                     intstr.FromString("WARN"),
					"general_log":                   intstr.FromString("OFF"),
					"log_queries_not_using_indexes": intstr.FromString("ON"),
					"binlog_format":                 intstr.FromString("ROW"),

					// InnoDB parameters
					"innodb_lock_wait_timeout":       intstr.FromString("30"),
					"innodb_log_file_size":           intstr.FromString("1G"),
					"innodb_flush_log_at_trx_commit": intstr.FromString("2"),

					// Custom parameters
					"custom_param_1": intstr.FromString("custom_value_1"),
					"custom_param_2": intstr.FromInt(100),
				},
			},
		}

		body, _ := json.Marshal(clusterKnobs)
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/cluster-knobs?namespace=default", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusCreated, w.Code)

		// Verify the response contains the created cluster knobs
		var responseKnobs polardbxv1.PolarDBXClusterKnobs
		err := json.Unmarshal(w.Body.Bytes(), &responseKnobs)
		assert.NoError(t, err)
		assert.Equal(t, "production-cluster", responseKnobs.Spec.ClusterName)
		assert.Len(t, responseKnobs.Spec.Knobs, 20)

		// Verify specific parameter values
		assert.Equal(t, "2000", responseKnobs.Spec.Knobs["max_connections"].StrVal)
		assert.Equal(t, "8G", responseKnobs.Spec.Knobs["innodb_buffer_pool_size"].StrVal)
		assert.Equal(t, "ON", responseKnobs.Spec.Knobs["slow_query_log"].StrVal)
		assert.Equal(t, "WARN", responseKnobs.Spec.Knobs["log_level"].StrVal)
		assert.Equal(t, "30", responseKnobs.Spec.Knobs["innodb_lock_wait_timeout"].StrVal)
		assert.Equal(t, "custom_value_1", responseKnobs.Spec.Knobs["custom_param_1"].StrVal)
		assert.Equal(t, int32(100), responseKnobs.Spec.Knobs["custom_param_2"].IntVal)
	})

	// Test performance tuning scenarios
	t.Run("TestPerformanceTuningScenarios", func(t *testing.T) {
		scenarios := []struct {
			name        string
			scenario    string
			knobs       map[string]intstr.IntOrString
			description string
		}{
			{
				"HighConcurrency",
				"High concurrency optimization",
				map[string]intstr.IntOrString{
					"max_connections":           intstr.FromString("5000"),
					"thread_cache_size":         intstr.FromString("100"),
					"table_open_cache":          intstr.FromString("2000"),
					"innodb_thread_concurrency": intstr.FromString("0"),
				},
				"Optimized for high concurrent connections",
			},
			{
				"LargeDataset",
				"Large dataset optimization",
				map[string]intstr.IntOrString{
					"innodb_buffer_pool_size": intstr.FromString("16G"),
					"innodb_log_file_size":    intstr.FromString("2G"),
					"read_buffer_size":        intstr.FromString("8M"),
					"sort_buffer_size":        intstr.FromString("8M"),
				},
				"Optimized for large dataset processing",
			},
			{
				"QueryOptimization",
				"Query performance optimization",
				map[string]intstr.IntOrString{
					"query_cache_size":    intstr.FromString("1G"),
					"query_cache_type":    intstr.FromString("ON"),
					"tmp_table_size":      intstr.FromString("512M"),
					"max_heap_table_size": intstr.FromString("512M"),
				},
				"Optimized for query performance",
			},
		}

		for _, scenario := range scenarios {
			t.Run(scenario.name, func(t *testing.T) {
				fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

				router := gin.New()
				router.Use(func(c *gin.Context) {
					c.Set("k8sClient", fakeClient)
					c.Next()
				})
				router.POST("/cluster-knobs", api_clusterknobs.Create)

				clusterKnobs := &polardbxv1.PolarDBXClusterKnobs{
					ObjectMeta: metav1.ObjectMeta{
						Name: "perf-" + scenario.name,
						Annotations: map[string]string{
							"scenario":    scenario.scenario,
							"description": scenario.description,
						},
					},
					Spec: polardbxv1.PolarDBXClusterKnobsSpec{
						ClusterName: "perf-test-cluster",
						Knobs:       scenario.knobs,
					},
				}

				body, _ := json.Marshal(clusterKnobs)
				w := httptest.NewRecorder()
				req, _ := http.NewRequest(http.MethodPost, "/cluster-knobs?namespace=default", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				router.ServeHTTP(w, req)

				assert.Equal(t, http.StatusCreated, w.Code)

				// Verify the response contains the performance tuning scenario
				var responseKnobs polardbxv1.PolarDBXClusterKnobs
				err := json.Unmarshal(w.Body.Bytes(), &responseKnobs)
				assert.NoError(t, err)
				assert.Equal(t, "perf-test-cluster", responseKnobs.Spec.ClusterName)
				assert.Equal(t, scenario.scenario, responseKnobs.Annotations["scenario"])
				assert.Equal(t, scenario.description, responseKnobs.Annotations["description"])
				assert.Len(t, responseKnobs.Spec.Knobs, len(scenario.knobs))
			})
		}
	})
}
