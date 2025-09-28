package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/api/v1/systemtask"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api_systemtask "polardbx-ui-backend/pkg/api/systemtask"
)

func setupSystemTaskTest() (*gin.Engine, client.Client) {
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

	// Register SystemTask routes
	RegisterSystemTaskRoutes(router)

	return router, fakeClient
}

func TestSystemTaskEndpoints(t *testing.T) {
	router, fakeClient := setupSystemTaskTest()

	// Create test SystemTask
	testSystemTask := &polardbxv1.SystemTask{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-system-task",
			Namespace: "default",
		},
		Spec: polardbxv1.SystemTaskSpec{
			TaskType:   systemtask.BalanceResource,
			CnReplicas: 3,
			CnResources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("2Gi"),
				},
			},
			DnResources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("2"),
					corev1.ResourceMemory: resource.MustParse("4Gi"),
				},
			},
			Nodes: []string{"node1", "node2", "node3"},
		},
	}

	// Create the task in fake client for tests that need it
	err := fakeClient.Create(context.TODO(), testSystemTask)
	assert.NoError(t, err)

	t.Run("ListSystemTasks", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/system-tasks?namespace=default", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)

		var tasks []polardbxv1.SystemTask
		err := json.Unmarshal(resp.Body.Bytes(), &tasks)
		assert.NoError(t, err)
		assert.Len(t, tasks, 1)
		assert.Equal(t, "test-system-task", tasks[0].Name)
		assert.Equal(t, systemtask.BalanceResource, tasks[0].Spec.TaskType)
	})

	t.Run("GetSystemTask", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/system-tasks/default/test-system-task", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)

		var task polardbxv1.SystemTask
		err := json.Unmarshal(resp.Body.Bytes(), &task)
		assert.NoError(t, err)
		assert.Equal(t, "test-system-task", task.Name)
		assert.Equal(t, systemtask.BalanceResource, task.Spec.TaskType)
		assert.Equal(t, 3, task.Spec.CnReplicas)
		assert.Equal(t, []string{"node1", "node2", "node3"}, task.Spec.Nodes)
	})

	t.Run("CreateSystemTask", func(t *testing.T) {
		newTask := polardbxv1.SystemTask{
			ObjectMeta: metav1.ObjectMeta{
				Name: "new-system-task",
			},
			Spec: polardbxv1.SystemTaskSpec{
				TaskType:   systemtask.BalanceResource,
				CnReplicas: 2,
				CnResources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
				},
				Nodes: []string{"node1"},
			},
		}

		jsonData, _ := json.Marshal(newTask)
		req, _ := http.NewRequest("POST", "/api/v1/system-tasks?namespace=default", bytes.NewBuffer(jsonData))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusCreated, resp.Code)

		var createdTask polardbxv1.SystemTask
		err := json.Unmarshal(resp.Body.Bytes(), &createdTask)
		assert.NoError(t, err)
		assert.Equal(t, "new-system-task", createdTask.Name)
		assert.Equal(t, "default", createdTask.Namespace)
		assert.Equal(t, systemtask.BalanceResource, createdTask.Spec.TaskType)
	})

	t.Run("UpdateSystemTask", func(t *testing.T) {
		updatedTask := *testSystemTask
		updatedTask.Spec.CnReplicas = 5
		updatedTask.Spec.Nodes = []string{"node1", "node2", "node3", "node4", "node5"}

		jsonData, _ := json.Marshal(updatedTask)
		req, _ := http.NewRequest("PUT", "/api/v1/system-tasks/default/test-system-task", bytes.NewBuffer(jsonData))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)

		var returnedTask polardbxv1.SystemTask
		err := json.Unmarshal(resp.Body.Bytes(), &returnedTask)
		assert.NoError(t, err)
		assert.Equal(t, 5, returnedTask.Spec.CnReplicas)
		assert.Len(t, returnedTask.Spec.Nodes, 5)
	})

	t.Run("DeleteSystemTask", func(t *testing.T) {
		req, _ := http.NewRequest("DELETE", "/api/v1/system-tasks/default/test-system-task", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)

		// Verify the task is deleted
		var task polardbxv1.SystemTask
		err := fakeClient.Get(context.TODO(), client.ObjectKey{
			Namespace: "default",
			Name:      "test-system-task",
		}, &task)
		assert.Error(t, err) // Should be not found error
	})

	t.Run("CreateSystemTaskInvalidJSON", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/api/v1/system-tasks?namespace=default", bytes.NewBuffer([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("GetNonExistentSystemTask", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/system-tasks/default/non-existent-task", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusNotFound, resp.Code)
	})

	t.Run("DeleteNonExistentSystemTask", func(t *testing.T) {
		req, _ := http.NewRequest("DELETE", "/api/v1/system-tasks/default/non-existent-task", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}

func TestSystemTaskBusinessLogic(t *testing.T) {
	router, _ := setupSystemTaskTest()

	t.Run("TestResourceConfiguration", func(t *testing.T) {
		testCases := []struct {
			name        string
			task        polardbxv1.SystemTask
			description string
		}{
			{
				name: "HighResourceTask",
				task: polardbxv1.SystemTask{
					ObjectMeta: metav1.ObjectMeta{
						Name: "high-resource-task",
					},
					Spec: polardbxv1.SystemTaskSpec{
						TaskType:   systemtask.BalanceResource,
						CnReplicas: 10,
						CnResources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("4"),
								corev1.ResourceMemory: resource.MustParse("8Gi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("8"),
								corev1.ResourceMemory: resource.MustParse("16Gi"),
							},
						},
						DnResources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("8"),
								corev1.ResourceMemory: resource.MustParse("16Gi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("16"),
								corev1.ResourceMemory: resource.MustParse("32Gi"),
							},
						},
					},
				},
				description: "High resource system task for performance testing",
			},
			{
				name: "LowResourceTask",
				task: polardbxv1.SystemTask{
					ObjectMeta: metav1.ObjectMeta{
						Name: "low-resource-task",
					},
					Spec: polardbxv1.SystemTaskSpec{
						TaskType:   systemtask.BalanceResource,
						CnReplicas: 1,
						CnResources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("100m"),
								corev1.ResourceMemory: resource.MustParse("256Mi"),
							},
						},
						DnResources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("200m"),
								corev1.ResourceMemory: resource.MustParse("512Mi"),
							},
						},
					},
				},
				description: "Low resource system task for minimal deployments",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				jsonData, _ := json.Marshal(tc.task)
				req, _ := http.NewRequest("POST", "/api/v1/system-tasks?namespace=default", bytes.NewBuffer(jsonData))
				req.Header.Set("Content-Type", "application/json")
				resp := httptest.NewRecorder()

				router.ServeHTTP(resp, req)

				assert.Equal(t, http.StatusCreated, resp.Code)

				var createdTask polardbxv1.SystemTask
				err := json.Unmarshal(resp.Body.Bytes(), &createdTask)
				assert.NoError(t, err)
				assert.Equal(t, tc.task.Name, createdTask.Name)
				assert.Equal(t, tc.task.Spec.CnReplicas, createdTask.Spec.CnReplicas)
				assert.Equal(t, systemtask.BalanceResource, createdTask.Spec.TaskType)
			})
		}
	})

	t.Run("TestNodeConfiguration", func(t *testing.T) {
		taskWithNodes := polardbxv1.SystemTask{
			ObjectMeta: metav1.ObjectMeta{
				Name: "multi-node-task",
			},
			Spec: polardbxv1.SystemTaskSpec{
				TaskType:   systemtask.BalanceResource,
				CnReplicas: 3,
				Nodes:      []string{"worker-node-1", "worker-node-2", "worker-node-3", "worker-node-4"},
			},
		}

		jsonData, _ := json.Marshal(taskWithNodes)
		req, _ := http.NewRequest("POST", "/api/v1/system-tasks?namespace=default", bytes.NewBuffer(jsonData))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusCreated, resp.Code)

		var createdTask polardbxv1.SystemTask
		err := json.Unmarshal(resp.Body.Bytes(), &createdTask)
		assert.NoError(t, err)
		assert.Len(t, createdTask.Spec.Nodes, 4)
		assert.Contains(t, createdTask.Spec.Nodes, "worker-node-1")
		assert.Contains(t, createdTask.Spec.Nodes, "worker-node-4")
	})
}

// RegisterSystemTaskRoutes registers the SystemTask related routes
func RegisterSystemTaskRoutes(router *gin.Engine) {
	api := router.Group("/api/v1")
	{
		api.GET("/system-tasks", api_systemtask.List)
		api.POST("/system-tasks", api_systemtask.Create)
		api.GET("/system-tasks/:namespace/:name", api_systemtask.Get)
		api.PUT("/system-tasks/:namespace/:name", api_systemtask.Update)
		api.DELETE("/system-tasks/:namespace/:name", api_systemtask.Delete)
	}
}
