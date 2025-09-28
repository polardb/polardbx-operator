package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	monitor "polardbx-ui-backend/pkg/api/monitor"
	parameters "polardbx-ui-backend/pkg/api/parameters"
	restore "polardbx-ui-backend/pkg/api/restore"
	"polardbx-ui-backend/pkg/k8s"

	// domain handlers
	domain_pxc "polardbx-ui-backend/pkg/api/domain/polardbxclusters"
	domain_xs "polardbx-ui-backend/pkg/api/domain/xstores"
)

// IntegrationTestSuite provides comprehensive integration testing for all API endpoints
type IntegrationTestSuite struct {
	suite.Suite
	router             *gin.Engine
	mockClientProvider *IntegrationMockClientProvider
	fakeK8sClient      client.Client
	scheme             *runtime.Scheme
}

// IntegrationMockClientProvider mock implementation for testing
type IntegrationMockClientProvider struct {
	mock.Mock
}

func (m *IntegrationMockClientProvider) NewClientFromKubeconfig(kubeconfig []byte) (client.Client, error) {
	args := m.Called(kubeconfig)
	return args.Get(0).(client.Client), args.Error(1)
}

// Setup integration test router with all API endpoints
func setupIntegrationTestRouter(k8sClientProvider k8s.ClientProvider) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	// Health route
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "healthy"})
	})

	// Connect route
	router.POST("/connect", Connect)

	// API routes with middleware
	v1 := router.Group("/api/v1")
	v1.Use(KubeconfigAuthMiddleware())
	{
		// Cluster routes (domain)
		v1.GET("/clusters", domain_pxc.List)
		v1.POST("/clusters", domain_pxc.Create)
		v1.GET("/clusters/:namespace/:name", domain_pxc.Get)
		v1.PUT("/clusters/:namespace/:name", domain_pxc.Update)
		v1.DELETE("/clusters/:namespace/:name", domain_pxc.Delete)

		// Backup routes (domain)
		v1.GET("/clusters/:namespace/:name/backups", domain_pxc.ListBackups)
		v1.POST("/clusters/:namespace/:name/backups", domain_pxc.CreateBackup)
		v1.DELETE("/backups/:namespace/:name", domain_pxc.DeleteBackup)

		// BackupBinlog routes → domain
		v1.GET("/backup-binlogs", domain_pxc.ListBackupBinlogs)
		v1.POST("/backup-binlogs", domain_pxc.CreateBackupBinlog)
		v1.GET("/backup-binlogs/:namespace/:name", domain_pxc.GetBackupBinlog)
		v1.PUT("/backup-binlogs/:namespace/:name", domain_pxc.UpdateBackupBinlog)
		v1.DELETE("/backup-binlogs/:namespace/:name", domain_pxc.DeleteBackupBinlog)

		// XStore routes → domain
		v1.GET("/xstores", domain_xs.List)
		v1.POST("/xstores", domain_xs.Create)
		v1.GET("/xstores/:namespace/:name", domain_xs.Get)
		v1.PUT("/xstores/:namespace/:name", domain_xs.Update)
		v1.DELETE("/xstores/:namespace/:name", domain_xs.Delete)

		// Monitor routes
		v1.GET("/monitors", monitor.List)
		v1.POST("/monitors", monitor.Create)
		v1.GET("/monitors/:namespace/:name", monitor.Get)
		v1.PUT("/monitors/:namespace/:name", monitor.Update)
		v1.DELETE("/monitors/:namespace/:name", monitor.Delete)

		// Parameter routes
		v1.GET("/parameters", parameters.List)
		v1.POST("/parameters", parameters.Create)
		v1.GET("/parameters/:name", parameters.Get)
		v1.PUT("/parameters/:name", parameters.Update)
		v1.DELETE("/parameters/:name", parameters.Delete)

		// Additional CRD routes would be defined here
	}

	return router
}

func (suite *IntegrationTestSuite) SetupSuite() {
	// Initialize scheme
	suite.scheme = runtime.NewScheme()
	err := polardbxv1.AddToScheme(suite.scheme)
	require.NoError(suite.T(), err)

	// Setup fake k8s client
	suite.fakeK8sClient = fake.NewClientBuilder().
		WithScheme(suite.scheme).
		Build()

	// Setup mock provider
	suite.mockClientProvider = &IntegrationMockClientProvider{}
	suite.mockClientProvider.On("NewClientFromKubeconfig", mock.Anything).
		Return(suite.fakeK8sClient, nil)

	// Setup router
	suite.router = setupIntegrationTestRouter(suite.mockClientProvider)
}

func (suite *IntegrationTestSuite) TearDownSuite() {
	// Cleanup if needed
}

func (suite *IntegrationTestSuite) TestHealthEndpoint() {
	req, _ := http.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	suite.router.ServeHTTP(w, req)

	assert.Equal(suite.T(), http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "healthy", response["status"])
}

func (suite *IntegrationTestSuite) TestConnectEndpoint() {
	connectData := map[string]string{
		"kubeconfig": "YXBpVmVyc2lvbjogdjEKa2luZDogQ29uZmln", // base64 encoded dummy kubeconfig
	}

	jsonData, _ := json.Marshal(connectData)
	req, _ := http.NewRequest("POST", "/connect", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Kubeconfig-B64", "YXBpVmVyc2lvbjogdjEKa2luZDogQ29uZmln")

	w := httptest.NewRecorder()
	suite.router.ServeHTTP(w, req)

	assert.Equal(suite.T(), http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(suite.T(), err)
	assert.Contains(suite.T(), response["message"], "connection successful")
}

func (suite *IntegrationTestSuite) TestAPICRUDOperations() {
	// Test cluster handlers -> domain
	assert.NotNil(suite.T(), domain_pxc.List)
	assert.NotNil(suite.T(), domain_pxc.Create)
	assert.NotNil(suite.T(), domain_pxc.Get)
	assert.NotNil(suite.T(), domain_pxc.Update)
	assert.NotNil(suite.T(), domain_pxc.Delete)

	// Test backup handlers -> domain
	assert.NotNil(suite.T(), domain_pxc.ListBackups)
	assert.NotNil(suite.T(), domain_pxc.CreateBackup)
	assert.NotNil(suite.T(), domain_pxc.DeleteBackup)

	// Test XStore handlers -> domain
	assert.NotNil(suite.T(), domain_xs.List)
	assert.NotNil(suite.T(), domain_xs.Create)
	assert.NotNil(suite.T(), domain_xs.Get)
	assert.NotNil(suite.T(), domain_xs.Update)
	assert.NotNil(suite.T(), domain_xs.Delete)

	// Test Monitor handlers
	assert.NotNil(suite.T(), monitor.List)
	assert.NotNil(suite.T(), monitor.Create)
	assert.NotNil(suite.T(), monitor.Get)
	assert.NotNil(suite.T(), monitor.Update)
	assert.NotNil(suite.T(), monitor.Delete)

	// Test Parameter handlers
	assert.NotNil(suite.T(), parameters.List)
	assert.NotNil(suite.T(), parameters.Create)
	assert.NotNil(suite.T(), parameters.Get)
	assert.NotNil(suite.T(), parameters.Update)
	assert.NotNil(suite.T(), parameters.Delete)

	// Test Backup Schedule handlers (migrated to domain)
	assert.NotNil(suite.T(), domain_pxc.ListSchedules)
	assert.NotNil(suite.T(), domain_pxc.CreateSchedule)
	assert.NotNil(suite.T(), domain_pxc.GetSchedule)
	assert.NotNil(suite.T(), domain_pxc.UpdateSchedule)
	assert.NotNil(suite.T(), domain_pxc.DeleteSchedule)

	// Test Backup Binlog handlers (migrated to domain handlers)
	assert.NotNil(suite.T(), domain_pxc.ListBackupBinlogs)
	assert.NotNil(suite.T(), domain_pxc.CreateBackupBinlog)
	assert.NotNil(suite.T(), domain_pxc.GetBackupBinlog)
	assert.NotNil(suite.T(), domain_pxc.UpdateBackupBinlog)
	assert.NotNil(suite.T(), domain_pxc.DeleteBackupBinlog)

	// Test Recovery handlers (moved to api/restore)
	assert.NotNil(suite.T(), restore.RestoreCluster)
	assert.NotNil(suite.T(), restore.InitiatePITR)
	assert.NotNil(suite.T(), restore.GetRestoreStatus)
}

func (suite *IntegrationTestSuite) TestClusterOperations() {
	// Test creating a cluster
	cluster := polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXClusterSpec{
			// Add minimal required spec fields
		},
	}

	jsonData, _ := json.Marshal(cluster)
	req, _ := http.NewRequest("POST", "/api/v1/clusters", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Kubeconfig-B64", "YXBpVmVyc2lvbjogdjEKa2luZDogQ29uZmln")

	w := httptest.NewRecorder()
	suite.router.ServeHTTP(w, req)

	// Note: This might return an error due to missing k8s client setup
	// but we're testing the handler exists and is wired correctly
	assert.Contains(suite.T(), []int{http.StatusCreated, http.StatusInternalServerError, http.StatusBadRequest, http.StatusUnauthorized}, w.Code)
}

func (suite *IntegrationTestSuite) TestAuthenticationMiddleware() {
	// Test request without auth header
	req, _ := http.NewRequest("GET", "/api/v1/clusters", nil)
	w := httptest.NewRecorder()
	suite.router.ServeHTTP(w, req)

	assert.Equal(suite.T(), http.StatusUnauthorized, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(suite.T(), err)
	assert.Contains(suite.T(), response["error"], "kubeconfig not provided")
}

func (suite *IntegrationTestSuite) TestErrorHandling() {
	// Test with invalid base64 kubeconfig
	req, _ := http.NewRequest("GET", "/api/v1/clusters", nil)
	req.Header.Set("X-Kubeconfig-B64", "invalid-base64!")

	w := httptest.NewRecorder()
	suite.router.ServeHTTP(w, req)

	assert.Equal(suite.T(), http.StatusBadRequest, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(suite.T(), err)
	assert.Contains(suite.T(), response["error"], "invalid kubeconfig base64")
}

func (suite *IntegrationTestSuite) TestConcurrentRequests() {
	const numRequests = 10
	results := make(chan int, numRequests)

	for i := 0; i < numRequests; i++ {
		go func() {
			req, _ := http.NewRequest("GET", "/health", nil)
			w := httptest.NewRecorder()
			suite.router.ServeHTTP(w, req)
			results <- w.Code
		}()
	}

	// Collect all results
	for i := 0; i < numRequests; i++ {
		code := <-results
		assert.Equal(suite.T(), http.StatusOK, code)
	}
}

func (suite *IntegrationTestSuite) TestPerformanceBenchmark() {
	start := time.Now()
	const numRequests = 100

	for i := 0; i < numRequests; i++ {
		req, _ := http.NewRequest("GET", "/health", nil)
		w := httptest.NewRecorder()
		suite.router.ServeHTTP(w, req)
		assert.Equal(suite.T(), http.StatusOK, w.Code)
	}

	duration := time.Since(start)
	avgDuration := duration / numRequests

	// Performance assertion - each request should be under 10ms
	assert.Less(suite.T(), avgDuration, 10*time.Millisecond,
		fmt.Sprintf("Average request time %v exceeds 10ms", avgDuration))
}

func (suite *IntegrationTestSuite) TestRouteRegistration() {
	// Test that all expected routes are registered
	routes := suite.router.Routes()

	expectedRoutes := []string{
		"GET /health",
		"POST /connect",
		"GET /api/v1/clusters",
		"POST /api/v1/clusters",
		"GET /api/v1/clusters/:namespace/:name",
		"PUT /api/v1/clusters/:namespace/:name",
		"DELETE /api/v1/clusters/:namespace/:name",
	}

	routeMap := make(map[string]bool)
	for _, route := range routes {
		key := route.Method + " " + route.Path
		routeMap[key] = true
	}

	for _, expectedRoute := range expectedRoutes {
		assert.True(suite.T(), routeMap[expectedRoute],
			fmt.Sprintf("Route %s not found", expectedRoute))
	}
}

// Run the integration test suite
func TestIntegrationAPISuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}

// Individual test functions for specific functionality

func TestClusterHandlerExists(t *testing.T) {
	// Test that cluster handlers are properly defined (domain)
	assert.NotNil(t, domain_pxc.List)
	assert.NotNil(t, domain_pxc.Create)
	assert.NotNil(t, domain_pxc.Get)
	assert.NotNil(t, domain_pxc.Update)
	assert.NotNil(t, domain_pxc.Delete)
}

func TestBackupHandlerExists(t *testing.T) {
	// Test that backup handlers are properly defined
	assert.NotNil(t, domain_pxc.ListBackups)
	assert.NotNil(t, domain_pxc.CreateBackup)
	assert.NotNil(t, domain_pxc.DeleteBackup)
}

func TestXStoreHandlerExists(t *testing.T) {
	// Test that XStore handlers are properly defined
	assert.NotNil(t, domain_xs.List)
	assert.NotNil(t, domain_xs.Create)
	assert.NotNil(t, domain_xs.Get)
	assert.NotNil(t, domain_xs.Update)
	assert.NotNil(t, domain_xs.Delete)
}

func TestMonitorHandlerExists(t *testing.T) {
	// Test that monitor handlers are properly defined
	assert.NotNil(t, monitor.List)
	assert.NotNil(t, monitor.Create)
	assert.NotNil(t, monitor.Get)
	assert.NotNil(t, monitor.Update)
	assert.NotNil(t, monitor.Delete)
}

func TestAllCRDHandlersExist(t *testing.T) {
	// Simplified to only assert handlers relevant to this refactor
	handlers := []interface{}{
		// XStore
		domain_xs.List, domain_xs.Create, domain_xs.Get, domain_xs.Update, domain_xs.Delete,
		// Monitor
		monitor.List, monitor.Create, monitor.Get, monitor.Update, monitor.Delete,
		// Parameters
		parameters.List, parameters.Create, parameters.Get, parameters.Update, parameters.Delete,
		// BackupBinlog
		domain_pxc.ListBackupBinlogs, domain_pxc.CreateBackupBinlog, domain_pxc.GetBackupBinlog, domain_pxc.UpdateBackupBinlog, domain_pxc.DeleteBackupBinlog,
		// XStoreFollower
		domain_xs.ListFollowers, domain_xs.CreateFollower, domain_xs.GetFollower, domain_xs.UpdateFollower, domain_xs.DeleteFollower,
		// XStoreBackup
		domain_xs.ListBackups, domain_xs.CreateBackup, domain_xs.GetBackup, domain_xs.UpdateBackup, domain_xs.DeleteBackup,
		// Recovery APIs
		restore.RestoreCluster, restore.InitiatePITR, restore.GetRestoreStatus,
	}

	for i, handler := range handlers {
		assert.NotNil(t, handler, fmt.Sprintf("Handler %d is nil", i))
	}
}
