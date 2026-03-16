package fixtures

import (
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"

	crd_polardbxbackupbinlogs "polardbx-dashboard-backend/pkg/api/crd/polardbxbackupbinlogs"
	crd_polardbxbackups "polardbx-dashboard-backend/pkg/api/crd/polardbxbackups"
	crd_polardbxbackupschedules "polardbx-dashboard-backend/pkg/api/crd/polardbxbackupschedules"
	crd_polardbxclusters "polardbx-dashboard-backend/pkg/api/crd/polardbxclusters"
	crd_polardbxlogcollectors "polardbx-dashboard-backend/pkg/api/crd/polardbxlogcollectors"
	crd_polardbxmonitors "polardbx-dashboard-backend/pkg/api/crd/polardbxmonitors"
	crd_polardbxparameters "polardbx-dashboard-backend/pkg/api/crd/polardbxparameters"
	crd_polardbxparametertemplates "polardbx-dashboard-backend/pkg/api/crd/polardbxparametertemplates"
	crd_systemtasks "polardbx-dashboard-backend/pkg/api/crd/systemtasks"
	crd_xstorebackupbinlogs "polardbx-dashboard-backend/pkg/api/crd/xstorebackupbinlogs"
	crd_xstores "polardbx-dashboard-backend/pkg/api/crd/xstores"

	domain_diagnostics "polardbx-dashboard-backend/pkg/api/domain/platform/diagnostics/handler"
	domain_logcollector "polardbx-dashboard-backend/pkg/api/domain/platform/logcollector/handler"
	domain_restore "polardbx-dashboard-backend/pkg/api/domain/platform/restore/handler"
	domain_pxc "polardbx-dashboard-backend/pkg/api/domain/polardbxclusters"
	domain_alerts "polardbx-dashboard-backend/pkg/api/domain/platform/alerts/handler"
	domain_logs "polardbx-dashboard-backend/pkg/api/domain/platform/logs/handler"
	domain_monitoring "polardbx-dashboard-backend/pkg/api/domain/monitoring"
	domain_logstrategy "polardbx-dashboard-backend/pkg/api/domain/platform/logstrategy/handler"
	domain_prometheusrule "polardbx-dashboard-backend/pkg/api/domain/platform/prometheusrule/handler"
	domain_system "polardbx-dashboard-backend/pkg/api/domain/platform/system/handler"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// SetupCRDRouter sets up a test router with all CRD routes
func SetupCRDRouter(t *testing.T, objs ...runtime.Object) (*gin.Engine, client.Client) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")

	// Build scheme with all required types
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = polardbxv1.AddToScheme(scheme)

	ctrlClient := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(objs...).
		Build()
	cs := k8sfake.NewSimpleClientset()

	v1.Use(func(c *gin.Context) {
		c.Set("k8sClient", ctrlClient)
		c.Set("clientset", cs)
		c.Set("k8sDefaultNamespace", "default")
		c.Next()
	})

	// Register all CRD routes
	crdGroup := v1.Group("/crd")
	crd_polardbxclusters.RegisterRoutes(crdGroup)
	crd_xstores.RegisterRoutes(crdGroup)
	crd_systemtasks.RegisterRoutes(crdGroup)
	crd_polardbxbackups.RegisterRoutes(crdGroup)
	crd_polardbxbackupschedules.RegisterRoutes(crdGroup)
	crd_polardbxbackupbinlogs.RegisterRoutes(crdGroup)
	crd_xstorebackupbinlogs.RegisterRoutes(crdGroup)
	crd_polardbxparameters.RegisterRoutes(crdGroup)
	crd_polardbxparametertemplates.RegisterRoutes(crdGroup)
	crd_polardbxmonitors.RegisterRoutes(crdGroup)
	crd_polardbxlogcollectors.RegisterRoutes(crdGroup)

	return r, ctrlClient
}

// SetupBackupRouter sets up a test router with backup routes
func SetupBackupRouter(t *testing.T, objs ...runtime.Object) (*gin.Engine, client.Client) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")

	// Build scheme with all required types
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = polardbxv1.AddToScheme(scheme)

	ctrlClient := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(objs...).
		Build()
	cs := k8sfake.NewSimpleClientset()

	v1.Use(func(c *gin.Context) {
		c.Set("k8sClient", ctrlClient)
		c.Set("clientset", cs)
		c.Set("k8sDefaultNamespace", "default")
		c.Next()
	})

	// Register backup routes
	crdGroup := v1.Group("/crd")
	crd_polardbxbackups.RegisterRoutes(crdGroup)

	// Also register domain routes for backup operations
	v1.GET("/backups/overview", domain_pxc.GetBackupOverview)
	v1.GET("/backups/binlog/metrics", domain_pxc.GetBinlogMetrics)
	v1.POST("/backups/validate", domain_pxc.ValidateBackup)

	return r, ctrlClient
}

// SetupCriticalAPIsRouter sets up test router with critical API routes
func SetupCriticalAPIsRouter(t *testing.T, objs ...runtime.Object) (*gin.Engine, client.Client) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")

	// Build scheme with all required types
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = polardbxv1.AddToScheme(scheme)

	ctrlClient := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(objs...).
		Build()
	cs := k8sfake.NewSimpleClientset()

	v1.Use(func(c *gin.Context) {
		c.Set("k8sClient", ctrlClient)
		c.Set("clientset", cs)
		c.Set("k8sDefaultNamespace", "default")
		c.Next()
	})

	// Diagnostics routes
	v1.POST("/diagnostics/:namespace/:cluster/start", domain_diagnostics.Start)
	v1.GET("/diagnostics/:namespace/:id/status", domain_diagnostics.GetStatus)
	v1.GET("/diagnostics/reports", domain_diagnostics.ListReports)
	v1.DELETE("/diagnostics/:namespace/:id", domain_diagnostics.DeleteJob)

	// Restore routes
	v1.POST("/clusters/:namespace/:name/restore", domain_restore.RestoreCluster)
	v1.GET("/clusters/:namespace/:name/restore-status", domain_restore.GetRestoreStatus)
	v1.GET("/restore-jobs", domain_restore.ListJobs)
	v1.GET("/restore-jobs/:namespace/:name", domain_restore.GetJob)
	v1.DELETE("/restore-jobs/:namespace/:name", domain_restore.CancelJob)

	// LogCollector routes
	v1.GET("/log-collectors", domain_logcollector.List)
	v1.POST("/log-collectors", domain_logcollector.Create)
	v1.GET("/log-collectors/:namespace/:name", domain_logcollector.Get)
	v1.PUT("/log-collectors/:namespace/:name", domain_logcollector.Update)
	v1.DELETE("/log-collectors/:namespace/:name", domain_logcollector.Delete)
	v1.GET("/log-collectors/:namespace/:name/status", domain_logcollector.GetLogCollectorStatus)

	return r, ctrlClient
}

// SetupRouterE2E sets up a test router with necessary routes and injected fake clients.
func SetupRouterE2E(t *testing.T, objs ...runtime.Object) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")

	// fake controller-runtime client (for CRUD/status)
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	ctrlClient := crfake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	// fake clientset (for pod logs listing)
	cs := k8sfake.NewSimpleClientset()

	v1.Use(func(c *gin.Context) {
		// inject using util expected keys
		c.Set("k8sClient", ctrlClient)
		c.Set("clientset", cs)
		// optional defaults
		c.Set("k8sDefaultNamespace", "polardbx-operator-system")
		c.Next()
	})

	// minimal routes under test
	v1.GET("/alerts", domain_alerts.List)
	v1.GET("/monitoring/bootstrap/status", domain_monitoring.BootstrapStatus)
	v1.GET("/logs/bootstrap/status", domain_logs.BootstrapStatus)

	return r
}

// SetupIntegrationRouter sets up the router with all the new API routes
func SetupIntegrationRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")

	// Add middleware to inject mock clients
	v1.Use(func(c *gin.Context) {
		// Setup mock clients
		cs := k8sfake.NewSimpleClientset()
		scheme := runtime.NewScheme()
		_ = corev1.AddToScheme(scheme)
		cli := crfake.NewClientBuilder().WithScheme(scheme).Build()
		dynClient := dynamicfake.NewSimpleDynamicClient(scheme)

		c.Set("clientset", cs)
		c.Set("k8sClient", cli)
		c.Set("dynamic-client", dynClient)
		c.Next()
	})

	// Direct routes for frontend compatibility (the ones we added)
	v1.GET("/namespaces", domain_system.ListNamespaces)
	v1.GET("/prometheus-rules", domain_prometheusrule.List)
	v1.GET("/prometheus-rules/:namespace/:name/yaml", domain_prometheusrule.GetYAML)
	v1.POST("/prometheus-rules/validate", domain_prometheusrule.ValidateRule)
	v1.GET("/log-strategies/apply-records", domain_logstrategy.ListApplyRecords)

	// Include some existing routes for comparison
	v1.GET("/system/namespaces", domain_system.ListNamespaces)
	v1.GET("/log-strategies", domain_logstrategy.List)
	v1.POST("/log-strategies", domain_logstrategy.Create)

	return r
}

