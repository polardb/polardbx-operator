package monitoring

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func setupRouterForMonitoring(t *testing.T, objs ...runtime.Object) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")
	v1.Use(func(c *gin.Context) {
		scheme := runtime.NewScheme()
		_ = appsv1.AddToScheme(scheme)
		_ = corev1.AddToScheme(scheme)
		cli := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
		c.Set("k8sClient", cli)
		c.Next()
	})
	v1.GET("/monitoring/status", Status)
	return r
}

func TestMonitoringStatus_ExistsFlags(t *testing.T) {
	ns := "polardbx-monitor"
	router := setupRouterForMonitoring(t)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/monitoring/status?namespace="+ns, nil)
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	comps := resp["components"].(map[string]any)
	// 在空集群 exists 可能为 false，至少字段应存在
	if _, ok := comps["prometheus"].(map[string]any)["exists"]; !ok {
		t.Fatalf("prometheus.exists field not present")
	}
	if _, ok := comps["grafana"].(map[string]any)["exists"]; !ok {
		t.Fatalf("grafana.exists field not present")
	}
	if _, ok := comps["alertmanager"].(map[string]any)["exists"]; !ok {
		t.Fatalf("alertmanager.exists field not present")
	}
}
