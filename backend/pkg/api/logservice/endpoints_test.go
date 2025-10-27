package logservice

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

// helper to setup router with fake clients
func setupRouterWithClientsForLogs(t *testing.T, cs *k8sfake.Clientset, _ runtime.Object) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")
	// inject fake clients
	v1.Use(func(c *gin.Context) {
		if cs != nil {
			c.Set("clientset", cs)
		}
		// Status 仅需要 clientset
		c.Next()
	})
	v1.GET("/log-service/status", Status)
	return r
}

func TestLogServiceStatus_ExistsFlags(t *testing.T) {
	// create fake clientset with DS/Deployment/Service
	ns := "polardbx-logcollector"
	cs := k8sfake.NewSimpleClientset(
		&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "filebeat", Namespace: ns},
			Status: appsv1.DaemonSetStatus{NumberReady: 1, DesiredNumberScheduled: 1}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "logstash", Namespace: ns},
			Status: appsv1.DeploymentStatus{ReadyReplicas: 1, Replicas: 1}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "logstash-pipeline", Namespace: ns}},
	)

	router := setupRouterWithClientsForLogs(t, cs, &corev1.ConfigMap{})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/log-service/status?namespace="+ns, nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	comps := resp["components"].(map[string]any)
	fb := comps["filebeat"].(map[string]any)
	ls := comps["logstash"].(map[string]any)
	if fb["exists"] != true || ls["exists"] != true {
		t.Fatalf("expect exists flags true, got fb=%v ls=%v", fb["exists"], ls["exists"])
	}
}

// 404 场景在 e2e 测试中已覆盖，这里不再重复
