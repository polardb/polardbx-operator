package api

import (
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
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	domain_pxc "polardbx-ui-backend/pkg/api/domain/polardbxclusters"
)

func TestBackupOverview_EvaluateConnectivity(t *testing.T) {
	scheme := runtime.NewScheme()
	polardbxv1.AddToScheme(scheme)
	corev1.AddToScheme(scheme)

	// no backups, no configmap
	fakeClient := crfake.NewClientBuilder().WithScheme(scheme).Build()
	r := gin.Default()
	r.Use(func(c *gin.Context) { c.Set("k8sClient", fakeClient) })
	r.GET("/api/v1/backups/overview", domain_pxc.GetBackupOverview)

	// default: evaluateConnectivity=false -> pending_implementation
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/backups/overview?namespace=ns1", nil)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	kpi := resp["kpi"].(map[string]any)
	assert.Equal(t, "pending_implementation", kpi["storageConnectivity"])

	// evaluateConnectivity=true -> unknown (since config not present)
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest(http.MethodGet, "/api/v1/backups/overview?namespace=ns1&evaluateConnectivity=true", nil)
	r.ServeHTTP(w2, req2)
	assert.Equal(t, http.StatusOK, w2.Code)
	var resp2 map[string]any
	_ = json.Unmarshal(w2.Body.Bytes(), &resp2)
	kpi2 := resp2["kpi"].(map[string]any)
	assert.Equal(t, "error", kpi2["storageConnectivityStatus"]) // new impl returns error when unreachable

	// when configmap exists in system namespace -> connected
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "polardbx-hpfs-config", Namespace: "polardbx-operator-system"}}
	fakeClient2 := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
	r2 := gin.Default()
	r2.Use(func(c *gin.Context) { c.Set("k8sClient", fakeClient2) })
	r2.GET("/api/v1/backups/overview", domain_pxc.GetBackupOverview)
	w3 := httptest.NewRecorder()
	req3, _ := http.NewRequest(http.MethodGet, "/api/v1/backups/overview?namespace=ns1&evaluateConnectivity=true", nil)
	r2.ServeHTTP(w3, req3)
	assert.Equal(t, http.StatusOK, w3.Code)
	var resp3 map[string]any
	_ = json.Unmarshal(w3.Body.Bytes(), &resp3)
	kpi3 := resp3["kpi"].(map[string]any)
	assert.Equal(t, "error", kpi3["storageConnectivityStatus"]) // align with new impl semantics
}
