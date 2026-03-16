package polardbxclusters

import (
	"net/http"
	"net/http/httptest"
	"testing"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestGetUpgradePlan_UnauthorizedWhenNoClient(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Params = gin.Params{
		{Key: "namespace", Value: "ns"},
		{Key: "name", Value: "demo"},
	}
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/polardbxclusters/ns/demo/upgrade-plan", nil)

	GetUpgradePlan(c)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestGetUpgradePlan_NotFoundWhenClusterMissing(t *testing.T) {
	gin.SetMode(gin.TestMode)
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)
	cli := crfake.NewClientBuilder().WithScheme(scheme).Build()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("k8sClient", cli)
	c.Params = gin.Params{
		{Key: "namespace", Value: "ns"},
		{Key: "name", Value: "missing"},
	}
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/polardbxclusters/ns/missing/upgrade-plan", nil)

	GetUpgradePlan(c)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGetUpgradePlan_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)

	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns"},
		Spec: polardbxv1.PolarDBXClusterSpec{
			Topology: polardbx.Topology{Version: "5.4.17"},
		},
	}

	cli := crfake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("k8sClient", cli)
	c.Params = gin.Params{
		{Key: "namespace", Value: "ns"},
		{Key: "name", Value: "demo"},
	}
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/polardbxclusters/ns/demo/upgrade-plan", nil)

	GetUpgradePlan(c)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"currentVersion":"5.4.17"`)
	assert.Contains(t, w.Body.String(), `"recommended":true`)
	assert.Contains(t, w.Body.String(), `"matrix"`)
}

func TestBuildUpgradeCandidates_DefaultsWhenNoHigher(t *testing.T) {
	cands := buildUpgradeCandidates("5.4.19")
	assert.True(t, len(cands) >= 2, "should fallback to default candidates when none higher")
	assert.Equal(t, "5.4.19", cands[0].Version)
	assert.True(t, cands[0].Recommended)
}
