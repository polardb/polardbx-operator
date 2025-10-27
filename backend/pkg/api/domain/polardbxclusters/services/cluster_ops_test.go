package services

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func setupGinWithFakeClient(t *testing.T, objs ...runtime.Object) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)
	cli := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	c.Set("k8sClient", cli)
	return c, w
}

func TestUpgrade_ValidAndInvalid(t *testing.T) {
	cluster := &polardbxv1.PolarDBXCluster{}
	cluster.Name = "c1"
	cluster.Namespace = "ns"
	c, w := setupGinWithFakeClient(t, cluster)
	c.Params = gin.Params{{Key: "namespace", Value: "ns"}, {Key: "name", Value: "c1"}}

	// valid
	body := map[string]any{"targetVersion": "8.0.20"}
	b, _ := json.Marshal(body)
	c.Request = httptest.NewRequest(http.MethodPatch, "/", bytes.NewReader(b))
	c.Request.Header.Set("Content-Type", "application/json")
	_ = NewClusterService().Upgrade(c.Request.Context(), c)
	assert.Equal(t, http.StatusOK, w.Code)

	// invalid (missing targetVersion)
	c2, w2 := setupGinWithFakeClient(t, cluster.DeepCopy())
	c2.Params = gin.Params{{Key: "namespace", Value: "ns"}, {Key: "name", Value: "c1"}}
	c2.Request = httptest.NewRequest(http.MethodPatch, "/", bytes.NewReader([]byte(`{}`)))
	c2.Request.Header.Set("Content-Type", "application/json")
	_ = NewClusterService().Upgrade(c2.Request.Context(), c2)
	assert.Equal(t, http.StatusBadRequest, w2.Code)
}

func TestScale_ValidAndNoChanges(t *testing.T) {
	cluster := &polardbxv1.PolarDBXCluster{}
	cluster.Name = "c1"
	cluster.Namespace = "ns"
	c, w := setupGinWithFakeClient(t, cluster)
	c.Params = gin.Params{{Key: "namespace", Value: "ns"}, {Key: "name", Value: "c1"}}

	// valid
	body := map[string]any{"cnReplicas": 2}
	b, _ := json.Marshal(body)
	c.Request = httptest.NewRequest(http.MethodPatch, "/", bytes.NewReader(b))
	c.Request.Header.Set("Content-Type", "application/json")
	_ = NewClusterService().Scale(c.Request.Context(), c)
	assert.Equal(t, http.StatusOK, w.Code)

	// no changes
	c2, w2 := setupGinWithFakeClient(t, cluster.DeepCopy())
	c2.Params = gin.Params{{Key: "namespace", Value: "ns"}, {Key: "name", Value: "c1"}}
	c2.Request = httptest.NewRequest(http.MethodPatch, "/", bytes.NewReader([]byte(`{}`)))
	c2.Request.Header.Set("Content-Type", "application/json")
	_ = NewClusterService().Scale(c2.Request.Context(), c2)
	assert.Equal(t, http.StatusBadRequest, w2.Code)
}

func TestUpdateLogConfig_ValidAndInvalidNodeType(t *testing.T) {
	cluster := &polardbxv1.PolarDBXCluster{}
	cluster.Name = "c1"
	cluster.Namespace = "ns"

	// valid for cn
	c, w := setupGinWithFakeClient(t, cluster)
	c.Params = gin.Params{{Key: "namespace", Value: "ns"}, {Key: "name", Value: "c1"}, {Key: "nodeType", Value: "cn"}}
	body := map[string]any{"logLevel": "INFO"}
	b, _ := json.Marshal(body)
	c.Request = httptest.NewRequest(http.MethodPatch, "/", bytes.NewReader(b))
	c.Request.Header.Set("Content-Type", "application/json")
	_ = NewClusterService().UpdateLogConfig(c.Request.Context(), c)
	assert.Equal(t, http.StatusOK, w.Code)

	// invalid node type
	c2, w2 := setupGinWithFakeClient(t, cluster.DeepCopy())
	c2.Params = gin.Params{{Key: "namespace", Value: "ns"}, {Key: "name", Value: "c1"}, {Key: "nodeType", Value: "bad"}}
	c2.Request = httptest.NewRequest(http.MethodPatch, "/", bytes.NewReader([]byte(`{}`)))
	c2.Request.Header.Set("Content-Type", "application/json")
	_ = NewClusterService().UpdateLogConfig(c2.Request.Context(), c2)
	assert.Equal(t, http.StatusBadRequest, w2.Code)
}
