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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRunRestoreFlow_Succeeds(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Prepare scheme and fake client with a finished backup and a source cluster
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)

	ns := "default"
	source := &polardbxv1.PolarDBXCluster{}
	source.Name = "pxc-src"
	source.Namespace = ns

	backup := &polardbxv1.PolarDBXBackup{}
	backup.Name = "bk1"
	backup.Namespace = ns
	backup.Status.Phase = polardbxv1.BackupFinished

	cli := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(source, backup).Build()

	// Build gin context
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("k8sClient", cli)
	c.Params = gin.Params{
		{Key: "namespace", Value: ns},
		{Key: "name", Value: source.Name},
	}
	body, _ := json.Marshal(map[string]any{
		"backupSet": backup.Name,
	})
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/polardbxclusters/default/pxc-src/restore-flow/run", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	// Execute restore flow
	RunRestoreFlow(c)

	// Accept either 200 (flow wrapper) or 201 (underlying handler wrote directly)
	if w.Code != http.StatusOK && w.Code != http.StatusCreated {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	// Verify target cluster was created by apply step
	created := &polardbxv1.PolarDBXCluster{}
	err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: ns, Name: "pxc-src-restored"}, created)
	assert.NoError(t, err)
}

func TestRunRestoreFlow_Fails_WhenClientMissing(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Params = gin.Params{{Key: "namespace", Value: "default"}, {Key: "name", Value: "c1"}}
	c.Request = httptest.NewRequest(http.MethodPost, "/n/a", nil)

	RunRestoreFlow(c)
	// May return 401 (middleware validation) or 200 + failed (local validation)
	if w.Code == http.StatusOK {
		var rr map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &rr)
		assert.Equal(t, "failed", rr["status"])
	} else {
		assert.Equal(t, http.StatusUnauthorized, w.Code)
	}
}

func TestRunRestoreFlow_Fails_WhenParamsInvalid(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	// Missing required namespace/name
	RunRestoreFlow(c)
	if w.Code == http.StatusOK {
		var rr map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &rr)
		assert.Equal(t, "failed", rr["status"])
	} else {
		assert.Equal(t, http.StatusUnauthorized, w.Code)
	}
}
