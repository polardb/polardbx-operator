package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	domain_pxc "polardbx-ui-backend/pkg/api/domain/polardbxclusters"
)

func TestGetClusterBackupState(t *testing.T) {
	scheme := runtime.NewScheme()
	polardbxv1.AddToScheme(scheme)

	ns := "ns1"
	now := time.Now()

	cl := &polardbxv1.PolarDBXCluster{ObjectMeta: metav1.ObjectMeta{Name: "pxc-1", Namespace: ns}}
	bOld := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "b-old", Namespace: ns},
		Spec:       polardbxv1.PolarDBXBackupSpec{Cluster: polardbxv1.PolarDBXClusterReference{Name: "pxc-1"}},
		Status:     polardbxv1.PolarDBXBackupStatus{Phase: polardbxv1.BackupFinished, EndTime: &metav1.Time{Time: now.Add(-48 * time.Hour)}, LatestRecoverableTimestamp: &metav1.Time{Time: now.Add(-50 * time.Minute)}},
	}
	bNew := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "b-new", Namespace: ns},
		Spec:       polardbxv1.PolarDBXBackupSpec{Cluster: polardbxv1.PolarDBXClusterReference{Name: "pxc-1"}},
		Status:     polardbxv1.PolarDBXBackupStatus{Phase: polardbxv1.BackupFinished, EndTime: &metav1.Time{Time: now.Add(-2 * time.Hour)}, LatestRecoverableTimestamp: &metav1.Time{Time: now.Add(-30 * time.Minute)}},
	}
	sch := &polardbxv1.PolarDBXBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{Name: "pbs-1", Namespace: ns},
		Spec:       polardbxv1.PolarDBXBackupScheduleSpec{BackupSpec: polardbxv1.PolarDBXBackupSpec{Cluster: polardbxv1.PolarDBXClusterReference{Name: "pxc-1"}}},
		Status:     polardbxv1.PolarDBXBackupScheduleStatus{NextBackupTime: &metav1.Time{Time: now.Add(1 * time.Hour)}},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cl, bOld, bNew, sch).Build()
	r := gin.Default()
	r.Use(func(c *gin.Context) { c.Set("k8sClient", fakeClient) })
	r.GET("/api/v1/backups/cluster-state", domain_pxc.GetClusterBackupState)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/backups/cluster-state?namespace="+ns, nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	clusters := resp["clusters"].([]any)
	m := clusters[0].(map[string]any)
	lb := m["latestBackup"].(map[string]any)
	assert.Equal(t, "b-new", lb["name"]) // pick the newer one
	assert.NotNil(t, m["nextScheduledTime"])
	// rpoSeconds should be a non-negative number
	rpo, ok := m["rpoSeconds"].(float64)
	assert.True(t, ok)
	assert.True(t, rpo >= 0)
}
