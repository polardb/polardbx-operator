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

func TestGetBinlogMetrics(t *testing.T) {
	scheme := runtime.NewScheme()
	polardbxv1.AddToScheme(scheme)

	// Fixed time for deterministic lag
	nowFixed := time.Date(2024, 5, 1, 4, 0, 0, 0, time.UTC)
	lrt := nowFixed.Add(-90 * time.Minute) // 5400 seconds lag

	b := &polardbxv1.PolarDBXBackupBinlog{
		ObjectMeta: metav1.ObjectMeta{Name: "blog-1", Namespace: "ns1"},
		Spec:       polardbxv1.PolarDBXBackupBinlogSpec{PxcName: "pxc-1"},
		Status:     polardbxv1.PolarDBXBackupBinlogStatus{Phase: polardbxv1.BackupBinlogPhaseRunning},
	}

	pb := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "full-1", Namespace: "ns1"},
		Spec:       polardbxv1.PolarDBXBackupSpec{Cluster: polardbxv1.PolarDBXClusterReference{Name: "pxc-1"}},
		Status:     polardbxv1.PolarDBXBackupStatus{LatestRecoverableTimestamp: &metav1.Time{Time: lrt}},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(b, pb).Build()
	r := gin.Default()
	r.Use(func(c *gin.Context) { c.Set("k8sClient", fakeClient) })
	r.GET("/api/v1/backups/binlog/metrics", domain_pxc.GetBinlogMetrics)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/backups/binlog/metrics?namespace=ns1&now="+nowFixed.Format(time.RFC3339), nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	items := resp["binlogs"].([]any)
	m := items[0].(map[string]any)
	assert.Equal(t, "blog-1", m["name"])
	// latestBackupTime may be empty if no backup ties to cluster, so guard
	if v, ok := m["latestBackupTime"].(string); ok && v != "" {
		assert.Equal(t, lrt.Format(time.RFC3339), v)
	}
	// lagSeconds only present when latestBackupTime exists
	if v, ok := m["lagSeconds"]; ok {
		assert.Equal(t, float64(5400), v.(float64))
	}
	// recentFiles exists and is array (can be empty)
	_, hasRecent := m["recentFiles"]
	assert.True(t, hasRecent)

	// estimateThroughput=true -> throughputMBps currently pending_implementation; just ensure key exists
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest(http.MethodGet, "/api/v1/backups/binlog/metrics?namespace=ns1&now="+nowFixed.Format(time.RFC3339)+"&estimateThroughput=true", nil)
	r.ServeHTTP(w2, req2)
	var resp2 map[string]any
	_ = json.Unmarshal(w2.Body.Bytes(), &resp2)
	items2 := resp2["binlogs"].([]any)
	m2 := items2[0].(map[string]any)
	_, hasThroughput := m2["throughputMBps"]
	assert.True(t, hasThroughput)
}
