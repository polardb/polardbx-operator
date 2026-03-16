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

	domain_prechange "polardbx-dashboard-backend/pkg/api/domain/platform/prechange/handler"
)

func TestGetPrechangeChecklist(t *testing.T) {
	scheme := runtime.NewScheme()
	polardbxv1.AddToScheme(scheme)

	ns := "ns1"
	name := "c1"
	nowFixed := time.Date(2024, 5, 1, 4, 0, 0, 0, time.UTC)
	st := metav1.NewTime(nowFixed.Add(-2 * time.Hour))
	lrt := metav1.NewTime(nowFixed.Add(-90 * time.Minute))

	b := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "b1", Namespace: ns},
		Spec: polardbxv1.PolarDBXBackupSpec{
			Cluster: polardbxv1.PolarDBXClusterReference{Name: name},
		},
		Status: polardbxv1.PolarDBXBackupStatus{
			Phase:                      polardbxv1.BackupFinished,
			StartTime:                  &st,
			LatestRecoverableTimestamp: &lrt,
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(b).Build()

	r := gin.Default()
	r.Use(func(c *gin.Context) { c.Set("k8sClient", fakeClient) })
	r.GET("/api/v1/clusters/:namespace/:name/prechange-check", domain_prechange.GetPrechangeChecklist)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/clusters/ns1/c1/prechange-check?now=2024-05-01T04:00:00Z", nil)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	json.Unmarshal(w.Body.Bytes(), &resp)
	checks := resp["checks"].(map[string]any)
	assert.Equal(t, true, checks["hasRecentBackup"].(bool))
	assert.Equal(t, float64(5400), checks["rpoLagSeconds"].(float64))
}
