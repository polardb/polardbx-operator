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
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	domain_pxc "polardbx-dashboard-backend/pkg/api/domain/polardbxclusters"
)

func TestGetBackupScheduleNextRuns(t *testing.T) {
	scheme := runtime.NewScheme()
	polardbxv1.AddToScheme(scheme)
	// Fixed now for deterministic assertion: 2024-05-01T01:30:00Z
	nowFixed := time.Date(2024, 5, 1, 1, 30, 0, 0, time.UTC)
	ns := "ns1"

	s1 := &polardbxv1.PolarDBXBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{Name: "pbs-1", Namespace: ns},
		Spec:       polardbxv1.PolarDBXBackupScheduleSpec{Schedule: "0 2 * * *"},
		Status:     polardbxv1.PolarDBXBackupScheduleStatus{NextBackupTime: &metav1.Time{Time: nowFixed.Add(2 * time.Hour)}},
	}
	s2 := &polardbxv1.PolarDBXBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{Name: "pbs-2", Namespace: ns},
		Spec:       polardbxv1.PolarDBXBackupScheduleSpec{Schedule: "0 3 * * *"},
	}

	fakeClient := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(s1, s2).Build()
	r := gin.Default()
	r.Use(func(c *gin.Context) { c.Set("k8sClient", fakeClient) })
	r.GET("/api/v1/backup-schedules/next-run", domain_pxc.GetScheduleNextRuns)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/backup-schedules/next-run?namespace="+ns+"&now="+nowFixed.Format(time.RFC3339), nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	schedules := resp["schedules"].([]any)
	var seenTimestamp bool
	var cronComputedOK bool
	for _, it := range schedules {
		m := it.(map[string]any)
		if m["name"] == "pbs-1" {
			// should use status override, not cron
			_, ok := m["nextRunTime"].(string)
			assert.True(t, ok)
			seenTimestamp = true
		}
		if m["name"] == "pbs-2" {
			// cron fallback from 01:30Z with "0 3 * * *" => 2024-05-01T03:00:00Z
			exp := time.Date(2024, 5, 1, 3, 0, 0, 0, time.UTC).Format(time.RFC3339)
			cronComputedOK = (m["nextRunTime"] == exp)
		}
	}
	assert.True(t, seenTimestamp)
	assert.True(t, cronComputedOK)
}
