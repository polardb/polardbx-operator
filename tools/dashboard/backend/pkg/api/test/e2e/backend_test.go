package e2e

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"polardbx-dashboard-backend/pkg/api/test/fixtures"
	"polardbx-dashboard-backend/pkg/api/util"
)

// setupRouterE2E sets up a test router with necessary routes and injected fake clients.
func setupRouterE2E(t *testing.T, objs ...runtime.Object) *gin.Engine {
	return fixtures.SetupRouterE2E(t, objs...)
}

func TestE2E_AlertsAggregate_WithAMAndEvents(t *testing.T) {
	// mock Alertmanager server
	am := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/alerts" {
			payload := []map[string]any{
				{
					"labels": map[string]string{
						"severity":  "warning",
						"namespace": "default",
						"alertname": "TestAlert",
					},
					"annotations": map[string]string{
						"summary": "AM test summary",
					},
					"startsAt": time.Now().UTC().Format(time.RFC3339),
				},
			}
			_ = json.NewEncoder(w).Encode(payload)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer am.Close()

	// k8s Event
	ev := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "ev1"},
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Name:      "demo-cluster-pod",
			Namespace: "default",
		},
		Message:       "Pod restarted",
		Type:          corev1.EventTypeNormal,
		LastTimestamp: metav1.NewTime(time.Now().UTC()),
		Reason:        "Restarted",
	}

	router := setupRouterE2E(t, ev)

	// 1) without cluster filter
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/alerts?namespace=default&alertmanager="+am.URL, nil)
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	items, _ := resp["items"].([]any)
	assert.GreaterOrEqual(t, len(items), 1)

	// verify timestamp & labels
	first := items[0].(map[string]any)
	_, hasTS := first["timestamp"]
	assert.True(t, hasTS, "timestamp should exist")

	// 2) with cluster filter ensures labels.cluster present for event items
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest("GET", "/api/v1/alerts?namespace=default&cluster=demo&alertmanager="+am.URL, nil)
	router.ServeHTTP(w2, req2)
	assert.Equal(t, http.StatusOK, w2.Code)
	var resp2 map[string]any
	_ = json.Unmarshal(w2.Body.Bytes(), &resp2)
	items2, _ := resp2["items"].([]any)
	foundCluster := false
	for _, it := range items2 {
		m := it.(map[string]any)
		if m["source"] == "k8s-event" {
			if lbl, ok := m["labels"].(map[string]any); ok {
				if _, ok2 := lbl["cluster"]; ok2 {
					foundCluster = true
					break
				}
			}
		}
	}
	assert.True(t, foundCluster, "k8s-event labels.cluster should be present when cluster filter provided")
}

func TestE2E_MonitoringBootstrapStatus_NotFoundAndSucceeded(t *testing.T) {
	router := setupRouterE2E(t)

	// Not found
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/monitoring/bootstrap/status?jobName=not-exist&namespace=polardbx-operator-system", nil)
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)

	// Succeeded: create Job with Complete condition
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: util.DefaultNamespace(&gin.Context{Request: req}, "polardbx-operator-system"), Name: "job-ok"},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}},
		},
	}
	router = setupRouterE2E(t, job)
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest("GET", "/api/v1/monitoring/bootstrap/status?jobName=job-ok&namespace=polardbx-operator-system", nil)
	router.ServeHTTP(w2, req2)
	assert.Equal(t, http.StatusOK, w2.Code)
}

func TestE2E_LogsBootstrapStatus_NotFound(t *testing.T) {
	router := setupRouterE2E(t)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/logs/bootstrap/status?jobName=none&namespace=polardbx-operator-system", nil)
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}
