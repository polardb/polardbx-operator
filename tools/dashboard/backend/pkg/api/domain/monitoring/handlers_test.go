package domain_monitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"polardbx-dashboard-backend/pkg/api/domain/monitoring/service"
	spec "polardbx-dashboard-backend/pkg/api/domain/monitoring/spec"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func resetStore() {
	store = newSessionStore()
	executor = newInstallationExecutor(store)
	sessionTTL = defaultSessionTTL
	SetDetectionService(service.NewDetectionService(
		service.WithScenario(service.Scenario{
			Components: []spec.DetectedComponent{
				{Name: spec.Prometheus, Exists: boolPtr(true), Healthy: boolPtr(true)},
				{Name: spec.Grafana, Exists: boolPtr(true), Healthy: boolPtr(true)},
				{Name: spec.NodeExporter, Exists: boolPtr(true), Healthy: boolPtr(true)},
				{Name: spec.Thanos, Exists: boolPtr(true), Healthy: boolPtr(true)},
				{Name: spec.Alertmanager, Exists: boolPtr(true), Healthy: boolPtr(true)},
				{Name: spec.KubeStateMetrics, Exists: boolPtr(true), Healthy: boolPtr(true)},
				{Name: spec.BlackboxExporter, Exists: boolPtr(true), Healthy: boolPtr(true)},
			},
		}),
	))
	SetFailureDiagnosticService(nil)
}

type fakeDiagnosticService struct {
	response spec.DiagnoseResponse
	err      error

	mu        sync.Mutex
	called    bool
	lastCtx   context.Context
	lastReq   spec.DiagnoseRequest
	lastInput service.ProbeInput
}

func (f *fakeDiagnosticService) Diagnose(ctx context.Context, req spec.DiagnoseRequest, input service.ProbeInput) (spec.DiagnoseResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.called = true
	f.lastCtx = ctx
	f.lastReq = req
	f.lastInput = input
	if f.err != nil {
		return spec.DiagnoseResponse{}, f.err
	}
	return f.response, nil
}

func TestDetectEnvironment(t *testing.T) {
	resetStore()
	fixed := time.Date(2025, 10, 27, 9, 30, 0, 0, time.UTC)
	health := int32(92)
	scenario := service.Scenario{
		Components: []spec.DetectedComponent{
			{
				Name:    spec.Grafana,
				Exists:  boolPtr(true),
				Healthy: boolPtr(false),
				Version: stringPtr("v10.5.1"),
				ActionRecommendation: spec.ComponentAction{
					Action: spec.ComponentActionActionRepair,
					Reason: "Dashboard datasource mismatch",
				},
			},
		},
		HealthScore:     &health,
		Recommendations: []string{"Calibrate Grafana data source configuration"},
	}
	SetDetectionService(service.NewDetectionService(
		service.WithClock(func() time.Time { return fixed }),
		service.WithScenario(scenario),
	))
	defer SetDetectionService(nil)

	router := gin.New()
	router.GET("/monitoring/detect", DetectEnvironment)

	req := httptest.NewRequest(http.MethodGet, "/monitoring/detect?namespace=observability", nil)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)

	var snapshot spec.EnvironmentSnapshot
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &snapshot))
	require.Equal(t, "observability", snapshot.Namespace)
	require.Equal(t, fixed, snapshot.DetectedAt)
	require.Len(t, snapshot.Components, 1)
	require.Equal(t, spec.Grafana, snapshot.Components[0].Name)
	require.Equal(t, spec.ComponentActionActionRepair, snapshot.Components[0].ActionRecommendation.Action)
	require.NotNil(t, snapshot.HealthScore)
	require.Equal(t, int32(92), *snapshot.HealthScore)
	require.NotNil(t, snapshot.Recommendations)
	require.Contains(t, *snapshot.Recommendations, "Calibrate Grafana data source configuration")
}

func TestDetectEnvironmentNotImplemented(t *testing.T) {
	resetStore()
	SetDetectionService(service.NewDetectionService())
	defer SetDetectionService(nil)
	router := gin.New()
	router.GET("/monitoring/detect", DetectEnvironment)

	req := httptest.NewRequest(http.MethodGet, "/monitoring/detect", nil)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	require.Equal(t, http.StatusNotImplemented, resp.Code)

	var errBody spec.ErrorBody
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &errBody))
	require.Contains(t, errBody.Message, "not implemented")
	require.NotNil(t, errBody.Code)
	require.Equal(t, string(ErrorCodeDetectionUnavailable), string(*errBody.Code))
}

func TestDetectEnvironmentWithoutKubeconfig(t *testing.T) {
	resetStore()
	SetDetectionService(nil)
	router := gin.New()
	router.GET("/monitoring/detect", DetectEnvironment)

	req := httptest.NewRequest(http.MethodGet, "/monitoring/detect", nil)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	require.Equal(t, http.StatusUnauthorized, resp.Code)
}

func TestGetInstallStatusNotFoundIncludesErrorCode(t *testing.T) {
	resetStore()
	router := gin.New()
	router.GET("/monitoring/install/:sessionId/status", GetInstallStatus)

	req := httptest.NewRequest(http.MethodGet, "/monitoring/install/unknown/status", nil)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	require.Equal(t, http.StatusNotFound, resp.Code)

	var errBody spec.ErrorBody
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &errBody))
	require.NotNil(t, errBody.Code)
	require.Equal(t, string(ErrorCodeSessionNotFound), string(*errBody.Code))
}

func TestCreatePlan(t *testing.T) {
	resetStore()
	router := gin.New()
	router.POST("/monitoring/plan", CreatePlan)
	reqBody := spec.CreatePlanRequest{
		Detected: spec.EnvironmentSnapshot{
			Namespace:  "test-ns",
			DetectedAt: time.Now().UTC(),
			Components: []spec.DetectedComponent{
				{
					Name: spec.Prometheus,
					ActionRecommendation: spec.ComponentAction{
						Action: spec.ComponentActionActionInstall,
						Reason: "missing",
					},
				},
			},
		},
		Config: spec.InstallConfig{},
	}
	body, err := json.Marshal(reqBody)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/monitoring/plan", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)

	var out spec.CreatePlanResponse
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	require.Len(t, out.Plan.Steps, 1)
	require.Equal(t, spec.Prometheus, out.Plan.Steps[0].Component)
	require.NotZero(t, out.EstimatedDurationSeconds)
}

func TestStartAndStatusLifecycle(t *testing.T) {
	resetStore()
	router := gin.New()
	router.POST("/monitoring/install", StartInstallation)
	router.GET("/monitoring/install/:sessionId/status", GetInstallStatus)

	plan := spec.InstallationPlan{
		SessionTemplate: spec.SessionTemplate{Namespace: "demo"},
		Steps: []spec.PlanStep{
			{Order: 1, Component: spec.Prometheus, Action: spec.PlanStepActionInstall},
			{Order: 2, Component: spec.Grafana, Action: spec.PlanStepActionInstall},
		},
	}
	body, err := json.Marshal(spec.StartInstallRequest{Plan: plan})
	require.NoError(t, err)

	startReq := httptest.NewRequest(http.MethodPost, "/monitoring/install", bytes.NewReader(body))
	startReq.Header.Set("Content-Type", "application/json")
	startResp := httptest.NewRecorder()
	router.ServeHTTP(startResp, startReq)
	require.Equal(t, http.StatusAccepted, startResp.Code)

	var startOut spec.StartInstallResponse
	require.NoError(t, json.Unmarshal(startResp.Body.Bytes(), &startOut))
	require.NotEmpty(t, startOut.SessionId)

	statusReq := httptest.NewRequest(http.MethodGet, "/monitoring/install/"+startOut.SessionId+"/status", nil)
	statusResp := httptest.NewRecorder()
	router.ServeHTTP(statusResp, statusReq)
	require.Equal(t, http.StatusOK, statusResp.Code)

	var status spec.InstallStatusResponse
	require.NoError(t, json.Unmarshal(statusResp.Body.Bytes(), &status))
	require.Equal(t, startOut.SessionId, status.SessionId)
	require.GreaterOrEqual(t, len(status.Components), 2)
	require.NotNil(t, status.Progress)
}

func TestDiagnoseFailure(t *testing.T) {
	resetStore()
	plan := spec.InstallationPlan{
		SessionTemplate: spec.SessionTemplate{Namespace: "diagnostics"},
		Steps:           []spec.PlanStep{{Order: 1, Component: spec.Prometheus, Action: spec.PlanStepActionInstall}},
	}
	_, startResp, err := store.createSession(context.Background(), spec.StartInstallRequest{Plan: plan}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, startResp.SessionId)
	sessionID := startResp.SessionId

	component := spec.ComponentName(spec.Prometheus)
	errCtx := map[string]string{"sessionId": sessionID}
	reqCtx := map[string]string{"caller": "ui"}
	diagReq := spec.DiagnoseRequest{
		Context: &reqCtx,
		Error: spec.InstallError{
			Category:  spec.Timeout,
			Message:   "probe timed out",
			Component: &component,
			Context:   &errCtx,
		},
	}

	fakeResp := spec.DiagnoseResponse{
		Diagnosis: spec.Diagnosis{
			Category: spec.Timeout,
			Severity: spec.Major,
			Summary:  "analysis complete",
		},
	}
	fakeSvc := &fakeDiagnosticService{response: fakeResp}
	SetFailureDiagnosticService(fakeSvc)
	defer SetFailureDiagnosticService(nil)

	router := gin.New()
	router.POST("/monitoring/diagnose", DiagnoseFailure)

	body, err := json.Marshal(diagReq)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/monitoring/diagnose", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)

	var out spec.DiagnoseResponse
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	require.Equal(t, fakeResp, out)

	fakeSvc.mu.Lock()
	defer fakeSvc.mu.Unlock()
	require.True(t, fakeSvc.called)
	require.Equal(t, diagReq, fakeSvc.lastReq)
	require.Equal(t, sessionID, fakeSvc.lastInput.SessionID)
	require.NotNil(t, fakeSvc.lastInput.Plan)
	require.NotNil(t, fakeSvc.lastInput.Status)
	require.Equal(t, "diagnostics", fakeSvc.lastInput.Namespace)
	require.Equal(t, "ui", fakeSvc.lastInput.Context["caller"])
}

func TestApplyAutoFix(t *testing.T) {
	resetStore()
	fakeAutoFix := &fakeAutoFixService{response: spec.AutoFixResponse{Success: true}}
	SetAutoFixService(fakeAutoFix)
	defer SetAutoFixService(nil)
	router := gin.New()
	router.POST("/monitoring/install", StartInstallation)
	router.GET("/monitoring/install/:sessionId/status", GetInstallStatus)
	router.POST("/monitoring/auto-fix", ApplyAutoFix)

	plan := spec.InstallationPlan{
		SessionTemplate: spec.SessionTemplate{Namespace: "demo"},
		Steps:           []spec.PlanStep{{Order: 1, Component: spec.Prometheus, Action: spec.PlanStepActionInstall}},
	}
	body, err := json.Marshal(spec.StartInstallRequest{Plan: plan})
	require.NoError(t, err)

	startReq := httptest.NewRequest(http.MethodPost, "/monitoring/install", bytes.NewReader(body))
	startReq.Header.Set("Content-Type", "application/json")
	startResp := httptest.NewRecorder()
	router.ServeHTTP(startResp, startReq)
	require.Equal(t, http.StatusAccepted, startResp.Code)

	var startOut spec.StartInstallResponse
	require.NoError(t, json.Unmarshal(startResp.Body.Bytes(), &startOut))

	autoFixBody := spec.AutoFixRequest{FixId: "monitoring::prometheus::restart", SessionId: startOut.SessionId}
	autoFixBytes, err := json.Marshal(autoFixBody)
	require.NoError(t, err)

	autoFixReq := httptest.NewRequest(http.MethodPost, "/monitoring/auto-fix", bytes.NewReader(autoFixBytes))
	autoFixReq.Header.Set("Content-Type", "application/json")
	autoFixResp := httptest.NewRecorder()
	router.ServeHTTP(autoFixResp, autoFixReq)
	require.Equal(t, http.StatusOK, autoFixResp.Code)

	var autoFixOut spec.AutoFixResponse
	require.NoError(t, json.Unmarshal(autoFixResp.Body.Bytes(), &autoFixOut))
	require.True(t, autoFixOut.Success)

	fakeAutoFix.mu.Lock()
	defer fakeAutoFix.mu.Unlock()
	require.True(t, fakeAutoFix.called)
	require.Equal(t, autoFixBody, fakeAutoFix.lastReq)
	require.NotEmpty(t, fakeAutoFix.lastInput.Namespace)
	require.Equal(t, autoFixBody.SessionId, fakeAutoFix.lastInput.SessionID)
	require.NotNil(t, fakeAutoFix.lastInput.Plan)

	status, ok, err := store.snapshot(context.Background(), startOut.SessionId, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, status.Progress)
	require.Greater(t, *status.Progress, float32(0))
}

func TestSessionExpiration(t *testing.T) {
	resetStore()
	original := sessionTTL
	sessionTTL = 50 * time.Millisecond
	defer func() {
		sessionTTL = original
	}()

	plan := spec.InstallationPlan{
		SessionTemplate: spec.SessionTemplate{Namespace: "demo"},
		Steps:           []spec.PlanStep{{Order: 1, Component: spec.Prometheus, Action: spec.PlanStepActionInstall}},
	}

	_, startResp, err := store.createSession(context.Background(), spec.StartInstallRequest{Plan: plan}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, startResp.SessionId)

	time.Sleep(80 * time.Millisecond)

	store.cleanupExpired()

	_, ok, err := store.snapshot(context.Background(), startResp.SessionId, nil)
	require.NoError(t, err)
	require.False(t, ok, "session should be purged after TTL")
}

func TestStoreRestoreFromPersisted(t *testing.T) {
	resetStore()
	progress := float32(0.6)
	completed := []int{1}
	plan := spec.InstallationPlan{
		SessionTemplate: spec.SessionTemplate{Namespace: "demo"},
		Steps: []spec.PlanStep{
			{Order: 1, Component: spec.Prometheus, Action: spec.PlanStepActionInstall},
			{Order: 2, Component: spec.Grafana, Action: spec.PlanStepActionInstall},
		},
	}
	now := time.Now().UTC()
	snapshot := &service.PersistedSession{
		Namespace: "demo",
		Plan:      plan,
		Status: spec.InstallStatusResponse{
			SessionId:  "resume-1",
			Phase:      spec.Installing,
			Components: []spec.ComponentState{},
			Progress:   &progress,
			UpdatedAt:  now,
			StartedAt:  timePtr(now.Add(-10 * time.Minute)),
			Checkpoint: &spec.Checkpoint{
				CompletedSteps: &completed,
				Progress:       &progress,
			},
		},
	}

	restoredID, _, restored := store.restoreFromPersisted(snapshot)
	require.True(t, restored)
	require.Equal(t, "resume-1", restoredID)

	status, ok, err := store.snapshot(context.Background(), restoredID, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, status.Progress)
	require.InDelta(t, progress, *status.Progress, 0.1)
	require.NotNil(t, status.CompletedSteps)
	require.Greater(t, len(*status.CompletedSteps), 0)
	require.Equal(t, int32(1), (*status.CompletedSteps)[0].Order)
	require.NotEmpty(t, status.Components)
	found := false
	for _, comp := range status.Components {
		if comp.Name == spec.Prometheus {
			found = true
			require.Equal(t, spec.ComponentPhaseHealthy, comp.Phase)
		}
	}
	require.True(t, found, "expected prometheus component state")
}

func TestSessionBoostClampsProgress(t *testing.T) {
	resetStore()
	ctx := context.Background()
	plan := spec.InstallationPlan{
		SessionTemplate: spec.SessionTemplate{Namespace: "boost-ns"},
		Steps: []spec.PlanStep{
			{Order: 1, Component: spec.Prometheus, Action: spec.PlanStepActionInstall},
			{Order: 2, Component: spec.Grafana, Action: spec.PlanStepActionInstall},
		},
	}
	_, startResp, err := store.createSession(ctx, spec.StartInstallRequest{Plan: plan}, nil)
	require.NoError(t, err)
	sessionID := startResp.SessionId

	persist := newMemoryPersistence()

	ok, err := store.boost(ctx, sessionID, 0.9, persist)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = store.boost(ctx, sessionID, 0.2, persist)
	require.NoError(t, err)
	require.True(t, ok)

	status, got, err := store.snapshot(ctx, sessionID, nil)
	require.NoError(t, err)
	require.True(t, got)
	require.NotNil(t, status.Progress)
	require.InDelta(t, 1.0, float64(*status.Progress), 1e-6)
	require.False(t, store.shouldRun(sessionID))

	persist.mu.Lock()
	saved := persist.sessions[sessionID]
	persist.mu.Unlock()
	require.NotNil(t, saved)
	require.NotNil(t, saved.Status.Progress)
	require.InDelta(t, 1.0, float64(*saved.Status.Progress), 1e-6)
}

func TestAddErrorSchedulesRetry(t *testing.T) {
	resetStore()
	ctx := context.Background()
	plan := spec.InstallationPlan{
		SessionTemplate: spec.SessionTemplate{Namespace: "retry-ns"},
		Steps:           []spec.PlanStep{{Order: 1, Component: spec.Prometheus, Action: spec.PlanStepActionInstall}},
	}
	_, startResp, err := store.createSession(ctx, spec.StartInstallRequest{Plan: plan}, nil)
	require.NoError(t, err)
	sessionID := startResp.SessionId

	persist := newMemoryPersistence()
	comp := spec.ComponentName(spec.Prometheus)
	installErr := spec.InstallError{Category: spec.Timeout, Message: "probe timed out", Component: &comp}

	status, err := store.addError(ctx, sessionID, 2, installErr, persist)
	require.NoError(t, err)
	require.NotNil(t, status.Errors)
	require.Len(t, *status.Errors, 1)
	require.Equal(t, installErr.Message, (*status.Errors)[0].Message)

	require.NotNil(t, status.Checkpoint)
	require.NotNil(t, status.Checkpoint.CurrentStep)
	require.Equal(t, 2, *status.Checkpoint.CurrentStep)

	require.NotNil(t, status.Retry)
	require.NotNil(t, status.Retry.Mode)
	require.Equal(t, retryModeScheduled, *status.Retry.Mode)
	require.NotNil(t, status.Retry.Active)
	require.True(t, *status.Retry.Active)
	require.NotNil(t, status.Retry.BackoffSeconds)
	require.Equal(t, int64(defaultRetryDelays[0].Seconds()), *status.Retry.BackoffSeconds)
	require.NotNil(t, status.Retry.NextRetryAt)

	require.False(t, store.shouldRun(sessionID))

	persist.mu.Lock()
	saved := persist.sessions[sessionID]
	persist.mu.Unlock()
	require.NotNil(t, saved)
	require.NotNil(t, saved.Status.Errors)
	require.Len(t, *saved.Status.Errors, 1)
	require.Equal(t, installErr.Message, (*saved.Status.Errors)[0].Message)
}

func TestStartRetryClearsErrorsAndPersists(t *testing.T) {
	resetStore()
	ctx := context.Background()
	plan := spec.InstallationPlan{
		SessionTemplate: spec.SessionTemplate{Namespace: "retry-ns"},
		Steps:           []spec.PlanStep{{Order: 1, Component: spec.Prometheus, Action: spec.PlanStepActionInstall}},
	}
	_, startResp, err := store.createSession(ctx, spec.StartInstallRequest{Plan: plan}, nil)
	require.NoError(t, err)
	sessionID := startResp.SessionId

	persist := newMemoryPersistence()
	comp := spec.ComponentName(spec.Prometheus)
	installErr := spec.InstallError{Category: spec.Timeout, Message: "probe timed out", Component: &comp}

	_, err = store.addError(ctx, sessionID, 1, installErr, persist)
	require.NoError(t, err)

	status, persistedPlan, err := store.startRetry(ctx, sessionID, true, persist)
	require.NoError(t, err)
	require.NotNil(t, status.Retry)
	require.NotNil(t, status.Retry.Mode)
	require.Equal(t, retryModeRunning, *status.Retry.Mode)
	require.NotNil(t, status.Retry.Retries)
	require.Equal(t, int32(1), *status.Retry.Retries)
	require.NotNil(t, status.Errors)
	require.Len(t, *status.Errors, 0)
	require.Nil(t, status.Checkpoint.Errors)
	require.Len(t, persistedPlan.Steps, len(plan.Steps))
	require.True(t, store.shouldRun(sessionID))

	persist.mu.Lock()
	saved := persist.sessions[sessionID]
	persist.mu.Unlock()
	require.NotNil(t, saved)
	require.NotNil(t, saved.Status.Retry)
	require.NotNil(t, saved.Status.Retry.Mode)
	require.Equal(t, retryModeRunning, *saved.Status.Retry.Mode)
	if saved.Status.Errors != nil {
		require.Len(t, *saved.Status.Errors, 0)
	}
	require.Len(t, saved.Plan.Steps, len(plan.Steps))
}

func TestActivateDueRetryRespectsSchedule(t *testing.T) {
	resetStore()
	ctx := context.Background()
	plan := spec.InstallationPlan{
		SessionTemplate: spec.SessionTemplate{Namespace: "retry-ns"},
		Steps:           []spec.PlanStep{{Order: 1, Component: spec.Prometheus, Action: spec.PlanStepActionInstall}},
	}
	_, startResp, err := store.createSession(ctx, spec.StartInstallRequest{Plan: plan}, nil)
	require.NoError(t, err)
	sessionID := startResp.SessionId

	persist := newMemoryPersistence()
	comp := spec.ComponentName(spec.Prometheus)
	installErr := spec.InstallError{Category: spec.Timeout, Message: "probe timed out", Component: &comp}

	_, err = store.addError(ctx, sessionID, 1, installErr, persist)
	require.NoError(t, err)

	store.mu.Lock()
	rec := store.sessions[sessionID]
	require.NotNil(t, rec)
	rec.retry.nextRetry = time.Now().UTC().Add(5 * time.Minute)
	rec.retry.active = true
	store.mu.Unlock()

	status, persistedPlan, ran, err := store.activateDueRetry(ctx, sessionID, persist)
	require.NoError(t, err)
	require.False(t, ran)
	require.Empty(t, persistedPlan.Steps)
	require.Nil(t, status.Checkpoint)

	store.mu.Lock()
	rec = store.sessions[sessionID]
	rec.retry.nextRetry = time.Now().UTC().Add(-1 * time.Minute)
	rec.retry.active = true
	store.mu.Unlock()

	status, persistedPlan, ran, err = store.activateDueRetry(ctx, sessionID, persist)
	require.NoError(t, err)
	require.True(t, ran)
	require.NotNil(t, status.Retry)
	require.NotNil(t, status.Retry.Mode)
	require.Equal(t, retryModeRunning, *status.Retry.Mode)
	require.NotNil(t, status.Retry.Retries)
	require.Equal(t, int32(1), *status.Retry.Retries)
	require.NotNil(t, status.Errors)
	require.Len(t, *status.Errors, 0)
	require.NotNil(t, status.Checkpoint)
	require.Nil(t, status.Checkpoint.Errors)
	require.Len(t, persistedPlan.Steps, len(plan.Steps))
	require.True(t, store.shouldRun(sessionID))

	persist.mu.Lock()
	saved := persist.sessions[sessionID]
	persist.mu.Unlock()
	require.NotNil(t, saved)
	require.NotNil(t, saved.Status.Retry)
	require.Equal(t, retryModeRunning, *saved.Status.Retry.Mode)
	if saved.Status.Errors != nil {
		require.Len(t, *saved.Status.Errors, 0)
	}
}

func TestGetInstallStatusActivatesDueRetry(t *testing.T) {
	resetStore()
	ctx := context.Background()
	plan := spec.InstallationPlan{
		SessionTemplate: spec.SessionTemplate{Namespace: "retry-ns"},
		Steps:           []spec.PlanStep{{Order: 1, Component: spec.Prometheus, Action: spec.PlanStepActionInstall}},
	}
	_, startResp, err := store.createSession(ctx, spec.StartInstallRequest{Plan: plan}, nil)
	require.NoError(t, err)
	sessionID := startResp.SessionId

	comp := spec.ComponentName(spec.Prometheus)
	installErr := spec.InstallError{Category: spec.Timeout, Message: "probe timed out", Component: &comp}
	_, err = store.addError(ctx, sessionID, 1, installErr, nil)
	require.NoError(t, err)

	store.mu.Lock()
	rec := store.sessions[sessionID]
	require.NotNil(t, rec)
	rec.retry.nextRetry = time.Now().UTC().Add(-1 * time.Minute)
	rec.retry.active = true
	rec.retry.mode = retryModeScheduled
	store.mu.Unlock()

	router := gin.New()
	router.GET("/monitoring/install/:sessionId/status", GetInstallStatus)

	req := httptest.NewRequest(http.MethodGet, "/monitoring/install/"+sessionID+"/status", nil)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)

	var status spec.InstallStatusResponse
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &status))
	require.Equal(t, sessionID, status.SessionId)
	require.Equal(t, spec.Installing, status.Phase)
	require.NotNil(t, status.Retry)
	require.NotNil(t, status.Retry.Mode)
	require.Equal(t, retryModeRunning, *status.Retry.Mode)
	require.NotNil(t, status.Errors)
	require.Len(t, *status.Errors, 0)
}

func TestStartInstallationAutoExecutesPlan(t *testing.T) {
	resetStore()
	router := gin.New()
	router.POST("/monitoring/install", StartInstallation)

	plan := spec.InstallationPlan{
		SessionTemplate: spec.SessionTemplate{Namespace: "demo"},
		Steps: []spec.PlanStep{
			{Order: 1, Component: spec.Prometheus, Action: spec.PlanStepActionInstall},
			{Order: 2, Component: spec.Grafana, Action: spec.PlanStepActionInstall},
		},
	}
	body, err := json.Marshal(spec.StartInstallRequest{Plan: plan})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/monitoring/install", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	require.Equal(t, http.StatusAccepted, resp.Code)

	var out spec.StartInstallResponse
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	require.NotEmpty(t, out.SessionId)

	status := waitForSessionCompletion(t, out.SessionId, 3*time.Second)
	require.Equal(t, spec.Active, status.Phase)
	require.NotNil(t, status.CompletedSteps)
	require.Len(t, *status.CompletedSteps, len(plan.Steps))
	require.NotNil(t, status.Progress)
	require.InDelta(t, 1.0, float64(*status.Progress), 0.01)
}

func TestGetInstallStatusRestoresAndRunsExecutor(t *testing.T) {
	resetStore()
	persist := newMemoryPersistence()

	plan := spec.InstallationPlan{
		SessionTemplate: spec.SessionTemplate{Namespace: "demo"},
		Steps: []spec.PlanStep{
			{Order: 1, Component: spec.Prometheus, Action: spec.PlanStepActionInstall},
			{Order: 2, Component: spec.Grafana, Action: spec.PlanStepActionInstall},
		},
	}
	progress := float32(0.5)
	completed := []int{1}
	current := 2
	now := time.Now().UTC()
	sessionID := "resume-exec"
	persist.sessions[sessionID] = &service.PersistedSession{
		Namespace: "demo",
		Plan:      plan,
		Status: spec.InstallStatusResponse{
			SessionId:  sessionID,
			Phase:      spec.Installing,
			Components: []spec.ComponentState{},
			Progress:   &progress,
			UpdatedAt:  now,
			StartedAt:  timePtr(now.Add(-5 * time.Minute)),
			Checkpoint: &spec.Checkpoint{
				CompletedSteps: &completed,
				Progress:       &progress,
				CurrentStep:    &current,
			},
		},
	}

	router := gin.New()
	router.GET("/monitoring/install/:sessionId/status", func(c *gin.Context) {
		c.Set(persistenceContextKey, persist)
		GetInstallStatus(c)
	})

	req := httptest.NewRequest(http.MethodGet, "/monitoring/install/"+sessionID+"/status", nil)
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)

	var status spec.InstallStatusResponse
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &status))
	require.Equal(t, sessionID, status.SessionId)
	require.NotNil(t, status.CompletedSteps)
	require.Len(t, *status.CompletedSteps, 1)

	final := waitForSessionCompletion(t, sessionID, 3*time.Second)
	require.NotNil(t, final.Progress)
	require.InDelta(t, 1.0, float64(*final.Progress), 0.01)
	require.Equal(t, spec.Active, final.Phase)
	require.NotNil(t, final.CompletedSteps)
	require.Len(t, *final.CompletedSteps, len(plan.Steps))
}

type memoryPersistence struct {
	mu       sync.Mutex
	sessions map[string]*service.PersistedSession
}

func newMemoryPersistence() *memoryPersistence {
	return &memoryPersistence{sessions: map[string]*service.PersistedSession{}}
}

func (m *memoryPersistence) Save(ctx context.Context, namespace string, status spec.InstallStatusResponse, plan spec.InstallationPlan) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	clone := &service.PersistedSession{
		Namespace: namespace,
		Plan:      clonePlan(plan),
		Status:    cloneStatus(status),
	}
	if clone.Namespace == "" {
		clone.Namespace = namespace
	}
	m.sessions[status.SessionId] = clone
	return nil
}

func (m *memoryPersistence) Restore(ctx context.Context, namespace, sessionID string) (*service.PersistedSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if session, ok := m.sessions[sessionID]; ok {
		clone := &service.PersistedSession{
			Namespace: session.Namespace,
			Plan:      clonePlan(session.Plan),
			Status:    cloneStatus(session.Status),
		}
		if clone.Namespace == "" {
			clone.Namespace = namespace
		}
		return clone, nil
	}
	return nil, nil
}

func (m *memoryPersistence) RestoreAny(ctx context.Context, sessionID string) (*service.PersistedSession, error) {
	return m.Restore(ctx, "", sessionID)
}

func (m *memoryPersistence) Delete(ctx context.Context, namespace, sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, sessionID)
	return nil
}

func cloneStatus(status spec.InstallStatusResponse) spec.InstallStatusResponse {
	clone := status
	clone.Components = append([]spec.ComponentState{}, status.Components...)
	if status.CurrentStep != nil {
		step := *status.CurrentStep
		clone.CurrentStep = &step
	}
	if status.CompletedSteps != nil {
		steps := append([]spec.ExecutionStep{}, (*status.CompletedSteps)...)
		clone.CompletedSteps = &steps
	}
	if status.Errors != nil {
		errs := append([]spec.InstallError{}, (*status.Errors)...)
		clone.Errors = &errs
	}
	if status.Checkpoint != nil {
		clone.Checkpoint = cloneCheckpoint(status.Checkpoint)
	}
	if status.Progress != nil {
		progress := *status.Progress
		clone.Progress = &progress
	}
	if status.StartedAt != nil {
		started := *status.StartedAt
		clone.StartedAt = &started
	}
	return clone
}

func waitForSessionCompletion(t *testing.T, sessionID string, timeout time.Duration) spec.InstallStatusResponse {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		status, ok, err := store.snapshot(context.Background(), sessionID, nil)
		require.NoError(t, err)
		if ok && status.Progress != nil && *status.Progress >= 0.99 {
			return status
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("session %s did not complete before timeout", sessionID)
	return spec.InstallStatusResponse{}
}

type fakeAutoFixService struct {
	mu        sync.Mutex
	response  spec.AutoFixResponse
	err       error
	called    bool
	lastReq   spec.AutoFixRequest
	lastInput service.AutoFixInput
}

func (f *fakeAutoFixService) Apply(ctx context.Context, req spec.AutoFixRequest, input service.AutoFixInput) (spec.AutoFixResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.called = true
	f.lastReq = req
	f.lastInput = input
	if f.err != nil {
		return spec.AutoFixResponse{}, f.err
	}
	return f.response, nil
}
