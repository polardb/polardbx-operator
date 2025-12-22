package domain_monitoring

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"polardbx-dashboard-backend/pkg/api/domain/monitoring/service"
	spec "polardbx-dashboard-backend/pkg/api/domain/monitoring/spec"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
	"polardbx-dashboard-backend/pkg/config"
)

var (
	defaultSessionTTL = 24 * time.Hour
	sessionTTL        = resolveSessionTTL()
	detectionSvc      = service.NewDetectionService()
	customDetection   bool
	diagnosticsSvc    = service.NewFailureDiagnosticService()
	customDiagnostics bool
	autoFixSvc        = service.NewAutoFixService()
	customAutoFix     bool
	logger            = ctrllog.Log.WithName("monitoring")
)

var (
	store    = newSessionStore()
	executor = newInstallationExecutor(store)
)

var (
	defaultRetryDelays = []time.Duration{1 * time.Minute, 2 * time.Minute, 5 * time.Minute}
)

const defaultMaxRetries = 3

const (
	retryModeIdle      spec.RetryStatusMode = "idle"
	retryModeScheduled spec.RetryStatusMode = "scheduled"
	retryModeRunning   spec.RetryStatusMode = "running"
	retryModeExhausted spec.RetryStatusMode = "exhausted"
)

var (
	errRetryUnavailable = errors.New("retry unavailable")
	errRetryLimit       = errors.New("retry limit reached")
)

const persistenceContextKey = "monitoringv2/persist"

func SetDetectionService(svc service.DetectionService) {
	if svc == nil {
		detectionSvc = service.NewDetectionService()
		customDetection = false
		return
	}
	detectionSvc = svc
	customDetection = true
}

func prepareAutoFixInput(ctx context.Context, req spec.AutoFixRequest, persist sessionPersistence) (service.AutoFixInput, bool, error) {
	input := service.AutoFixInput{SessionID: req.SessionId}
	ctxValues := mergeStringMap(nil, req.Context)
	if len(ctxValues) > 0 {
		input.Context = ctxValues
	}
	input.Namespace = firstNonEmptyFromMap(ctxValues, "namespace", "Namespace", "targetNamespace")
	input.Component = componentFromFixID(req.FixId)

	if req.SessionId == "" {
		return input, false, nil
	}

	status, ok, err := store.snapshot(ctx, req.SessionId, persist)
	if err != nil {
		return input, false, err
	}
	if !ok && persist != nil {
		restored, err := persist.RestoreAny(ctx, req.SessionId)
		if err != nil {
			return input, false, err
		}
		if restored != nil {
			restoredID, restoredPlan, restored := store.restoreFromPersisted(restored)
			if restored {
				input.SessionID = restoredID
				clone := clonePlan(restoredPlan)
				input.Plan = &clone
				status, ok, err = store.snapshot(ctx, restoredID, persist)
				if err != nil {
					return input, false, err
				}
			}
		}
	}

	if !ok {
		return input, false, nil
	}

	input.Status = &status
	input.Checkpoint = status.Checkpoint
	if status.Checkpoint != nil {
		input.Context = mergeStringMap(input.Context, status.Checkpoint.Context)
		if input.Namespace == "" && status.Checkpoint.Context != nil {
			input.Namespace = firstNonEmptyFromMap(*status.Checkpoint.Context, "namespace", "targetNamespace")
		}
	}

	if input.Plan == nil {
		if plan, exists := store.planFor(input.SessionID); exists {
			clone := clonePlan(plan)
			input.Plan = &clone
		}
	}
	if input.Plan != nil && input.Namespace == "" {
		if ns := strings.TrimSpace(input.Plan.SessionTemplate.Namespace); ns != "" {
			input.Namespace = ns
		} else {
			input.Namespace = namespaceFallback(input.Plan)
		}
	}
	if input.Namespace == "" {
		input.Namespace = service.DefaultMonitoringNamespace
	}
	if input.Context != nil && len(input.Context) == 0 {
		input.Context = nil
	}
	return input, true, nil
}

func componentFromFixID(fixID string) *spec.ComponentName {
	parts := strings.Split(strings.TrimSpace(fixID), "::")
	if len(parts) < 3 {
		return nil
	}
	component := spec.ComponentName(parts[1])
	return &component
}

func getDetectionService() service.DetectionService {
	if detectionSvc == nil {
		detectionSvc = service.NewDetectionService()
	}
	return detectionSvc
}

func SetFailureDiagnosticService(svc service.FailureDiagnosticService) {
	if svc == nil {
		diagnosticsSvc = service.NewFailureDiagnosticService()
		customDiagnostics = false
		return
	}
	diagnosticsSvc = svc
	customDiagnostics = true
}

func getFailureDiagnosticService() service.FailureDiagnosticService {
	if diagnosticsSvc == nil {
		diagnosticsSvc = service.NewFailureDiagnosticService()
	}
	return diagnosticsSvc
}

func SetAutoFixService(svc service.AutoFixService) {
	if svc == nil {
		autoFixSvc = service.NewAutoFixService()
		customAutoFix = false
		return
	}
	autoFixSvc = svc
	customAutoFix = true
}

func getAutoFixService() service.AutoFixService {
	if autoFixSvc == nil {
		autoFixSvc = service.NewAutoFixService()
	}
	return autoFixSvc
}

func resolveSessionTTL() time.Duration {
	raw := os.Getenv("MONITORING_V2_SESSION_TTL")
	if raw == "" {
		return defaultSessionTTL
	}
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		return d
	}
	return defaultSessionTTL
}

func DetectEnvironment(c *gin.Context) {
	namespace := c.DefaultQuery("namespace", "polardbx-monitor")
	logger.WithValues("namespace", namespace).Info("detect environment requested")

	ctx := c.Request.Context()
	if !customDetection {
		cli, ok := util.K8sClientFromContext(c)
		if !ok {
			return
		}
		ctx = service.ContextWithControllerClient(ctx, cli)
		if cs, ok := util.ClientsetFromContext(c); ok {
			ctx = service.ContextWithClientset(ctx, cs)
		}
	}

	snapshot, err := getDetectionService().Detect(ctx, namespace)
	if err != nil {
		if errors.Is(err, service.ErrDetectionNotImplemented) {
			respondError(c, http.StatusNotImplemented, ErrorCodeDetectionUnavailable, "monitoring environment detection not implemented")
			return
		}
		respondError(c, http.StatusInternalServerError, ErrorCodeDetectionFailed, "failed to detect environment: "+err.Error(), map[string]string{"error": err.Error()})
		return
	}

	if snapshot.Namespace == "" {
		snapshot.Namespace = namespace
	}
	if snapshot.DetectedAt.IsZero() {
		snapshot.DetectedAt = time.Now().UTC()
	}
	logger.WithValues("namespace", snapshot.Namespace, "components", len(snapshot.Components)).Info("detect environment succeeded")

	apierr.OK(c, snapshot)
}

func CreatePlan(c *gin.Context) {
	var req spec.CreatePlanRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, ErrorCodePlanInvalid, "invalid plan request: "+err.Error(), map[string]string{"error": err.Error()})
		return
	}

	steps := buildPlanSteps(req)
	estimated := int64(len(steps)*120 + 120)
	risk := spec.Low

	plan := spec.InstallationPlan{
		SessionTemplate:          buildSessionTemplate(req),
		Steps:                    steps,
		EstimatedDurationSeconds: &estimated,
		RiskLevel:                &risk,
	}
	logger.WithValues(
		"namespace", plan.SessionTemplate.Namespace,
		"steps", len(steps),
	).Info("installation plan generated")

	warnings := []string{}
	if len(steps) == 0 {
		warnings = append(warnings, "No actionable steps detected; monitoring components appear healthy")
	}

	resp := spec.CreatePlanResponse{
		Plan:                     plan,
		EstimatedDurationSeconds: estimated,
	}
	if len(warnings) > 0 {
		resp.Warnings = &warnings
	}

	apierr.OK(c, resp)
}

func StartInstallation(c *gin.Context) {
	var req spec.StartInstallRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, ErrorCodeInstallInvalid, "invalid start request: "+err.Error(), map[string]string{"error": err.Error()})
		return
	}
	if len(req.Plan.Steps) == 0 {
		respondError(c, http.StatusBadRequest, ErrorCodePlanEmpty, "installation plan must contain at least one step")
		return
	}

	execCtx, ready := executionContextFromRequest(c)
	if !ready {
		logger.WithValues("endpoint", "start").Info("kubernetes client not initialized")
		respondError(c, http.StatusUnauthorized, ErrorCodeKubernetesClientMissing, "kubernetes client not initialized")
		return
	}

	ctx := c.Request.Context()
	persist := checkpointManagerFromContext(c)
	rec, startResp, err := store.createSession(ctx, req, persist)
	if err != nil {
		logger.Error(err, "failed to create installation session")
		respondError(c, http.StatusInternalServerError, ErrorCodeSessionCreateFailed, "failed to create session: "+err.Error(), map[string]string{"error": err.Error()})
		return
	}
	resumed := startResp.Resumed != nil && *startResp.Resumed
	logger.WithValues(
		"sessionId", rec.id,
		"namespace", rec.namespace,
		"steps", len(rec.plan.Steps),
		"resumed", resumed,
	).Info("installation session accepted")

	executor.start(execCtx, rec.id, clonePlan(rec.plan), persist)
	apierr.Accepted(c, startResp)
}

func GetInstallStatus(c *gin.Context) {
	sessionID := c.Param("sessionId")
	if sessionID == "" {
		respondError(c, http.StatusBadRequest, ErrorCodeSessionRequired, "sessionId is required")
		return
	}
	logger.WithValues("sessionId", sessionID).Info("get install status invoked")

	ctx := c.Request.Context()
	persist := checkpointManagerFromContext(c)
	status, ok, err := store.snapshot(ctx, sessionID, persist)
	if err != nil {
		respondError(c, http.StatusInternalServerError, ErrorCodeSessionPersistFailed, "failed to persist session status: "+err.Error(), map[string]string{"error": err.Error()})
		return
	}
	if !ok {
		if persist != nil {
			restored, err := persist.RestoreAny(ctx, sessionID)
			if err != nil {
				logger.Error(err, "failed to restore session from persistence", "sessionId", sessionID)
				respondError(c, http.StatusInternalServerError, ErrorCodeSessionRestoreFailed, "failed to restore session: "+err.Error(), map[string]string{"error": err.Error()})
				return
			}
			if restored != nil {
				restoredID, plan, restored := store.restoreFromPersisted(restored)
				if restored {
					logger.WithValues("sessionId", restoredID, "restored", true).Info("session restored from persistence")
					maybeStartExecutor(c, restoredID, &plan, persist)
					status, ok, err = store.snapshot(ctx, restoredID, persist)
					if err != nil {
						logger.Error(err, "failed to persist session status after restore", "sessionId", restoredID)
						respondError(c, http.StatusInternalServerError, ErrorCodeSessionPersistFailed, "failed to persist session status: "+err.Error(), map[string]string{"error": err.Error()})
						return
					}
					if ok {
						activatedStatus, activatedPlan, activated, err := store.activateDueRetry(ctx, restoredID, persist)
						if err != nil {
							logger.Error(err, "failed to activate scheduled retry after restore", "sessionId", restoredID)
							respondError(c, http.StatusInternalServerError, ErrorCodeRetryActivateFailed, "failed to activate retry: "+err.Error(), map[string]string{"error": err.Error()})
							return
						}
						if activated {
							maybeStartExecutor(c, restoredID, &activatedPlan, persist)
							apierr.OK(c, activatedStatus)
							return
						}
						logger.WithValues("sessionId", restoredID).Info("returning restored session status")
						apierr.OK(c, status)
						return
					}
				}
			}
		}
		logger.WithValues("sessionId", sessionID).Info("session not found")
		respondError(c, http.StatusNotFound, ErrorCodeSessionNotFound, fmt.Sprintf("session %s not found", sessionID))
		return
	}

	activatedStatus, activatedPlan, activated, err := store.activateDueRetry(ctx, sessionID, persist)
	if err != nil {
		logger.Error(err, "failed to activate scheduled retry", "sessionId", sessionID)
		respondError(c, http.StatusInternalServerError, ErrorCodeRetryActivateFailed, "failed to activate retry: "+err.Error(), map[string]string{"error": err.Error()})
		return
	}
	if activated {
		maybeStartExecutor(c, sessionID, &activatedPlan, persist)
		apierr.OK(c, activatedStatus)
		return
	}

	maybeStartExecutor(c, sessionID, nil, persist)
	logger.WithValues("sessionId", sessionID, "phase", status.Phase).Info("returning session status")

	apierr.OK(c, status)
}

func TriggerRetry(c *gin.Context) {
	sessionID := c.Param("sessionId")
	if sessionID == "" {
		respondError(c, http.StatusBadRequest, ErrorCodeSessionRequired, "sessionId is required")
		return
	}

	var req spec.RetryRequest
	hasPayload := false
	if err := c.ShouldBindJSON(&req); err != nil {
		if !errors.Is(err, io.EOF) {
			respondError(c, http.StatusBadRequest, ErrorCodeRetryInvalidRequest, "invalid retry request: "+err.Error(), map[string]string{"error": err.Error()})
			return
		}
	} else {
		hasPayload = true
	}

	mode := spec.Manual
	if hasPayload && req.Mode != nil {
		switch *req.Mode {
		case spec.Automatic:
			mode = spec.Automatic
		case spec.Manual:
			mode = spec.Manual
		default:
			respondError(c, http.StatusBadRequest, ErrorCodeRetryInvalidMode, "invalid retry mode")
			return
		}
	}
	force := hasPayload && req.Force != nil && *req.Force
	reason := ""
	if hasPayload && req.Reason != nil {
		reason = strings.TrimSpace(*req.Reason)
	}

	log := logger.WithValues(
		"sessionId", sessionID,
		"mode", mode,
		"force", force,
	)
	if reason != "" {
		log = log.WithValues("reason", reason)
	}
	log.Info("retry requested")

	ctx := c.Request.Context()
	persist := checkpointManagerFromContext(c)

	if mode == spec.Automatic && force {
		mode = spec.Manual
	}

	if mode == spec.Automatic {
		status, plan, activated, err := store.activateDueRetry(ctx, sessionID, persist)
		if err != nil {
			logger.Error(err, "failed to activate scheduled retry", "sessionId", sessionID)
			respondError(c, http.StatusInternalServerError, ErrorCodeRetryActivateFailed, "failed to activate retry: "+err.Error(), map[string]string{"error": err.Error()})
			return
		}
		if activated {
			maybeStartExecutor(c, sessionID, &plan, persist)
			apierr.Accepted(c, status)
			return
		}
		status, ok, err := store.snapshot(ctx, sessionID, persist)
		if err != nil {
			logger.Error(err, "failed to persist session status during retry snapshot", "sessionId", sessionID)
			respondError(c, http.StatusInternalServerError, ErrorCodeRetryPersistFailed, "failed to persist session status: "+err.Error(), map[string]string{"error": err.Error()})
			return
		}
		if !ok {
			respondError(c, http.StatusNotFound, ErrorCodeSessionNotFound, fmt.Sprintf("session %s not found", sessionID))
			return
		}
		apierr.Accepted(c, status)
		return
	}

	status, plan, err := store.startRetry(ctx, sessionID, force, persist)
	if err != nil {
		switch {
		case errors.Is(err, errRetryUnavailable):
			respondError(c, http.StatusConflict, ErrorCodeRetryUnavailable, "retry unavailable: installation is not in a failed state")
			return
		case errors.Is(err, errRetryLimit):
			respondError(c, http.StatusTooManyRequests, ErrorCodeRetryLimitReached, "retry limit reached for this session")
			return
		default:
			if strings.Contains(err.Error(), "not found") {
				respondError(c, http.StatusNotFound, ErrorCodeSessionNotFound, fmt.Sprintf("session %s not found", sessionID))
				return
			}
			logger.Error(err, "failed to start retry", "sessionId", sessionID)
			respondError(c, http.StatusInternalServerError, ErrorCodeRetryStartFailed, "failed to start retry: "+err.Error(), map[string]string{"error": err.Error()})
			return
		}
	}

	maybeStartExecutor(c, sessionID, &plan, persist)
	apierr.Accepted(c, status)
}

func DiagnoseFailure(c *gin.Context) {
	var req spec.DiagnoseRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, ErrorCodeDiagnoseInvalid, "invalid diagnose request: "+err.Error(), map[string]string{"error": err.Error()})
		return
	}
	component := componentName(req.Error.Component)
	logger.WithValues("component", component, "category", req.Error.Category).Info("diagnosis requested")

	diagCtx := diagnosticContextFromRequest(c)
	persist := checkpointManagerFromContext(c)
	input, err := prepareProbeInput(diagCtx, req, persist)
	if err != nil {
		logger.Error(err, "failed to prepare diagnostic context", "component", component)
		respondError(c, http.StatusInternalServerError, ErrorCodeDiagnoseContextFailed, "failed to prepare diagnostic context: "+err.Error(), map[string]string{"error": err.Error()})
		return
	}

	resp, err := getFailureDiagnosticService().Diagnose(diagCtx, req, input)
	if err != nil {
		logger.Error(err, "diagnosis execution failed", "component", component, "sessionId", input.SessionID)
		respondError(c, http.StatusInternalServerError, ErrorCodeDiagnoseFailed, "failed to diagnose failure: "+err.Error(), map[string]string{"error": err.Error()})
		return
	}

	autoFixCount := 0
	if resp.AutoFixes != nil {
		autoFixCount = len(*resp.AutoFixes)
	}
	logger.WithValues(
		"component", component,
		"category", req.Error.Category,
		"sessionId", input.SessionID,
		"namespace", input.Namespace,
		"autoFixes", autoFixCount,
	).Info("diagnosis response generated")

	apierr.OK(c, resp)
}

func diagnosticContextFromRequest(c *gin.Context) context.Context {
	ctx := c.Request.Context()
	if customDiagnostics {
		return ctx
	}
	if cli, ok := util.K8sClientFromContext(c); ok {
		ctx = service.ContextWithControllerClient(ctx, cli)
	}
	if cs, ok := util.ClientsetFromContext(c); ok {
		ctx = service.ContextWithClientset(ctx, cs)
	}
	return ctx
}

func prepareProbeInput(ctx context.Context, req spec.DiagnoseRequest, persist sessionPersistence) (service.ProbeInput, error) {
	input := service.ProbeInput{}
	contextValues := mergeStringMap(nil, req.Context)
	contextValues = mergeStringMap(contextValues, req.Error.Context)
	if len(contextValues) > 0 {
		input.Context = contextValues
	}
	input.SessionID = firstNonEmptyFromMap(contextValues, "sessionId", "sessionID", "SessionId")
	input.Namespace = firstNonEmptyFromMap(contextValues, "namespace", "Namespace", "targetNamespace")

	if input.SessionID == "" {
		return input, nil
	}

	status, ok, err := store.snapshot(ctx, input.SessionID, persist)
	if err != nil {
		return input, err
	}
	if !ok && persist != nil {
		restored, err := persist.RestoreAny(ctx, input.SessionID)
		if err != nil {
			return input, err
		}
		if restored != nil {
			restoredID, restoredPlan, restored := store.restoreFromPersisted(restored)
			if restored {
				input.SessionID = restoredID
				planClone := clonePlan(restoredPlan)
				input.Plan = &planClone
				status, ok, err = store.snapshot(ctx, restoredID, persist)
				if err != nil {
					return input, err
				}
				if input.Namespace == "" {
					input.Namespace = restoredPlan.SessionTemplate.Namespace
				}
			}
		}
	}

	if ok {
		input.Status = &status
		input.Checkpoint = status.Checkpoint
		if status.Checkpoint != nil {
			input.Context = mergeStringMap(input.Context, status.Checkpoint.Context)
			if input.Namespace == "" && status.Checkpoint.Context != nil {
				input.Namespace = firstNonEmptyFromMap(*status.Checkpoint.Context, "namespace", "targetNamespace")
			}
		}
	}

	if input.Plan == nil {
		if plan, exists := store.planFor(input.SessionID); exists {
			clone := clonePlan(plan)
			input.Plan = &clone
		}
	}
	if input.Plan != nil && input.Namespace == "" {
		if ns := strings.TrimSpace(input.Plan.SessionTemplate.Namespace); ns != "" {
			input.Namespace = ns
		} else {
			input.Namespace = namespaceFallback(input.Plan)
		}
	}

	if input.Context != nil && input.Namespace == "" {
		input.Namespace = firstNonEmptyFromMap(input.Context, "namespace", "targetNamespace")
	}

	if input.Context != nil && len(input.Context) == 0 {
		input.Context = nil
	}

	return input, nil
}

func ApplyAutoFix(c *gin.Context) {
	var req spec.AutoFixRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, ErrorCodeAutoFixInvalid, "invalid auto-fix request: "+err.Error(), map[string]string{"error": err.Error()})
		return
	}
	if req.SessionId == "" {
		respondError(c, http.StatusBadRequest, ErrorCodeSessionRequired, "sessionId is required")
		return
	}

	autoCtx := c.Request.Context()
	if customAutoFix {
		if v, ok := c.Get("k8sClient"); ok {
			if cli, ok := v.(client.Client); ok && cli != nil {
				autoCtx = service.ContextWithControllerClient(autoCtx, cli)
			}
		}
	} else {
		cli, ok := util.K8sClientFromContext(c)
		if !ok {
			logger.WithValues("sessionId", req.SessionId, "fixId", req.FixId).Info("auto-fix aborted: kubernetes client not initialized")
			respondError(c, http.StatusUnauthorized, ErrorCodeKubernetesClientMissing, "kubernetes client not initialized")
			return
		}
		autoCtx = service.ContextWithControllerClient(autoCtx, cli)
	}
	if cs, ok := util.ClientsetFromContext(c); ok {
		autoCtx = service.ContextWithClientset(autoCtx, cs)
	}

	persist := checkpointManagerFromContext(c)
	input, found, err := prepareAutoFixInput(autoCtx, req, persist)
	if err != nil {
		logger.Error(err, "failed to prepare auto-fix context", "sessionId", req.SessionId, "fixId", req.FixId)
		respondError(c, http.StatusInternalServerError, ErrorCodeAutoFixContextFailed, "failed to prepare auto-fix context: "+err.Error(), map[string]string{"error": err.Error()})
		return
	}
	if !found {
		logger.WithValues("sessionId", req.SessionId, "fixId", req.FixId).Info("auto-fix session not found")
		respondError(c, http.StatusNotFound, ErrorCodeSessionNotFound, fmt.Sprintf("session %s not found", req.SessionId))
		return
	}

	resp, err := getAutoFixService().Apply(autoCtx, req, input)
	if err != nil {
		logger.Error(err, "auto-fix execution failed", "sessionId", input.SessionID, "fixId", req.FixId)
		respondError(c, http.StatusInternalServerError, ErrorCodeAutoFixFailed, "failed to execute auto-fix: "+err.Error(), map[string]string{"error": err.Error()})
		return
	}

	if resp.Success {
		if ok, boostErr := store.boost(autoCtx, input.SessionID, 0.2, persist); boostErr != nil {
			logger.Error(boostErr, "failed to persist auto-fix progress", "sessionId", input.SessionID, "fixId", req.FixId)
		} else if !ok {
			logger.WithValues("sessionId", input.SessionID, "fixId", req.FixId).Info("auto-fix progress boost skipped: session missing")
		}
	}

	if resp.Message == nil {
		msg := fmt.Sprintf("Auto-fix %s processed", req.FixId)
		resp.Message = &msg
	}
	logger.WithValues(
		"sessionId", input.SessionID,
		"fixId", req.FixId,
		"success", resp.Success,
	).Info("auto-fix completed")
	apierr.OK(c, resp)
}

// ---- helpers & state management ----

type sessionPersistence interface {
	Save(ctx context.Context, namespace string, status spec.InstallStatusResponse, plan spec.InstallationPlan) error
	Restore(ctx context.Context, namespace, sessionID string) (*service.PersistedSession, error)
	RestoreAny(ctx context.Context, sessionID string) (*service.PersistedSession, error)
	Delete(ctx context.Context, namespace, sessionID string) error
}

type sessionRecord struct {
	id          string
	plan        spec.InstallationPlan
	checkpoint  *spec.Checkpoint
	started     time.Time
	estimated   time.Duration
	manual      float64
	lastTouched time.Time
	namespace   string
	currentStep *int32
	errors      []spec.InstallError
	retry       retryState
}

type sessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*sessionRecord
}

type retryState struct {
	retries     int
	maxRetries  int
	nextRetry   time.Time
	active      bool
	mode        spec.RetryStatusMode
	lastError   string
	lastAttempt time.Time
	backoff     time.Duration
}

func newRetryState() retryState {
	max := defaultMaxRetries
	if len(defaultRetryDelays) > max {
		max = len(defaultRetryDelays)
	}
	return retryState{
		maxRetries: max,
		mode:       retryModeIdle,
	}
}

func (r *retryState) ensureDefaults() {
	if r.maxRetries <= 0 {
		r.maxRetries = len(defaultRetryDelays)
		if r.maxRetries <= 0 {
			r.maxRetries = defaultMaxRetries
		}
	}
	if r.mode == "" {
		r.mode = retryModeIdle
	}
}

func (r *retryState) nextBackoff() time.Duration {
	r.ensureDefaults()
	if len(defaultRetryDelays) == 0 {
		return 0
	}
	index := r.retries
	if index < 0 {
		index = 0
	}
	if index >= len(defaultRetryDelays) {
		return defaultRetryDelays[len(defaultRetryDelays)-1]
	}
	return defaultRetryDelays[index]
}

func (r *retryState) toSpec() *spec.RetryStatus {
	r.ensureDefaults()
	status := spec.RetryStatus{
		Retries:    int32Ptr(int32(r.retries)),
		MaxRetries: int32Ptr(int32(r.maxRetries)),
	}
	if r.mode != "" {
		mode := r.mode
		status.Mode = &mode
	}
	status.Active = boolPtr(r.active)
	if !r.nextRetry.IsZero() {
		next := r.nextRetry
		status.NextRetryAt = &next
	}
	if !r.lastAttempt.IsZero() {
		last := r.lastAttempt
		status.LastAttemptAt = &last
	}
	if r.lastError != "" {
		err := r.lastError
		status.LastError = &err
	}
	if r.backoff > 0 {
		secs := int64(r.backoff.Seconds())
		status.BackoffSeconds = &secs
	}
	return &status
}

func (r *retryState) ingest(status spec.RetryStatus) {
	r.ensureDefaults()
	if status.Retries != nil {
		r.retries = int(*status.Retries)
	}
	if status.MaxRetries != nil {
		r.maxRetries = int(*status.MaxRetries)
	}
	if status.NextRetryAt != nil {
		r.nextRetry = status.NextRetryAt.UTC()
	} else {
		r.nextRetry = time.Time{}
	}
	if status.LastAttemptAt != nil {
		r.lastAttempt = status.LastAttemptAt.UTC()
	} else {
		r.lastAttempt = time.Time{}
	}
	if status.LastError != nil {
		r.lastError = *status.LastError
	} else {
		r.lastError = ""
	}
	if status.Mode != nil {
		r.mode = *status.Mode
	}
	if status.Active != nil {
		r.active = *status.Active
	} else {
		r.active = false
	}
	if status.BackoffSeconds != nil {
		r.backoff = time.Duration(*status.BackoffSeconds) * time.Second
	} else {
		r.backoff = 0
	}
	r.ensureDefaults()
}

func newSessionStore() *sessionStore {
	return &sessionStore{sessions: map[string]*sessionRecord{}}
}

func (rec *sessionRecord) syncFromStatus(status spec.InstallStatusResponse) {
	if status.Progress != nil {
		if progress := float64(*status.Progress); progress > rec.manual {
			rec.manual = progress
		}
	}
	if status.Errors != nil {
		rec.errors = append([]spec.InstallError{}, (*status.Errors)...)
	} else {
		rec.errors = nil
	}
	if status.Retry != nil {
		rec.retry.ingest(*status.Retry)
	} else if len(rec.errors) == 0 {
		rec.retry = newRetryState()
	}
	rec.applyCheckpoint(cloneCheckpoint(status.Checkpoint))
}

func (rec *sessionRecord) applyCheckpoint(cp *spec.Checkpoint) {
	rec.checkpoint = cp
	rec.currentStep = nil
	if cp == nil {
		return
	}
	if cp.Progress != nil {
		if progress := float64(*cp.Progress); progress > rec.manual {
			rec.manual = progress
		}
	}
	if cp.CurrentStep != nil {
		val := int32(*cp.CurrentStep)
		rec.currentStep = &val
	}
}

func (rec *sessionRecord) scheduleAutoRetry(now time.Time) {
	rec.retry.ensureDefaults()
	if rec.retry.retries >= rec.retry.maxRetries {
		rec.retry.active = false
		rec.retry.mode = retryModeExhausted
		rec.retry.nextRetry = time.Time{}
		rec.retry.backoff = 0
		return
	}
	delay := rec.retry.nextBackoff()
	if delay <= 0 {
		delay = 30 * time.Second
	}
	rec.retry.backoff = delay
	rec.retry.nextRetry = now.Add(delay)
	rec.retry.active = true
	rec.retry.mode = retryModeScheduled
}

func (s *sessionStore) startRetryLocked(rec *sessionRecord, now time.Time, force bool) (spec.InstallStatusResponse, spec.InstallationPlan, string, error) {
	rec.retry.ensureDefaults()
	if len(rec.errors) == 0 {
		return spec.InstallStatusResponse{}, spec.InstallationPlan{}, "", errRetryUnavailable
	}
	if !force && rec.retry.maxRetries > 0 && rec.retry.retries >= rec.retry.maxRetries {
		rec.retry.active = false
		rec.retry.mode = retryModeExhausted
		rec.retry.nextRetry = time.Time{}
		rec.retry.backoff = 0
		return spec.InstallStatusResponse{}, spec.InstallationPlan{}, "", errRetryLimit
	}
	rec.retry.retries++
	rec.retry.active = false
	rec.retry.mode = retryModeRunning
	rec.retry.nextRetry = time.Time{}
	rec.retry.backoff = 0
	rec.retry.lastAttempt = now
	rec.retry.ensureDefaults()
	rec.errors = nil
	if rec.checkpoint != nil {
		rec.checkpoint.Errors = nil
	}
	status := buildStatus(rec, now)
	rec.syncFromStatus(status)
	plan := clonePlan(rec.plan)
	return status, plan, rec.namespace, nil
}

func (s *sessionStore) startRetry(ctx context.Context, sessionID string, force bool, persist sessionPersistence) (spec.InstallStatusResponse, spec.InstallationPlan, error) {
	s.mu.Lock()
	now := time.Now().UTC()
	rec, ok := s.sessions[sessionID]
	if !ok {
		s.mu.Unlock()
		return spec.InstallStatusResponse{}, spec.InstallationPlan{}, fmt.Errorf("session %s not found", sessionID)
	}
	status, plan, namespace, err := s.startRetryLocked(rec, now, force)
	if err != nil {
		s.mu.Unlock()
		return spec.InstallStatusResponse{}, spec.InstallationPlan{}, err
	}
	s.mu.Unlock()
	if persist != nil {
		if err := persist.Save(ctx, namespace, status, plan); err != nil {
			return status, plan, err
		}
	}
	return status, plan, nil
}

func (s *sessionStore) activateDueRetry(ctx context.Context, sessionID string, persist sessionPersistence) (spec.InstallStatusResponse, spec.InstallationPlan, bool, error) {
	s.mu.Lock()
	now := time.Now().UTC()
	rec, ok := s.sessions[sessionID]
	if !ok {
		s.mu.Unlock()
		return spec.InstallStatusResponse{}, spec.InstallationPlan{}, false, nil
	}
	if !rec.retry.active {
		s.mu.Unlock()
		return spec.InstallStatusResponse{}, spec.InstallationPlan{}, false, nil
	}
	if len(rec.errors) == 0 {
		rec.retry.active = false
		rec.retry.mode = retryModeIdle
		rec.retry.nextRetry = time.Time{}
		rec.retry.backoff = 0
		s.mu.Unlock()
		return spec.InstallStatusResponse{}, spec.InstallationPlan{}, false, nil
	}
	if now.Before(rec.retry.nextRetry) {
		s.mu.Unlock()
		return spec.InstallStatusResponse{}, spec.InstallationPlan{}, false, nil
	}
	status, plan, namespace, err := s.startRetryLocked(rec, now, false)
	if err != nil {
		if errors.Is(err, errRetryLimit) {
			s.mu.Unlock()
			return spec.InstallStatusResponse{}, spec.InstallationPlan{}, false, nil
		}
		s.mu.Unlock()
		return spec.InstallStatusResponse{}, spec.InstallationPlan{}, false, err
	}
	s.mu.Unlock()
	if persist != nil {
		if err := persist.Save(ctx, namespace, status, plan); err != nil {
			return status, plan, false, err
		}
	}
	return status, plan, true, nil
}

func (s *sessionStore) restoreFromPersisted(snapshot *service.PersistedSession) (string, spec.InstallationPlan, bool) {
	if snapshot == nil {
		return "", spec.InstallationPlan{}, false
	}
	sessionID := snapshot.Status.SessionId
	if sessionID == "" {
		return "", spec.InstallationPlan{}, false
	}

	plan := clonePlan(snapshot.Plan)
	if plan.SessionTemplate.Namespace == "" {
		plan.SessionTemplate.Namespace = namespaceFallback(&plan)
	}

	started := time.Now().UTC()
	if snapshot.Status.StartedAt != nil && !snapshot.Status.StartedAt.IsZero() {
		started = *snapshot.Status.StartedAt
	}
	lastTouched := snapshot.Status.UpdatedAt
	if lastTouched.IsZero() {
		lastTouched = time.Now().UTC()
	}
	progress := 0.0
	if snapshot.Status.Progress != nil {
		progress = float64(*snapshot.Status.Progress)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.sessions[sessionID]
	if !ok {
		rec = &sessionRecord{}
		s.sessions[sessionID] = rec
	}
	rec.id = sessionID
	rec.plan = plan
	rec.started = started
	rec.estimated = estimateDuration(plan)
	rec.manual = progress
	rec.lastTouched = lastTouched
	rec.namespace = plan.SessionTemplate.Namespace
	rec.retry = newRetryState()
	rec.syncFromStatus(snapshot.Status)
	return sessionID, clonePlan(plan), true
}

func (s *sessionStore) planFor(sessionID string) (spec.InstallationPlan, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.sessions[sessionID]
	if !ok {
		return spec.InstallationPlan{}, false
	}
	return clonePlan(rec.plan), true
}

func (s *sessionStore) getInstallConfig(sessionID string) (*spec.InstallConfig, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.sessions[sessionID]
	if !ok {
		return nil, false
	}
	if rec.checkpoint == nil || rec.checkpoint.Config == nil {
		return nil, true
	}
	return rec.checkpoint.Config, true
}

func (s *sessionStore) setContext(ctx context.Context, sessionID string, kv map[string]string, persist sessionPersistence) (spec.InstallStatusResponse, error) {
	s.mu.Lock()
	now := time.Now().UTC()
	rec, ok := s.sessions[sessionID]
	if !ok {
		s.mu.Unlock()
		return spec.InstallStatusResponse{}, fmt.Errorf("session %s not found", sessionID)
	}
	rec.lastTouched = now
	if rec.checkpoint == nil {
		rec.checkpoint = &spec.Checkpoint{}
	}
	if rec.checkpoint.Context == nil {
		m := map[string]string{}
		rec.checkpoint.Context = &m
	}
	for k, v := range kv {
		(*rec.checkpoint.Context)[k] = v
	}
	status := buildStatus(rec, now)
	rec.syncFromStatus(status)
	namespace := rec.namespace
	s.mu.Unlock()

	if persist != nil {
		if err := persist.Save(ctx, namespace, status, rec.plan); err != nil {
			return status, err
		}
	}
	return status, nil
}

func (s *sessionStore) shouldRun(sessionID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.sessions[sessionID]
	if !ok {
		return false
	}
	if len(rec.errors) > 0 {
		return false
	}
	now := time.Now().UTC()
	return recProgress(rec, now) < 0.999
}

func (s *sessionStore) isStepCompleted(sessionID string, order int32) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.sessions[sessionID]
	if !ok {
		return false
	}
	if rec.checkpoint == nil || rec.checkpoint.CompletedSteps == nil {
		return false
	}
	for _, existing := range *rec.checkpoint.CompletedSteps {
		if int32(existing) == order {
			return true
		}
	}
	return false
}

func (s *sessionStore) createSession(ctx context.Context, req spec.StartInstallRequest, persist sessionPersistence) (*sessionRecord, spec.StartInstallResponse, error) {
	s.mu.Lock()

	plan := clonePlan(req.Plan)
	if plan.SessionTemplate.Namespace == "" {
		plan.SessionTemplate.Namespace = namespaceFallback(&plan)
	}
	sessionID := uuid.NewString()
	started := time.Now().UTC()
	estimated := estimateDuration(plan)

	now := time.Now().UTC()
	s.cleanupExpiredLocked(now)

	rec := &sessionRecord{
		id:          sessionID,
		plan:        plan,
		checkpoint:  cloneCheckpoint(req.Checkpoint),
		started:     started,
		estimated:   estimated,
		lastTouched: now,
		namespace:   plan.SessionTemplate.Namespace,
		retry:       newRetryState(),
	}

	if rec.checkpoint == nil {
		rec.checkpoint = &spec.Checkpoint{}
	}
	if rec.checkpoint.Context == nil {
		m := map[string]string{}
		rec.checkpoint.Context = &m
	}

	cfg := rec.checkpoint.Config
	mode := spec.Managed
	if cfg != nil && cfg.InstallMode != nil && *cfg.InstallMode != "" {
		mode = *cfg.InstallMode
	}
	if mode == spec.Managed && shouldBootstrap(plan) {
		jobName := bootstrapJobName(sessionID)
		(*rec.checkpoint.Context)[checkpointContextJobName] = jobName
		(*rec.checkpoint.Context)[checkpointContextJobNamespace] = bootstrapJobNamespace
		(*rec.checkpoint.Context)[checkpointContextJobStatus] = "pending"

		releaseName := "polardbx-monitor"
		if plan.SessionTemplate.ReleaseName != nil && strings.TrimSpace(*plan.SessionTemplate.ReleaseName) != "" {
			releaseName = strings.TrimSpace(*plan.SessionTemplate.ReleaseName)
		}
		(*rec.checkpoint.Context)[checkpointContextReleaseName] = releaseName

		targetNS := plan.SessionTemplate.Namespace
		if cfg != nil && cfg.TargetNamespace != nil && strings.TrimSpace(*cfg.TargetNamespace) != "" {
			targetNS = strings.TrimSpace(*cfg.TargetNamespace)
		}
		(*rec.checkpoint.Context)[checkpointContextNamespace] = targetNS
	}

	s.sessions[sessionID] = rec
	status := buildStatus(rec, now)
	rec.syncFromStatus(status)
	namespace := rec.namespace
	s.mu.Unlock()

	resumed := req.Checkpoint != nil && req.Checkpoint.SessionId != nil && *req.Checkpoint.SessionId != ""
	startResp := spec.StartInstallResponse{
		SessionId: sessionID,
		Namespace: plan.SessionTemplate.Namespace,
		CreatedAt: started,
		Resumed:   boolPtr(resumed),
	}
	if mode == spec.Managed && shouldBootstrap(plan) {
		jobName := (*rec.checkpoint.Context)[checkpointContextJobName]
		if strings.TrimSpace(jobName) != "" {
			startResp.JobName = stringPtr(jobName)
		}
	}

	if persist != nil {
		if err := persist.Save(ctx, namespace, status, plan); err != nil {
			s.mu.Lock()
			delete(s.sessions, sessionID)
			s.mu.Unlock()
			return nil, spec.StartInstallResponse{}, err
		}
	}
	return rec, startResp, nil
}

func (s *sessionStore) snapshot(ctx context.Context, sessionID string, persist sessionPersistence) (spec.InstallStatusResponse, bool, error) {
	s.mu.Lock()
	now := time.Now().UTC()
	s.cleanupExpiredLocked(now)

	rec, ok := s.sessions[sessionID]
	if !ok {
		s.mu.Unlock()
		return spec.InstallStatusResponse{}, false, nil
	}

	status := buildStatus(rec, now)
	rec.syncFromStatus(status)
	namespace := rec.namespace
	s.mu.Unlock()

	if persist != nil {
		if err := persist.Save(ctx, namespace, status, rec.plan); err != nil {
			return status, true, err
		}
	}
	return status, true, nil
}

func (s *sessionStore) boost(ctx context.Context, sessionID string, delta float64, persist sessionPersistence) (bool, error) {
	s.mu.Lock()
	now := time.Now().UTC()
	s.cleanupExpiredLocked(now)
	rec, ok := s.sessions[sessionID]
	if !ok {
		s.mu.Unlock()
		return false, nil
	}
	rec.manual = math.Min(1, rec.manual+delta)
	status := buildStatus(rec, now)
	rec.syncFromStatus(status)
	namespace := rec.namespace
	s.mu.Unlock()

	if persist != nil {
		if err := persist.Save(ctx, namespace, status, rec.plan); err != nil {
			return true, err
		}
	}
	return true, nil
}

func (s *sessionStore) setCurrentStep(ctx context.Context, sessionID string, order int32, persist sessionPersistence) (spec.InstallStatusResponse, error) {
	s.mu.Lock()
	now := time.Now().UTC()
	rec, ok := s.sessions[sessionID]
	if !ok {
		s.mu.Unlock()
		return spec.InstallStatusResponse{}, fmt.Errorf("session %s not found", sessionID)
	}
	rec.lastTouched = now
	if rec.checkpoint == nil {
		rec.checkpoint = &spec.Checkpoint{}
	}
	cur := int(order)
	rec.checkpoint.CurrentStep = &cur
	rec.currentStep = &order
	status := buildStatus(rec, now)
	rec.syncFromStatus(status)
	namespace := rec.namespace
	s.mu.Unlock()

	if persist != nil {
		if err := persist.Save(ctx, namespace, status, rec.plan); err != nil {
			return status, err
		}
	}
	return status, nil
}

func (s *sessionStore) completeStep(ctx context.Context, sessionID string, order int32, persist sessionPersistence) (spec.InstallStatusResponse, error) {
	s.mu.Lock()
	now := time.Now().UTC()
	rec, ok := s.sessions[sessionID]
	if !ok {
		s.mu.Unlock()
		return spec.InstallStatusResponse{}, fmt.Errorf("session %s not found", sessionID)
	}
	rec.lastTouched = now
	if rec.checkpoint == nil {
		rec.checkpoint = &spec.Checkpoint{}
	}
	list := []int{}
	if rec.checkpoint.CompletedSteps != nil {
		list = append(list, (*rec.checkpoint.CompletedSteps)...)
	}
	exists := false
	for _, existing := range list {
		if existing == int(order) {
			exists = true
			break
		}
	}
	if !exists {
		list = append(list, int(order))
	}
	rec.checkpoint.CompletedSteps = intSlicePtr(list)
	rec.checkpoint.CurrentStep = nil
	rec.currentStep = nil
	totalSteps := len(rec.plan.Steps)
	if totalSteps > 0 {
		rec.manual = math.Min(1, float64(len(list))/float64(totalSteps))
	}
	status := buildStatus(rec, now)
	rec.syncFromStatus(status)
	namespace := rec.namespace
	s.mu.Unlock()

	if persist != nil {
		if err := persist.Save(ctx, namespace, status, rec.plan); err != nil {
			return status, err
		}
	}
	return status, nil
}

func (s *sessionStore) addError(ctx context.Context, sessionID string, order int32, installErr spec.InstallError, persist sessionPersistence) (spec.InstallStatusResponse, error) {
	s.mu.Lock()
	now := time.Now().UTC()
	rec, ok := s.sessions[sessionID]
	if !ok {
		s.mu.Unlock()
		return spec.InstallStatusResponse{}, fmt.Errorf("session %s not found", sessionID)
	}
	rec.lastTouched = now
	rec.errors = append(rec.errors, installErr)
	if rec.checkpoint == nil {
		rec.checkpoint = &spec.Checkpoint{}
	}
	if order > 0 {
		cur := int(order)
		rec.checkpoint.CurrentStep = &cur
		rec.currentStep = &order
	}
	rec.retry.lastError = installErr.Message
	rec.scheduleAutoRetry(now)
	status := buildStatus(rec, now)
	rec.syncFromStatus(status)
	namespace := rec.namespace
	s.mu.Unlock()

	if persist != nil {
		if err := persist.Save(ctx, namespace, status, rec.plan); err != nil {
			return status, err
		}
	}
	return status, nil
}

func (s *sessionStore) cleanupExpiredLocked(now time.Time) {
	if len(s.sessions) == 0 {
		return
	}
	for id, rec := range s.sessions {
		if now.Sub(rec.lastTouched) > sessionTTL {
			delete(s.sessions, id)
		}
	}
}

func (s *sessionStore) cleanupExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupExpiredLocked(time.Now().UTC())
}

func buildStatus(rec *sessionRecord, now time.Time) spec.InstallStatusResponse {

	completed, current, componentStates, completedOrders := deriveProgress(rec, now)
	progress := recProgress(rec, now)

	phase := spec.Installing
	if len(rec.errors) > 0 {
		phase = spec.Failed
	} else if progress >= 0.999 {
		phase = spec.Active
		progress = 1
	}

	errors := make([]spec.InstallError, len(rec.errors))
	copy(errors, rec.errors)

	status := spec.InstallStatusResponse{
		SessionId:      rec.id,
		Phase:          phase,
		Components:     componentStates,
		Errors:         &errors,
		UpdatedAt:      now,
		Progress:       float32Ptr(progress),
		CompletedSteps: &completed,
		CurrentStep:    current,
		StartedAt:      timePtr(rec.started),
	}

	if current != nil && current.Message == nil && len(errors) == 0 {
		if msg := bootstrapStepMessage(rec); msg != "" {
			current.Message = stringPtr(msg)
			status.CurrentStep = current
		}
	}

	checkpoint := spec.Checkpoint{
		SessionId:      &rec.id,
		Phase:          &phase,
		Progress:       float32Ptr(progress),
		StartedAt:      timePtr(rec.started),
		LastUpdatedAt:  timePtr(now),
		CompletedSteps: intSlicePtr(completedOrders),
		Components:     &componentStates,
	}
	if current != nil {
		cur := int(current.Order)
		checkpoint.CurrentStep = &cur
	}
	if len(errors) > 0 {
		errCopy := make([]spec.InstallError, len(errors))
		copy(errCopy, errors)
		checkpoint.Errors = &errCopy
	}
	if rec.checkpoint != nil {
		checkpoint.Config = rec.checkpoint.Config
		checkpoint.Context = rec.checkpoint.Context
		if rec.checkpoint.Errors != nil {
			checkpoint.Errors = rec.checkpoint.Errors
		}
		if rec.checkpoint.CurrentStep != nil {
			checkpoint.CurrentStep = rec.checkpoint.CurrentStep
		}
	}
	rec.retry.ensureDefaults()
	if len(errors) == 0 {
		rec.retry.active = false
		if rec.retry.mode != retryModeRunning {
			rec.retry.mode = retryModeIdle
		}
		rec.retry.nextRetry = time.Time{}
		rec.retry.backoff = 0
	}
	status.Retry = rec.retry.toSpec()
	status.Checkpoint = cloneCheckpoint(&checkpoint)

	return status
}

func bootstrapStepMessage(rec *sessionRecord) string {
	if rec == nil || rec.checkpoint == nil || rec.checkpoint.Context == nil {
		return ""
	}
	if rec.checkpoint.CompletedSteps != nil && len(*rec.checkpoint.CompletedSteps) > 0 {
		return ""
	}
	ctx := *rec.checkpoint.Context
	jobName := strings.TrimSpace(ctx[checkpointContextJobName])
	jobNS := strings.TrimSpace(ctx[checkpointContextJobNamespace])
	if jobName == "" || jobNS == "" {
		return ""
	}
	jobStatus := strings.TrimSpace(ctx[checkpointContextJobStatus])
	targetNS := strings.TrimSpace(ctx[checkpointContextNamespace])
	releaseName := strings.TrimSpace(ctx[checkpointContextReleaseName])
	meta := []string{}
	if releaseName != "" {
		meta = append(meta, "release="+releaseName)
	}
	if targetNS != "" {
		meta = append(meta, "namespace="+targetNS)
	}
	suffix := ""
	if len(meta) > 0 {
		suffix = " (" + strings.Join(meta, ", ") + ")"
	}
	statusLabel := "running"
	if jobStatus != "" {
		statusLabel = jobStatus
	}
	return fmt.Sprintf(
		"Bootstrap job %s/%s is %s%s. Use kubectl logs -n %s job/%s to follow progress.",
		jobNS,
		jobName,
		statusLabel,
		suffix,
		jobNS,
		jobName,
	)
}

func deriveProgress(rec *sessionRecord, now time.Time) ([]spec.ExecutionStep, *spec.ExecutionStep, []spec.ComponentState, []int) {
	steps := rec.plan.Steps
	if len(steps) == 0 {
		steps = []spec.PlanStep{{
			Order:     1,
			Component: spec.Prometheus,
			Action:    spec.PlanStepActionInstall,
		}}
	}

	estimated := rec.estimated
	if estimated <= 0 {
		estimated = estimateDuration(rec.plan)
	}
	stepDuration := estimated / time.Duration(len(steps))
	if stepDuration <= 0 {
		stepDuration = 90 * time.Second
	}

	progress := recProgress(rec, now)

	completedSet := map[int32]struct{}{}
	if rec.checkpoint != nil && rec.checkpoint.CompletedSteps != nil {
		for _, order := range *rec.checkpoint.CompletedSteps {
			completedSet[int32(order)] = struct{}{}
		}
	}

	var currentOrder *int32
	if rec.checkpoint != nil && rec.checkpoint.CurrentStep != nil {
		val := int32(*rec.checkpoint.CurrentStep)
		currentOrder = &val
	}
	errorByOrder := map[int32]spec.InstallError{}
	if len(rec.errors) > 0 {
		for _, installErr := range rec.errors {
			if installErr.StepOrder != nil {
				errorByOrder[*installErr.StepOrder] = installErr
			}
		}
		if currentOrder == nil {
			var minOrder int32
			for order := range errorByOrder {
				if minOrder == 0 || order < minOrder {
					minOrder = order
				}
			}
			if minOrder != 0 {
				val := minOrder
				currentOrder = &val
			}
		}
	}

	fallbackCompleted := 0
	if len(completedSet) == 0 && len(steps) > 0 {
		fallbackCompleted = int(math.Floor(progress * float64(len(steps))))
	}

	completed := make([]spec.ExecutionStep, 0, len(steps))
	completedOrders := make([]int, 0, len(steps))
	var current *spec.ExecutionStep

	componentMap := map[spec.ComponentName]*spec.ComponentState{}
	for _, step := range steps {
		componentMap[step.Component] = &spec.ComponentState{
			Name:  step.Component,
			Phase: spec.ComponentPhasePlanned,
		}
	}

	for idx, step := range steps {
		exec := spec.ExecutionStep{
			Order:     step.Order,
			Component: step.Component,
			Action:    mapExecutionAction(step.Action),
			Status:    spec.ExecutionStepStatusPending,
		}
		stepStart := rec.started.Add(stepDuration * time.Duration(idx))
		stepEnd := stepStart.Add(stepDuration)

		_, explicitlyCompleted := completedSet[step.Order]
		markCompleted := explicitlyCompleted || (len(completedSet) == 0 && idx < fallbackCompleted)
		if markCompleted {
			exec.Status = spec.ExecutionStepStatusSucceeded
			exec.StartedAt = timePtr(stepStart)
			exec.CompletedAt = timePtr(stepEnd)
			completed = append(completed, exec)
			completedOrders = append(completedOrders, int(step.Order))
			markComponent(componentMap[step.Component], spec.ComponentPhaseHealthy, true, stepEnd, now)
			continue
		}

		isCurrent := false
		if currentOrder != nil && *currentOrder == step.Order {
			isCurrent = true
		} else if len(completedSet) == 0 && idx == fallbackCompleted && progress > 0 && progress < 0.999 {
			isCurrent = true
		}

		if isCurrent {
			if installErr, hasErr := errorByOrder[step.Order]; hasErr {
				exec.Status = spec.ExecutionStepStatusFailed
				exec.StartedAt = timePtr(stepStart)
				exec.CompletedAt = timePtr(now)
				exec.Message = stringPtr(installErr.Message)
				current = cloneExecution(exec)

				state := componentMap[step.Component]
				if state != nil {
					state.Phase = spec.ComponentPhaseFailed
					state.Healthy = boolPtr(false)
					state.LastCheckAt = timePtr(now)
					state.ErrorMessage = stringPtr(installErr.Message)
				}
				continue
			}
			exec.Status = spec.ExecutionStepStatusRunning
			exec.StartedAt = timePtr(stepStart)
			current = cloneExecution(exec)
			markComponent(componentMap[step.Component], spec.ComponentPhaseInstalling, false, stepStart, now)
		} else {
			if installErr, hasErr := errorByOrder[step.Order]; hasErr {
				state := componentMap[step.Component]
				if state != nil {
					state.Phase = spec.ComponentPhaseFailed
					state.Healthy = boolPtr(false)
					state.LastCheckAt = timePtr(now)
					state.ErrorMessage = stringPtr(installErr.Message)
				}
				continue
			}
			markComponent(componentMap[step.Component], spec.ComponentPhasePlanned, false, stepStart, now)
		}
	}

	componentStates := toComponentSlice(componentMap)
	if rec.checkpoint != nil && rec.checkpoint.Components != nil {
		overlay := map[spec.ComponentName]spec.ComponentState{}
		for _, st := range *rec.checkpoint.Components {
			overlay[st.Name] = st
		}
		for i, state := range componentStates {
			if override, ok := overlay[state.Name]; ok {
				merged := state
				merged.Phase = override.Phase
				if override.Healthy != nil {
					merged.Healthy = override.Healthy
				}
				if override.LastCheckAt != nil {
					merged.LastCheckAt = override.LastCheckAt
				}
				if override.InstalledAt != nil {
					merged.InstalledAt = override.InstalledAt
				}
				if override.ErrorMessage != nil {
					merged.ErrorMessage = override.ErrorMessage
				}
				if override.RetryCount != nil {
					merged.RetryCount = override.RetryCount
				}
				if override.MaxRetries != nil {
					merged.MaxRetries = override.MaxRetries
				}
				if override.Version != nil {
					merged.Version = override.Version
				}
				componentStates[i] = merged
			}
		}
	}

	return completed, current, componentStates, completedOrders
}

func markComponent(state *spec.ComponentState, phase spec.ComponentPhase, healthy bool, eventTime time.Time, now time.Time) {
	if state == nil {
		return
	}
	state.Phase = phase
	switch phase {
	case spec.ComponentPhaseHealthy:
		state.Healthy = boolPtr(true)
		state.LastCheckAt = timePtr(now)
		state.InstalledAt = timePtr(eventTime)
	case spec.ComponentPhaseInstalling:
		state.Healthy = boolPtr(false)
		state.LastCheckAt = timePtr(now)
	default:
		state.Healthy = boolPtr(false)
	}
	if healthy {
		state.Healthy = boolPtr(true)
	}
}

func recProgress(rec *sessionRecord, now time.Time) float64 {
	if rec.manual > 0 {
		return math.Min(1, rec.manual)
	}
	if rec.checkpoint != nil {
		if rec.checkpoint.Progress != nil {
			return math.Min(1, float64(*rec.checkpoint.Progress))
		}
		if rec.checkpoint.CompletedSteps != nil && len(*rec.checkpoint.CompletedSteps) > 0 && len(rec.plan.Steps) > 0 {
			return math.Min(1, float64(len(*rec.checkpoint.CompletedSteps))/float64(len(rec.plan.Steps)))
		}
	}
	return 0
}

func mapExecutionAction(action spec.PlanStepAction) spec.ExecutionStepAction {
	switch action {
	case spec.PlanStepActionInstall:
		return spec.ExecutionStepActionInstall
	case spec.PlanStepActionUpgrade:
		return spec.ExecutionStepActionUpgrade
	case spec.PlanStepActionRepair:
		return spec.ExecutionStepActionRepair
	case spec.PlanStepActionUninstall:
		return spec.ExecutionStepActionCleanup
	default:
		return spec.ExecutionStepActionVerify
	}
}

func toComponentSlice(m map[spec.ComponentName]*spec.ComponentState) []spec.ComponentState {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, string(name))
	}
	sort.Strings(names)

	out := make([]spec.ComponentState, 0, len(m))
	for _, n := range names {
		state := *m[spec.ComponentName(n)]
		out = append(out, state)
	}
	return out
}

func buildPlanSteps(req spec.CreatePlanRequest) []spec.PlanStep {
	steps := make([]spec.PlanStep, 0, len(req.Detected.Components))
	order := int32(1)
	for _, comp := range req.Detected.Components {
		action := planAction(comp.ActionRecommendation.Action)
		reason := comp.ActionRecommendation.Reason
		step := spec.PlanStep{
			Order:     order,
			Component: comp.Name,
			Action:    action,
			Reason:    &reason,
		}
		steps = append(steps, step)
		order++
	}
	if len(steps) == 0 {
		steps = append(steps, spec.PlanStep{
			Order:     1,
			Component: spec.Prometheus,
			Action:    spec.PlanStepActionVerify,
			Reason:    stringPtr("No specific actions required; verifying existing components"),
		})
	}
	return steps
}

func buildSessionTemplate(req spec.CreatePlanRequest) spec.SessionTemplate {
	namespace := req.Detected.Namespace
	if req.Config.TargetNamespace != nil && *req.Config.TargetNamespace != "" {
		namespace = *req.Config.TargetNamespace
	}
	intent := spec.Install
	if req.Intent != nil {
		switch *req.Intent {
		case spec.CreatePlanRequestIntentUpgrade:
			intent = spec.Upgrade
		case spec.CreatePlanRequestIntentRepair:
			intent = spec.Repair
		}
	}
	template := spec.SessionTemplate{
		Namespace:       namespace,
		Intent:          &intent,
		TargetNamespace: stringPtr(namespace),
	}
	// Align with official installation: release name defaults to "polardbx-monitor".
	// (Still allows flexibility via namespace + valuesYaml.)
	template.ReleaseName = stringPtr("polardbx-monitor")
	return template
}

func planAction(action spec.ComponentActionAction) spec.PlanStepAction {
	switch action {
	case spec.ComponentActionActionInstall, spec.ComponentActionActionReinstall:
		return spec.PlanStepActionInstall
	case spec.ComponentActionActionUpgrade:
		return spec.PlanStepActionUpgrade
	case spec.ComponentActionActionRepair:
		return spec.PlanStepActionRepair
	default:
		return spec.PlanStepActionVerify
	}
}

func estimateDuration(plan spec.InstallationPlan) time.Duration {
	if plan.EstimatedDurationSeconds != nil && *plan.EstimatedDurationSeconds > 0 {
		return time.Duration(*plan.EstimatedDurationSeconds) * time.Second
	}
	steps := len(plan.Steps)
	if steps == 0 {
		steps = 1
	}
	return time.Duration(steps*150) * time.Second
}

func checkpointManagerFromContext(c *gin.Context) sessionPersistence {
	if v, ok := c.Get(persistenceContextKey); ok {
		if persist, ok := v.(sessionPersistence); ok {
			return persist
		}
	}
	if cs, ok := util.ClientsetFromContext(c); ok {
		return service.NewConfigMapCheckpointManager(cs)
	}
	return nil
}

func respondError(c *gin.Context, status int, code ErrorCode, message string, details ...map[string]string) {
	errBody := spec.ErrorBody{Message: message}
	if code != "" {
		copy := spec.ErrorCode(code)
		errBody.Code = &copy
	}
	if len(details) > 0 && len(details[0]) > 0 {
		copy := make(map[string]string, len(details[0]))
		for k, v := range details[0] {
			copy[k] = v
		}
		errBody.Details = &copy
	}
	c.JSON(status, errBody)
}

func executionContextFromRequest(c *gin.Context) (context.Context, bool) {
	base := context.Background()
	if customDetection {
		return base, true
	}
	v, ok := c.Get("k8sClient")
	if !ok {
		return base, false
	}
	cli, ok := v.(client.Client)
	if !ok || cli == nil {
		return base, false
	}
	ctx := service.ContextWithControllerClient(base, cli)
	if cs, ok := util.ClientsetFromContext(c); ok {
		ctx = service.ContextWithClientset(ctx, cs)
	}
	return ctx, true
}

func maybeStartExecutor(c *gin.Context, sessionID string, plan *spec.InstallationPlan, persist sessionPersistence) {
	if sessionID == "" {
		return
	}
	var execPlan spec.InstallationPlan
	var ok bool
	if plan != nil {
		execPlan = clonePlan(*plan)
		ok = true
	} else {
		execPlan, ok = store.planFor(sessionID)
	}
	if !ok {
		logger.WithValues("sessionId", sessionID).Info("executor start skipped: plan not available")
		return
	}
	if !store.shouldRun(sessionID) {
		logger.WithValues("sessionId", sessionID).Info("executor start skipped: session already complete or failed")
		return
	}
	execCtx, ready := executionContextFromRequest(c)
	if !ready {
		logger.WithValues("sessionId", sessionID).Info("executor start skipped: kubernetes client unavailable")
		return
	}
	logger.WithValues("sessionId", sessionID, "steps", len(execPlan.Steps)).Info("starting executor")
	executor.start(execCtx, sessionID, execPlan, persist)
}

func stringPtr(v string) *string     { return &v }
func boolPtr(v bool) *bool           { return &v }
func timePtr(t time.Time) *time.Time { return &t }
func float32Ptr(v float64) *float32 {
	f := float32(v)
	return &f
}
func intSlicePtr(values []int) *[]int { return &values }
func int32Ptr(v int32) *int32         { return &v }

func clonePlan(plan spec.InstallationPlan) spec.InstallationPlan {
	clone := plan
	if plan.Steps != nil {
		clone.Steps = append([]spec.PlanStep{}, plan.Steps...)
	}
	return clone
}

func mergeStringMap(dest map[string]string, src *map[string]string) map[string]string {
	if src == nil || len(*src) == 0 {
		return dest
	}
	if dest == nil {
		dest = make(map[string]string, len(*src))
	}
	for k, v := range *src {
		dest[k] = v
	}
	return dest
}

func firstNonEmptyFromMap(m map[string]string, keys ...string) string {
	if m == nil {
		return ""
	}
	for _, key := range keys {
		if val, ok := m[key]; ok {
			if trimmed := strings.TrimSpace(val); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func cloneCheckpoint(cp *spec.Checkpoint) *spec.Checkpoint {
	if cp == nil {
		return nil
	}
	clone := *cp
	if cp.CompletedSteps != nil {
		clone.CompletedSteps = intSlicePtr(append([]int{}, (*cp.CompletedSteps)...))
	}
	if cp.Components != nil {
		components := append([]spec.ComponentState{}, (*cp.Components)...)
		clone.Components = &components
	}
	if cp.Errors != nil {
		errors := append([]spec.InstallError{}, (*cp.Errors)...)
		clone.Errors = &errors
	}
	if cp.Context != nil {
		ctx := make(map[string]string, len(*cp.Context))
		for k, v := range *cp.Context {
			ctx[k] = v
		}
		clone.Context = &ctx
	}
	return &clone
}

func cloneExecution(step spec.ExecutionStep) *spec.ExecutionStep {
	copy := step
	return &copy
}

func namespaceFallback(plan *spec.InstallationPlan) string {
	if plan == nil {
		return "polardbx-monitor"
	}
	if plan.SessionTemplate.TargetNamespace != nil && *plan.SessionTemplate.TargetNamespace != "" {
		return *plan.SessionTemplate.TargetNamespace
	}
	return "polardbx-monitor"
}

func componentName(name *spec.ComponentName) string {
	if name == nil {
		return "unknown"
	}
	return string(*name)
}

// Uninstall removes monitoring stack components and cleans up resources.
// This consolidates the v1 uninstall functionality into v2.
func Uninstall(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		respondError(c, http.StatusUnauthorized, ErrorCodeKubernetesClientMissing, "kubernetes client not initialized")
		return
	}

	namespace := c.DefaultQuery("namespace", "polardbx-monitor")
	force := c.DefaultQuery("force", "false") == "true"
	logger.WithValues("namespace", namespace, "force", force).Info("uninstall requested")

	ctx := c.Request.Context()

	// Clean up installation plan ConfigMap if exists
	planNs := c.DefaultQuery("planNamespace", "polardbx-operator-system")
	planCM := &corev1.ConfigMap{}
	planKey := client.ObjectKey{Namespace: planNs, Name: "polardbx-monitoring-plan"}
	if err := cli.Get(ctx, planKey, planCM); err == nil {
		if err := cli.Delete(ctx, planCM); err != nil {
			logger.Error(err, "failed to delete monitoring plan ConfigMap", "namespace", planNs)
		}
	}

	// Clean up any active sessions for this namespace
	store.mu.Lock()
	for id, rec := range store.sessions {
		if rec.namespace == namespace {
			delete(store.sessions, id)
			logger.WithValues("sessionId", id, "namespace", namespace).Info("cleaned up session during uninstall")
		}
	}
	store.mu.Unlock()

	// Prepare uninstall response
	response := gin.H{
		"message":   "monitoring uninstall request accepted",
		"namespace": namespace,
		"force":     force,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}

	// If force mode, we could trigger helm uninstall here
	// For now, just mark the plan as removed and let user handle actual component removal
	if force {
		response["warning"] = "force mode enabled - manual component cleanup may be required"
	}

	logger.WithValues("namespace", namespace).Info("uninstall completed")
	apierr.OK(c, response)
}

// GetBootstrapLogs retrieves logs from a bootstrap/install job.
// This provides compatibility with v1 bootstrap logs endpoint.
func GetBootstrapLogs(c *gin.Context) {
	cs, ok := util.ClientsetFromContext(c)
	if !ok {
		respondError(c, http.StatusUnauthorized, ErrorCodeKubernetesClientMissing, "kubernetes clientset not initialized")
		return
	}

	// Support both legacy query parameters and v2 path parameter (/monitoring/install/:sessionId/logs).
	sessionID := c.Query("sessionId")
	if sessionID == "" {
		sessionID = c.Param("sessionId")
	}
	jobName := c.Query("jobName")
	namespace := c.DefaultQuery("namespace", "polardbx-operator-system")

	// Security: Set reasonable limits for tailLines to prevent OOM
	const (
		defaultTailLines = int64(100)
		maxTailLines     = int64(10000) // Max 10k lines to prevent memory exhaustion
	)
	tailLines := defaultTailLines

	if tailParam := c.Query("tailLines"); tailParam != "" {
		if parsed, err := strconv.ParseInt(tailParam, 10, 64); err == nil && parsed > 0 {
			tailLines = parsed
			// Enforce upper limit
			if tailLines > maxTailLines {
				tailLines = maxTailLines
			}
		}
	}

	// If sessionId provided, try to find associated job
	if sessionID != "" && jobName == "" {
		// Look for jobs with session label
		jobs, err := cs.BatchV1().Jobs(namespace).List(c.Request.Context(), metav1.ListOptions{
			LabelSelector: fmt.Sprintf("sessionId=%s", sessionID),
		})
		if err == nil && len(jobs.Items) > 0 {
			jobName = jobs.Items[0].Name
		}
	}

	if jobName == "" {
		respondError(c, http.StatusBadRequest, ErrorCodeInstallInvalid, "jobName or sessionId is required")
		return
	}

	// List pods created by this job
	pods, err := cs.CoreV1().Pods(namespace).List(c.Request.Context(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	if err != nil {
		respondError(c, http.StatusInternalServerError, ErrorCodeInstallFailed, "failed to list job pods: "+err.Error())
		return
	}

	if len(pods.Items) == 0 {
		respondError(c, http.StatusNotFound, ErrorCodeSessionNotFound, fmt.Sprintf("no pods found for job %s", jobName))
		return
	}

	// Get logs from the first pod
	pod := pods.Items[0]
	logOptions := &corev1.PodLogOptions{TailLines: &tailLines}
	if len(pod.Spec.Containers) > 0 {
		logOptions.Container = pod.Spec.Containers[0].Name
	}

	logReq := cs.CoreV1().Pods(namespace).GetLogs(pod.Name, logOptions)
	rc, err := logReq.Stream(c.Request.Context())
	if err != nil {
		respondError(c, http.StatusInternalServerError, ErrorCodeInstallFailed, "failed to get pod logs: "+err.Error())
		return
	}
	defer rc.Close()

	logs, err := io.ReadAll(rc)
	if err != nil {
		respondError(c, http.StatusInternalServerError, ErrorCodeInstallFailed, "failed to read logs: "+err.Error())
		return
	}

	apierr.OK(c, gin.H{
		"jobName":   jobName,
		"namespace": namespace,
		"podName":   pod.Name,
		"logs":      string(logs),
		"tailLines": tailLines,
	})
}

// ============================================================================
// Legacy v1 handlers migrated to v2
// ============================================================================

// Bootstrap installs or registers monitoring stack via Helm job.
// Migrated from pkg/api/monitoring/endpoints.go
func Bootstrap(c *gin.Context) {
	type req struct {
		Mode string `json:"mode"` // managed|assisted|byo
		Dry  bool   `json:"dryRun"`
		NS   string `json:"namespace"`
		Name string `json:"releaseName"`
	}
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		respondError(c, http.StatusUnauthorized, ErrorCodeKubernetesClientMissing, "kubernetes client not initialized")
		return
	}
	var r req
	_ = c.ShouldBindJSON(&r)
	if r.NS == "" {
		r.NS = "polardbx-operator-system"
	}

	logger.WithValues("namespace", r.NS, "mode", r.Mode, "dryRun", r.Dry).Info("bootstrap requested")

	// Check for existing ongoing bootstrap jobs (idempotent)
	if !r.Dry {
		existingJobs := &batchv1.JobList{}
		labelSelector := client.MatchingLabels{
			"app":       "polardbx-monitor-bootstrap",
			"createdBy": "dashboard",
		}
		if err := cli.List(c.Request.Context(), existingJobs, client.InNamespace(r.NS), labelSelector); err == nil {
			for _, job := range existingJobs.Items {
				isComplete := false
				isFailed := false
				for _, condition := range job.Status.Conditions {
					if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
						isComplete = true
						break
					}
					if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
						isFailed = true
						break
					}
				}
				if !isComplete && !isFailed {
					apierr.Accepted(c, gin.H{
						"message":      "monitoring bootstrap already in progress",
						"namespace":    r.NS,
						"targetNs":     "polardbx-monitor",
						"mode":         r.Mode,
						"releaseName":  r.Name,
						"jobName":      job.Name,
						"instructions": "Use kubectl logs -n " + r.NS + " job/" + job.Name + " to see progress",
						"existing":     true,
					})
					return
				}
			}
		}
	}

	// Persist plan (idempotent)
	cm := corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: r.NS, Name: "polardbx-monitoring-plan"}
	if err := cli.Get(c.Request.Context(), key, &cm); err != nil {
		cm = corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: r.NS, Name: "polardbx-monitoring-plan"}, Data: map[string]string{}}
		_ = cli.Create(c.Request.Context(), &cm)
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data["mode"] = r.Mode
	cm.Data["releaseName"] = r.Name
	cm.Data["dryRun"] = map[bool]string{true: "true", false: "false"}[r.Dry]
	_ = cli.Update(c.Request.Context(), &cm)

	if r.Dry {
		apierr.Accepted(c, gin.H{"message": "monitoring bootstrap accepted (dry-run)", "namespace": r.NS, "mode": r.Mode, "releaseName": r.Name, "dryRun": r.Dry})
		return
	}

	// Ensure target monitoring namespace exists
	monitorNS := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "polardbx-monitor"}}
	if err := cli.Create(c.Request.Context(), monitorNS); err != nil && !apierrors.IsAlreadyExists(err) {
		respondError(c, http.StatusInternalServerError, ErrorCodeInstallFailed, "failed to create namespace polardbx-monitor: "+err.Error())
		return
	}

	// Create Helm install Job
	jobName := fmt.Sprintf("polardbx-monitor-bootstrap-%d", time.Now().Unix())
	correlationId := fmt.Sprintf("monitor-%d", time.Now().UnixNano())
	command := strings.Join([]string{
		"set -e",
		"helm version || (echo 'helm not found in image' && exit 1)",
		"helm repo add polardbx https://polardbx-charts.oss-cn-beijing.aliyuncs.com || true",
		"helm repo update",
		"helm upgrade --install polardbx-monitor polardbx/polardbx-monitor --namespace polardbx-monitor --create-namespace",
	}, " && ")

	backoff := int32(0)
	ttl := int32(600)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.NS,
			Name:      jobName,
			Labels: map[string]string{
				"app":           "polardbx-monitor-bootstrap",
				"createdBy":     "dashboard",
				"correlationId": correlationId,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: boolPtr(true),
					Containers: []corev1.Container{{
						Name:            "helm",
						Image:           config.GetGlobalConfig().GetHelmImage(),
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command:         []string{"sh", "-c", command},
					}},
				},
			},
		},
	}
	if err := cli.Create(c.Request.Context(), job); err != nil {
		respondError(c, http.StatusInternalServerError, ErrorCodeInstallFailed, "failed to create helm install job: "+err.Error())
		return
	}

	logger.WithValues("jobName", jobName, "namespace", r.NS).Info("bootstrap job created")

	apierr.Accepted(c, gin.H{
		"message":       "monitoring bootstrap started",
		"namespace":     r.NS,
		"targetNs":      "polardbx-monitor",
		"mode":          r.Mode,
		"releaseName":   r.Name,
		"jobName":       jobName,
		"correlationId": correlationId,
		"instructions":  "Use kubectl logs -n " + r.NS + " job/" + jobName + " to see progress",
	})
}

// BootstrapStatus returns the status of a monitoring bootstrap job.
// Migrated from pkg/api/monitoring/endpoints.go
func BootstrapStatus(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		respondError(c, http.StatusUnauthorized, ErrorCodeKubernetesClientMissing, "kubernetes client not initialized")
		return
	}

	jobName := c.Query("jobName")
	namespace := c.DefaultQuery("namespace", "polardbx-operator-system")

	if jobName == "" {
		respondError(c, http.StatusBadRequest, ErrorCodeInstallInvalid, "jobName parameter is required")
		return
	}

	job := &batchv1.Job{}
	key := client.ObjectKey{Namespace: namespace, Name: jobName}
	if err := cli.Get(c.Request.Context(), key, job); err != nil {
		if apierrors.IsNotFound(err) {
			respondError(c, http.StatusNotFound, ErrorCodeSessionNotFound, fmt.Sprintf("job %s not found in namespace %s", jobName, namespace))
			return
		}
		respondError(c, http.StatusInternalServerError, ErrorCodeInstallFailed, "failed to get job: "+err.Error())
		return
	}

	phase := "Running"
	var completionTime *metav1.Time
	var failureReason string

	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			phase = "Succeeded"
			completionTime = &condition.LastTransitionTime
			break
		}
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			phase = "Failed"
			failureReason = condition.Message
			completionTime = &condition.LastTransitionTime
			break
		}
	}

	if phase == "Running" && job.Status.Active == 0 && job.Status.Succeeded == 0 && job.Status.Failed == 0 {
		phase = "Pending"
	}

	response := gin.H{
		"jobName":   jobName,
		"namespace": namespace,
		"phase":     phase,
		"startTime": job.Status.StartTime,
		"active":    job.Status.Active,
		"succeeded": job.Status.Succeeded,
		"failed":    job.Status.Failed,
	}

	if completionTime != nil {
		response["completionTime"] = completionTime
	}
	if failureReason != "" {
		response["failureReason"] = failureReason
	}
	response["conditions"] = job.Status.Conditions

	apierr.OK(c, response)
}

// Status summarizes discovered monitoring components readiness.
// Migrated from pkg/api/monitoring/endpoints.go
func Status(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		respondError(c, http.StatusUnauthorized, ErrorCodeKubernetesClientMissing, "kubernetes client not initialized")
		return
	}

	ns := c.DefaultQuery("namespace", "polardbx-monitor")

	namespaceExists := true
	namespaceError := ""
	if err := cli.Get(c.Request.Context(), client.ObjectKey{Name: ns}, &corev1.Namespace{}); err != nil {
		if apierrors.IsNotFound(err) {
			namespaceExists = false
		} else {
			namespaceExists = false
			namespaceError = err.Error()
		}
	}

	checkDeploy := func(name string) (ready, desired int32, ok bool) {
		dep := appsv1.Deployment{}
		if err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: ns, Name: name}, &dep); err == nil {
			return dep.Status.ReadyReplicas, dep.Status.Replicas, true
		}
		return 0, 0, false
	}
	checkStateful := func(name string) (ready, desired int32, ok bool) {
		sts := appsv1.StatefulSet{}
		if err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: ns, Name: name}, &sts); err == nil {
			if sts.Spec.Replicas != nil {
				return sts.Status.ReadyReplicas, *sts.Spec.Replicas, true
			}
			return sts.Status.ReadyReplicas, sts.Status.Replicas, true
		}
		return 0, 0, false
	}
	checkService := func(name string) (*corev1.Service, bool) {
		svc := &corev1.Service{}
		ok := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: ns, Name: name}, svc) == nil
		return svc, ok
	}

	generateServiceAccessURL := func(svcName string) string {
		svc, ok := checkService(svcName)
		if !ok {
			return ""
		}
		if svc.Spec.Type == corev1.ServiceTypeLoadBalancer {
			if len(svc.Status.LoadBalancer.Ingress) > 0 {
				ingress := svc.Status.LoadBalancer.Ingress[0]
				if ingress.Hostname != "" {
					return fmt.Sprintf("http://%s", ingress.Hostname)
				}
				if ingress.IP != "" {
					return fmt.Sprintf("http://%s", ingress.IP)
				}
			}
		}
		if svc.Spec.Type == corev1.ServiceTypeNodePort {
			for _, port := range svc.Spec.Ports {
				if port.NodePort > 0 {
					return fmt.Sprintf("NodePort: %d (access via <node-ip>:%d)", port.NodePort, port.NodePort)
				}
			}
		}
		if svc.Spec.Type == corev1.ServiceTypeClusterIP {
			for _, port := range svc.Spec.Ports {
				return fmt.Sprintf("port-forward svc/%s -n %s %d:3000", svcName, ns, port.Port)
			}
		}
		return ""
	}

	prom := gin.H{"ready": false, "readyReplicas": nil, "replicas": nil, "service": false, "exists": false}
	if r, d, ok := checkStateful("prometheus-k8s"); ok {
		prom["ready"] = r == d
		prom["readyReplicas"] = r
		prom["replicas"] = d
	} else if r, d, ok := checkStateful("kube-prometheus-stack-prometheus"); ok {
		prom["ready"] = r == d
		prom["readyReplicas"] = r
		prom["replicas"] = d
	}
	promSvc, promExists := checkService("prometheus-k8s")
	if !promExists {
		_, promExists = checkService("kube-prometheus-stack-prometheus")
		promSvc, _ = checkService("kube-prometheus-stack-prometheus")
	}
	prom["service"] = promExists
	prom["exists"] = prom["readyReplicas"] != nil || promExists
	if promExists && promSvc != nil {
		prom["accessUrl"] = generateServiceAccessURL(promSvc.Name)
	}

	graf := gin.H{"ready": false, "readyReplicas": nil, "replicas": nil, "service": false, "exists": false}
	if r, d, ok := checkDeploy("grafana"); ok {
		graf["ready"] = r == d
		graf["readyReplicas"] = r
		graf["replicas"] = d
	} else if r, d, ok := checkDeploy("kube-prometheus-stack-grafana"); ok {
		graf["ready"] = r == d
		graf["readyReplicas"] = r
		graf["replicas"] = d
	}
	grafSvc, grafExists := checkService("grafana")
	if !grafExists {
		_, grafExists = checkService("kube-prometheus-stack-grafana")
		grafSvc, _ = checkService("kube-prometheus-stack-grafana")
	}
	graf["service"] = grafExists
	graf["exists"] = graf["readyReplicas"] != nil || grafExists
	if grafExists && grafSvc != nil {
		graf["accessUrl"] = generateServiceAccessURL(grafSvc.Name)
	}

	am := gin.H{"configured": false, "exists": false}
	_, amExists := checkService("alertmanager-main")
	if !amExists {
		_, amExists = checkService("kube-prometheus-stack-alertmanager")
	}
	am["configured"] = amExists
	am["exists"] = amExists

	apierr.OK(c, gin.H{
		"namespace":       ns,
		"namespaceExists": namespaceExists,
		"namespaceError":  namespaceError,
		"components": gin.H{
			"prometheus":   prom,
			"grafana":      graf,
			"alertmanager": am,
		},
	})
}
