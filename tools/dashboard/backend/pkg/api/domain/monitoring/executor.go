package domain_monitoring

import (
	"context"
	"crypto/sha1"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"polardbx-dashboard-backend/pkg/api/domain/monitoring/service"
	spec "polardbx-dashboard-backend/pkg/api/domain/monitoring/spec"
	"polardbx-dashboard-backend/pkg/config"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type installationExecutor struct {
	store    *sessionStore
	running  sync.Map
	detector func() service.DetectionService
}

var executorLogger = ctrllog.Log.WithName("monitoring").WithName("executor")

const (
	bootstrapJobNamespace = "polardbx-operator-system"

	checkpointContextJobName      = "jobName"
	checkpointContextJobNamespace = "jobNamespace"
	checkpointContextReleaseName  = "releaseName"
	checkpointContextNamespace    = "namespace"
	checkpointContextJobStatus    = "jobStatus"
)

func newInstallationExecutor(store *sessionStore) *installationExecutor {
	return &installationExecutor{
		store:    store,
		detector: getDetectionService,
	}
}

func bootstrapPrimaryOrder(plan spec.InstallationPlan) int32 {
	order := int32(0)
	for _, step := range plan.Steps {
		switch step.Action {
		case spec.PlanStepActionInstall, spec.PlanStepActionUpgrade, spec.PlanStepActionRepair:
			if order == 0 || step.Order < order {
				order = step.Order
			}
		default:
		}
	}
	if order != 0 {
		return order
	}
	if len(plan.Steps) > 0 {
		return plan.Steps[0].Order
	}
	return 1
}

func (e *installationExecutor) start(ctx context.Context, sessionID string, plan spec.InstallationPlan, persist sessionPersistence) {
	if sessionID == "" {
		executorLogger.Info("executor start aborted: empty session id")
		return
	}
	if !e.store.shouldRun(sessionID) {
		executorLogger.WithValues("sessionId", sessionID).Info("executor start skipped: session not runnable")
		return
	}
	if _, loaded := e.running.LoadOrStore(sessionID, struct{}{}); loaded {
		executorLogger.WithValues("sessionId", sessionID).Info("executor already running")
		return
	}

	execCtx := ctx
	if execCtx == nil {
		execCtx = context.Background()
	}
	executorLogger.WithValues("sessionId", sessionID, "steps", len(plan.Steps)).Info("executor start queued")

	go e.run(execCtx, sessionID, plan, persist)
}

func (e *installationExecutor) run(ctx context.Context, sessionID string, plan spec.InstallationPlan, persist sessionPersistence) {
	defer e.running.Delete(sessionID)
	executorLogger.WithValues("sessionId", sessionID, "steps", len(plan.Steps)).Info("executor loop started")

	detector := e.detector()
	namespace := plan.SessionTemplate.Namespace
	steps := append([]spec.PlanStep{}, plan.Steps...)
	if len(steps) == 0 {
		executorLogger.WithValues("sessionId", sessionID).Info("no steps to execute; marking session complete")
		_, _ = e.store.boost(ctx, sessionID, 1, persist)
		e.store.snapshot(ctx, sessionID, persist)
		executorLogger.WithValues("sessionId", sessionID).Info("executor loop finished")
		return
	}

	// Managed install/repair/upgrade: align with official dashboard implementation by creating a Helm Job.
	// Use checkpoint.config to drive install mode & values overrides.
	if shouldBootstrap(plan) {
		if err := e.ensureBootstrap(ctx, sessionID, plan, persist); err != nil {
			executorLogger.WithValues("sessionId", sessionID).Error(err, "bootstrap ensure failed")
			return
		}
	}

	sort.Slice(steps, func(i, j int) bool {
		return steps[i].Order < steps[j].Order
	})

	for _, step := range steps {
		stepLog := executorLogger.WithValues(
			"sessionId", sessionID,
			"stepOrder", step.Order,
			"component", step.Component,
			"action", step.Action,
		)
		if e.store.isStepCompleted(sessionID, step.Order) {
			stepLog.Info("step already completed; skipping")
			continue
		}
		if !e.store.shouldRun(sessionID) {
			stepLog.Info("session no longer runnable; stopping executor")
			return
		}
		select {
		case <-ctx.Done():
			stepLog.Info("executor context cancelled")
			return
		default:
		}

		if _, err := e.store.setCurrentStep(ctx, sessionID, step.Order, persist); err != nil {
			stepLog.Error(err, "failed to set current step")
			return
		}
		stepLog.Info("verifying step state")

		healthy, detail, err := e.verifyStep(ctx, detector, namespace, step)
		if err != nil {
			installErr := e.newInstallError(step, spec.Unknown, fmt.Sprintf("step verification failed: %v", err))
			e.store.addError(ctx, sessionID, step.Order, installErr, persist)
			stepLog.Error(err, "step verification errored")
			return
		}
		if !healthy {
			installErr := e.newInstallError(step, spec.Dependency, detail)
			e.store.addError(ctx, sessionID, step.Order, installErr, persist)
			stepLog.Info("step verification reported unhealthy", "detail", detail)
			return
		}

		if _, err := e.store.completeStep(ctx, sessionID, step.Order, persist); err != nil {
			stepLog.Error(err, "failed to mark step complete")
			return
		}
		stepLog.Info("step completed successfully")

		select {
		case <-ctx.Done():
			stepLog.Info("executor context cancelled after completion")
			return
		case <-time.After(200 * time.Millisecond):
		}
	}

	e.store.snapshot(ctx, sessionID, persist)
	executorLogger.WithValues("sessionId", sessionID).Info("executor loop finished")
}

func (e *installationExecutor) verifyStep(ctx context.Context, detector service.DetectionService, namespace string, step spec.PlanStep) (bool, string, error) {
	if detector == nil {
		executorLogger.WithValues("namespace", namespace, "component", step.Component).Error(fmt.Errorf("detection service not configured"), "unable to verify step")
		return false, "detection service unavailable", fmt.Errorf("detection service not configured")
	}

	snapshot, err := detector.Detect(ctx, namespace)
	if err != nil {
		executorLogger.WithValues("namespace", namespace, "component", step.Component).Error(err, "failed to run detection during verification")
		return false, "failed to detect monitoring environment", err
	}

	var component *spec.DetectedComponent
	for i := range snapshot.Components {
		if snapshot.Components[i].Name == step.Component {
			component = &snapshot.Components[i]
			break
		}
	}

	exists := component != nil && component.Exists != nil && *component.Exists
	healthy := component != nil && component.Healthy != nil && *component.Healthy

	switch step.Action {
	case spec.PlanStepActionVerify:
		if healthy {
			return true, "", nil
		}
		return false, fmt.Sprintf("component %s not healthy", step.Component), nil
	case spec.PlanStepActionInstall, spec.PlanStepActionUpgrade, spec.PlanStepActionRepair:
		if healthy {
			return true, "", nil
		}
		if !exists {
			return false, fmt.Sprintf("component %s is still missing", step.Component), nil
		}
		return false, fmt.Sprintf("component %s is not healthy", step.Component), nil
	case spec.PlanStepActionUninstall:
		if !exists {
			return true, "", nil
		}
		return false, fmt.Sprintf("component %s still present", step.Component), nil
	default:
		if healthy {
			return true, "", nil
		}
		return false, fmt.Sprintf("component %s verification failed", step.Component), nil
	}
}

func (e *installationExecutor) newInstallError(step spec.PlanStep, category spec.FailureCategory, message string) spec.InstallError {
	component := step.Component
	now := time.Now().UTC()
	return spec.InstallError{
		Category:   category,
		Component:  &component,
		Message:    message,
		OccurredAt: &now,
		StepOrder:  &step.Order,
	}
}

func shouldBootstrap(plan spec.InstallationPlan) bool {
	for _, step := range plan.Steps {
		switch step.Action {
		case spec.PlanStepActionInstall, spec.PlanStepActionUpgrade, spec.PlanStepActionRepair:
			return true
		default:
		}
	}
	return false
}

func (e *installationExecutor) ensureBootstrap(ctx context.Context, sessionID string, plan spec.InstallationPlan, persist sessionPersistence) error {
	cli := service.ControllerClientFromContext(ctx)
	if cli == nil {
		// No cluster client (e.g. unit tests); skip actual bootstrap.
		return nil
	}

	cfg, _ := e.store.getInstallConfig(sessionID)
	mode := spec.Managed
	if cfg != nil && cfg.InstallMode != nil && *cfg.InstallMode != "" {
		mode = *cfg.InstallMode
	}
	if mode != spec.Managed {
		// Assisted/Byo: executor only verifies; it won't create the Job.
		return nil
	}

	targetNS := plan.SessionTemplate.Namespace
	if cfg != nil && cfg.TargetNamespace != nil && strings.TrimSpace(*cfg.TargetNamespace) != "" {
		targetNS = strings.TrimSpace(*cfg.TargetNamespace)
	}
	releaseName := "polardbx-monitor"
	if plan.SessionTemplate.ReleaseName != nil && strings.TrimSpace(*plan.SessionTemplate.ReleaseName) != "" {
		releaseName = strings.TrimSpace(*plan.SessionTemplate.ReleaseName)
	}
	planNS := bootstrapJobNamespace

	jobName := bootstrapJobName(sessionID)
	valuesSecret := ""
	if cfg != nil && cfg.ValuesYaml != nil && strings.TrimSpace(*cfg.ValuesYaml) != "" {
		valuesSecret = bootstrapValuesSecretName(sessionID)
	}

	// Ensure namespace exists (idempotent).
	nsObj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: targetNS}}
	if err := cli.Create(ctx, nsObj); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("ensure target namespace %s: %w", targetNS, err)
	}

	// Ensure values secret exists (optional).
	if valuesSecret != "" {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: planNS,
				Name:      valuesSecret,
				Labels: map[string]string{
					"app":       "polardbx-monitor-bootstrap",
					"createdBy": "dashboard",
					"sessionId": sessionID,
				},
			},
			Type: corev1.SecretTypeOpaque,
			StringData: map[string]string{
				"values.yaml": strings.TrimSpace(*cfg.ValuesYaml),
			},
		}
		if err := cli.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create values secret %s/%s: %w", planNS, valuesSecret, err)
		}
	}

	// Ensure job exists.
	job := &batchv1.Job{}
	if err := cli.Get(ctx, client.ObjectKey{Namespace: planNS, Name: jobName}, job); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get bootstrap job %s/%s: %w", planNS, jobName, err)
		}

		command := []string{
			"set -euo pipefail",
			"helm version || (echo 'helm not found in image' && exit 1)",
			"helm repo add polardbx https://polardbx-charts.oss-cn-beijing.aliyuncs.com || true",
			"helm repo update",
		}
		helmCmd := fmt.Sprintf(
			"helm upgrade --install %s polardbx/polardbx-monitor --namespace %s --create-namespace --wait --timeout 15m",
			shellQuote(releaseName),
			shellQuote(targetNS),
		)
		if valuesSecret != "" {
			helmCmd += " -f /config/values.yaml"
		}
		command = append(command, helmCmd)

		backoff := int32(0)
		ttl := int32(3600)
		newJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: planNS,
				Name:      jobName,
				Labels: map[string]string{
					"app":       "polardbx-monitor-bootstrap",
					"createdBy": "dashboard",
					"sessionId": sessionID,
					"v2":        "true",
				},
			},
			Spec: batchv1.JobSpec{
				BackoffLimit:            &backoff,
				TTLSecondsAfterFinished: &ttl,
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app":       "polardbx-monitor-bootstrap",
							"createdBy": "dashboard",
							"sessionId": sessionID,
						},
					},
					Spec: corev1.PodSpec{
						RestartPolicy:                corev1.RestartPolicyNever,
						AutomountServiceAccountToken: boolPtr(true),
						Containers: []corev1.Container{{
							Name:            "helm",
							Image:           config.GetGlobalConfig().GetHelmImage(),
							ImagePullPolicy: corev1.PullIfNotPresent,
							Command:         []string{"sh", "-c", strings.Join(command, " && ")},
						}},
					},
				},
			},
		}
		if valuesSecret != "" {
			newJob.Spec.Template.Spec.Volumes = []corev1.Volume{{
				Name: "values",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: valuesSecret},
				},
			}}
			newJob.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
				Name:      "values",
				MountPath: "/config",
				ReadOnly:  true,
			}}
		}
		if err := cli.Create(ctx, newJob); err != nil {
			return fmt.Errorf("create bootstrap job %s/%s: %w", planNS, jobName, err)
		}
	}

	// Persist job metadata for UI/logs.
	_, _ = e.store.setContext(ctx, sessionID, map[string]string{
		checkpointContextJobName:      jobName,
		checkpointContextJobNamespace: planNS,
		checkpointContextReleaseName:  releaseName,
		checkpointContextNamespace:    targetNS,
		checkpointContextJobStatus:    "running",
	}, persist)
	_, _ = e.store.setCurrentStep(ctx, sessionID, bootstrapPrimaryOrder(plan), persist)

	// Wait for completion or failure.
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	timeout := time.NewTimer(20 * time.Minute)
	defer timeout.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			installErr := e.newInstallError(spec.PlanStep{Order: 1, Component: spec.Prometheus, Action: spec.PlanStepActionInstall}, spec.Timeout, "bootstrap job timed out")
			_, _ = e.store.addError(ctx, sessionID, 1, installErr, persist)
			return fmt.Errorf("bootstrap job timed out")
		case <-ticker.C:
			cur := &batchv1.Job{}
			if err := cli.Get(ctx, client.ObjectKey{Namespace: planNS, Name: jobName}, cur); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return err
			}
			if jobSucceeded(cur) {
				_, _ = e.store.setContext(ctx, sessionID, map[string]string{
					checkpointContextJobStatus: "succeeded",
				}, persist)
				_, _ = e.store.boost(ctx, sessionID, 0.2, persist)
				return nil
			}
			if jobFailed(cur) {
				msg := "bootstrap job failed"
				if m := jobFailureMessage(cur); m != "" {
					msg = msg + ": " + m
				}
				_, _ = e.store.setContext(ctx, sessionID, map[string]string{
					checkpointContextJobStatus: "failed",
				}, persist)
				installErr := e.newInstallError(spec.PlanStep{Order: 1, Component: spec.Prometheus, Action: spec.PlanStepActionInstall}, spec.HelmChart, msg)
				_, _ = e.store.addError(ctx, sessionID, 1, installErr, persist)
				return fmt.Errorf(msg)
			}
		}
	}
}

func bootstrapJobName(sessionID string) string {
	sum := sha1.Sum([]byte(sessionID))
	return fmt.Sprintf("polardbx-monitor-bootstrap-%x", sum[:6])
}

func bootstrapValuesSecretName(sessionID string) string {
	sum := sha1.Sum([]byte("values:" + sessionID))
	return fmt.Sprintf("polardbx-monitor-values-%x", sum[:6])
}

func jobSucceeded(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func jobFailed(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func jobFailureMessage(job *batchv1.Job) string {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			if strings.TrimSpace(c.Message) != "" {
				return c.Message
			}
			if strings.TrimSpace(c.Reason) != "" {
				return c.Reason
			}
		}
	}
	return ""
}

func shellQuote(v string) string {
	// minimal POSIX shell quoting
	if v == "" {
		return "''"
	}
	if !strings.ContainsAny(v, " \t\n'\"\\$`") {
		return v
	}
	return "'" + strings.ReplaceAll(v, "'", `'\''`) + "'"
}
