package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	spec "polardbx-dashboard-backend/pkg/api/domain/monitoring/spec"
)

const (
	prometheusHealthProbeID        = "prometheus-health"
	prometheusNotFoundSummary      = "Prometheus StatefulSet not found in monitoring namespace"
	prometheusReplicasZeroSummary  = "Prometheus StatefulSet has 0 replicas"
	prometheusPodsNotReadySummary  = "Prometheus StatefulSet is not fully ready"
	prometheusRevisionDriftSummary = "Prometheus StatefulSet is undergoing rolling update"
)

var (
	prometheusStatefulSetCandidates = []string{"prometheus-k8s", "kube-prometheus-stack-prometheus"}
)

type prometheusHealthProbe struct{}

func newPrometheusHealthProbe() DiagnosticProbe {
	return &prometheusHealthProbe{}
}

func (p *prometheusHealthProbe) ID() string {
	return prometheusHealthProbeID
}

func (p *prometheusHealthProbe) Description() string {
	return "Check if Prometheus StatefulSet, Pod status, and rolling updates are healthy"
}

func (p *prometheusHealthProbe) Run(ctx context.Context, input ProbeInput) ([]DiagnosticFinding, error) {
	cli := controllerClientFromContext(ctx)
	if cli == nil {
		return nil, fmt.Errorf("controller client unavailable for prometheus diagnostics")
	}

	namespace := strings.TrimSpace(input.Namespace)
	if namespace == "" {
		namespace = DefaultMonitoringNamespace
	}
	if namespace == "" {
		return nil, fmt.Errorf("namespace is required for prometheus diagnostics")
	}

	sts, err := locatePrometheusStatefulSet(ctx, cli, namespace)
	if err != nil {
		return nil, err
	}
	if sts == nil {
		return []DiagnosticFinding{buildPrometheusMissingFinding(namespace)}, nil
	}

	finding := analysePrometheusStatefulSet(ctx, cli, namespace, sts)
	if finding == nil {
		return nil, nil
	}
	return []DiagnosticFinding{*finding}, nil
}

func locatePrometheusStatefulSet(ctx context.Context, cli client.Client, namespace string) (*appsv1.StatefulSet, error) {
	for _, name := range prometheusStatefulSetCandidates {
		if strings.TrimSpace(name) == "" {
			continue
		}
		sts := &appsv1.StatefulSet{}
		if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, sts); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		return sts, nil
	}
	return nil, nil
}

func buildPrometheusMissingFinding(namespace string) DiagnosticFinding {
	cause := fmt.Sprintf("Prometheus StatefulSet does not exist in namespace %s", namespace)
	return DiagnosticFinding{
		ProbeID:        prometheusHealthProbeID,
		Summary:        prometheusNotFoundSummary,
		Category:       spec.Dependency,
		Severity:       spec.Major,
		PossibleCauses: []string{cause},
		Details: map[string]string{
			"namespace": namespace,
		},
	}
}

func analysePrometheusStatefulSet(ctx context.Context, cli client.Client, namespace string, sts *appsv1.StatefulSet) *DiagnosticFinding {
	desired := desiredReplicas(sts)
	ready := sts.Status.ReadyReplicas
	detail := map[string]string{
		"namespace":       namespace,
		"statefulset":     sts.Name,
		"replicasDesired": fmt.Sprintf("%d", desired),
		"replicasReady":   fmt.Sprintf("%d", ready),
		"currentRevision": strings.TrimSpace(sts.Status.CurrentRevision),
		"updateRevision":  strings.TrimSpace(sts.Status.UpdateRevision),
	}

	if desired == 0 {
		return &DiagnosticFinding{
			ProbeID:        prometheusHealthProbeID,
			Summary:        prometheusReplicasZeroSummary,
			Category:       spec.Configuration,
			Severity:       spec.Major,
			PossibleCauses: []string{"Prometheus StatefulSet replicas configured to 0, unable to provide monitoring capability"},
			Details:        detail,
		}
	}

	if ready >= desired && strings.TrimSpace(sts.Status.CurrentRevision) == strings.TrimSpace(sts.Status.UpdateRevision) {
		return nil
	}

	causes, podDetails := collectPrometheusPodIssues(ctx, cli, namespace, sts)
	for key, value := range podDetails {
		detail[key] = value
	}

	severity := spec.Major
	summary := prometheusPodsNotReadySummary
	fixes := make([]SuggestedFix, 0, 1)

	if ready == 0 {
		severity = spec.Critical
		causes = append(causes, "All Prometheus Pods are not ready")
	} else if ready < desired {
		causes = append(causes, fmt.Sprintf("Only %d/%d Prometheus Pods are ready", ready, desired))
	}

	if strings.TrimSpace(sts.Status.CurrentRevision) != strings.TrimSpace(sts.Status.UpdateRevision) {
		causes = append(causes, "Prometheus StatefulSet is undergoing rolling upgrade, Pods have not completed switching")
		if summary == prometheusPodsNotReadySummary {
			summary = prometheusRevisionDriftSummary
		}
	}

	causes = uniqueSortedStrings(causes)

	fixes = append(fixes, SuggestedFix{
		ID:                fixPrometheusRestart,
		Title:             "Try to automatically restart Prometheus",
		Description:       "Trigger rolling restart by annotating StatefulSet template, commonly used to resolve CrashLoopBackOff or stuck rolling upgrades",
		Type:              SuggestedFixTypeOperatorHook,
		Automated:         true,
		Verification:      fmt.Sprintf("kubectl rollout status statefulset/%s -n %s", sts.Name, namespace),
		RelatedComponents: []spec.ComponentName{spec.Prometheus},
		Metadata:          map[string]string{"statefulset": sts.Name},
	})

	return &DiagnosticFinding{
		ProbeID:        prometheusHealthProbeID,
		Summary:        summary,
		Category:       spec.Dependency,
		Severity:       severity,
		PossibleCauses: causes,
		Details:        detail,
		SuggestedFixes: fixes,
	}
}

func desiredReplicas(sts *appsv1.StatefulSet) int32 {
	if sts.Spec.Replicas != nil {
		return *sts.Spec.Replicas
	}
	if sts.Status.Replicas > 0 {
		return sts.Status.Replicas
	}
	return 0
}

func collectPrometheusPodIssues(ctx context.Context, cli client.Client, namespace string, sts *appsv1.StatefulSet) ([]string, map[string]string) {
	selector := labels.Everything()
	if sts.Spec.Selector != nil {
		if sel, err := metav1.LabelSelectorAsSelector(sts.Spec.Selector); err == nil {
			selector = sel
		}
	}

	podList := &corev1.PodList{}
	if err := cli.List(ctx, podList, client.InNamespace(namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return []string{fmt.Sprintf("Unable to list Prometheus Pods: %v", err)}, map[string]string{
			"podsListError": err.Error(),
		}
	}

	podDetails := map[string]string{}
	causes := make([]string, 0)
	if len(podList.Items) == 0 {
		causes = append(causes, "No Prometheus Pods found matching StatefulSet selector")
		return causes, podDetails
	}

	sort.SliceStable(podList.Items, func(i, j int) bool {
		return podList.Items[i].Name < podList.Items[j].Name
	})

	for idx := range podList.Items {
		pod := &podList.Items[idx]
		readyCond := getPodCondition(pod.Status.Conditions, corev1.PodReady)
		if readyCond != nil && readyCond.Status == corev1.ConditionTrue {
			continue
		}

		description := describePodIssue(pod)
		if description != "" {
			causes = append(causes, fmt.Sprintf("Pod %s：%s", pod.Name, description))
		} else {
			causes = append(causes, fmt.Sprintf("Pod %s is not ready", pod.Name))
		}

		podDetails[fmt.Sprintf("pod.%s.phase", pod.Name)] = string(pod.Status.Phase)
		if readyCond != nil {
			if readyCond.Reason != "" {
				podDetails[fmt.Sprintf("pod.%s.readyReason", pod.Name)] = readyCond.Reason
			}
			if readyCond.Message != "" {
				podDetails[fmt.Sprintf("pod.%s.readyMessage", pod.Name)] = readyCond.Message
			}
		}

		for _, cs := range pod.Status.ContainerStatuses {
			prefix := fmt.Sprintf("pod.%s.container.%s", pod.Name, cs.Name)
			if cs.State.Waiting != nil {
				podDetails[prefix+".state"] = "waiting"
				podDetails[prefix+".reason"] = cs.State.Waiting.Reason
				if cs.State.Waiting.Message != "" {
					podDetails[prefix+".message"] = cs.State.Waiting.Message
				}
			} else if cs.State.Terminated != nil {
				podDetails[prefix+".state"] = "terminated"
				podDetails[prefix+".reason"] = cs.State.Terminated.Reason
				podDetails[prefix+".exitCode"] = fmt.Sprintf("%d", cs.State.Terminated.ExitCode)
				if cs.State.Terminated.Message != "" {
					podDetails[prefix+".message"] = cs.State.Terminated.Message
				}
				if cs.State.Terminated.FinishedAt != (metav1.Time{}) {
					podDetails[prefix+".finishedAt"] = cs.State.Terminated.FinishedAt.UTC().Format(time.RFC3339)
				}
			} else if !cs.Ready {
				podDetails[prefix+".state"] = "not-ready"
			}

			if cs.LastTerminationState.Terminated != nil {
				terminated := cs.LastTerminationState.Terminated
				podDetails[prefix+".lastTerminatedReason"] = terminated.Reason
				if terminated.FinishedAt != (metav1.Time{}) {
					podDetails[prefix+".lastTerminatedAt"] = terminated.FinishedAt.UTC().Format(time.RFC3339)
				}
			}
		}
	}

	return causes, podDetails
}

func getPodCondition(conditions []corev1.PodCondition, condType corev1.PodConditionType) *corev1.PodCondition {
	for idx := range conditions {
		c := &conditions[idx]
		if c.Type == condType {
			return c
		}
	}
	return nil
}

func describePodIssue(pod *corev1.Pod) string {
	readyCond := getPodCondition(pod.Status.Conditions, corev1.PodReady)
	if readyCond != nil && readyCond.Reason != "" {
		if readyCond.Message != "" {
			return fmt.Sprintf("%s：%s", readyCond.Reason, readyCond.Message)
		}
		return readyCond.Reason
	}

	waitingMessages := make([]string, 0)
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil {
			msg := cs.State.Waiting.Reason
			if cs.State.Waiting.Message != "" {
				msg = fmt.Sprintf("%s：%s", msg, cs.State.Waiting.Message)
			}
			waitingMessages = append(waitingMessages, fmt.Sprintf("Container %s waiting (%s)", cs.Name, msg))
		} else if cs.State.Terminated != nil {
			terminated := cs.State.Terminated
			desc := fmt.Sprintf("Container %s terminated (%s)", cs.Name, terminated.Reason)
			if terminated.ExitCode != 0 {
				desc = fmt.Sprintf("%s, exit code %d", desc, terminated.ExitCode)
			}
			waitingMessages = append(waitingMessages, desc)
		} else if !cs.Ready {
			waitingMessages = append(waitingMessages, fmt.Sprintf("Container %s is not ready", cs.Name))
		}
	}

	if len(waitingMessages) > 0 {
		return strings.Join(waitingMessages, "; ")
	}

	if pod.Status.Message != "" {
		return pod.Status.Message
	}
	if pod.Status.Reason != "" {
		return pod.Status.Reason
	}
	return ""
}

func uniqueSortedStrings(values []string) []string {
	if len(values) == 0 {
		return values
	}
	set := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			continue
		}
		if _, ok := set[trimmed]; ok {
			continue
		}
		set[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	sort.Strings(out)
	return out
}
