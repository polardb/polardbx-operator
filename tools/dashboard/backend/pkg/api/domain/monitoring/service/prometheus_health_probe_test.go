package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	spec "polardbx-dashboard-backend/pkg/api/domain/monitoring/spec"
)

func newPrometheusScheme(t *testing.T) *runtime.Scheme {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	return scheme
}

func TestPrometheusHealthProbeHealthy(t *testing.T) {
	scheme := newPrometheusScheme(t)
	replicas := int32(1)

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "prometheus-k8s", Namespace: "observability"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: pointer.Int32(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "prometheus"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "prometheus"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "prometheus", Image: "prom/prometheus:v2.48.0"}},
				},
			},
		},
		Status: appsv1.StatefulSetStatus{
			Replicas:        replicas,
			ReadyReplicas:   replicas,
			CurrentRevision: "rev-1",
			UpdateRevision:  "rev-1",
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "prometheus-k8s-0", Namespace: "observability", Labels: map[string]string{"app": "prometheus"}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "prometheus",
				Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(time.Now())}},
			}},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(sts, pod).Build()
	probe := newPrometheusHealthProbe()

	ctx := ContextWithControllerClient(context.Background(), cli)
	findings, err := probe.Run(ctx, ProbeInput{Namespace: "observability"})
	require.NoError(t, err)
	require.Empty(t, findings)
}

func TestPrometheusHealthProbeUnhealthyPods(t *testing.T) {
	scheme := newPrometheusScheme(t)
	replicas := int32(2)

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "prometheus-k8s", Namespace: "observability"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: pointer.Int32(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "prometheus"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "prometheus"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "prometheus", Image: "prom/prometheus:v2.48.0"}},
				},
			},
		},
		Status: appsv1.StatefulSetStatus{
			Replicas:        replicas,
			ReadyReplicas:   1,
			CurrentRevision: "rev-1",
			UpdateRevision:  "rev-2",
		},
	}

	readyPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "prometheus-k8s-0", Namespace: "observability", Labels: map[string]string{"app": "prometheus"}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "prometheus",
				Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(time.Now())}},
			}},
		},
	}

	crashPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "prometheus-k8s-1", Namespace: "observability", Labels: map[string]string{"app": "prometheus"}},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type:    corev1.PodReady,
				Status:  corev1.ConditionFalse,
				Reason:  "ContainersNotReady",
				Message: "containers with unready status: [prometheus]",
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "prometheus",
				Ready: false,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason:  "CrashLoopBackOff",
					Message: "back-off 5m restarting failed container",
				}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1}},
			}},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(sts, readyPod, crashPod).Build()
	probe := newPrometheusHealthProbe()

	ctx := ContextWithControllerClient(context.Background(), cli)
	findings, err := probe.Run(ctx, ProbeInput{Namespace: "observability"})
	require.NoError(t, err)
	require.Len(t, findings, 1)

	finding := findings[0]
	require.Equal(t, prometheusHealthProbeID, finding.ProbeID)
	require.Equal(t, spec.Dependency, finding.Category)
	require.Equal(t, spec.Major, finding.Severity)
	require.Contains(t, finding.Summary, "Prometheus")
	require.NotEmpty(t, finding.PossibleCauses)
	require.NotEmpty(t, finding.SuggestedFixes)

	fixMatched := false
	for _, fix := range finding.SuggestedFixes {
		if fix.ID == fixPrometheusRestart {
			fixMatched = true
			require.True(t, fix.Automated)
			require.Equal(t, SuggestedFixTypeOperatorHook, fix.Type)
			break
		}
	}
	require.True(t, fixMatched, "expected auto fix suggestion for prometheus restart")

	matched := false
	for _, cause := range finding.PossibleCauses {
		if strings.Contains(cause, "Pod prometheus-k8s-1") {
			matched = true
			break
		}
	}
	require.True(t, matched, "expected pod level cause description")

	require.Equal(t, "1", finding.Details["replicasReady"])
	require.Equal(t, "2", finding.Details["replicasDesired"])
	require.Equal(t, "prometheus-k8s", finding.Details["statefulset"])
}
