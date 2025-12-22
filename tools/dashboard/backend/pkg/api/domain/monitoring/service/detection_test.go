package service

import (
	"context"
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

func newScheme(t *testing.T) *runtime.Scheme {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	return scheme
}

func TestDetectionService_AllComponentsMissing(t *testing.T) {
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	svc := NewDetectionService()
	ctx := ContextWithControllerClient(context.Background(), cli)

	snapshot, err := svc.Detect(ctx, "polardbx-monitor")
	require.NoError(t, err)
	require.Len(t, snapshot.Components, len(defaultComponentDefinitions))

	for _, comp := range snapshot.Components {
		require.NotNil(t, comp.Exists)
		require.False(t, *comp.Exists)
		require.Equal(t, spec.ComponentActionActionInstall, comp.ActionRecommendation.Action)
	}

	require.NotNil(t, snapshot.HealthScore)
	require.Equal(t, int32(0), *snapshot.HealthScore)
}

func TestDetectionService_HealthyComponents(t *testing.T) {
	scheme := newScheme(t)
	replicas := int32(1)

	prom := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "prometheus-k8s", Namespace: "observability"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: pointer.Int32(replicas),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "prometheus", Image: "prom/prometheus:v2.48.0"}},
				},
			},
		},
		Status: appsv1.StatefulSetStatus{
			Replicas:        replicas,
			ReadyReplicas:   replicas,
			CurrentRevision: "rev",
			UpdateRevision:  "rev",
		},
	}

	graf := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "grafana", Namespace: "observability"},
		Spec: appsv1.DeploymentSpec{
			Replicas: pointer.Int32(replicas),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "grafana", Image: "grafana/grafana:10.4.0"}},
				},
			},
		},
		Status: appsv1.DeploymentStatus{
			ReadyReplicas:       replicas,
			AvailableReplicas:   replicas,
			Replicas:            replicas,
			UnavailableReplicas: 0,
		},
	}

	alert := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "alertmanager-main", Namespace: "observability"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: pointer.Int32(replicas),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "alertmanager", Image: "quay.io/prometheus/alertmanager:v0.26.0"}},
				},
			},
		},
		Status: appsv1.StatefulSetStatus{
			Replicas:        replicas,
			ReadyReplicas:   replicas,
			CurrentRevision: "rev",
			UpdateRevision:  "rev",
		},
	}

	nodeExporter := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "node-exporter", Namespace: "observability"},
		Spec: appsv1.DaemonSetSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "node-exporter", Image: "quay.io/prometheus/node-exporter:v1.7.0"}},
				},
			},
		},
		Status: appsv1.DaemonSetStatus{
			DesiredNumberScheduled: replicas,
			NumberReady:            replicas,
		},
	}

	ksm := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "kube-state-metrics", Namespace: "observability"},
		Spec: appsv1.DeploymentSpec{
			Replicas: pointer.Int32(replicas),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "ksm", Image: "k8s.gcr.io/kube-state-metrics:v2.10.0"}},
				},
			},
		},
		Status: appsv1.DeploymentStatus{
			ReadyReplicas:       replicas,
			AvailableReplicas:   replicas,
			Replicas:            replicas,
			UnavailableReplicas: 0,
		},
	}

	blackbox := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "blackbox-exporter", Namespace: "observability"},
		Spec: appsv1.DeploymentSpec{
			Replicas: pointer.Int32(replicas),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "blackbox-exporter", Image: "prom/blackbox-exporter:v0.24.0"}},
				},
			},
		},
		Status: appsv1.DeploymentStatus{
			ReadyReplicas:       replicas,
			AvailableReplicas:   replicas,
			Replicas:            replicas,
			UnavailableReplicas: 0,
		},
	}

	thanos := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "thanos-query", Namespace: "observability"},
		Spec: appsv1.DeploymentSpec{
			Replicas: pointer.Int32(replicas),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "thanos", Image: "quay.io/thanos/thanos:v0.35.0"}},
				},
			},
		},
		Status: appsv1.DeploymentStatus{
			ReadyReplicas:       replicas,
			AvailableReplicas:   replicas,
			Replicas:            replicas,
			UnavailableReplicas: 0,
		},
	}

	svcBuilder := func(name string) *corev1.Service {
		return &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "observability"},
			Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
		}
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(
		prom, graf, alert, nodeExporter, ksm, blackbox, thanos,
		svcBuilder("prometheus-k8s"),
		svcBuilder("grafana"),
		svcBuilder("alertmanager-main"),
		svcBuilder("node-exporter"),
		svcBuilder("kube-state-metrics"),
		svcBuilder("blackbox-exporter"),
		svcBuilder("thanos-query"),
	).Build()

	svc := NewDetectionService(WithClock(func() time.Time { return time.Date(2025, 10, 27, 8, 0, 0, 0, time.UTC) }))
	ctx := ContextWithControllerClient(context.Background(), cli)

	snapshot, err := svc.Detect(ctx, "observability")
	require.NoError(t, err)
	require.Len(t, snapshot.Components, len(defaultComponentDefinitions))

	for _, comp := range snapshot.Components {
		require.NotNil(t, comp.Exists)
		require.True(t, *comp.Exists)
		require.NotNil(t, comp.Healthy)
		require.True(t, *comp.Healthy)
		require.Equal(t, spec.ComponentActionActionSkip, comp.ActionRecommendation.Action)
		require.Contains(t, comp.ActionRecommendation.Reason, "healthy")
	}

	require.NotNil(t, snapshot.HealthScore)
	require.Equal(t, int32(100), *snapshot.HealthScore)
	require.Equal(t, "observability", snapshot.Namespace)
	require.False(t, snapshot.DetectedAt.IsZero())
}
