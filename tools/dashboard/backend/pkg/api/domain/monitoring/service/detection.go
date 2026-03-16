package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	spec "polardbx-dashboard-backend/pkg/api/domain/monitoring/spec"
)

var ErrDetectionNotImplemented = errors.New("monitoring environment detection not implemented")

type ctxKey string

const (
	ctxKeyControllerClient ctxKey = "monitoringv2/detect/controller"
	ctxKeyClientset        ctxKey = "monitoringv2/detect/clientset"
)

func ContextWithControllerClient(ctx context.Context, cli client.Client) context.Context {
	if cli == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyControllerClient, cli)
}

func ContextWithClientset(ctx context.Context, cs kubernetes.Interface) context.Context {
	if cs == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyClientset, cs)
}

func controllerClientFromContext(ctx context.Context) client.Client {
	if v := ctx.Value(ctxKeyControllerClient); v != nil {
		if cli, ok := v.(client.Client); ok {
			return cli
		}
	}
	return nil
}

func clientsetFromContext(ctx context.Context) kubernetes.Interface {
	if v := ctx.Value(ctxKeyClientset); v != nil {
		if cs, ok := v.(kubernetes.Interface); ok {
			return cs
		}
	}
	return nil
}

// ControllerClientFromContext returns controller-runtime client if present.
func ControllerClientFromContext(ctx context.Context) client.Client {
	return controllerClientFromContext(ctx)
}

// ClientsetFromContext returns client-go clientset if present.
func ClientsetFromContext(ctx context.Context) kubernetes.Interface {
	return clientsetFromContext(ctx)
}

type DetectionService interface {
	Detect(ctx context.Context, namespace string) (spec.EnvironmentSnapshot, error)
}

type Option func(*detectionService)

type Scenario struct {
	Components      []spec.DetectedComponent
	HealthScore     *int32
	Recommendations []string
}

type detectionService struct {
	clock    func() time.Time
	scenario *Scenario
}

func NewDetectionService(opts ...Option) DetectionService {
	svc := &detectionService{
		clock: time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(svc)
		}
	}
	return svc
}

func WithClock(clock func() time.Time) Option {
	return func(svc *detectionService) {
		if clock != nil {
			svc.clock = clock
		}
	}
}

func WithScenario(s Scenario) Option {
	return func(svc *detectionService) {
		scenario := cloneScenario(s)
		svc.scenario = &scenario
	}
}

func (s *detectionService) Detect(ctx context.Context, namespace string) (spec.EnvironmentSnapshot, error) {
	if s.scenario != nil {
		now := s.clock().UTC()
		scenario := cloneScenario(*s.scenario)
		comps := cloneComponents(scenario.Components)

		snapshot := spec.EnvironmentSnapshot{
			Namespace:  namespace,
			DetectedAt: now,
			Components: comps,
		}
		if scenario.HealthScore != nil {
			snapshot.HealthScore = cloneInt32Ptr(scenario.HealthScore)
		}
		if len(scenario.Recommendations) > 0 {
			recs := append([]string{}, scenario.Recommendations...)
			snapshot.Recommendations = &recs
		}
		return snapshot, nil
	}

	cli := controllerClientFromContext(ctx)
	if cli == nil {
		return spec.EnvironmentSnapshot{}, ErrDetectionNotImplemented
	}

	now := s.clock().UTC()
	components := make([]spec.DetectedComponent, 0, len(defaultComponentDefinitions))
	recommendations := make([]string, 0)
	healthyCount := 0

	for _, def := range defaultComponentDefinitions {
		component, rec, err := s.inspectComponent(ctx, cli, namespace, def)
		if err != nil {
			return spec.EnvironmentSnapshot{}, err
		}
		components = append(components, component)
		if component.Healthy != nil && *component.Healthy {
			healthyCount++
		}
		if rec != "" {
			recommendations = append(recommendations, rec)
		}
	}

	snapshot := spec.EnvironmentSnapshot{
		Namespace:  namespace,
		DetectedAt: now,
		Components: components,
	}
	if len(defaultComponentDefinitions) > 0 {
		score := int32(math.Round(float64(healthyCount) / float64(len(defaultComponentDefinitions)) * 100))
		snapshot.HealthScore = &score
	}
	if len(recommendations) > 0 {
		snapshot.Recommendations = &recommendations
	}
	return snapshot, nil
}

func cloneScenario(s Scenario) Scenario {
	return Scenario{
		Components:      cloneComponents(s.Components),
		HealthScore:     cloneInt32Ptr(s.HealthScore),
		Recommendations: append([]string{}, s.Recommendations...),
	}
}

func cloneComponents(components []spec.DetectedComponent) []spec.DetectedComponent {
	if len(components) == 0 {
		return nil
	}
	out := make([]spec.DetectedComponent, len(components))
	for i, c := range components {
		out[i] = cloneDetectedComponent(c)
	}
	return out
}

func cloneDetectedComponent(c spec.DetectedComponent) spec.DetectedComponent {
	clone := c
	if c.Exists != nil {
		clone.Exists = boolPtr(*c.Exists)
	}
	if c.Healthy != nil {
		clone.Healthy = boolPtr(*c.Healthy)
	}
	if c.Version != nil {
		clone.Version = stringPtr(*c.Version)
	}
	if c.ActionRecommendation.Reason != "" {
		clone.ActionRecommendation.Reason = c.ActionRecommendation.Reason
	}
	return clone
}

func boolPtr(v bool) *bool       { return &v }
func stringPtr(v string) *string { return &v }
func int32Ptr(v int32) *int32    { return &v }

func cloneInt32Ptr(v *int32) *int32 {
	if v == nil {
		return nil
	}
	copy := *v
	return &copy
}

type componentDefinition struct {
	name         spec.ComponentName
	statefulSets []string
	deployments  []string
	daemonSets   []string
	services     []string
}

var defaultComponentDefinitions = []componentDefinition{
	{
		name:         spec.Prometheus,
		statefulSets: []string{"prometheus-k8s", "kube-prometheus-stack-prometheus"},
		services:     []string{"prometheus-k8s", "kube-prometheus-stack-prometheus"},
	},
	{
		name:        spec.Grafana,
		deployments: []string{"grafana", "kube-prometheus-stack-grafana"},
		services:    []string{"grafana", "kube-prometheus-stack-grafana"},
	},
	{
		name:         spec.Alertmanager,
		statefulSets: []string{"alertmanager-main", "kube-prometheus-stack-alertmanager"},
		services:     []string{"alertmanager-main", "kube-prometheus-stack-alertmanager"},
	},
	{
		name:       spec.NodeExporter,
		daemonSets: []string{"node-exporter", "kube-prometheus-stack-prometheus-node-exporter"},
		services:   []string{"node-exporter", "kube-prometheus-stack-prometheus-node-exporter"},
	},
	{
		name:        spec.KubeStateMetrics,
		deployments: []string{"kube-state-metrics", "kube-prometheus-stack-kube-state-metrics"},
		services:    []string{"kube-state-metrics", "kube-prometheus-stack-kube-state-metrics"},
	},
	{
		name:        spec.BlackboxExporter,
		deployments: []string{"blackbox-exporter", "kube-prometheus-stack-prometheus-blackbox-exporter"},
		services:    []string{"blackbox-exporter", "kube-prometheus-stack-prometheus-blackbox-exporter"},
	},
	{
		name:        spec.Thanos,
		deployments: []string{"thanos-query", "kube-prometheus-stack-thanos-query"},
		services:    []string{"thanos-query", "kube-prometheus-stack-thanos-query"},
	},
}

type workloadInfo struct {
	resource string
	ready    int32
	desired  int32
	healthy  bool
	version  string
}

func (s *detectionService) inspectComponent(ctx context.Context, cli client.Client, namespace string, def componentDefinition) (spec.DetectedComponent, string, error) {
	result := spec.DetectedComponent{Name: def.name}
	details := map[string]string{}
	var exists bool
	var healthy bool
	var version string

	if info, err := s.checkStatefulSets(ctx, cli, namespace, def.statefulSets); err != nil {
		return spec.DetectedComponent{}, "", err
	} else if info != nil {
		exists = true
		healthy = info.healthy
		version = info.version
		details["workload"] = info.resource
		details["readyReplicas"] = strconv.Itoa(int(info.ready))
		details["replicas"] = strconv.Itoa(int(info.desired))
	}

	if !exists {
		if info, err := s.checkDeployments(ctx, cli, namespace, def.deployments); err != nil {
			return spec.DetectedComponent{}, "", err
		} else if info != nil {
			exists = true
			healthy = info.healthy
			version = info.version
			details["workload"] = info.resource
			details["readyReplicas"] = strconv.Itoa(int(info.ready))
			details["replicas"] = strconv.Itoa(int(info.desired))
		}
	}

	if !exists {
		if info, err := s.checkDaemonSets(ctx, cli, namespace, def.daemonSets); err != nil {
			return spec.DetectedComponent{}, "", err
		} else if info != nil {
			exists = true
			healthy = info.healthy
			version = info.version
			details["workload"] = info.resource
		}
	}

	if svcName, err := s.findService(ctx, cli, namespace, def.services); err != nil {
		return spec.DetectedComponent{}, "", err
	} else if svcName != "" {
		details["service"] = svcName
	}

	var action spec.ComponentActionAction
	var reason string
	switch {
	case !exists:
		action = spec.ComponentActionActionInstall
		reason = fmt.Sprintf("%s not found in namespace %s", def.name, namespace)
	case !healthy:
		action = spec.ComponentActionActionRepair
		reason = fmt.Sprintf("%s workload is not healthy", def.name)
	default:
		action = spec.ComponentActionActionSkip
		reason = "component is installed and healthy"
	}

	result.ActionRecommendation = spec.ComponentAction{Action: action, Reason: reason}
	result.Exists = boolPtr(exists)
	result.Healthy = boolPtr(exists && healthy)
	if version != "" {
		result.Version = stringPtr(version)
	}
	if len(details) > 0 {
		result.Details = &details
	}

	recommendation := ""
	if action != spec.ComponentActionActionSkip {
		recommendation = reason
	}
	return result, recommendation, nil
}

func (s *detectionService) checkStatefulSets(ctx context.Context, cli client.Client, namespace string, names []string) (*workloadInfo, error) {
	for _, name := range names {
		if name == "" {
			continue
		}
		sts := &appsv1.StatefulSet{}
		if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, sts); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		desired := int32(1)
		if sts.Spec.Replicas != nil {
			desired = *sts.Spec.Replicas
		} else if sts.Status.Replicas > 0 {
			desired = sts.Status.Replicas
		}
		ready := sts.Status.ReadyReplicas
		healthy := desired > 0 && ready >= desired && sts.Status.CurrentRevision == sts.Status.UpdateRevision
		return &workloadInfo{
			resource: fmt.Sprintf("StatefulSet/%s", name),
			ready:    ready,
			desired:  desired,
			healthy:  healthy,
			version:  firstContainerVersion(sts.Spec.Template.Spec.Containers),
		}, nil
	}
	return nil, nil
}

func (s *detectionService) checkDeployments(ctx context.Context, cli client.Client, namespace string, names []string) (*workloadInfo, error) {
	for _, name := range names {
		if name == "" {
			continue
		}
		dep := &appsv1.Deployment{}
		if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, dep); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		desired := int32(1)
		if dep.Spec.Replicas != nil {
			desired = *dep.Spec.Replicas
		}
		ready := dep.Status.ReadyReplicas
		healthy := desired > 0 && ready >= desired && dep.Status.UnavailableReplicas == 0
		return &workloadInfo{
			resource: fmt.Sprintf("Deployment/%s", name),
			ready:    ready,
			desired:  desired,
			healthy:  healthy,
			version:  firstContainerVersion(dep.Spec.Template.Spec.Containers),
		}, nil
	}
	return nil, nil
}

func (s *detectionService) checkDaemonSets(ctx context.Context, cli client.Client, namespace string, names []string) (*workloadInfo, error) {
	for _, name := range names {
		if name == "" {
			continue
		}
		daemon := &appsv1.DaemonSet{}
		if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, daemon); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		desired := daemon.Status.DesiredNumberScheduled
		ready := daemon.Status.NumberReady
		healthy := desired > 0 && ready >= desired
		return &workloadInfo{
			resource: fmt.Sprintf("DaemonSet/%s", name),
			ready:    ready,
			desired:  desired,
			healthy:  healthy,
			version:  firstContainerVersion(daemon.Spec.Template.Spec.Containers),
		}, nil
	}
	return nil, nil
}

func (s *detectionService) findService(ctx context.Context, cli client.Client, namespace string, names []string) (string, error) {
	for _, name := range names {
		if name == "" {
			continue
		}
		svc := &corev1.Service{}
		if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, svc); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return "", err
		}
		return svc.Name, nil
	}
	return "", nil
}

func firstContainerVersion(containers []corev1.Container) string {
	for _, container := range containers {
		image := container.Image
		if image == "" {
			continue
		}
		if at := strings.Index(image, "@"); at > -1 {
			image = image[:at]
		}
		if colon := strings.LastIndex(image, ":"); colon > -1 {
			return image[colon+1:]
		}
	}
	return ""
}
