package service

import (
	context "context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/google/uuid"

	spec "polardbx-dashboard-backend/pkg/api/domain/monitoring/spec"
)

const (
	DefaultMonitoringNamespace = "polardbx-monitor"

	fixPrometheusRestart      = "monitoring::prometheus::restart"
	fixGrafanaRestoreAdmin    = "monitoring::grafana::restore-admin-secret"
	fixAlertmanagerRefresh    = "monitoring::alertmanager::refresh-config"
	grafanaAdminSecretName    = "grafana-admin"
	alertmanagerConfigMapName = "alertmanager-main"
	alertmanagerAltConfigName = "kube-prometheus-stack-alertmanager"
	prometheusPrimarySts      = "prometheus-k8s"
	prometheusAlternateSts    = "kube-prometheus-stack-prometheus"
)

// AutoFixInput carries contextual information for executing auto-fix actions.
type AutoFixInput struct {
	SessionID  string
	Namespace  string
	Plan       *spec.InstallationPlan
	Checkpoint *spec.Checkpoint
	Status     *spec.InstallStatusResponse
	Context    map[string]string
	Component  *spec.ComponentName
}

// AutoFixService executes automated remediation for known fixes.
type AutoFixService interface {
	Apply(ctx context.Context, req spec.AutoFixRequest, input AutoFixInput) (spec.AutoFixResponse, error)
}

type autoFixService struct{}

// NewAutoFixService constructs the default auto-fix executor.
func NewAutoFixService() AutoFixService {
	return &autoFixService{}
}

func (s *autoFixService) Apply(ctx context.Context, req spec.AutoFixRequest, input AutoFixInput) (spec.AutoFixResponse, error) {
	fixID := strings.TrimSpace(req.FixId)
	if fixID == "" {
		return spec.AutoFixResponse{}, fmt.Errorf("fixId is required")
	}

	namespace := strings.TrimSpace(input.Namespace)
	if namespace == "" {
		namespace = DefaultMonitoringNamespace
	}

	cli := controllerClientFromContext(ctx)
	cs := clientsetFromContext(ctx)

	if err := ensureAutoFixOverlay(ctx, namespace, cli, cs); err != nil {
		return spec.AutoFixResponse{}, err
	}

	resp := spec.AutoFixResponse{}
	var message string

	switch fixID {
	case fixPrometheusRestart:
		if cli == nil {
			return spec.AutoFixResponse{}, fmt.Errorf("kubernetes client unavailable for prometheus restart")
		}
		name, err := restartStatefulSet(ctx, cli, namespace, []string{prometheusPrimarySts, prometheusAlternateSts})
		if err != nil {
			return spec.AutoFixResponse{}, err
		}
		message = fmt.Sprintf("Prometheus StatefulSet %s restarted", name)

	case fixGrafanaRestoreAdmin:
		if cs == nil {
			return spec.AutoFixResponse{}, fmt.Errorf("kubernetes clientset unavailable for grafana secret restore")
		}
		secretName, err := ensureGrafanaAdminSecret(ctx, cs, namespace, []string{grafanaAdminSecretName})
		if err != nil {
			return spec.AutoFixResponse{}, err
		}
		message = fmt.Sprintf("Grafana admin secret %s refreshed", secretName)

	case fixAlertmanagerRefresh:
		if cli == nil {
			return spec.AutoFixResponse{}, fmt.Errorf("kubernetes client unavailable for alertmanager config refresh")
		}
		name, err := touchConfigMap(ctx, cli, namespace, []string{alertmanagerConfigMapName, alertmanagerAltConfigName}, "monitoring.polardbx.com/refreshRequested")
		if err != nil {
			return spec.AutoFixResponse{}, err
		}
		message = fmt.Sprintf("Alertmanager configmap %s annotated for refresh", name)

	default:
		return spec.AutoFixResponse{}, fmt.Errorf("unsupported fixId %s", fixID)
	}

	resp.Success = true
	resp.Message = &message
	return resp, nil
}

func restartStatefulSet(ctx context.Context, cli client.Client, namespace string, candidates []string) (string, error) {
	for _, name := range candidates {
		if name == "" {
			continue
		}
		sts := &appsv1.StatefulSet{}
		if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, sts); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return "", err
		}
		original := sts.DeepCopy()
		if sts.Spec.Template.Annotations == nil {
			sts.Spec.Template.Annotations = map[string]string{}
		}
		sts.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().UTC().Format(time.RFC3339)
		if err := cli.Patch(ctx, sts, client.MergeFrom(original)); err != nil {
			return "", err
		}
		return name, nil
	}
	return "", fmt.Errorf("no matching prometheus statefulset found in namespace %s", namespace)
}

func ensureGrafanaAdminSecret(ctx context.Context, cs kubernetes.Interface, namespace string, candidates []string) (string, error) {
	password := uuid.NewString()
	data := map[string][]byte{
		"admin-user":     []byte("admin"),
		"admin-password": []byte(password),
	}
	for _, name := range candidates {
		if name == "" {
			continue
		}
		existing, err := cs.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return "", err
			}
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
				Type:       corev1.SecretTypeOpaque,
				Data:       data,
			}
			if _, err := cs.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
				return "", err
			}
			return name, nil
		}
		existingCopy := existing.DeepCopy()
		existingCopy.Data = data
		if _, err := cs.CoreV1().Secrets(namespace).Update(ctx, existingCopy, metav1.UpdateOptions{}); err != nil {
			return "", err
		}
		return name, nil
	}
	return "", fmt.Errorf("no grafana admin secret candidates found")
}

func touchConfigMap(ctx context.Context, cli client.Client, namespace string, candidates []string, annotation string) (string, error) {
	for _, name := range candidates {
		if name == "" {
			continue
		}
		cm := &corev1.ConfigMap{}
		if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cm); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return "", err
		}
		original := cm.DeepCopy()
		if cm.Annotations == nil {
			cm.Annotations = map[string]string{}
		}
		cm.Annotations[annotation] = time.Now().UTC().Format(time.RFC3339)
		if err := cli.Patch(ctx, cm, client.MergeFrom(original)); err != nil {
			return "", err
		}
		return name, nil
	}
	return "", fmt.Errorf("no alertmanager configmap found in namespace %s", namespace)
}
