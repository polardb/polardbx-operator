package service

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	spec "polardbx-dashboard-backend/pkg/api/domain/monitoring/spec"
	"polardbx-dashboard-backend/pkg/config"
)

func withAutoFixOverlayConfig(t *testing.T, mutate func(cfg *config.AutoFixOverlayConfig)) {
	cfg := config.GetAutoFixOverlayConfig()
	original := *cfg
	if mutate != nil {
		mutate(cfg)
	}
	t.Cleanup(func() {
		*cfg = original
	})
}

func TestAutoFixServiceRestartPrometheus(t *testing.T) {
	withAutoFixOverlayConfig(t, func(cfg *config.AutoFixOverlayConfig) {
		cfg.Enabled = false
	})

	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add apps scheme: %v", err)
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "polardbx-monitor", Name: prometheusPrimarySts},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts).Build()
	svc := NewAutoFixService()
	ctx := ContextWithControllerClient(context.Background(), cli)

	resp, err := svc.Apply(ctx, spec.AutoFixRequest{FixId: fixPrometheusRestart}, AutoFixInput{Namespace: "polardbx-monitor"})
	if err != nil {
		t.Fatalf("apply returned error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success response: %+v", resp)
	}
	if resp.Message == nil || *resp.Message == "" {
		t.Fatalf("expected non-empty message")
	}

	updated := &appsv1.StatefulSet{}
	if err := cli.Get(ctx, ctrlclient.ObjectKey{Namespace: sts.Namespace, Name: sts.Name}, updated); err != nil {
		t.Fatalf("failed to read statefulset: %v", err)
	}
	value, ok := updated.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"]
	if !ok || value == "" {
		t.Fatalf("expected restart annotation to be set")
	}
}

func TestAutoFixServiceEnsureGrafanaSecret(t *testing.T) {
	withAutoFixOverlayConfig(t, func(cfg *config.AutoFixOverlayConfig) {
		cfg.Enabled = false
	})

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "polardbx-monitor", Name: grafanaAdminSecretName},
		Data:       map[string][]byte{"admin-user": []byte("old"), "admin-password": []byte("stale")},
	}

	cs := k8sfake.NewSimpleClientset(secret)
	svc := NewAutoFixService()
	ctx := ContextWithClientset(context.Background(), cs)

	resp, err := svc.Apply(ctx, spec.AutoFixRequest{FixId: fixGrafanaRestoreAdmin}, AutoFixInput{Namespace: "polardbx-monitor"})
	if err != nil {
		t.Fatalf("apply returned error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success response")
	}

	updated, err := cs.CoreV1().Secrets("polardbx-monitor").Get(ctx, grafanaAdminSecretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to read secret: %v", err)
	}
	if string(updated.Data["admin-user"]) != "admin" {
		t.Fatalf("expected admin-user to be reset to admin")
	}
	if len(updated.Data["admin-password"]) == 0 {
		t.Fatalf("expected admin-password to be regenerated")
	}
}

func TestAutoFixServiceRefreshAlertmanagerConfig(t *testing.T) {
	withAutoFixOverlayConfig(t, func(cfg *config.AutoFixOverlayConfig) {
		cfg.Enabled = false
	})

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}

	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "polardbx-monitor", Name: alertmanagerConfigMapName}}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
	svc := NewAutoFixService()
	ctx := ContextWithControllerClient(context.Background(), cli)

	resp, err := svc.Apply(ctx, spec.AutoFixRequest{FixId: fixAlertmanagerRefresh}, AutoFixInput{Namespace: "polardbx-monitor"})
	if err != nil {
		t.Fatalf("apply returned error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success response")
	}

	updated := &corev1.ConfigMap{}
	if err := cli.Get(ctx, ctrlclient.ObjectKey{Namespace: cm.Namespace, Name: cm.Name}, updated); err != nil {
		t.Fatalf("failed to read configmap: %v", err)
	}
	if updated.Annotations["monitoring.polardbx.com/refreshRequested"] == "" {
		t.Fatalf("expected refresh annotation to be set")
	}
}

func TestEnsureAutoFixOverlayAnnotatesAndCreatesRBAC(t *testing.T) {
	withAutoFixOverlayConfig(t, func(cfg *config.AutoFixOverlayConfig) {
		cfg.Enabled = true
		cfg.Namespace = "obs"
		cfg.ServiceAccount = "installer"
		cfg.ClusterRoleName = "polardbx-monitor-autofix"
		cfg.ClusterRoleBinding = "polardbx-monitor-autofix"
		cfg.AnnotationKey = "monitoring.polardbx.com/auto-fix-ids"
		cfg.PrometheusTargets = []string{"primary"}
		cfg.GrafanaTargets = []string{"grafana"}
		cfg.AlertmanagerTargets = []string{"alert"}
	})

	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	promGVK := schema.GroupVersionKind{Group: "monitoring.coreos.com", Version: "v1", Kind: "Prometheus"}
	alertGVK := schema.GroupVersionKind{Group: "monitoring.coreos.com", Version: "v1", Kind: "Alertmanager"}
	scheme.AddKnownTypeWithName(promGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(alertGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "polardbx.aliyun.com", Version: "v1", Kind: "PolarDBXMonitor"}, &unstructured.Unstructured{})

	prom := &unstructured.Unstructured{}
	prom.SetGroupVersionKind(promGVK)
	prom.SetNamespace("obs")
	prom.SetName("primary")

	grafana := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "obs", Name: "grafana"}}

	alert := &unstructured.Unstructured{}
	alert.SetGroupVersionKind(alertGVK)
	alert.SetNamespace("obs")
	alert.SetName("alert")

	monitor := &unstructured.Unstructured{}
	monitor.SetGroupVersionKind(schema.GroupVersionKind{Group: "polardbx.aliyun.com", Version: "v1", Kind: "PolarDBXMonitor"})
	monitor.SetNamespace("obs")
	monitor.SetName("stack")

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(prom, grafana, alert, monitor).Build()
	cs := k8sfake.NewSimpleClientset()

	ctx := ContextWithControllerClient(context.Background(), cli)
	ctx = ContextWithClientset(ctx, cs)

	if err := ensureAutoFixOverlay(ctx, "", cli, cs); err != nil {
		t.Fatalf("overlay returned error: %v", err)
	}

	fetchedProm := &unstructured.Unstructured{}
	fetchedProm.SetGroupVersionKind(promGVK)
	if err := cli.Get(ctx, ctrlclient.ObjectKey{Namespace: "obs", Name: "primary"}, fetchedProm); err != nil {
		t.Fatalf("fetch prom: %v", err)
	}
	if fetchedProm.GetAnnotations()["monitoring.polardbx.com/auto-fix-ids"] != fixPrometheusRestart {
		t.Fatalf("expected prom annotation to be %s", fixPrometheusRestart)
	}

	fetchedGrafana := &appsv1.Deployment{}
	if err := cli.Get(ctx, ctrlclient.ObjectKey{Namespace: "obs", Name: "grafana"}, fetchedGrafana); err != nil {
		t.Fatalf("fetch grafana: %v", err)
	}
	if fetchedGrafana.Annotations["monitoring.polardbx.com/auto-fix-ids"] != fixGrafanaRestoreAdmin {
		t.Fatalf("expected grafana annotation to be %s", fixGrafanaRestoreAdmin)
	}

	fetchedAlert := &unstructured.Unstructured{}
	fetchedAlert.SetGroupVersionKind(alertGVK)
	if err := cli.Get(ctx, ctrlclient.ObjectKey{Namespace: "obs", Name: "alert"}, fetchedAlert); err != nil {
		t.Fatalf("fetch alert: %v", err)
	}
	if fetchedAlert.GetAnnotations()["monitoring.polardbx.com/auto-fix-ids"] != fixAlertmanagerRefresh {
		t.Fatalf("expected alert annotation to be %s", fixAlertmanagerRefresh)
	}
	fetchedMonitor := &unstructured.Unstructured{}
	fetchedMonitor.SetGroupVersionKind(schema.GroupVersionKind{Group: "polardbx.aliyun.com", Version: "v1", Kind: "PolarDBXMonitor"})
	if err := cli.Get(ctx, ctrlclient.ObjectKey{Namespace: "obs", Name: "stack"}, fetchedMonitor); err != nil {
		t.Fatalf("fetch monitor: %v", err)
	}

	role, err := cs.RbacV1().ClusterRoles().Get(ctx, "polardbx-monitor-autofix", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("fetch cluster role: %v", err)
	}
	expectedRules := []rbacv1.PolicyRule{
		{APIGroups: []string{"apps"}, Resources: []string{"statefulsets", "deployments", "daemonsets"}, Verbs: []string{"get", "list", "watch", "patch"}},
		{APIGroups: []string{""}, Resources: []string{"services", "pods"}, Verbs: []string{"get", "list", "watch"}},
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch"}},
		{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch"}},
		{APIGroups: []string{"batch"}, Resources: []string{"jobs"}, Verbs: []string{"create", "delete", "get", "list", "watch"}},
		{APIGroups: []string{"monitoring.coreos.com"}, Resources: []string{"prometheuses", "alertmanagers"}, Verbs: []string{"get", "list", "watch", "patch"}},
		{APIGroups: []string{"polardbx.aliyun.com"}, Resources: []string{"polardbxmonitors"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch"}},
	}
	if !policyRulesEqual(role.Rules, expectedRules) {
		t.Fatalf("cluster role rules mismatch: %#v", role.Rules)
	}

	binding, err := cs.RbacV1().ClusterRoleBindings().Get(ctx, "polardbx-monitor-autofix", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("fetch cluster role binding: %v", err)
	}
	if len(binding.Subjects) != 1 || binding.Subjects[0].Kind != "ServiceAccount" || binding.Subjects[0].Name != "installer" || binding.Subjects[0].Namespace != "obs" {
		t.Fatalf("unexpected cluster role binding subjects: %+v", binding.Subjects)
	}
}

func TestEnsureAutoFixOverlaySkipsWhenDisabled(t *testing.T) {
	withAutoFixOverlayConfig(t, func(cfg *config.AutoFixOverlayConfig) {
		cfg.Enabled = false
	})

	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	cs := k8sfake.NewSimpleClientset()

	if err := ensureAutoFixOverlay(context.Background(), "", cli, cs); err != nil {
		t.Fatalf("expected no error when overlay disabled: %v", err)
	}
}
