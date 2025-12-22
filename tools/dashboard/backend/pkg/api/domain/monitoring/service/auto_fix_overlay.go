package service

import (
	"context"
	"fmt"
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
	coreerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"k8s.io/client-go/kubernetes"

	"polardbx-dashboard-backend/pkg/config"
)

func ensureAutoFixOverlay(ctx context.Context, namespace string, cli client.Client, cs kubernetes.Interface) error {
	cfg := config.GetAutoFixOverlayConfig()
	if !cfg.Enabled {
		return nil
	}

	targetNamespace := strings.TrimSpace(namespace)
	if targetNamespace == "" {
		targetNamespace = cfg.Namespace
	}
	if targetNamespace == "" {
		targetNamespace = DefaultMonitoringNamespace
	}

	if cli == nil {
		return fmt.Errorf("auto-fix overlay requires controller client when enabled")
	}

	if err := annotateAutoFixTargets(ctx, cli, targetNamespace, cfg); err != nil {
		return err
	}

	if cs != nil && cfg.ServiceAccount != "" {
		if err := ensureAutoFixRBAC(ctx, cs, targetNamespace, cfg); err != nil {
			return err
		}
	}

	return nil
}

func annotateAutoFixTargets(ctx context.Context, cli client.Client, namespace string, cfg *config.AutoFixOverlayConfig) error {
	annotationKey := cfg.AnnotationKey
	targets := []struct {
		gvk   schema.GroupVersionKind
		names []string
		value string
	}{
		{schema.GroupVersionKind{Group: "monitoring.coreos.com", Version: "v1", Kind: "Prometheus"}, cfg.PrometheusTargets, fixPrometheusRestart},
		{schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}, cfg.GrafanaTargets, fixGrafanaRestoreAdmin},
		{schema.GroupVersionKind{Group: "monitoring.coreos.com", Version: "v1", Kind: "Alertmanager"}, cfg.AlertmanagerTargets, fixAlertmanagerRefresh},
	}

	for _, target := range targets {
		if err := annotateFirstMatch(ctx, cli, namespace, target.gvk, target.names, annotationKey, target.value); err != nil {
			return err
		}
	}
	return nil
}

func annotateFirstMatch(ctx context.Context, cli client.Client, namespace string, gvk schema.GroupVersionKind, names []string, key, value string) error {
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(gvk)
		if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, obj); err != nil {
			if coreerrors.IsNotFound(err) {
				continue
			}
			return err
		}

		ann := obj.GetAnnotations()
		if ann == nil {
			ann = map[string]string{}
		}
		if current, ok := ann[key]; ok && current == value {
			return nil
		}
		original := obj.DeepCopy()
		ann[key] = value
		obj.SetAnnotations(ann)
		if err := cli.Patch(ctx, obj, client.MergeFrom(original)); err != nil {
			return err
		}
		return nil
	}
	return nil
}

func ensureAutoFixRBAC(ctx context.Context, cs kubernetes.Interface, namespace string, cfg *config.AutoFixOverlayConfig) error {
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: cfg.ClusterRoleName},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"apps"}, Resources: []string{"statefulsets", "deployments", "daemonsets"}, Verbs: []string{"get", "list", "watch", "patch"}},
			{APIGroups: []string{""}, Resources: []string{"services", "pods"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch"}},
			{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch"}},
			{APIGroups: []string{"batch"}, Resources: []string{"jobs"}, Verbs: []string{"create", "delete", "get", "list", "watch"}},
			{APIGroups: []string{"monitoring.coreos.com"}, Resources: []string{"prometheuses", "alertmanagers"}, Verbs: []string{"get", "list", "watch", "patch"}},
			{APIGroups: []string{"polardbx.aliyun.com"}, Resources: []string{"polardbxmonitors"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch"}},
		},
	}

	existingRole, err := cs.RbacV1().ClusterRoles().Get(ctx, role.Name, metav1.GetOptions{})
	if err != nil {
		if coreerrors.IsNotFound(err) {
			if _, createErr := cs.RbacV1().ClusterRoles().Create(ctx, role, metav1.CreateOptions{}); createErr != nil {
				return createErr
			}
		} else {
			return err
		}
	} else {
		if !policyRulesEqual(existingRole.Rules, role.Rules) {
			updated := existingRole.DeepCopy()
			updated.Rules = role.Rules
			if _, updateErr := cs.RbacV1().ClusterRoles().Update(ctx, updated, metav1.UpdateOptions{}); updateErr != nil {
				return updateErr
			}
		}
	}

	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: cfg.ClusterRoleBinding},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: cfg.ClusterRoleName},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: cfg.ServiceAccount, Namespace: namespace}},
	}

	existingBinding, err := cs.RbacV1().ClusterRoleBindings().Get(ctx, binding.Name, metav1.GetOptions{})
	if err != nil {
		if coreerrors.IsNotFound(err) {
			if _, createErr := cs.RbacV1().ClusterRoleBindings().Create(ctx, binding, metav1.CreateOptions{}); createErr != nil {
				return createErr
			}
		} else {
			return err
		}
	} else {
		needsUpdate := existingBinding.RoleRef.Name != binding.RoleRef.Name || !subjectsEqual(existingBinding.Subjects, binding.Subjects)
		if needsUpdate {
			updated := existingBinding.DeepCopy()
			updated.RoleRef = binding.RoleRef
			updated.Subjects = binding.Subjects
			if _, updateErr := cs.RbacV1().ClusterRoleBindings().Update(ctx, updated, metav1.UpdateOptions{}); updateErr != nil {
				return updateErr
			}
		}
	}

	return nil
}

func policyRulesEqual(a, b []rbacv1.PolicyRule) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !policyRuleEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func policyRuleEqual(a, b rbacv1.PolicyRule) bool {
	if !stringSlicesEqual(a.APIGroups, b.APIGroups) {
		return false
	}
	if !stringSlicesEqual(a.Resources, b.Resources) {
		return false
	}
	if !stringSlicesEqual(a.Verbs, b.Verbs) {
		return false
	}
	if !stringSlicesEqual(a.ResourceNames, b.ResourceNames) {
		return false
	}
	if !stringSlicesEqual(a.NonResourceURLs, b.NonResourceURLs) {
		return false
	}
	return true
}

func subjectsEqual(a, b []rbacv1.Subject) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Kind != b[i].Kind || a[i].Name != b[i].Name || a[i].Namespace != b[i].Namespace || a[i].APIGroup != b[i].APIGroup {
			return false
		}
	}
	return true
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
