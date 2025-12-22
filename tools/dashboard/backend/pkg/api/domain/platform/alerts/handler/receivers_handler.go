package handler

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
)

const (
	alertmanagerConfigLabelKey       = "polardbx.com/alertmanager"
	alertmanagerConfigNamespaceLabel = "polardbx.com/alertmanager-config"
	alertmanagerConfigNamespaceValue = "true"
	managedByLabelKey                = "app.kubernetes.io/managed-by"
	managedByLabelValue              = "polardbx-dashboard"
)

type ApplyReceiverRequest struct {
	TargetNamespace       string `json:"targetNamespace"`
	ReceiverName          string `json:"receiverName"`
	ChannelType           string `json:"channelType"`
	AlertmanagerNamespace string `json:"alertmanagerNamespace"`
	AlertmanagerName      string `json:"alertmanagerName"`
	AlertmanagerURL       string `json:"alertmanagerUrl"`
	AutoEnable            *bool  `json:"autoEnable"`

	Email    *ApplyEmailConfig    `json:"email,omitempty"`
	DingTalk *ApplyDingTalkConfig `json:"dingtalk,omitempty"`
}

type ApplyEmailConfig struct {
	Smarthost  string   `json:"smarthost"`
	From       string   `json:"from"`
	Username   string   `json:"username"`
	Password   string   `json:"password"`
	RequireTLS *bool    `json:"requireTLS"`
	To         []string `json:"to"`
}

type ApplyDingTalkConfig struct {
	AdapterURL string `json:"adapterUrl"`
}

type ApplyReceiverResponse struct {
	TargetNamespace       string   `json:"targetNamespace"`
	AlertmanagerNamespace string   `json:"alertmanagerNamespace"`
	AlertmanagerName      string   `json:"alertmanagerName"`
	ReceiverName          string   `json:"receiverName"`
	Resources             []string `json:"resources"`
	Message               string   `json:"message"`
}

var dns1123LabelRe = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// ApplyReceiverConfig applies (upserts) AlertmanagerConfig + Secrets to Kubernetes.
// It optionally patches Alertmanager (main) to enable AlertmanagerConfig discovery and labels the target namespace.
func ApplyReceiverConfig(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	var req ApplyReceiverRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	targetNS := strings.TrimSpace(req.TargetNamespace)
	if targetNS == "" {
		apierr.AbortWithError(c, apierr.ValidationError("targetNamespace required", nil))
		return
	}
	receiverName := strings.TrimSpace(req.ReceiverName)
	if receiverName == "" || !dns1123LabelRe.MatchString(receiverName) || len(receiverName) > 63 {
		apierr.AbortWithError(c, apierr.ValidationError("receiverName must be a DNS-1123 label (<=63 chars, lowercase letters/digits/hyphen)", nil))
		return
	}

	channelType := strings.ToLower(strings.TrimSpace(req.ChannelType))
	if channelType == "" {
		channelType = "email"
	}

	needEmail := channelType == "email" || channelType == "both"
	needDingTalk := channelType == "dingtalk" || channelType == "both"
	if !needEmail && !needDingTalk {
		apierr.AbortWithError(c, apierr.ValidationError("channelType must be one of: email, dingtalk, both", nil))
		return
	}
	if needEmail && req.Email == nil {
		apierr.AbortWithError(c, apierr.ValidationError("email config required for channelType=email|both", nil))
		return
	}
	if needDingTalk && req.DingTalk == nil {
		apierr.AbortWithError(c, apierr.ValidationError("dingtalk config required for channelType=dingtalk|both", nil))
		return
	}

	alertmanagerNS := strings.TrimSpace(req.AlertmanagerNamespace)
	if alertmanagerNS == "" {
		alertmanagerNS = "polardbx-monitor"
	}
	alertmanagerName := strings.TrimSpace(req.AlertmanagerName)
	if alertmanagerName == "" {
		alertmanagerName = "main"
	}
	autoEnable := req.AutoEnable == nil || *req.AutoEnable

	ctx := c.Request.Context()
	if autoEnable {
		if err := ensureNamespaceLabeled(ctx, cli, targetNS); err != nil {
			apierr.AbortWithError(c, apierr.InternalServiceError("failed to label target namespace", err))
			return
		}
		if err := ensureAlertmanagerConfigEnabled(ctx, cli, alertmanagerNS, alertmanagerName); err != nil {
			if apierrors.IsNotFound(err) {
				apierr.AbortWithError(c, apierr.ValidationError(fmt.Sprintf("Alertmanager %s/%s not found; enable it first (see docs: https://doc.polardbx.com/operator/ops/monitor/5-alert-config.html)", alertmanagerNS, alertmanagerName), nil))
				return
			}
			apierr.AbortWithError(c, apierr.InternalServiceError("failed to enable AlertmanagerConfig discovery", err))
			return
		}
	}

	resources := make([]string, 0, 4)

	var smtpSecretName string
	if needEmail {
		smtpSecretName = fmt.Sprintf("polardbx-alert-receiver-%s-smtp", receiverName)
		if err := applySMTPSecret(ctx, cli, targetNS, smtpSecretName, req.Email); err != nil {
			apierr.AbortWithError(c, apierr.InternalServiceError("failed to apply smtp secret", err))
			return
		}
		resources = append(resources, fmt.Sprintf("Secret/%s", smtpSecretName))
	}

	amcName := fmt.Sprintf("polardbx-alert-receiver-%s", receiverName)
	if err := applyAlertmanagerConfig(ctx, cli, targetNS, amcName, receiverName, alertmanagerName, smtpSecretName, req); err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("failed to apply AlertmanagerConfig", err))
		return
	}
	resources = append(resources, fmt.Sprintf("AlertmanagerConfig/%s", amcName))

	// Persist default alertmanager URL (best-effort) for UI features like silences/test.
	if strings.TrimSpace(req.AlertmanagerURL) != "" {
		if handler, ok := NewAlertsHandlerFromContext(c); ok {
			content, _, _ := handler.service.GetRoutes(ctx)
			_ = handler.service.PutRoutes(ctx, content, strings.TrimSpace(req.AlertmanagerURL))
		}
	}

	apierr.OK(c, ApplyReceiverResponse{
		TargetNamespace:       targetNS,
		AlertmanagerNamespace: alertmanagerNS,
		AlertmanagerName:      alertmanagerName,
		ReceiverName:          receiverName,
		Resources:             resources,
		Message:               "applied",
	})
}

func ensureNamespaceLabeled(ctx context.Context, cli client.Client, namespace string) error {
	ns := &corev1.Namespace{}
	if err := cli.Get(ctx, client.ObjectKey{Name: namespace}, ns); err != nil {
		return err
	}
	if ns.Labels == nil {
		ns.Labels = map[string]string{}
	}
	if ns.Labels[alertmanagerConfigNamespaceLabel] == alertmanagerConfigNamespaceValue {
		return nil
	}
	ns.Labels[alertmanagerConfigNamespaceLabel] = alertmanagerConfigNamespaceValue
	return cli.Update(ctx, ns)
}

func ensureAlertmanagerConfigEnabled(ctx context.Context, cli client.Client, namespace, name string) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "monitoring.coreos.com",
		Version: "v1",
		Kind:    "Alertmanager",
	})
	if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, obj); err != nil {
		return err
	}

	spec, _ := obj.Object["spec"].(map[string]any)
	if spec == nil {
		spec = map[string]any{}
	}

	selector := map[string]any{
		"matchLabels": map[string]any{
			alertmanagerConfigLabelKey: name,
		},
	}
	nsSelector := map[string]any{
		"matchLabels": map[string]any{
			alertmanagerConfigNamespaceLabel: alertmanagerConfigNamespaceValue,
		},
	}

	// Idempotent: overwrite to desired shape.
	spec["alertmanagerConfigSelector"] = selector
	spec["alertmanagerConfigNamespaceSelector"] = nsSelector
	obj.Object["spec"] = spec

	return cli.Update(ctx, obj)
}

func applySMTPSecret(ctx context.Context, cli client.Client, namespace, name string, cfg *ApplyEmailConfig) error {
	if cfg == nil {
		return fmt.Errorf("email config required")
	}
	if strings.TrimSpace(cfg.Password) == "" {
		return fmt.Errorf("smtp password required")
	}

	secret := &corev1.Secret{}
	err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, secret)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if apierrors.IsNotFound(err) {
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      name,
			},
		}
	}
	if secret.Labels == nil {
		secret.Labels = map[string]string{}
	}
	secret.Labels[managedByLabelKey] = managedByLabelValue
	secret.Type = corev1.SecretTypeOpaque
	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	secret.Data["smtpPassword"] = []byte(cfg.Password)

	if apierrors.IsNotFound(err) {
		return cli.Create(ctx, secret)
	}
	return cli.Update(ctx, secret)
}

func applyAlertmanagerConfig(
	ctx context.Context,
	cli client.Client,
	namespace, name, receiverName, alertmanagerName, smtpSecretName string,
	req ApplyReceiverRequest,
) error {
	amc := &unstructured.Unstructured{}
	amc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "monitoring.coreos.com",
		Version: "v1alpha1",
		Kind:    "AlertmanagerConfig",
	})
	err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, amc)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	labels := map[string]any{
		alertmanagerConfigLabelKey: alertmanagerName,
		managedByLabelKey:          managedByLabelValue,
	}

	receiver := map[string]any{
		"name": receiverName,
	}

	channelType := strings.ToLower(strings.TrimSpace(req.ChannelType))
	needEmail := channelType == "email" || channelType == "both"
	needDingTalk := channelType == "dingtalk" || channelType == "both"

	if needEmail {
		email := req.Email
		to := strings.Join(filterNonEmpty(email.To), ",")
		if strings.TrimSpace(to) == "" {
			return fmt.Errorf("email recipients required")
		}
		if strings.TrimSpace(email.Smarthost) == "" || strings.TrimSpace(email.From) == "" || strings.TrimSpace(email.Username) == "" {
			return fmt.Errorf("email smarthost/from/username required")
		}
		requireTLS := true
		if email.RequireTLS != nil {
			requireTLS = *email.RequireTLS
		}

		emailCfg := map[string]any{
			"to":           to,
			"from":         strings.TrimSpace(email.From),
			"smarthost":    strings.TrimSpace(email.Smarthost),
			"authUsername": strings.TrimSpace(email.Username),
			"authIdentity": strings.TrimSpace(email.Username),
			"requireTLS":   requireTLS,
			"sendResolved": true,
			"text": `{{ range .Alerts -}}
[{{ .Status }}] {{ .Labels.alertname }} ({{ .Labels.severity }})
namespace: {{ .Labels.namespace }}
instance: {{ .Labels.instance }}
summary: {{ .Annotations.summary }}
description: {{ .Annotations.description }}
{{ end }}`,
			"authPassword": map[string]any{
				"name": smtpSecretName,
				"key":  "smtpPassword",
			},
		}
		receiver["emailConfigs"] = []any{emailCfg}
	}

	if needDingTalk {
		adapterURL := strings.TrimSpace(req.DingTalk.AdapterURL)
		if adapterURL == "" {
			return fmt.Errorf("dingtalk adapterUrl required")
		}
		receiver["webhookConfigs"] = []any{
			map[string]any{
				"url":          adapterURL,
				"sendResolved": true,
			},
		}
	}

	spec := map[string]any{
		"route": map[string]any{
			"receiver":       receiverName,
			"groupBy":        []any{"namespace", "alertname"},
			"groupWait":      "30s",
			"groupInterval":  "5m",
			"repeatInterval": "12h",
		},
		"receivers": []any{receiver},
	}

	if apierrors.IsNotFound(err) {
		amc.SetName(name)
		amc.SetNamespace(namespace)
		amc.Object["metadata"] = map[string]any{
			"name":      name,
			"namespace": namespace,
			"labels":    labels,
		}
		amc.Object["spec"] = spec
		return cli.Create(ctx, amc)
	}

	meta, _ := amc.Object["metadata"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
	}
	meta["labels"] = mergeAnyMap(meta["labels"], labels)
	amc.Object["metadata"] = meta
	amc.Object["spec"] = spec
	return cli.Update(ctx, amc)
}

func filterNonEmpty(values []string) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if s := strings.TrimSpace(v); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func mergeAnyMap(old any, add map[string]any) map[string]any {
	out := map[string]any{}
	if m, ok := old.(map[string]any); ok {
		for k, v := range m {
			out[k] = v
		}
	}
	for k, v := range add {
		out[k] = v
	}
	return out
}
