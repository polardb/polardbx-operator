package service

import (
	context "context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"

	spec "polardbx-dashboard-backend/pkg/api/domain/monitoring/spec"
)

const (
	checkpointConfigMapPrefix   = "monitoring-wizard-session-"
	checkpointConfigMapKey      = "status.json"
	checkpointLabelSession      = "polardbx.com/monitoring-session"
	checkpointLabelManagedBy    = "app.kubernetes.io/managed-by"
	checkpointManagedByValue    = "polardbx-monitoring-wizard"
	checkpointAnnotationUpdated = "monitoring.polardbx.com/updated-at"
	checkpointAnnotationVersion = "monitoring.polardbx.com/checkpoint-version"
)

// checkpointFormatVersion identifies the persistence format. When Phase 3 introduces diagnostic results,
// this version number is still used, and the latest diagnostic reports are stored through extended fields
// of PersistedSession. If multi-version compatibility is needed in the future, conditional branching
// can be performed based on this constant.
const checkpointFormatVersion = "v1alpha1"

// CheckpointManager persists and restores installation checkpoints to Kubernetes.
type CheckpointManager interface {
	Save(ctx context.Context, namespace string, status spec.InstallStatusResponse, plan spec.InstallationPlan) error
	Restore(ctx context.Context, namespace, sessionID string) (*PersistedSession, error)
	RestoreAny(ctx context.Context, sessionID string) (*PersistedSession, error)
	Delete(ctx context.Context, namespace, sessionID string) error
}

// PersistedSession represents the payload stored within the ConfigMap. Diagnostics module (Phase 3)
// will append findings to this structure so that checkpoints and diagnosis share the same retention
// and garbage-collection policy. Controllers periodically sweep ConfigMaps older than the TTL by
// inspecting checkpointAnnotationUpdated.
type PersistedSession struct {
	Namespace string                     `json:"namespace"`
	Status    spec.InstallStatusResponse `json:"status"`
	Plan      spec.InstallationPlan      `json:"plan"`
}

// ConfigMapCheckpointManager persists checkpoints into ConfigMaps.
type ConfigMapCheckpointManager struct {
	client kubernetes.Interface
}

// NewConfigMapCheckpointManager constructs a new configmap-backed manager.
func NewConfigMapCheckpointManager(client kubernetes.Interface) *ConfigMapCheckpointManager {
	return &ConfigMapCheckpointManager{client: client}
}

// Save writes the latest status snapshot for a session.
func (m *ConfigMapCheckpointManager) Save(ctx context.Context, namespace string, status spec.InstallStatusResponse, plan spec.InstallationPlan) error {
	if m == nil || m.client == nil {
		return nil
	}
	if status.SessionId == "" {
		return fmt.Errorf("session id is required for checkpoint save")
	}
	if namespace == "" {
		return fmt.Errorf("namespace is required for checkpoint save")
	}
	safeName := sessionConfigMapName(status.SessionId)
	name := fmt.Sprintf("%s%s", checkpointConfigMapPrefix, safeName)

	payload, err := json.Marshal(PersistedSession{
		Namespace: namespace,
		Status:    status,
		Plan:      plan,
	})
	if err != nil {
		return fmt.Errorf("marshal checkpoint status: %w", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				checkpointLabelManagedBy: checkpointManagedByValue,
				checkpointLabelSession:   status.SessionId,
			},
			Annotations: map[string]string{
				checkpointAnnotationUpdated: time.Now().UTC().Format(time.RFC3339Nano),
				checkpointAnnotationVersion: checkpointFormatVersion,
			},
		},
		Data: map[string]string{
			checkpointConfigMapKey: string(payload),
		},
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client := m.client.CoreV1().ConfigMaps(namespace)
	_, err = client.Create(ctx, cm, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create checkpoint configmap: %w", err)
	}

	existing, getErr := client.Get(ctx, name, metav1.GetOptions{})
	if getErr != nil {
		return fmt.Errorf("get existing checkpoint configmap: %w", getErr)
	}
	if existing.Data == nil {
		existing.Data = map[string]string{}
	}
	existing.Data[checkpointConfigMapKey] = string(payload)
	if existing.Annotations == nil {
		existing.Annotations = map[string]string{}
	}
	existing.Annotations[checkpointAnnotationUpdated] = time.Now().UTC().Format(time.RFC3339Nano)
	existing.Annotations[checkpointAnnotationVersion] = checkpointFormatVersion

	if existing.Labels == nil {
		existing.Labels = map[string]string{}
	}
	existing.Labels[checkpointLabelManagedBy] = checkpointManagedByValue
	existing.Labels[checkpointLabelSession] = status.SessionId

	if _, err = client.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update checkpoint configmap: %w", err)
	}
	return nil
}

// Restore fetches a previously saved status snapshot.
func (m *ConfigMapCheckpointManager) Restore(ctx context.Context, namespace, sessionID string) (*PersistedSession, error) {
	if m == nil || m.client == nil {
		return nil, nil
	}
	if namespace == "" || sessionID == "" {
		return nil, fmt.Errorf("namespace and session id are required for restore")
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	name := fmt.Sprintf("%s%s", checkpointConfigMapPrefix, sessionConfigMapName(sessionID))
	cm, err := m.client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get checkpoint configmap: %w", err)
	}
	statusJSON, ok := cm.Data[checkpointConfigMapKey]
	if !ok || statusJSON == "" {
		return nil, nil
	}
	var payload PersistedSession
	if err := json.Unmarshal([]byte(statusJSON), &payload); err != nil {
		return nil, fmt.Errorf("unmarshal checkpoint status: %w", err)
	}
	if payload.Namespace == "" {
		payload.Namespace = namespace
	}
	return &payload, nil
}

// RestoreAny searches across namespaces for a persisted checkpoint.
func (m *ConfigMapCheckpointManager) RestoreAny(ctx context.Context, sessionID string) (*PersistedSession, error) {
	if m == nil || m.client == nil {
		return nil, nil
	}
	if sessionID == "" {
		return nil, fmt.Errorf("session id is required for restore")
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	selector := fmt.Sprintf("%s=%s", checkpointLabelSession, sessionID)
	list, err := m.client.CoreV1().ConfigMaps(metav1.NamespaceAll).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("list checkpoint configmaps: %w", err)
	}
	if len(list.Items) == 0 {
		return nil, nil
	}
	cm := list.Items[0]
	statusJSON := cm.Data[checkpointConfigMapKey]
	if statusJSON == "" {
		return nil, nil
	}
	var payload PersistedSession
	if err := json.Unmarshal([]byte(statusJSON), &payload); err != nil {
		return nil, fmt.Errorf("unmarshal checkpoint status: %w", err)
	}
	if payload.Namespace == "" {
		payload.Namespace = cm.Namespace
	}
	return &payload, nil
}

// Delete removes the persisted snapshot for a session.
func (m *ConfigMapCheckpointManager) Delete(ctx context.Context, namespace, sessionID string) error {
	if m == nil || m.client == nil {
		return nil
	}
	if namespace == "" || sessionID == "" {
		return fmt.Errorf("namespace and session id are required for delete")
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	name := fmt.Sprintf("%s%s", checkpointConfigMapPrefix, sessionConfigMapName(sessionID))
	err := m.client.CoreV1().ConfigMaps(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete checkpoint configmap: %w", err)
	}
	return nil
}

func sessionConfigMapName(sessionID string) string {
	if sessionID == "" {
		return "unknown"
	}
	if errs := validation.IsDNS1123Label(sessionID); len(errs) == 0 && len(sessionID) <= 63-len(checkpointConfigMapPrefix) {
		return sessionID
	}
	sum := sha1.Sum([]byte(sessionID))
	return hex.EncodeToString(sum[:16])
}
