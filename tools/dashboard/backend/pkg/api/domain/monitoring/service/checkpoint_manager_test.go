package service

import (
	context "context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	spec "polardbx-dashboard-backend/pkg/api/domain/monitoring/spec"
)

func TestConfigMapCheckpointManager_SaveRestoreDelete(t *testing.T) {
	client := fake.NewSimpleClientset()
	manager := NewConfigMapCheckpointManager(client)
	ctx := context.Background()

	progress := float32(0.25)
	status := spec.InstallStatusResponse{
		SessionId: "session-123",
		Phase:     spec.Installing,
		Progress:  &progress,
		UpdatedAt: time.Unix(1700000000, 0).UTC(),
	}
	plan := spec.InstallationPlan{
		SessionTemplate: spec.SessionTemplate{Namespace: "default"},
	}

	if err := manager.Save(ctx, "default", status, plan); err != nil {
		t.Fatalf("save checkpoint failed: %v", err)
	}

	cmName := checkpointConfigMapPrefix + status.SessionId
	cm, err := client.CoreV1().ConfigMaps("default").Get(ctx, cmName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected configmap to exist: %v", err)
	}
	if cm.Data[checkpointConfigMapKey] == "" {
		t.Fatalf("expected checkpoint payload in configmap")
	}
	if cm.Labels[checkpointLabelSession] != status.SessionId {
		t.Fatalf("expected session label to be original id")
	}

	restored, err := manager.Restore(ctx, "default", status.SessionId)
	if err != nil {
		t.Fatalf("restore checkpoint failed: %v", err)
	}
	if restored == nil || restored.Status.SessionId != status.SessionId {
		t.Fatalf("restored checkpoint mismatch: %#v", restored)
	}
	if restored.Status.Progress == nil || *restored.Status.Progress != progress {
		t.Fatalf("restored progress mismatch")
	}
	if restored.Plan.SessionTemplate.Namespace != "default" {
		t.Fatalf("expected plan to be persisted")
	}

	restoredAny, err := manager.RestoreAny(ctx, status.SessionId)
	if err != nil {
		t.Fatalf("restore any failed: %v", err)
	}
	if restoredAny == nil || restoredAny.Namespace != "default" {
		t.Fatalf("restore any should return checkpoint and namespace, got %v", restoredAny)
	}
	if restoredAny.Status.Progress == nil || *restoredAny.Status.Progress != progress {
		t.Fatalf("restore any progress mismatch")
	}

	// Update existing payload
	newProgress := float32(0.9)
	status.Progress = &newProgress
	if err := manager.Save(ctx, "default", status, plan); err != nil {
		t.Fatalf("second save failed: %v", err)
	}

	restored, err = manager.Restore(ctx, "default", status.SessionId)
	if err != nil {
		t.Fatalf("restore after update failed: %v", err)
	}
	if restored.Status.Progress == nil || *restored.Status.Progress != newProgress {
		t.Fatalf("restore should return latest progress")
	}

	if err := manager.Delete(ctx, "default", status.SessionId); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if _, err := client.CoreV1().ConfigMaps("default").Get(ctx, cmName, metav1.GetOptions{}); err == nil {
		t.Fatalf("expected configmap to be deleted")
	}
	restoredAny, err = manager.RestoreAny(ctx, status.SessionId)
	if err != nil {
		t.Fatalf("restore any after delete failed: %v", err)
	}
	if restoredAny != nil {
		t.Fatalf("expected no checkpoint after delete")
	}
}

func TestSessionConfigMapNameHashed(t *testing.T) {
	longID := "this-session-id-is-way-too-long-to-fit-into-a-configmap-name-because-it-exceeds-limit"
	name := sessionConfigMapName(longID)
	if len(name) == 0 {
		t.Fatalf("expected hashed name")
	}
	if len(name) > 63 {
		t.Fatalf("hashed name should respect dns1123 limit")
	}

	// Directly combining with prefix should still respect the 63 char limit.
	combined := checkpointConfigMapPrefix + name
	if len(combined) > 63 {
		t.Fatalf("combined configmap name exceeds dns1123 limit: %d", len(combined))
	}
}
