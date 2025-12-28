package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// Diagnostic Pod name prefix
	ClinicPodPrefix = "polardbx-clinic-"
	// Diagnostic Pod label
	ClinicLabelKey = "polardbx/clinic"
	// Diagnostic status
	DiagStatusRunning   = "running"
	DiagStatusSucceeded = "succeeded"
	DiagStatusFailed    = "failed"
	DiagStatusPending   = "pending"

	ClinicServiceAccountName = "polardbx-clinic"
	ClinicRoleName           = "polardbx-clinic"
	ClinicRoleBindingName    = "polardbx-clinic"
)

// DiagnosticJob diagnostic task information
type DiagnosticJob struct {
	ID          string    `json:"id"`
	Namespace   string    `json:"namespace"`
	Cluster     string    `json:"cluster"`
	Status      string    `json:"status"`
	Progress    int       `json:"progress"`
	StartedAt   time.Time `json:"startedAt"`
	CompletedAt time.Time `json:"completedAt,omitempty"`
	OutputPath  string    `json:"outputPath,omitempty"`
	Message     string    `json:"message,omitempty"`
}

// DiagnosticsService diagnostic service
type DiagnosticsService struct {
	cli client.Client
}

// NewDiagnosticsService creates diagnostic service
func NewDiagnosticsService(cli client.Client) *DiagnosticsService {
	return &DiagnosticsService{cli: cli}
}

// StartDiagnosis starts diagnostic task
// Create polardbx-clinic Pod to collect cluster diagnostic information
func (s *DiagnosticsService) StartDiagnosis(ctx context.Context, namespace, clusterName string) (*DiagnosticJob, error) {
	if err := s.ensureClinicRBAC(ctx, namespace); err != nil {
		return nil, err
	}

	// Generate unique diagnostic task ID
	jobID := fmt.Sprintf("%s-%d", clusterName, time.Now().Unix())
	podName := ClinicPodPrefix + jobID

	// Build diagnostic Pod
	pod := s.buildClinicPod(namespace, podName, clusterName, jobID)

	// Create Pod
	if err := s.cli.Create(ctx, pod); err != nil {
		return nil, err
	}

	return &DiagnosticJob{
		ID:        jobID,
		Namespace: namespace,
		Cluster:   clusterName,
		Status:    DiagStatusRunning,
		Progress:  0,
		StartedAt: time.Now(),
		Message:   "diagnostic task started",
	}, nil
}

func (s *DiagnosticsService) ensureClinicRBAC(ctx context.Context, namespace string) error {
	// ServiceAccount
	sa := &corev1.ServiceAccount{}
	saKey := client.ObjectKey{Namespace: namespace, Name: ClinicServiceAccountName}
	if err := s.cli.Get(ctx, saKey, sa); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		sa = &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: ClinicServiceAccountName}}
		if err := s.cli.Create(ctx, sa); err != nil {
			return err
		}
	}

	// Role (namespace-scoped, read-only-ish; intentionally does not include secrets)
	desiredRules := []rbacv1.PolicyRule{
		{
			APIGroups: []string{""},
			Resources: []string{"pods", "pods/log", "services", "configmaps", "persistentvolumeclaims", "events"},
			Verbs:     []string{"get", "list", "watch"},
		},
		{
			APIGroups: []string{"polardbx.aliyun.com"},
			Resources: []string{"polardbxclusters", "xstores"},
			Verbs:     []string{"get", "list", "watch"},
		},
	}

	role := &rbacv1.Role{}
	roleKey := client.ObjectKey{Namespace: namespace, Name: ClinicRoleName}
	if err := s.cli.Get(ctx, roleKey, role); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		role = &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: ClinicRoleName},
			Rules:      desiredRules,
		}
		if err := s.cli.Create(ctx, role); err != nil {
			return err
		}
	} else {
		role.Rules = desiredRules
		if err := s.cli.Update(ctx, role); err != nil {
			return err
		}
	}

	// RoleBinding
	rb := &rbacv1.RoleBinding{}
	rbKey := client.ObjectKey{Namespace: namespace, Name: ClinicRoleBindingName}
	desiredSubjects := []rbacv1.Subject{{Kind: "ServiceAccount", Name: ClinicServiceAccountName, Namespace: namespace}}
	desiredRef := rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: ClinicRoleName}
	if err := s.cli.Get(ctx, rbKey, rb); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		rb = &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: ClinicRoleBindingName},
			Subjects:   desiredSubjects,
			RoleRef:    desiredRef,
		}
		if err := s.cli.Create(ctx, rb); err != nil {
			return err
		}
	} else {
		rb.Subjects = desiredSubjects
		rb.RoleRef = desiredRef
		if err := s.cli.Update(ctx, rb); err != nil {
			return err
		}
	}

	return nil
}

// GetDiagnosisStatus gets diagnostic task status
func (s *DiagnosticsService) GetDiagnosisStatus(ctx context.Context, namespace, jobID string) (*DiagnosticJob, error) {
	podName := ClinicPodPrefix + jobID

	var pod corev1.Pod
	key := client.ObjectKey{Namespace: namespace, Name: podName}
	if err := s.cli.Get(ctx, key, &pod); err != nil {
		return nil, fmt.Errorf("failed to get diagnostic Pod status: %w", err)
	}

	job := &DiagnosticJob{
		ID:        jobID,
		Namespace: namespace,
		Cluster:   pod.Labels["polardbx/cluster"],
	}

	deriveJobStatusFromPod(&pod, job)

	// Set timestamps
	if !pod.CreationTimestamp.IsZero() {
		job.StartedAt = pod.CreationTimestamp.Time
	}
	job.CompletedAt = deriveCompletionTime(&pod)

	return job, nil
}

// ListDiagnosisReports lists diagnostic reports
func (s *DiagnosticsService) ListDiagnosisReports(ctx context.Context, namespace string) ([]DiagnosticJob, error) {
	var podList corev1.PodList

	// Build label selector
	labelSelector := labels.SelectorFromSet(map[string]string{
		ClinicLabelKey: "true",
	})

	listOpts := &client.ListOptions{
		LabelSelector: labelSelector,
	}
	if namespace != "" {
		listOpts.Namespace = namespace
	}

	if err := s.cli.List(ctx, &podList, listOpts); err != nil {
		return nil, fmt.Errorf("failed to list diagnostic Pods: %w", err)
	}

	var reports []DiagnosticJob
	for _, pod := range podList.Items {
		jobID := strings.TrimPrefix(pod.Name, ClinicPodPrefix)
		job := DiagnosticJob{
			ID:        jobID,
			Namespace: pod.Namespace,
			Cluster:   pod.Labels["polardbx/cluster"],
			StartedAt: pod.CreationTimestamp.Time,
		}

		deriveJobStatusFromPod(&pod, &job)
		job.CompletedAt = deriveCompletionTime(&pod)

		reports = append(reports, job)
	}

	return reports, nil
}

// GetDownloadInfo gets diagnostic report download information
func (s *DiagnosticsService) GetDownloadInfo(ctx context.Context, namespace, jobID string) (string, error) {
	// Check Pod status
	job, err := s.GetDiagnosisStatus(ctx, namespace, jobID)
	if err != nil {
		return "", err
	}

	if job.Status != DiagStatusSucceeded {
		return "", fmt.Errorf("diagnostic task not completed yet, current status: %s", job.Status)
	}

	// Return diagnostic report path
	// In actual use, may need to get file via kubectl cp or other methods
	return job.OutputPath, nil
}

// buildClinicPod builds diagnostic Pod configuration
func (s *DiagnosticsService) buildClinicPod(namespace, podName, clusterName, jobID string) *corev1.Pod {
	// Diagnostic script - collect various diagnostic information (runs in init container)
	diagScript := `#!/bin/bash
set -e

CLUSTER_NAME="${CLUSTER_NAME:-unknown}"
OUTPUT_DIR="/tmp/polardbx-clinic"
REPORT_FILE="${OUTPUT_DIR}/${JOB_ID}.tar.gz"

mkdir -p ${OUTPUT_DIR}/data

echo "=== PolarDB-X Clinic Diagnostic Tool ==="
echo "Cluster: ${CLUSTER_NAME}"
echo "Namespace: ${NAMESPACE}"
echo "Start Time: $(date)"

# Collect cluster information
echo "Collecting cluster information..."
kubectl get pxc ${CLUSTER_NAME} -n ${NAMESPACE} -o yaml > ${OUTPUT_DIR}/data/pxc.yaml 2>/dev/null || echo "Failed to get PXC"

# Collect XStore information
echo "Collecting XStore information..."
kubectl get xstore -n ${NAMESPACE} -l polardbx/name=${CLUSTER_NAME} -o yaml > ${OUTPUT_DIR}/data/xstores.yaml 2>/dev/null || echo "No XStore found"

# Collect Pod information
echo "Collecting Pod information..."
kubectl get pods -n ${NAMESPACE} -l polardbx/name=${CLUSTER_NAME} -o wide > ${OUTPUT_DIR}/data/pods.txt 2>/dev/null || echo "No Pods found"

# Collect events
echo "Collecting events..."
kubectl get events -n ${NAMESPACE} --field-selector involvedObject.name=${CLUSTER_NAME} > ${OUTPUT_DIR}/data/events.txt 2>/dev/null || echo "No events"

# Collect CN logs
echo "Collecting CN logs..."
for pod in $(kubectl get pods -n ${NAMESPACE} -l polardbx/name=${CLUSTER_NAME},polardbx/role=cn -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    kubectl logs ${pod} -n ${NAMESPACE} --tail=1000 > ${OUTPUT_DIR}/data/cn-${pod}.log 2>/dev/null || true
done

# Collect DN logs
echo "Collecting DN logs..."
for pod in $(kubectl get pods -n ${NAMESPACE} -l polardbx/name=${CLUSTER_NAME},polardbx/role=dn -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    kubectl logs ${pod} -n ${NAMESPACE} --tail=1000 > ${OUTPUT_DIR}/data/dn-${pod}.log 2>/dev/null || true
done

# Collect GMS logs
echo "Collecting GMS logs..."
for pod in $(kubectl get pods -n ${NAMESPACE} -l polardbx/name=${CLUSTER_NAME},polardbx/role=gms -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    kubectl logs ${pod} -n ${NAMESPACE} --tail=1000 > ${OUTPUT_DIR}/data/gms-${pod}.log 2>/dev/null || true
done

# Collect CDC logs
echo "Collecting CDC logs..."
for pod in $(kubectl get pods -n ${NAMESPACE} -l polardbx/name=${CLUSTER_NAME},polardbx/role=cdc -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    kubectl logs ${pod} -n ${NAMESPACE} --tail=1000 > ${OUTPUT_DIR}/data/cdc-${pod}.log 2>/dev/null || true
done

# Collect node information
echo "Collecting node information..."
kubectl get nodes -o wide > ${OUTPUT_DIR}/data/nodes.txt 2>/dev/null || echo "Failed to get nodes"

# Collect ConfigMap
echo "Collecting ConfigMaps..."
kubectl get configmap -n ${NAMESPACE} -l polardbx/name=${CLUSTER_NAME} -o yaml > ${OUTPUT_DIR}/data/configmaps.yaml 2>/dev/null || echo "No ConfigMaps"

# Collect Secret (metadata only)
echo "Collecting Secrets metadata..."
kubectl get secrets -n ${NAMESPACE} -l polardbx/name=${CLUSTER_NAME} -o jsonpath='{.items[*].metadata.name}' > ${OUTPUT_DIR}/data/secrets.txt 2>/dev/null || echo "No Secrets"

# Collect PVC information
echo "Collecting PVC information..."
kubectl get pvc -n ${NAMESPACE} -l polardbx/name=${CLUSTER_NAME} -o yaml > ${OUTPUT_DIR}/data/pvcs.yaml 2>/dev/null || echo "No PVCs"

# Generate report summary
echo "Generating summary..."
cat > ${OUTPUT_DIR}/data/summary.txt << EOF
=== PolarDB-X Diagnostic Report ===
Generated: $(date)
Cluster: ${CLUSTER_NAME}
Namespace: ${NAMESPACE}

Files collected:
$(ls -la ${OUTPUT_DIR}/data/)
EOF

# Package
echo "Creating archive..."
cd ${OUTPUT_DIR}
tar -czf ${REPORT_FILE} data/

echo "=== Diagnostic completed ==="
echo "Report: ${REPORT_FILE}"
echo "End Time: $(date)"
`

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels: map[string]string{
				ClinicLabelKey:     "true",
				"polardbx/cluster": clusterName,
				"polardbx/job-id":  jobID,
			},
			Annotations: map[string]string{
				"polardbx/clinic-version": "1.0",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:      corev1.RestartPolicyNever,
			ServiceAccountName: ClinicServiceAccountName,
			InitContainers: []corev1.Container{
				{
					Name:  "collector",
					Image: "bitnami/kubectl:latest",
					Command: []string{
						"/bin/bash",
						"-c",
						diagScript,
					},
					Env: []corev1.EnvVar{
						{Name: "CLUSTER_NAME", Value: clusterName},
						{Name: "NAMESPACE", Value: namespace},
						{Name: "JOB_ID", Value: jobID},
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "output",
							MountPath: "/tmp/polardbx-clinic",
						},
					},
				},
			},
			// Keep the pod running after collection finishes so the report can be downloaded via exec/streaming.
			// Note: exec/cp is not possible for completed pods; using a "sleeper" container avoids 0-byte downloads.
			Containers: []corev1.Container{
				{
					Name:    "clinic",
					Image:   "bitnami/kubectl:latest",
					Command: []string{"/bin/sh", "-c", "sleep 365d"},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "output",
							MountPath: "/tmp/polardbx-clinic",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "output",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
		},
	}
}

func deriveCompletionTime(pod *corev1.Pod) time.Time {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil && !cs.State.Terminated.FinishedAt.IsZero() {
			return cs.State.Terminated.FinishedAt.Time
		}
	}
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.State.Terminated != nil && !cs.State.Terminated.FinishedAt.IsZero() {
			return cs.State.Terminated.FinishedAt.Time
		}
	}
	return time.Time{}
}

func deriveJobStatusFromPod(pod *corev1.Pod, job *DiagnosticJob) {
	// New-style diagnostic pod: init container collects data, long-running container keeps report downloadable.
	if len(pod.Spec.InitContainers) > 0 {
		for _, ics := range pod.Status.InitContainerStatuses {
			if ics.Name != "collector" {
				continue
			}
			if ics.State.Terminated != nil {
				if ics.State.Terminated.ExitCode == 0 {
					job.Status = DiagStatusSucceeded
					job.Progress = 100
					job.Message = "diagnostic completed"
					job.OutputPath = fmt.Sprintf("/tmp/polardbx-clinic/%s.tar.gz", job.ID)
					return
				}
				job.Status = DiagStatusFailed
				job.Progress = 0
				reason := strings.TrimSpace(ics.State.Terminated.Reason)
				if reason == "" {
					reason = "collector exited"
				}
				job.Message = fmt.Sprintf("diagnostic failed: %s", reason)
				return
			}
			if ics.State.Running != nil {
				job.Status = DiagStatusRunning
				job.Progress = 50
				job.Message = "collecting diagnostic information"
				return
			}
			job.Status = DiagStatusPending
			job.Progress = 0
			job.Message = "diagnostic Pod starting"
			return
		}

		// Default: init containers exist but status not reported yet.
		job.Status = DiagStatusPending
		job.Progress = 0
		job.Message = "diagnostic Pod starting"
		return
	}

	// Legacy diagnostic pod: runs a single container then completes; completed pods cannot be exec'd into for download.
	switch pod.Status.Phase {
	case corev1.PodPending:
		job.Status = DiagStatusPending
		job.Progress = 0
		job.Message = "diagnostic Pod starting"
	case corev1.PodRunning:
		job.Status = DiagStatusRunning
		job.Progress = 50
		job.Message = "collecting diagnostic information"
	case corev1.PodSucceeded:
		job.Status = DiagStatusFailed
		job.Progress = 100
		job.Message = "diagnostic finished (legacy mode): report is not downloadable from a completed pod; please re-run diagnostics"
	case corev1.PodFailed:
		job.Status = DiagStatusFailed
		job.Progress = 0
		job.Message = "diagnostic task failed"
		if len(pod.Status.ContainerStatuses) > 0 {
			cs := pod.Status.ContainerStatuses[0]
			if cs.State.Terminated != nil && cs.State.Terminated.Reason != "" {
				job.Message = fmt.Sprintf("diagnostic failed: %s", cs.State.Terminated.Reason)
			}
		}
	default:
		job.Status = DiagStatusPending
		job.Progress = 0
	}
}
