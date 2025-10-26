package logs

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"polardbx-ui-backend/pkg/api/util"
	"polardbx-ui-backend/pkg/config"

	"github.com/gin-gonic/gin"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Bootstrap installs log collection stack
func Bootstrap(c *gin.Context) {
	type req struct {
		Mode           string `json:"mode"` // managed|assisted|byo
		Dry            bool   `json:"dryRun"`
		NS             string `json:"namespace"`
		Name           string `json:"releaseName"`
		EnableFilebeat bool   `json:"enableFilebeat"`
		EnableLogstash bool   `json:"enableLogstash"`
		ESHost         string `json:"esHost"`
		ESUser         string `json:"esUser"`
		ESPassword     string `json:"esPassword"`
		ESIndex        string `json:"esIndex"`
		DeploymentType string `json:"deploymentType"` // default|production|minimal|custom
	}
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	var r req
	_ = c.ShouldBindJSON(&r)
	if r.NS == "" {
		r.NS = "polardbx-operator-system"
	}
	if r.Name == "" {
		r.Name = "polardbx-logcollector"
	}

	// Check for existing ongoing bootstrap jobs (idempotent)
	if !r.Dry {
		existingJobs := &batchv1.JobList{}
		labelSelector := client.MatchingLabels{
			"app":       "polardbx-logs-bootstrap",
			"createdBy": "dashboard",
		}
		if err := cli.List(c.Request.Context(), existingJobs, client.InNamespace(r.NS), labelSelector); err == nil {
			// Look for non-completed jobs
			for _, job := range existingJobs.Items {
				isComplete := false
				isFailed := false
				for _, condition := range job.Status.Conditions {
					if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
						isComplete = true
						break
					}
					if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
						isFailed = true
						break
					}
				}
				// If job is still running, return existing job info
				if !isComplete && !isFailed {
					c.JSON(http.StatusAccepted, gin.H{
						"message":      "logs bootstrap already in progress",
						"namespace":    r.NS,
						"targetNs":     "polardbx-logcollector",
						"mode":         r.Mode,
						"releaseName":  r.Name,
						"jobName":      job.Name,
						"instructions": "Use kubectl logs -n " + r.NS + " job/" + job.Name + " to see progress",
						"existing":     true,
					})
					return
				}
			}
		}
	}

	// Persist plan (idempotent)
	cm := corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: r.NS, Name: "polardbx-logs-plan"}
	if err := cli.Get(c.Request.Context(), key, &cm); err != nil {
		cm = corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: r.NS, Name: "polardbx-logs-plan"}, Data: map[string]string{}}
		_ = cli.Create(c.Request.Context(), &cm)
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data["mode"] = r.Mode
	cm.Data["releaseName"] = r.Name
	cm.Data["deploymentType"] = r.DeploymentType
	cm.Data["enableFilebeat"] = fmt.Sprintf("%t", r.EnableFilebeat)
	cm.Data["enableLogstash"] = fmt.Sprintf("%t", r.EnableLogstash)
	cm.Data["esHost"] = r.ESHost
	cm.Data["esIndex"] = r.ESIndex
	cm.Data["dryRun"] = map[bool]string{true: "true", false: "false"}[r.Dry]
	_ = cli.Update(c.Request.Context(), &cm)

	// If dry-run, just accept
	if r.Dry {
		c.JSON(http.StatusAccepted, gin.H{
			"message":        "logs bootstrap accepted (dry-run)",
			"namespace":      r.NS,
			"mode":           r.Mode,
			"releaseName":    r.Name,
			"deploymentType": r.DeploymentType,
			"dryRun":         r.Dry,
		})
		return
	}

	// Ensure target logs namespace exists
	logsNS := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "polardbx-logcollector"}}
	if err := cli.Create(c.Request.Context(), logsNS); err != nil && !apierrors.IsAlreadyExists(err) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create namespace polardbx-logcollector", "details": err.Error()})
		return
	}

	// Create ConfigMap with manifests for the installer Job to apply
	// This approach avoids needing to mount charts directory or use Helm repo
	// TODO: In production, consider using a Helm repository like monitoring does
	manifestsCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.NS,
			Name:      "polardbx-logs-manifests",
		},
		Data: map[string]string{
			"manifests.yaml": getLogCollectorManifests(r.Name, r.DeploymentType),
		},
	}
	// Create or update manifests ConfigMap
	existingCM := &corev1.ConfigMap{}
	cmKey := client.ObjectKey{Namespace: r.NS, Name: "polardbx-logs-manifests"}
	if err := cli.Get(c.Request.Context(), cmKey, existingCM); err != nil {
		if err := cli.Create(c.Request.Context(), manifestsCM); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create manifests ConfigMap", "details": err.Error()})
			return
		}
	} else {
		existingCM.Data = manifestsCM.Data
		if err := cli.Update(c.Request.Context(), existingCM); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update manifests ConfigMap", "details": err.Error()})
			return
		}
	}

	// Create a short-lived Job to run log collection stack installation
	jobName := fmt.Sprintf("polardbx-logs-bootstrap-%d", time.Now().Unix())
	correlationId := fmt.Sprintf("logs-%d", time.Now().UnixNano())

	// Build Helm installation commands
	// Use helm upgrade --install to deploy from Helm repository
	// If chart is not in repository, this will fail gracefully and show manual installation instructions
	installCommands := []string{
		"set -e",
		"echo 'Starting PolarDB-X LogCollector installation...'",
		"echo 'Checking prerequisites...'",
		"helm version || (echo 'ERROR: helm not found' && exit 1)",

		// Add Helm repository
		"echo 'Adding Helm repository...'",
		"helm repo add polardbx https://polardbx-charts.oss-cn-beijing.aliyuncs.com || echo 'WARN: Failed to add repository'",
		"helm repo update || true",

		// Try to install from Helm repository
		fmt.Sprintf("echo 'Attempting to install %s from Helm repository...'", r.Name),
		fmt.Sprintf("helm upgrade --install %s polardbx/polardbx-logcollector --namespace polardbx-logcollector --create-namespace --wait --timeout 300s || "+
			"(echo 'WARN: Chart not found in repository. Please install manually:' && "+
			"echo '  helm install %s ./charts/polardbx-logcollector -n polardbx-logcollector --create-namespace' && "+
			"exit 1)", r.Name, r.Name),

		// Verify installation
		"echo 'Verifying installation...'",
		"helm list -n polardbx-logcollector",

		"echo '========================================='",
		"echo 'Log collection stack installed successfully'",
		"echo 'Namespace: polardbx-logcollector'",
		fmt.Sprintf("echo 'Release: %s'", r.Name),
		"echo 'Components: Filebeat DaemonSet + Logstash Deployment'",
		"echo '========================================='",
	}

	command := strings.Join(installCommands, " && ")

	// Always deploy in polardbx-logcollector namespace as per official documentation
	targetNamespace := "polardbx-logcollector"

	// Ensure the target namespace exists
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: targetNamespace,
		},
	}
	if err := cli.Create(c.Request.Context(), ns); err != nil && !apierrors.IsAlreadyExists(err) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create namespace", "details": err.Error()})
		return
	}

	// Create ServiceAccount for Helm installer Job
	installerSA := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "polardbx-logcollector-installer",
			Namespace: targetNamespace,
		},
	}
	if err := cli.Create(c.Request.Context(), installerSA); err != nil && !apierrors.IsAlreadyExists(err) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create installer ServiceAccount", "details": err.Error()})
		return
	}

	// Create ClusterRole with permissions for Helm installation
	installerRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: "polardbx-logcollector-installer-role",
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps", "secrets", "services", "serviceaccounts", "pods", "namespaces", "nodes"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			{
				APIGroups: []string{"apps"},
				Resources: []string{"deployments", "daemonsets", "replicasets", "statefulsets"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			{
				APIGroups: []string{"batch"},
				Resources: []string{"jobs", "cronjobs"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			{
				APIGroups: []string{"rbac.authorization.k8s.io"},
				Resources: []string{"roles", "rolebindings", "clusterroles", "clusterrolebindings"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			{
				APIGroups: []string{"polardbx.aliyun.com"},
				Resources: []string{"polardbxlogcollectors", "polardbxlogcollectors/status", "polardbxlogcollectors/finalizers"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
		},
	}
	if err := cli.Create(c.Request.Context(), installerRole); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create installer ClusterRole", "details": err.Error()})
			return
		}

		existingRole := &rbacv1.ClusterRole{}
		if getErr := cli.Get(c.Request.Context(), client.ObjectKey{Name: installerRole.Name}, existingRole); getErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to refresh installer ClusterRole", "details": getErr.Error()})
			return
		}

		existingRole.Rules = installerRole.Rules
		if updateErr := cli.Update(c.Request.Context(), existingRole); updateErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update installer ClusterRole", "details": updateErr.Error()})
			return
		}
	}

	// Create ClusterRoleBinding
	installerBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: "polardbx-logcollector-installer-binding",
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      "polardbx-logcollector-installer",
				Namespace: targetNamespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "polardbx-logcollector-installer-role",
		},
	}
	if err := cli.Create(c.Request.Context(), installerBinding); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create installer ClusterRoleBinding", "details": err.Error()})
			return
		}

		existingBinding := &rbacv1.ClusterRoleBinding{}
		if getErr := cli.Get(c.Request.Context(), client.ObjectKey{Name: installerBinding.Name}, existingBinding); getErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to refresh installer ClusterRoleBinding", "details": getErr.Error()})
			return
		}

		existingBinding.Subjects = installerBinding.Subjects
		existingBinding.RoleRef = installerBinding.RoleRef
		if updateErr := cli.Update(c.Request.Context(), existingBinding); updateErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update installer ClusterRoleBinding", "details": updateErr.Error()})
			return
		}
	}

	backoff := int32(0)
	ttl := int32(600)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: targetNamespace,
			Name:      jobName,
			Labels: map[string]string{
				"app":           "polardbx-logs-bootstrap",
				"createdBy":     "dashboard",
				"correlationId": correlationId,
				"targetType":    "logcollector",
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					ServiceAccountName:           "polardbx-logcollector-installer",
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: func(b bool) *bool { return &b }(true),
					Containers: []corev1.Container{{
						Name:            "logs-installer",
						Image:           config.GetGlobalConfig().GetHelmImage(), // Using helm image for kubectl access
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command:         []string{"sh", "-c", command},
						Env: []corev1.EnvVar{
							{Name: "ES_HOST", Value: r.ESHost},
							{Name: "ES_USER", Value: r.ESUser},
							{Name: "ES_INDEX", Value: r.ESIndex},
							{Name: "ENABLE_FILEBEAT", Value: fmt.Sprintf("%t", r.EnableFilebeat)},
							{Name: "ENABLE_LOGSTASH", Value: fmt.Sprintf("%t", r.EnableLogstash)},
							{Name: "DEPLOYMENT_TYPE", Value: r.DeploymentType},
						},
					}},
				},
			},
		},
	}

	// Add ES password as secret if provided
	if r.ESPassword != "" {
		job.Spec.Template.Spec.Containers[0].Env = append(job.Spec.Template.Spec.Containers[0].Env,
			corev1.EnvVar{Name: "ES_PASSWORD", Value: r.ESPassword})
	}

	if err := cli.Create(c.Request.Context(), job); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create logs install job", "details": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"message":        "logs bootstrap started",
		"namespace":      targetNamespace,
		"targetNs":       targetNamespace,
		"mode":           r.Mode,
		"releaseName":    r.Name,
		"deploymentType": r.DeploymentType,
		"jobName":        jobName,
		"correlationId":  correlationId,
		"instructions":   "Use kubectl logs -n " + r.NS + " job/" + jobName + " to see progress",
	})
}

// BootstrapStatus returns the status of a logs bootstrap job
func BootstrapStatus(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	jobName := c.Query("jobName")
	namespace := util.DefaultNamespace(c, "polardbx-operator-system")

	if jobName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "jobName parameter is required"})
		return
	}

	// Get the job
	job := &batchv1.Job{}
	key := client.ObjectKey{Namespace: namespace, Name: jobName}
	if err := cli.Get(c.Request.Context(), key, job); err != nil {
		if apierrors.IsNotFound(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found", "jobName": jobName, "namespace": namespace})
			return
		}
		util.HandleK8sError(c, "failed to get job", err)
		return
	}

	// Determine job phase
	phase := "Running"
	var completionTime *metav1.Time
	var failureReason string

	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			phase = "Succeeded"
			completionTime = &condition.LastTransitionTime
			break
		}
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			phase = "Failed"
			failureReason = condition.Message
			completionTime = &condition.LastTransitionTime
			break
		}
	}

	// Check if job is still running by looking at active pods
	if phase == "Running" && job.Status.Active == 0 && job.Status.Succeeded == 0 && job.Status.Failed == 0 {
		phase = "Pending"
	}

	response := gin.H{
		"jobName":   jobName,
		"namespace": namespace,
		"phase":     phase,
		"startTime": job.Status.StartTime,
		"active":    job.Status.Active,
		"succeeded": job.Status.Succeeded,
		"failed":    job.Status.Failed,
	}

	if completionTime != nil {
		response["completionTime"] = completionTime
	}

	if failureReason != "" {
		response["failureReason"] = failureReason
	}

	// Add conditions for detailed status
	response["conditions"] = job.Status.Conditions

	c.JSON(http.StatusOK, response)
}

// BootstrapLogs returns the logs of a logs bootstrap job
func BootstrapLogs(c *gin.Context) {
	cs, ok := util.ClientsetFromContext(c)
	if !ok {
		return
	}

	jobName := c.Query("jobName")
	namespace := util.DefaultNamespace(c, "polardbx-operator-system")
	tailLines := int64(100) // Default to last 100 lines

	if jobName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "jobName parameter is required"})
		return
	}

	if tailParam := c.Query("tailLines"); tailParam != "" {
		if parsed, err := strconv.ParseInt(tailParam, 10, 64); err == nil && parsed > 0 {
			tailLines = parsed
		}
	}

	// List pods created by this job
	pods, err := cs.CoreV1().Pods(namespace).List(c.Request.Context(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	if err != nil {
		util.HandleK8sError(c, "failed to list job pods", err)
		return
	}

	if len(pods.Items) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "no pods found for job", "jobName": jobName})
		return
	}

	// Get logs from the first pod (usually there's only one for our jobs)
	pod := pods.Items[0]

	logOptions := &corev1.PodLogOptions{
		TailLines: &tailLines,
	}

	// If pod has multiple containers, get logs from the first one
	if len(pod.Spec.Containers) > 0 {
		logOptions.Container = pod.Spec.Containers[0].Name
	}

	logReq := cs.CoreV1().Pods(namespace).GetLogs(pod.Name, logOptions)
	rc, err := logReq.Stream(c.Request.Context())
	if err != nil {
		util.HandleK8sError(c, "failed to get pod logs", err)
		return
	}
	defer rc.Close()

	logs, err := io.ReadAll(rc)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read logs", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"jobName":   jobName,
		"namespace": namespace,
		"podName":   pod.Name,
		"logs":      string(logs),
		"tailLines": tailLines,
	})
}

// getLogCollectorManifests returns pre-rendered Kubernetes manifests for log collector components
// This is a simplified approach - in production, consider using helm template or a Helm repository
func getLogCollectorManifests(releaseName, deploymentType string) string {
	// TODO: Ideally, this should call `helm template` or use Helm Go SDK to render the chart
	// For now, return a minimal working manifest as a placeholder
	// In production deployment, consider one of these approaches:
	// 1. Use helm template command to render /charts/polardbx-logcollector
	// 2. Upload chart to Helm repository and use helm upgrade --install
	// 3. Pre-render manifests during build and embed them

	return `# PolarDB-X LogCollector Manifests
# NOTE: This is a simplified placeholder manifest
# For full-featured deployment, use: helm template polardbx-logcollector ./charts/polardbx-logcollector

# TODO: Replace with actual rendered manifests from charts/polardbx-logcollector
# Current approach: The Job will use Helm repository installation

# Placeholder - actual manifests should be generated from Helm chart
apiVersion: v1
kind: ConfigMap
metadata:
  name: placeholder-logcollector-info
  namespace: polardbx-logcollector
data:
  message: |
    PolarDB-X LogCollector installation requires Helm chart deployment.
    Please use one of the following methods:
    
    1. From Helm repository (recommended):
       helm repo add polardbx https://polardbx-charts.oss-cn-beijing.aliyuncs.com
       helm upgrade --install ` + releaseName + ` polardbx/polardbx-logcollector -n polardbx-logcollector --create-namespace
    
    2. From local chart:
       helm install ` + releaseName + ` ./charts/polardbx-logcollector -n polardbx-logcollector --create-namespace
    
    3. kubectl apply with pre-rendered manifests:
       helm template ` + releaseName + ` ./charts/polardbx-logcollector | kubectl apply -n polardbx-logcollector -f -
    
    Deployment Type: ` + deploymentType + `
`
}
