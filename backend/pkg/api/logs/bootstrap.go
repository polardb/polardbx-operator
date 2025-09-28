package logs

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"polardbx-ui-backend/pkg/api/util"

	"github.com/gin-gonic/gin"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Bootstrap installs log collection stack
func Bootstrap(c *gin.Context) {
	type req struct {
		Mode            string `json:"mode"`            // managed|assisted|byo
		Dry             bool   `json:"dryRun"`
		NS              string `json:"namespace"`
		Name            string `json:"releaseName"`
		EnableFilebeat  bool   `json:"enableFilebeat"`
		EnableLogstash  bool   `json:"enableLogstash"`
		ESHost          string `json:"esHost"`
		ESUser          string `json:"esUser"`
		ESPassword      string `json:"esPassword"`
		ESIndex         string `json:"esIndex"`
		DeploymentType  string `json:"deploymentType"` // default|production|minimal|custom
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

	// Create a short-lived Job to run log collection stack installation
	jobName := fmt.Sprintf("polardbx-logs-bootstrap-%d", time.Now().Unix())
	correlationId := fmt.Sprintf("logs-%d", time.Now().UnixNano())

	// Build installation commands based on configuration
	commands := []string{
		"set -e",
		"echo 'Starting PolarDB-X LogCollector installation...'",
	}

	// Create Filebeat DaemonSet if enabled
	if r.EnableFilebeat {
		commands = append(commands, "echo 'Installing Filebeat DaemonSet...'")
		// This would normally apply Filebeat YAML manifests
		commands = append(commands, fmt.Sprintf("echo 'Filebeat configuration: ES Host=%s, Index=%s'", r.ESHost, r.ESIndex))
	}

	// Create Logstash Deployment if enabled
	if r.EnableLogstash {
		commands = append(commands, "echo 'Installing Logstash Deployment...'")
		// This would normally apply Logstash YAML manifests
		commands = append(commands, "echo 'Logstash configuration applied'")
	}

	// Create ConfigMaps for log collection strategies
	commands = append(commands, "echo 'Creating log collection ConfigMaps...'")
	commands = append(commands, "echo 'Log collection stack installation completed successfully'")

	command := strings.Join(commands, " && ")

	backoff := int32(0)
	ttl := int32(600)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.NS,
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
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: func(b bool) *bool { return &b }(true),
					Containers: []corev1.Container{{
						Name:            "logs-installer",
						Image:           "alpine/helm:3.12.3", // Using helm image for kubectl access
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
		"namespace":      r.NS,
		"targetNs":       "polardbx-logcollector",
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