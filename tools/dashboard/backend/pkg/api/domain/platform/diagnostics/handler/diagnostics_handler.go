package handler

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"

	"polardbx-dashboard-backend/pkg/api/domain/platform/diagnostics/service"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
	"polardbx-dashboard-backend/pkg/logger"
)

// Start triggers cluster diagnostic task.
// @Summary Start diagnosis
// @Description Create a polardbx-clinic Pod to collect diagnostic information for the given cluster.
// @Tags diagnostics
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the cluster"
// @Param cluster path string true "Name of the PolarDB-X cluster"
// @Success 202 {object} service.DiagnosticJob "Diagnostic task started"
// @Failure 400 {object} apierr.ErrorResponse "Request parameter error"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/diagnostics/{namespace}/{cluster}/start [post]
func Start(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")

	if namespace == "" || cluster == "" {
		apierr.AbortWithError(c, apierr.ValidationError("namespace and cluster name cannot be empty", nil))
		return
	}

	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	svc := service.NewDiagnosticsService(cli)
	job, err := svc.StartDiagnosis(c.Request.Context(), namespace, cluster)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	apierr.Accepted(c, job)
}

// GetStatus returns diagnostic task progress/status.
// @Summary Get diagnostic status
// @Description Get current status and progress of specified diagnostic task
// @Tags diagnostics
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the diagnostic job"
// @Param id path string true "diagnostic task ID"
// @Success 200 {object} service.DiagnosticJob "Diagnostic task status"
// @Failure 400 {object} apierr.ErrorResponse "Request parameter error"
// @Failure 404 {object} apierr.ErrorResponse "Diagnostic task not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/diagnostics/{namespace}/{id}/status [get]
func GetStatus(c *gin.Context) {
	namespace := c.Param("namespace")
	id := c.Param("id")

	if namespace == "" || id == "" {
		apierr.AbortWithError(c, apierr.ValidationError("namespace and task ID cannot be empty", nil))
		return
	}

	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	svc := service.NewDiagnosticsService(cli)
	job, err := svc.GetDiagnosisStatus(c.Request.Context(), namespace, id)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	apierr.OK(c, job)
}

// ListReports lists diagnostic reports.
// @Summary List diagnostic reports
// @Description List all diagnostic reports under specified namespace
// @Tags diagnostics
// @Accept json
// @Produce json
// @Param namespace query string false "Kubernetes namespace; list all namespaces if not specified"
// @Success 200 {array} service.DiagnosticJob "Diagnostic reports list"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/diagnostics/reports [get]
func ListReports(c *gin.Context) {
	namespace := c.DefaultQuery("namespace", "")

	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	svc := service.NewDiagnosticsService(cli)
	reports, err := svc.ListDiagnosisReports(c.Request.Context(), namespace)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	if reports == nil {
		reports = []service.DiagnosticJob{}
	}

	apierr.OK(c, gin.H{
		"namespace": namespace,
		"reports":   reports,
		"total":     len(reports),
	})
}

// Download returns report download information.
// @Summary Download diagnostic report
// @Description Get download link or path for diagnostic report
// @Tags diagnostics
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the diagnostic job"
// @Param id path string true "diagnostic task ID"
// @Success 200 {object} map[string]any "Download information including path, URL, and helper command"
// @Failure 400 {object} apierr.ErrorResponse "Request parameter error"
// @Failure 404 {object} apierr.ErrorResponse "Diagnostic report not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/diagnostics/{namespace}/{id}/download [get]
func Download(c *gin.Context) {
	namespace := c.Param("namespace")
	id := c.Param("id")

	if namespace == "" || id == "" {
		apierr.AbortWithError(c, apierr.ValidationError("namespace and task ID cannot be empty", nil))
		return
	}

	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	svc := service.NewDiagnosticsService(cli)
	outputPath, err := svc.GetDownloadInfo(c.Request.Context(), namespace, id)
	if err != nil {
		// Keep backward-compatible behavior: treat "not ready" as not found for the download endpoint.
		apierr.AbortWithError(c, apierr.NotFoundError("diagnostic report", id))
		return
	}

	// Return download information
	// Note: In production environment, may need to:
	// 1. Copy files from Pod to accessible storage
	// 2. Generate pre-signed URLs
	// 3. Stream files through API proxy
	apierr.OK(c, gin.H{
		"id":        id,
		"namespace": namespace,
		"path":      outputPath,
		"url":       "/api/v1/diagnostics/" + namespace + "/" + id + "/file",
		"message":   "diagnostic report ready, can be downloaded via kubectl cp command or url",
		"command":   "kubectl cp " + namespace + "/polardbx-clinic-" + id + ":" + outputPath + " ./" + id + ".tar.gz",
	})
}

// GetFile gets diagnostic report file (streaming download).
// @Summary Get diagnostic report file
// @Description Get diagnostic report file content from Pod
// @Tags diagnostics
// @Produce application/gzip
// @Param namespace path string true "Kubernetes namespace of the diagnostic job"
// @Param id path string true "diagnostic task ID"
// @Success 200 {file} binary "Diagnostic report file"
// @Failure 400 {object} apierr.ErrorResponse "Request parameter error (e.g., invalid ID)"
// @Failure 401 {object} apierr.ErrorResponse "Authentication required or invalid authentication state"
// @Failure 404 {object} apierr.ErrorResponse "Diagnostic job or file not found"
// @Failure 409 {object} apierr.ErrorResponse "Diagnostic report not ready yet"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/diagnostics/{namespace}/{id}/file [get]
func GetFile(c *gin.Context) {
	namespace := c.Param("namespace")
	id := c.Param("id")

	// Validate inputs early to avoid command injection and invalid paths.
	if namespace == "" || id == "" {
		apierr.AbortWithError(c, apierr.ValidationError("namespace and task ID cannot be empty", nil))
		return
	}
	if !isValidDiagnosticID(id) {
		apierr.Abort(c, apierr.InvalidParam("id", "invalid diagnostic task id"))
		return
	}

	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	// Check status first to provide a predictable UX.
	svc := service.NewDiagnosticsService(cli)
	job, err := svc.GetDiagnosisStatus(c.Request.Context(), namespace, id)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	if job.Status != service.DiagStatusSucceeded {
		apierr.Abort(c, apierr.Conflict("diagnostic report not ready").WithDetails(gin.H{
			"id":       id,
			"status":   job.Status,
			"progress": job.Progress,
			"message":  job.Message,
		}))
		return
	}

	// Use authenticated kubeconfig from middleware to exec into the diagnostic pod.
	normalizedKubeconfig, exists := c.Get("normalizedKubeconfig")
	if !exists {
		abortFileFallback(c, namespace, id, apierr.Unauthorized("authentication required"))
		return
	}
	kubeconfig, ok := normalizedKubeconfig.([]byte)
	if !ok || len(kubeconfig) == 0 {
		abortFileFallback(c, namespace, id, apierr.Unauthorized("invalid authentication state"))
		return
	}
	restCfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		abortFileFallback(c, namespace, id, apierr.Internal("configuration error"))
		return
	}
	restCfg.APIPath = "/api"
	restCfg.GroupVersion = &corev1.SchemeGroupVersion
	restCfg.NegotiatedSerializer = scheme.Codecs.WithoutConversion()

	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		abortFileFallback(c, namespace, id, apierr.Internal("client initialization failed"))
		return
	}

	podName := service.ClinicPodPrefix + id
	filePath := fmt.Sprintf("/tmp/polardbx-clinic/%s.tar.gz", id)

	// Basic audit log
	user := c.GetString("k8sUser")
	logger.Info("AUDIT: diagnostics file download requested",
		"user", user,
		"namespace", namespace,
		"id", id,
		"pod", podName,
		"clientIP", c.ClientIP())

	// Stream tar.gz from pod to response.
	c.Header("Content-Type", "application/gzip")
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%q", id+".tar.gz"))
	c.Header("Cache-Control", "no-store")
	c.Status(http.StatusOK)

	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec")

	execOpts := &corev1.PodExecOptions{
		Container: "clinic",
		Command:   []string{"/bin/sh", "-c", "cat " + shellEscapePath(filePath)},
		Stdin:     false,
		Stdout:    true,
		Stderr:    true,
		TTY:       false,
	}
	req.VersionedParams(execOpts, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(restCfg, http.MethodPost, req.URL())
	if err != nil {
		abortFileFallback(c, namespace, id, apierr.Wrap(apierr.ErrBadGateway, "exec initialization failed", err))
		return
	}

	// Use request context with an upper bound to avoid hanging streams.
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Minute)
	defer cancel()

	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: c.Writer,
		Stderr: &limitedLogWriter{limit: 8 << 10},
		Tty:    false,
	})
	if err != nil {
		// If the stream fails after headers have been written, we can't change status.
		// Log and let client observe truncated download / connection close.
		if errors.IsForbidden(err) {
			logger.Warn("diagnostics file download forbidden", "namespace", namespace, "id", id, "error", err)
		} else {
			logger.Error("diagnostics file download failed", "namespace", namespace, "id", id, "error", err)
		}
		return
	}
}

// DeleteJob deletes diagnostic task (cleanup Pod)
// @Summary Delete diagnostic task
// @Description Delete diagnostic Pod and related resources
// @Tags diagnostics
// @Accept json
// @Produce json
// @Param namespace path string true "namespace"
// @Param id path string true "diagnostic task ID"
// @Success 200 {object} map[string]string "deletion successful"
// @Failure 404 {object} map[string]string "task not found"
// @Failure 500 {object} map[string]string "server error"
// @Router /api/v1/diagnostics/{namespace}/{id} [delete]
func DeleteJob(c *gin.Context) {
	namespace := c.Param("namespace")
	id := c.Param("id")

	if namespace == "" || id == "" {
		apierr.AbortWithError(c, apierr.ValidationError("namespace and task ID cannot be empty", nil))
		return
	}

	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	// Delete diagnostic Pod
	podName := service.ClinicPodPrefix + id
	pod := &corev1.Pod{}
	pod.SetName(podName)
	pod.SetNamespace(namespace)

	if err := cli.Delete(c.Request.Context(), pod); err != nil {
		apierr.AbortK8sError(c, "delete diagnostic pod", err)
		return
	}

	apierr.OK(c, gin.H{
		"message": "diagnostic task deleted",
		"id":      id,
	})
}

var diagnosticIDRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func isValidDiagnosticID(id string) bool {
	return id != "" && diagnosticIDRe.MatchString(id)
}

// shellEscapePath escapes a path for safe use in a simple `sh -c` command.
// Since we already validate id and we only format a fixed directory with id,
// this is an extra safety measure.
func shellEscapePath(p string) string {
	// single-quote escape for POSIX shell: ' -> '\''.
	// Note: p should not contain newlines; id validation ensures that.
	out := "'"
	for _, r := range p {
		if r == '\'' {
			out += `'\''`
		} else {
			out += string(r)
		}
	}
	out += "'"
	return out
}

func abortFileFallback(c *gin.Context, namespace, id string, err *apierr.APIError) {
	detail := gin.H{
		"message": "online download is not available; please use kubectl cp",
		"command": fmt.Sprintf("kubectl cp %s/%s%s:%s ./%s.tar.gz", namespace, service.ClinicPodPrefix, id, "/tmp/polardbx-clinic/"+id+".tar.gz", id),
	}
	apierr.Abort(c, err.WithDetails(detail))
}

// limitedLogWriter captures a small amount of stderr for debugging without OOM risk.
type limitedLogWriter struct {
	buf   []byte
	limit int
}

func (w *limitedLogWriter) Write(p []byte) (int, error) {
	remain := w.limit - len(w.buf)
	if remain > 0 {
		if len(p) > remain {
			w.buf = append(w.buf, p[:remain]...)
		} else {
			w.buf = append(w.buf, p...)
		}
	}
	return len(p), nil
}
