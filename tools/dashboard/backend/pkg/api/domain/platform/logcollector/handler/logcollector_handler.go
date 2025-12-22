package handler

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	apierr "polardbx-dashboard-backend/pkg/api/errors"

	"github.com/gin-gonic/gin"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"

	"polardbx-dashboard-backend/pkg/api/domain/platform/logcollector/repository"
	"polardbx-dashboard-backend/pkg/api/util"
	"polardbx-dashboard-backend/pkg/k8s"
)

// LogCollectorHandler handles HTTP requests related to log collectors
type LogCollectorHandler struct {
	repo repository.LogCollectorRepository
}

// NewLogCollectorHandler creates a new LogCollectorHandler
func NewLogCollectorHandler(repo repository.LogCollectorRepository) *LogCollectorHandler {
	return &LogCollectorHandler{repo: repo}
}

// NewLogCollectorHandlerFromContext creates handler from gin.Context
func NewLogCollectorHandlerFromContext(c *gin.Context) (*LogCollectorHandler, bool) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return nil, false
	}
	repo := repository.NewK8sLogCollectorRepository(cli)
	return NewLogCollectorHandler(repo), true
}

// k8sClientFromContext gets k8s client from context (for backward compatibility)
func k8sClientFromContext(c *gin.Context) (client.Client, bool) {
	v, ok := c.Get("k8sClient")
	if !ok {
		apierr.AbortForbidden(c, "kubernetes client not initialized")
		return nil, false
	}
	cli, ok := v.(client.Client)
	if !ok || cli == nil {
		apierr.AbortForbidden(c, "invalid kubernetes client in context")
		return nil, false
	}
	return cli, true
}

// List lists log collectors in a namespace.
// @Summary List log collectors
// @Description List all PolarDB-X log collectors in the specified namespace.
// @Tags platform, logcollector
// @Produce json
// @Param namespace query string false "Kubernetes namespace (default: default)"
// @Success 200 {array} map[string]any "List of log collectors"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/logcollectors [get]
func List(c *gin.Context) {
	h, ok := NewLogCollectorHandlerFromContext(c)
	if !ok {
		return
	}
	h.list(c)
}

func (h *LogCollectorHandler) list(c *gin.Context) {
	namespace := c.DefaultQuery("namespace", "default")
	collectors, err := h.repo.List(c.Request.Context(), namespace)
	if err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("failed to list log collectors", err))
		return
	}
	apierr.OK(c, collectors)
}

// Create creates a new log collector.
// @Summary Create log collector
// @Description Create a new PolarDB-X log collector resource.
// @Tags platform, logcollector
// @Accept json
// @Produce json
// @Param namespace query string false "Kubernetes namespace (default: default)"
// @Param body body map[string]any true "Log collector specification"
// @Success 201 {object} map[string]any "Created log collector"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/logcollectors [post]
func Create(c *gin.Context) {
	h, ok := NewLogCollectorHandlerFromContext(c)
	if !ok {
		return
	}
	h.create(c)
}

func (h *LogCollectorHandler) create(c *gin.Context) {
	namespace := c.DefaultQuery("namespace", "default")
	var collector polardbxv1.PolarDBXLogCollector
	if err := c.ShouldBindJSON(&collector); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	collector.Namespace = namespace
	createdCollector, err := h.repo.Create(c.Request.Context(), &collector)
	if err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("failed to create log collector", err))
		return
	}
	apierr.Created(c, createdCollector)
}

// Get retrieves a specific log collector by namespace and name.
// @Summary Get log collector
// @Description Get a PolarDB-X log collector by namespace and name.
// @Tags platform, logcollector
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Log collector name"
// @Success 200 {object} map[string]any "Log collector details"
// @Failure 404 {object} apierr.ErrorResponse "Log collector not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/logcollectors/{namespace}/{name} [get]
func Get(c *gin.Context) {
	h, ok := NewLogCollectorHandlerFromContext(c)
	if !ok {
		return
	}
	h.get(c)
}

func (h *LogCollectorHandler) get(c *gin.Context) {
	namespace := c.Param("namespace")
	name := c.Param("name")
	collector, err := h.repo.Get(c.Request.Context(), namespace, name)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, collector)
}

// Update updates an existing log collector.
// @Summary Update log collector
// @Description Update an existing PolarDB-X log collector resource.
// @Tags platform, logcollector
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Log collector name"
// @Param body body map[string]any true "Updated log collector specification"
// @Success 200 {object} map[string]any "Updated log collector"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/logcollectors/{namespace}/{name} [put]
func Update(c *gin.Context) {
	h, ok := NewLogCollectorHandlerFromContext(c)
	if !ok {
		return
	}
	h.update(c)
}

func (h *LogCollectorHandler) update(c *gin.Context) {
	namespace := c.Param("namespace")
	var collector polardbxv1.PolarDBXLogCollector
	if err := c.ShouldBindJSON(&collector); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	collector.Namespace = namespace
	updatedCollector, err := h.repo.Update(c.Request.Context(), &collector)
	if err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("failed to update log collector", err))
		return
	}
	apierr.OK(c, updatedCollector)
}

// Delete deletes a log collector.
// @Summary Delete log collector
// @Description Delete a PolarDB-X log collector resource.
// @Tags platform, logcollector
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Log collector name"
// @Success 200 {object} map[string]any "Deletion confirmation"
// @Failure 404 {object} apierr.ErrorResponse "Log collector not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/logcollectors/{namespace}/{name} [delete]
func Delete(c *gin.Context) {
	h, ok := NewLogCollectorHandlerFromContext(c)
	if !ok {
		return
	}
	h.delete(c)
}

func (h *LogCollectorHandler) delete(c *gin.Context) {
	namespace := c.Param("namespace")
	name := c.Param("name")
	if err := h.repo.Delete(c.Request.Context(), namespace, name); err != nil {
		// Surface K8s error via unified error conversion path.
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"message": "log collector deleted successfully"})
}

// ======================== Pipeline related functions ========================

// clientsetFromContext gets kubernetes clientset from context
func clientsetFromContext(c *gin.Context) (kubernetes.Interface, bool) {
	v, ok := c.Get("clientset")
	if !ok {
		apierr.AbortForbidden(c, "kubeconfig not provided or invalid")
		return nil, false
	}
	cs, ok := v.(kubernetes.Interface)
	if !ok || cs == nil {
		apierr.AbortForbidden(c, "invalid clientset in context")
		return nil, false
	}
	return cs, true
}

// GetLogstashPipeline retrieves the Logstash pipeline ConfigMap content.
// @Summary Get Logstash pipeline
// @Description Get the Logstash pipeline ConfigMap content for a namespace.
// @Tags platform, logcollector
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param configMap query string false "ConfigMap name (default: logstash-pipeline)"
// @Param key query string false "Specific key to retrieve from ConfigMap"
// @Success 200 {object} map[string]any "Pipeline ConfigMap data"
// @Failure 404 {object} apierr.ErrorResponse "ConfigMap or key not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/logcollectors/{namespace}/pipeline [get]
func GetLogstashPipeline(c *gin.Context) {
	clientset, ok := clientsetFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	cmName := c.DefaultQuery("configMap", "logstash-pipeline")
	key := c.Query("key")
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()
	cm, err := clientset.CoreV1().ConfigMaps(namespace).Get(ctx, cmName, metav1.GetOptions{})
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	if key != "" {
		if v, ok := cm.Data[key]; ok {
			apierr.OK(c, gin.H{"namespace": namespace, "configMap": cmName, "key": key, "value": v})
			return
		}
		apierr.AbortWithError(c, apierr.NotFoundError("configmap key", key))
		return
	}
	apierr.OK(c, gin.H{"namespace": namespace, "configMap": cmName, "data": cm.Data})
}

type updatePipelineRequest struct {
	Data map[string]string `json:"data"`
}

// UpdateLogstashPipeline updates or creates the Logstash pipeline ConfigMap.
// @Summary Update Logstash pipeline
// @Description Update or create Logstash pipeline ConfigMap content.
// @Tags platform, logcollector
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param configMap query string false "ConfigMap name (default: logstash-pipeline)"
// @Param body body map[string]any true "Pipeline data payload"
// @Success 200 {object} map[string]any "Updated pipeline ConfigMap"
// @Success 201 {object} map[string]any "Created pipeline ConfigMap"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request body"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/logcollectors/{namespace}/pipeline [put]
func UpdateLogstashPipeline(c *gin.Context) {
	clientset, ok := clientsetFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	cmName := c.DefaultQuery("configMap", "logstash-pipeline")
	var req updatePipelineRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	if req.Data == nil {
		req.Data = map[string]string{}
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()
	cm, err := clientset.CoreV1().ConfigMaps(namespace).Get(ctx, cmName, metav1.GetOptions{})
	if err != nil {
		newCm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: cmName, Namespace: namespace}, Data: req.Data}
		if _, err2 := clientset.CoreV1().ConfigMaps(namespace).Create(ctx, newCm, metav1.CreateOptions{}); err2 != nil {
			apierr.AbortWithError(c, err2)
			return
		}
		apierr.Created(c, gin.H{"namespace": namespace, "configMap": cmName, "data": req.Data})
		return
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	for k, v := range req.Data {
		cm.Data[k] = v
	}
	if _, err := clientset.CoreV1().ConfigMaps(namespace).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"namespace": namespace, "configMap": cmName, "data": cm.Data})
}

type esCertRequest struct {
	CACrt string `json:"caCrt"`
}

// GetElasticsearchCert retrieves the Elasticsearch CA certificate information.
// @Summary Get Elasticsearch certificate
// @Description Get Elasticsearch CA certificate information from the secret.
// @Tags platform, logcollector
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name query string false "Secret name (default: elastic-certs-public)"
// @Param include query string false "Include certificate content in response (default: false)"
// @Success 200 {object} map[string]any "Certificate information"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/logcollectors/{namespace}/es-cert [get]
func GetElasticsearchCert(c *gin.Context) {
	clientset, ok := clientsetFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	secName := c.DefaultQuery("name", "elastic-certs-public")
	include := strings.ToLower(c.DefaultQuery("include", "false")) == "true"
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()
	sec, err := clientset.CoreV1().Secrets(namespace).Get(ctx, secName, metav1.GetOptions{})
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	v := string(sec.Data["ca.crt"])
	resp := gin.H{"namespace": namespace, "secret": secName, "hasCA": v != "", "size": len(v)}
	if include {
		resp["caCrt"] = v
	}
	apierr.OK(c, resp)
}

// UpdateElasticsearchCert updates or creates the Elasticsearch CA certificate secret.
// @Summary Update Elasticsearch certificate
// @Description Update or create Elasticsearch CA certificate in the secret.
// @Tags platform, logcollector
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name query string false "Secret name (default: elastic-certs-public)"
// @Param body body map[string]any true "Certificate payload (caCrt field)"
// @Success 200 {object} map[string]any "Updated certificate info"
// @Success 201 {object} map[string]any "Created certificate info"
// @Failure 400 {object} apierr.ErrorResponse "Invalid caCrt"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/logcollectors/{namespace}/es-cert [put]
func UpdateElasticsearchCert(c *gin.Context) {
	clientset, ok := clientsetFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	secName := c.DefaultQuery("name", "elastic-certs-public")
	var req esCertRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	if strings.TrimSpace(req.CACrt) == "" {
		apierr.AbortWithError(c, apierr.ValidationError("invalid caCrt", nil))
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()
	sec, err := clientset.CoreV1().Secrets(namespace).Get(ctx, secName, metav1.GetOptions{})
	if err != nil {
		newSec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secName, Namespace: namespace}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{"ca.crt": []byte(req.CACrt)}}
		if _, err2 := clientset.CoreV1().Secrets(namespace).Create(ctx, newSec, metav1.CreateOptions{}); err2 != nil {
			apierr.AbortWithError(c, err2)
			return
		}
		apierr.Created(c, gin.H{"namespace": namespace, "secret": secName, "size": len(req.CACrt)})
		return
	}
	if sec.Data == nil {
		sec.Data = map[string][]byte{}
	}
	sec.Data["ca.crt"] = []byte(req.CACrt)
	if _, err := clientset.CoreV1().Secrets(namespace).Update(ctx, sec, metav1.UpdateOptions{}); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	apierr.OK(c, gin.H{"namespace": namespace, "secret": secName, "size": len(req.CACrt)})
}

// GetLogCollectorStatus retrieves the status of a log collector including Filebeat/Logstash readiness.
// @Summary Get log collector status
// @Description Get aggregated Filebeat/Logstash ready status and output mode for a log collector.
// @Tags platform, logcollector
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Log collector name"
// @Success 200 {object} map[string]any "Log collector status"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/logcollectors/{namespace}/{name}/status [get]
func GetLogCollectorStatus(c *gin.Context) {
	k8sCli, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	clientset, ok2 := clientsetFromContext(c)
	if !ok2 {
		return
	}
	namespace := c.Param("namespace")
	name := c.Param("name")
	fbName := "filebeat"
	lsName := "logstash"
	if collector, err := k8s.GetPolarDBXLogCollector(k8sCli, namespace, name); err == nil && collector != nil {
		if collector.Spec.FileBeatName != "" {
			fbName = collector.Spec.FileBeatName
		}
		if collector.Spec.LogStashName != "" {
			lsName = collector.Spec.LogStashName
		}
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()
	var ds *appsv1.DaemonSet
	var dep *appsv1.Deployment
	ds, dsErr := clientset.AppsV1().DaemonSets(namespace).Get(ctx, fbName, metav1.GetOptions{})
	dep, depErr := clientset.AppsV1().Deployments(namespace).Get(ctx, lsName, metav1.GetOptions{})
	cm, _ := clientset.CoreV1().ConfigMaps(namespace).Get(ctx, "logstash-pipeline", metav1.GetOptions{})
	outputType := "unknown"
	endpoint := ""
	if cm != nil {
		for _, v := range cm.Data {
			s := strings.ToLower(v)
			if strings.Contains(s, "elasticsearch") {
				outputType = "elasticsearch"
				if i := strings.Index(s, "hosts =>"); i >= 0 {
					endpoint = v[i:]
				}
				break
			}
			if strings.Contains(s, "stdout") {
				outputType = "stdout"
			}
		}
	}
	resp := gin.H{"namespace": namespace, "name": name, "outputs": gin.H{"type": outputType, "endpoint": endpoint}}
	if dsErr == nil {
		resp["filebeat"] = gin.H{"ready": ds.Status.NumberReady, "desired": ds.Status.DesiredNumberScheduled}
	} else {
		resp["filebeatError"] = dsErr.Error()
	}
	if depErr == nil {
		resp["logstash"] = gin.H{"ready": dep.Status.ReadyReplicas, "desired": dep.Status.Replicas}
	} else {
		resp["logstashError"] = depErr.Error()
	}
	apierr.OK(c, resp)
}

// StreamLogstashLogs streams Logstash pod logs.
// @Summary Stream Logstash logs
// @Description Stream logs from a Logstash pod in the specified namespace.
// @Tags platform, logcollector
// @Produce text/plain
// @Param namespace path string true "Kubernetes namespace"
// @Param pod query string false "Pod name (auto-detected if not specified)"
// @Param container query string false "Container name (default: logstash)"
// @Param follow query string false "Follow log stream (default: true)"
// @Param tailLines query string false "Number of tail lines (default: 200)"
// @Success 200 {string} string "Log stream"
// @Failure 404 {object} apierr.ErrorResponse "Logstash pod not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/logcollectors/{namespace}/logs [get]
func StreamLogstashLogs(c *gin.Context) {
	clientset, ok := clientsetFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	pod := c.Query("pod")
	container := c.DefaultQuery("container", "logstash")
	follow := strings.ToLower(c.DefaultQuery("follow", "true")) == "true"
	tailStr := c.DefaultQuery("tailLines", "200")
	tail, _ := strconv.ParseInt(tailStr, 10, 64)
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()
	if pod == "" {
		pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: "app=logstash"})
		if err == nil && len(pods.Items) > 0 {
			pod = pods.Items[0].Name
		}
		if pod == "" {
			pods2, err2 := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
			if err2 == nil {
				for _, p := range pods2.Items {
					for _, ctn := range p.Spec.Containers {
						if strings.Contains(strings.ToLower(ctn.Name), "logstash") {
							pod = p.Name
							break
						}
					}
					if pod != "" {
						break
					}
				}
			}
		}
		if pod == "" {
			apierr.AbortWithError(c, apierr.NotFoundError("logstash pod", ""))
			return
		}
	}
	opts := &corev1.PodLogOptions{Container: container}
	if follow {
		opts.Follow = true
	}
	if tail > 0 {
		opts.TailLines = &tail
	}
	stream, err := clientset.CoreV1().Pods(namespace).GetLogs(pod, opts).Stream(ctx)
	if err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	defer stream.Close()
	c.Header("Content-Type", "text/plain; charset=utf-8")
	flusher, _ := c.Writer.(http.Flusher)
	buf := make([]byte, 8*1024)
	for {
		n, rerr := stream.Read(buf)
		if n > 0 {
			_, _ = c.Writer.Write(buf[:n])
			if flusher != nil {
				flusher.Flush()
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			break
		}
	}
}

// TestLogCollector performs diagnostic checks on the log collector configuration.
// @Summary Test log collector
// @Description Perform simple diagnostic checks to find common configuration errors.
// @Tags platform, logcollector
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Success 200 {object} map[string]any "Diagnostic check results"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/logcollectors/{namespace}/test [get]
func TestLogCollector(c *gin.Context) {
	clientset, ok := clientsetFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()
	checks := make([]gin.H, 0, 6)
	fb, fbErr := clientset.AppsV1().DaemonSets(namespace).Get(ctx, "filebeat", metav1.GetOptions{})
	ls, lsErr := clientset.AppsV1().Deployments(namespace).Get(ctx, "logstash", metav1.GetOptions{})
	if fbErr == nil {
		checks = append(checks, gin.H{"name": "filebeat ready", "ok": fb.Status.NumberReady == fb.Status.DesiredNumberScheduled})
	} else {
		checks = append(checks, gin.H{"name": "filebeat fetch", "ok": false, "details": fbErr.Error()})
	}
	if lsErr == nil {
		checks = append(checks, gin.H{"name": "logstash ready", "ok": ls.Status.ReadyReplicas == ls.Status.Replicas})
	} else {
		checks = append(checks, gin.H{"name": "logstash fetch", "ok": false, "details": lsErr.Error()})
	}
	cm, cmErr := clientset.CoreV1().ConfigMaps(namespace).Get(ctx, "logstash-pipeline", metav1.GetOptions{})
	if cmErr == nil {
		hasES := false
		for _, v := range cm.Data {
			if strings.Contains(strings.ToLower(v), "elasticsearch") {
				hasES = true
				break
			}
		}
		checks = append(checks, gin.H{"name": "pipeline exists", "ok": true})
		if hasES {
			if strings.Contains(strings.ToLower(joinValues(cm.Data)), "https://") {
				sec, err := clientset.CoreV1().Secrets(namespace).Get(ctx, "elastic-certs-public", metav1.GetOptions{})
				checks = append(checks, gin.H{"name": "es https cert", "ok": err == nil && len(sec.Data["ca.crt"]) > 0})
			}
		}
	} else {
		checks = append(checks, gin.H{"name": "pipeline exists", "ok": false, "details": cmErr.Error()})
	}
	apierr.OK(c, gin.H{"checks": checks})
}

func joinValues(m map[string]string) string {
	b, _ := json.Marshal(m)
	return string(b)
}
