package handler

import (
	"context"

	"github.com/gin-gonic/gin"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
)

// LogServiceHandler handles HTTP requests related to log service status
type LogServiceHandler struct {
	clientset kubernetes.Interface
}

// NewLogServiceHandler creates a new LogServiceHandler
func NewLogServiceHandler(clientset kubernetes.Interface) *LogServiceHandler {
	return &LogServiceHandler{clientset: clientset}
}

// NewLogServiceHandlerFromContext creates handler from gin.Context
func NewLogServiceHandlerFromContext(c *gin.Context) (*LogServiceHandler, bool) {
	clientset, ok := util.ClientsetFromContext(c)
	if !ok {
		return nil, false
	}
	return NewLogServiceHandler(clientset), true
}

const (
	// DefaultLogCollectorNamespace default log collector namespace
	DefaultLogCollectorNamespace = "polardbx-logcollector"
	// RestartFlappingThreshold restart flapping threshold
	RestartFlappingThreshold int32 = 5
)

// Status returns the aggregated readiness status of log collection components.
// @Summary Get log service status
// @Description Get aggregated readiness status and configuration of Filebeat/Logstash components.
// @Tags platform, logservice
// @Produce json
// @Param namespace query string false "Kubernetes namespace (default: polardbx-logcollector)"
// @Success 200 {object} map[string]any "Log service status including component health and state"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/logservice/status [get]
func Status(c *gin.Context) {
	h, ok := NewLogServiceHandlerFromContext(c)
	if !ok {
		return
	}
	h.status(c)
}

func (h *LogServiceHandler) status(c *gin.Context) {
	ctx := c.Request.Context()
	ns := c.DefaultQuery("namespace", DefaultLogCollectorNamespace)

	// Get component status
	fbInfo := h.getFilebeatStatus(ctx, ns)
	lsInfo := h.getLogstashStatus(ctx, ns)
	cmExists := h.checkPipelineConfigMap(ctx, ns)

	// Calculate overall status
	state := h.calculateOverallState(fbInfo, lsInfo)

	resp := gin.H{
		"namespace": ns,
		"components": gin.H{
			"filebeat": fbInfo,
			"logstash": lsInfo,
		},
		"pipelineConfigMapExists": cmExists,
		"state":                   state,
		"status":                  state,
	}

	if state == "not_installed" {
		resp["installHint"] = "helm install polardbx-logcollector charts/polardbx-logcollector -n polardbx-logcollector --create-namespace"
	}
	apierr.OK(c, resp)
}

// ComponentInfo component status information
type ComponentInfo struct {
	Status   string      `json:"status"`
	Replicas ReplicaInfo `json:"replicas"`
	Exists   bool        `json:"exists"`
	Error    string      `json:"error,omitempty"`
	Pod      *PodInfo    `json:"pod,omitempty"`
}

// ReplicaInfo replica information
type ReplicaInfo struct {
	Ready int32 `json:"ready"`
	Total int32 `json:"total"`
}

// PodInfo Pod detailed information
type PodInfo struct {
	Name        string `json:"name"`
	Restarts    int32  `json:"restarts"`
	LastReason  string `json:"lastReason,omitempty"`
	CrashReason string `json:"crashReason,omitempty"`
}

func (h *LogServiceHandler) getFilebeatStatus(ctx context.Context, ns string) gin.H {
	ds, dsErr := h.clientset.AppsV1().DaemonSets(ns).Get(ctx, "filebeat", metav1.GetOptions{})

	if dsErr != nil || ds == nil {
		return gin.H{
			"status":   "not_found",
			"replicas": gin.H{"ready": int32(0), "total": int32(0)},
			"exists":   false,
			"error":    errString(dsErr),
		}
	}

	readyFB := ds.Status.NumberReady
	desiredFB := ds.Status.DesiredNumberScheduled

	// Get Pod level details
	podInfo := h.getPodStatus(ctx, ns, "app=filebeat")
	status := h.calculateComponentStatus(true, readyFB, desiredFB, podInfo)

	result := gin.H{
		"status":   status,
		"replicas": gin.H{"ready": readyFB, "total": desiredFB},
		"exists":   true,
		"error":    "",
	}
	if podInfo != nil {
		result["pod"] = podInfo
	}
	return result
}

func (h *LogServiceHandler) getLogstashStatus(ctx context.Context, ns string) gin.H {
	dep, depErr := h.clientset.AppsV1().Deployments(ns).Get(ctx, "logstash", metav1.GetOptions{})

	if depErr != nil || dep == nil {
		return gin.H{
			"status":   "not_found",
			"replicas": gin.H{"ready": int32(0), "total": int32(0)},
			"exists":   false,
			"error":    errString(depErr),
		}
	}

	readyLS := dep.Status.ReadyReplicas
	desiredLS := dep.Status.Replicas

	// Get Pod level details
	podInfo := h.getPodStatus(ctx, ns, "app=logstash")
	status := h.calculateComponentStatus(true, readyLS, desiredLS, podInfo)

	result := gin.H{
		"status":   status,
		"replicas": gin.H{"ready": readyLS, "total": desiredLS},
		"exists":   true,
		"error":    "",
	}
	if podInfo != nil {
		result["pod"] = podInfo
	}
	return result
}

func (h *LogServiceHandler) getPodStatus(ctx context.Context, ns, labelSelector string) *gin.H {
	pods, err := h.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil || len(pods.Items) == 0 {
		return nil
	}

	var maxRestarts int32
	var podName, crashReason, lastReason string

	for i := range pods.Items {
		p := pods.Items[i]
		for _, cs := range p.Status.ContainerStatuses {
			if cs.RestartCount > maxRestarts {
				maxRestarts = cs.RestartCount
				podName = p.Name
				if cs.LastTerminationState.Terminated != nil {
					lastReason = cs.LastTerminationState.Terminated.Reason
				}
			}
			if cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
				crashReason = cs.State.Waiting.Reason
			}
		}
	}

	return &gin.H{
		"name":        podName,
		"restarts":    maxRestarts,
		"lastReason":  lastReason,
		"crashReason": crashReason,
	}
}

func (h *LogServiceHandler) calculateComponentStatus(exists bool, ready, desired int32, podInfo *gin.H) string {
	if !exists {
		return "not_found"
	}

	var crashReason string
	var restarts int32
	if podInfo != nil {
		if cr, ok := (*podInfo)["crashReason"].(string); ok {
			crashReason = cr
		}
		if r, ok := (*podInfo)["restarts"].(int32); ok {
			restarts = r
		}
	}

	if crashReason == "CrashLoopBackOff" {
		return "crashloop"
	}
	if desired > 0 && ready == desired {
		if restarts >= RestartFlappingThreshold {
			return "flapping"
		}
		return "running"
	}
	return "error"
}

func (h *LogServiceHandler) checkPipelineConfigMap(ctx context.Context, ns string) bool {
	cm, err := h.clientset.CoreV1().ConfigMaps(ns).Get(ctx, "logstash-pipeline", metav1.GetOptions{})
	return err == nil && cm != nil
}

func (h *LogServiceHandler) calculateOverallState(fbInfo, lsInfo gin.H) string {
	existsFB := fbInfo["exists"].(bool)
	existsLS := lsInfo["exists"].(bool)

	if !existsFB && !existsLS {
		return "not_installed"
	}

	fbReady := int32(0)
	fbTotal := int32(0)
	lsReady := int32(0)
	lsTotal := int32(0)

	if replicas, ok := fbInfo["replicas"].(gin.H); ok {
		if r, ok := replicas["ready"].(int32); ok {
			fbReady = r
		}
		if t, ok := replicas["total"].(int32); ok {
			fbTotal = t
		}
	}
	if replicas, ok := lsInfo["replicas"].(gin.H); ok {
		if r, ok := replicas["ready"].(int32); ok {
			lsReady = r
		}
		if t, ok := replicas["total"].(int32); ok {
			lsTotal = t
		}
	}

	if existsFB && existsLS && fbTotal > 0 && lsTotal > 0 && fbReady == fbTotal && lsReady == lsTotal {
		return "running"
	}
	return "degraded"
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Keep these types for future use
var _ = (*appsv1.DaemonSet)(nil)
var _ = (*appsv1.Deployment)(nil)
var _ = (*corev1.ConfigMap)(nil)
