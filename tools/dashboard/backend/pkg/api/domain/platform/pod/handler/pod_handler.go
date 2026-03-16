package handler

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"

	"polardbx-dashboard-backend/pkg/api/domain/platform/pod/service"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/provider"
	"polardbx-dashboard-backend/pkg/api/util"
	"polardbx-dashboard-backend/pkg/logger"
)

// PodHandler handles Pod-related HTTP requests
type PodHandler struct {
	service *service.PodService
}

// NewPodHandler creates new PodHandler
func NewPodHandler(svc *service.PodService) *PodHandler {
	return &PodHandler{service: svc}
}

// NewPodHandlerFromContext creates complete handler chain from gin.Context
func NewPodHandlerFromContext(c *gin.Context) (*PodHandler, bool) {
	p, ok := provider.FromContext(c)
	if !ok {
		apierr.AbortWithError(c, apierr.InternalServiceError("service provider missing", nil))
		return nil, false
	}
	svc, ok := p.PodService(c)
	if !ok {
		return nil, false
	}
	return NewPodHandler(svc), true
}

// GetLogs retrieves Pod logs
// @Summary Get pod logs
// @Description Retrieves logs from a specific pod, optionally filtered by container
// @Tags pods
// @Accept json
// @Produce text/plain
// @Param namespace query string false "Kubernetes namespace (default: default)"
// @Param pod_name path string true "Name of the pod"
// @Param container query string false "Container name (optional, defaults to first container)"
// @Param tailLines query int false "Number of lines to retrieve from the end (default: 1000, max: 10000)"
// @Success 200 {string} string "Pod logs in plain text"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request parameters"
// @Failure 404 {object} apierr.ErrorResponse "Pod not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/platform/logs/{namespace}/{pod_name} [get]
func GetLogs(c *gin.Context) {
	h, ok := NewPodHandlerFromContext(c)
	if !ok {
		return
	}
	h.getLogs(c)
}

func (h *PodHandler) getLogs(c *gin.Context) {
	ns := util.GetNamespace(c, "default")
	pod := c.Param("pod_name")
	container := c.Query("container")
	tailStr := c.DefaultQuery("tailLines", "1000")

	// Security: Enforce upper limit for tailLines to prevent OOM
	const maxTailLines = int64(10000)
	tail := int64(1000)
	if v, err := strconv.ParseInt(tailStr, 10, 64); err == nil && v > 0 {
		tail = v
		if tail > maxTailLines {
			tail = maxTailLines
		}
	}
	ctx, cancel := util.ListCtx(c)
	defer cancel()
	result, err := h.service.GetLogs(ctx, ns, pod, container, tail)
	if err != nil {
		apierr.AbortK8sError(c, "get pod logs", err)
		return
	}
	c.Data(http.StatusOK, "text/plain; charset=utf-8", []byte(result))
}

// ListForCluster lists all Pods for a cluster
func ListForCluster(c *gin.Context) {
	h, ok := NewPodHandlerFromContext(c)
	if !ok {
		return
	}
	h.listForCluster(c)
}

func (h *PodHandler) listForCluster(c *gin.Context) {
	ns := util.GetNamespace(c, "default")
	cluster := c.Param("name")
	ctx, cancel := util.ListCtx(c)
	defer cancel()
	pods, err := h.service.ListForCluster(ctx, ns, cluster)
	if err != nil {
		apierr.AbortK8sError(c, "list cluster pods", err)
		return
	}
	apierr.OK(c, pods)
}

// List lists all Pods in a namespace
// @Summary List pods
// @Description Lists all pods in the specified namespace
// @Tags pods
// @Accept json
// @Produce json
// @Param namespace query string false "Kubernetes namespace (default: default)"
// @Success 200 {array} map[string]any "List of pods"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/platform/pods [get]
func List(c *gin.Context) {
	h, ok := NewPodHandlerFromContext(c)
	if !ok {
		return
	}
	h.list(c)
}

func (h *PodHandler) list(c *gin.Context) {
	ns := util.GetNamespace(c, "default")
	ctx, cancel := util.ListCtx(c)
	defer cancel()
	pods, err := h.service.List(ctx, ns)
	if err != nil {
		apierr.AbortK8sError(c, "list pods", err)
		return
	}
	apierr.OK(c, pods)
}

// Get retrieves a specific Pod
// @Summary Get pod
// @Description Retrieves details of a specific pod
// @Tags pods
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Name of the pod"
// @Success 200 {object} map[string]any "Pod details"
// @Failure 404 {object} apierr.ErrorResponse "Pod not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/platform/pods/{namespace}/{name} [get]
func Get(c *gin.Context) {
	h, ok := NewPodHandlerFromContext(c)
	if !ok {
		return
	}
	h.get(c)
}

func (h *PodHandler) get(c *gin.Context) {
	ns := util.GetNamespace(c, "default")
	name := c.Param("name")
	ctx, cancel := util.ListCtx(c)
	defer cancel()
	pod, err := h.service.Get(ctx, ns, name)
	if err != nil {
		apierr.AbortK8sError(c, "get pod", err)
		return
	}
	apierr.OK(c, pod)
}

// Delete removes a specific Pod
// @Summary Delete pod
// @Description Deletes a specific pod
// @Tags pods
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Name of the pod"
// @Success 200 {object} map[string]any "Deletion confirmation"
// @Failure 404 {object} apierr.ErrorResponse "Pod not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/platform/pods/{namespace}/{name} [delete]
func Delete(c *gin.Context) {
	h, ok := NewPodHandlerFromContext(c)
	if !ok {
		return
	}
	h.delete(c)
}

func (h *PodHandler) delete(c *gin.Context) {
	ns := util.GetNamespace(c, "default")
	name := c.Param("name")
	ctx, cancel := util.CrudCtx(c)
	defer cancel()
	if err := h.service.Delete(ctx, ns, name); err != nil {
		apierr.AbortK8sError(c, "delete pod", err)
		return
	}
	apierr.OK(c, gin.H{"message": "pod deleted"})
}

// ExecWS proxies WebSocket to K8s Exec
// Security improvement: Use backend authenticated kubeconfig instead of allowing client-provided config
// @Summary Execute command in pod via WebSocket
// @Description Establishes a WebSocket connection to execute commands in a pod's container
// @Tags pods
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Name of the pod"
// @Param container query string false "Container name (optional)"
// @Param command query string false "Command to execute (default: /bin/sh)"
// @Success 101 {string} string "Switching protocols to WebSocket"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request or WebSocket upgrade failed"
// @Failure 404 {object} apierr.ErrorResponse "Pod not found"
// @Failure 502 {object} apierr.ErrorResponse "Kubernetes API error"
// @Router /api/v1/platform/pods/{namespace}/{name}/exec [get]
func ExecWS(c *gin.Context) {
	rows, cols := 24, 80
	handleExecWebSocketSecure(c, rows, cols)
}

// ---- WebSocket Exec Helper Code ----

type dynamicTerminalSizeQueue struct {
	ch chan remotecommand.TerminalSize
}

func newDynamicTerminalSizeQueue(rows, cols uint16) *dynamicTerminalSizeQueue {
	q := &dynamicTerminalSizeQueue{ch: make(chan remotecommand.TerminalSize, 1)}
	q.Set(rows, cols)
	return q
}

func (q *dynamicTerminalSizeQueue) Next() *remotecommand.TerminalSize {
	sz, ok := <-q.ch
	if !ok {
		return nil
	}
	return &sz
}

func (q *dynamicTerminalSizeQueue) Set(rows, cols uint16) {
	ts := remotecommand.TerminalSize{Width: cols, Height: rows}
	select {
	case q.ch <- ts:
	default:
		select {
		case <-q.ch:
		default:
		}
		q.ch <- ts
	}
}

func (q *dynamicTerminalSizeQueue) Close() { close(q.ch) }

type termMsg struct {
	Op   string `json:"op"`
	Data string `json:"data,omitempty"`
	Rows uint16 `json:"rows,omitempty"`
	Cols uint16 `json:"cols,omitempty"`
}

type wsJSONWriter struct {
	ws *websocket.Conn
	op string
}

func (w wsJSONWriter) Write(p []byte) (int, error) {
	msg := map[string]any{"op": w.op, "data": base64.StdEncoding.EncodeToString(p)}
	b, _ := json.Marshal(msg)
	if err := w.ws.WriteMessage(websocket.TextMessage, b); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Security constants for WebSocket exec
const (
	// maxExecIdleTimeout is the max idle time before closing the connection
	maxExecIdleTimeout = 30 * time.Minute
	// allowedExecShells are the shells allowed for exec
	allowedExecShell = "/bin/sh"
)

// allowedOrigins returns the list of allowed origins for WebSocket connections
func allowedOrigins() []string {
	// In production, this should be configured via environment variables
	origins := os.Getenv("ALLOWED_WS_ORIGINS")
	if origins == "" {
		// Default: only allow same-origin and localhost for development
		return []string{"localhost", "127.0.0.1"}
	}
	return strings.Split(origins, ",")
}

// checkOrigin validates the WebSocket origin header
func checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		// Same-origin requests may not have Origin header
		return true
	}

	u, err := url.Parse(origin)
	if err != nil {
		logger.Warn("SECURITY: WebSocket origin rejected (invalid Origin)",
			"origin", origin,
			"remoteAddr", r.RemoteAddr,
			"error", err)
		return false
	}

	originHost := u.Hostname()
	originPort := u.Port()
	if originHost == "" {
		logger.Warn("SECURITY: WebSocket origin rejected (empty host)",
			"origin", origin,
			"remoteAddr", r.RemoteAddr)
		return false
	}

	// Allow same-origin (by hostname) without additional configuration.
	reqHost := strings.TrimSpace(r.Host)
	if reqHost != "" {
		reqHostname := reqHost
		if h, _, err := net.SplitHostPort(reqHost); err == nil {
			reqHostname = h
		}
		if strings.EqualFold(originHost, reqHostname) {
			return true
		}
	}

	allowed := allowedOrigins()
	for _, raw := range allowed {
		a := strings.TrimSpace(raw)
		if a == "" {
			continue
		}
		if a == "*" {
			return true
		}

		// Allow entries in either "host" or "host:port" form (also tolerate full URLs).
		if strings.Contains(a, "://") {
			if au, err := url.Parse(a); err == nil {
				a = au.Host
			}
		}
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}

		if strings.Contains(a, ":") {
			// host:port match required
			if strings.EqualFold(u.Host, a) {
				return true
			}
			if originPort != "" && strings.EqualFold(net.JoinHostPort(originHost, originPort), a) {
				return true
			}
		} else {
			// hostname-only match (any port)
			if strings.EqualFold(originHost, a) {
				return true
			}
		}
	}

	// Log rejected origins for security audit
	logger.Warn("SECURITY: WebSocket origin rejected",
		"origin", origin,
		"remoteAddr", r.RemoteAddr)
	return false
}

// handleExecWebSocketSecure handles WebSocket exec using the backend's authenticated kubeconfig
// Security improvements:
// 1. Uses backend-controlled kubeconfig from middleware (not user-supplied)
// 2. Validates WebSocket origin
// 3. Limits allowed shell commands
// 4. Adds audit logging
func handleExecWebSocketSecure(c *gin.Context, rows, cols int) {
	ns := c.Param("namespace")
	name := c.Param("name")
	container := c.Query("container")

	// Security: Get the authenticated kubeconfig from context (set by KubeconfigAuthMiddleware)
	normalizedKubeconfig, exists := c.Get("normalizedKubeconfig")
	if !exists {
		apierr.Abort(c, apierr.Unauthorized("authentication required"))
		return
	}

	kubeconfig, ok := normalizedKubeconfig.([]byte)
	if !ok || len(kubeconfig) == 0 {
		apierr.Abort(c, apierr.Unauthorized("invalid authentication state"))
		return
	}

	// Security audit log
	user := c.GetString("k8sUser")
	logger.Info("AUDIT: Pod exec requested",
		"user", user,
		"namespace", ns,
		"pod", name,
		"container", container,
		"clientIP", c.ClientIP())

	restCfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		logger.Error("ERROR: Failed to create REST config for exec",
			"error", err)
		apierr.AbortWithError(c, apierr.InternalServiceError("configuration error", err))
		return
	}
	restCfg.APIPath = "/api"
	restCfg.GroupVersion = &corev1.SchemeGroupVersion
	restCfg.NegotiatedSerializer = scheme.Codecs.WithoutConversion()

	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		logger.Error("ERROR: Failed to create clientset for exec",
			"error", err)
		apierr.AbortWithError(c, apierr.InternalServiceError("client initialization failed", err))
		return
	}

	// Security: Validate WebSocket origin
	upgrader := websocket.Upgrader{
		CheckOrigin:     checkOrigin,
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		logger.Error("ERROR: WebSocket upgrade failed for exec",
			"error", err)
		return
	}
	defer ws.Close()

	// Set WebSocket timeouts
	ws.SetReadDeadline(time.Now().Add(maxExecIdleTimeout))
	ws.SetPongHandler(func(string) error {
		ws.SetReadDeadline(time.Now().Add(maxExecIdleTimeout))
		return nil
	})

	req := clientset.CoreV1().RESTClient().Post().Resource("pods").Name(name).Namespace(ns).SubResource("exec")
	// Security: Use restricted shell instead of bash -l
	execOpts := &corev1.PodExecOptions{
		Container: container,
		Command:   []string{allowedExecShell},
		Stdin:     true,
		Stdout:    true,
		Stderr:    true,
		TTY:       true,
	}
	req.VersionedParams(execOpts, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(restCfg, http.MethodPost, req.URL())
	if err != nil {
		logger.Error("ERROR: Failed to create SPDY executor for exec",
			"error", err)
		ws.WriteMessage(websocket.TextMessage, []byte(`{"op":"error","data":"exec initialization failed"}`))
		return
	}

	stdinReader, stdinWriter := io.Pipe()
	done := make(chan struct{})
	resizeQ := newDynamicTerminalSizeQueue(uint16(rows), uint16(cols))

	// WS -> stdin & resize
	go func() {
		defer func() { _ = stdinWriter.Close(); close(done) }()
		for {
			mt, data, err := ws.ReadMessage()
			if err != nil {
				return
			}
			// Reset read deadline on activity
			ws.SetReadDeadline(time.Now().Add(maxExecIdleTimeout))

			if mt != websocket.TextMessage {
				continue
			}
			var msg termMsg
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			switch msg.Op {
			case "stdin":
				if msg.Data != "" {
					if decoded, err := base64.StdEncoding.DecodeString(msg.Data); err == nil && len(decoded) > 0 {
						if _, err := stdinWriter.Write(decoded); err != nil {
							return
						}
					}
				}
			case "resize":
				if msg.Rows > 0 && msg.Cols > 0 {
					resizeQ.Set(msg.Rows, msg.Cols)
				}
			}
		}
	}()

	// stream exec
	go func() {
		defer ws.Close()
		_ = executor.Stream(remotecommand.StreamOptions{
			Stdin:             stdinReader,
			Stdout:            wsJSONWriter{ws: ws, op: "stdout"},
			Stderr:            wsJSONWriter{ws: ws, op: "stderr"},
			Tty:               true,
			TerminalSizeQueue: resizeQ,
		})
		logger.Info("AUDIT: Pod exec session ended",
			"user", user,
			"namespace", ns,
			"pod", name)
	}()

	<-done
}

// handleExecWebSocketFast is DEPRECATED - use handleExecWebSocketSecure instead
// Kept for reference but should not be called
func handleExecWebSocketFast(c *gin.Context, rows, cols int) {
	apierr.Abort(c, apierr.Forbidden("this endpoint has been disabled for security reasons: Pod exec now uses backend-controlled authentication"))
}
