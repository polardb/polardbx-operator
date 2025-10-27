package pod

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-ui-backend/pkg/api/util"
	"polardbx-ui-backend/pkg/k8s"
)

func k8sClientFromContext(c *gin.Context) (client.Client, bool) { return util.K8sClientFromContext(c) }
func clientsetFromContext(c *gin.Context) (kubernetes.Interface, bool) {
	return util.ClientsetFromContext(c)
}

// Logs
func GetLogs(c *gin.Context) {
	cs, ok := clientsetFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	pod := c.Param("pod_name")
	container := c.Query("container")
	tailStr := c.DefaultQuery("tailLines", "1000")
	tail := int64(1000)
	if v, err := strconv.ParseInt(tailStr, 10, 64); err == nil && v > 0 {
		tail = v
	}
	result, err := k8s.GetPodLogsWithContext(c.Request.Context(), cs, ns, pod, container, tail)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get logs", "details": err.Error()})
		return
	}
	c.Data(http.StatusOK, "text/plain; charset=utf-8", []byte(result))
}

// Pods of a cluster
func ListForCluster(c *gin.Context) {
	cli, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	cluster := c.Param("name")
	pods, err := k8s.ListPodsForPolarDBXCluster(cli, ns, cluster)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list pods", "details": err.Error()})
		return
	}
	c.JSON(http.StatusOK, pods)
}

func List(c *gin.Context) {
	cli, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.DefaultQuery("namespace", "default")
	var podList corev1.PodList
	if err := cli.List(c.Request.Context(), &podList, client.InNamespace(ns)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list pods", "details": err.Error()})
		return
	}
	c.JSON(http.StatusOK, podList.Items)
}

func Get(c *gin.Context) {
	cli, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	var pod corev1.Pod
	if err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: ns, Name: name}, &pod); err != nil {
		// 统一使用 K8s 错误映射，NotFound->404 等
		util.HandleK8sError(c, "failed to get pod", err)
		return
	}
	c.JSON(http.StatusOK, pod)
}

func Delete(c *gin.Context) {
	cli, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	var pod corev1.Pod
	pod.Namespace = ns
	pod.Name = name
	if err := cli.Delete(c.Request.Context(), &pod); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete pod", "details": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "pod deleted"})
}

// ExecWS - WebSocket proxy for K8s Exec
func ExecWS(c *gin.Context) {
	rows, cols := 24, 80
	handleExecWebSocketFast(c, rows, cols)
}

// ---- Helpers copied from root minimal impl ----

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

func handleExecWebSocketFast(c *gin.Context, rows, cols int) {
	kubeconfigB64 := c.GetHeader("X-Kubeconfig-B64")
	if kubeconfigB64 == "" {
		kubeconfigB64 = c.Query("k")
	}
	if kubeconfigB64 == "" {
		c.JSON(400, gin.H{"error": "missing kubeconfig"})
		return
	}
	kubeconfig, err := base64.StdEncoding.DecodeString(kubeconfigB64)
	if err != nil {
		c.JSON(400, gin.H{"error": "invalid kubeconfig"})
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	container := c.Query("container")

	restCfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		c.JSON(500, gin.H{"error": "config error"})
		return
	}
	restCfg.APIPath = "/api"
	restCfg.GroupVersion = &corev1.SchemeGroupVersion
	restCfg.NegotiatedSerializer = scheme.Codecs.WithoutConversion()

	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		c.JSON(500, gin.H{"error": "client error"})
		return
	}

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer ws.Close()

	req := clientset.CoreV1().RESTClient().Post().Resource("pods").Name(name).Namespace(ns).SubResource("exec")
	execOpts := &corev1.PodExecOptions{Container: container, Command: []string{"/bin/bash", "-l"}, Stdin: true, Stdout: true, Stderr: true, TTY: true}
	req.VersionedParams(execOpts, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(restCfg, http.MethodPost, req.URL())
	if err != nil {
		ws.WriteMessage(websocket.TextMessage, []byte("exec error"))
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
		_ = executor.Stream(remotecommand.StreamOptions{Stdin: stdinReader, Stdout: wsJSONWriter{ws: ws, op: "stdout"}, Stderr: wsJSONWriter{ws: ws, op: "stderr"}, Tty: true, TerminalSizeQueue: resizeQ})
	}()

	<-done
}
