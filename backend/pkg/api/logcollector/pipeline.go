package logcollector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"polardbx-ui-backend/pkg/k8s"
)

func clientsetFromContext(c *gin.Context) (kubernetes.Interface, bool) {
	v, ok := c.Get("clientset")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "kubeconfig not provided or invalid"})
		return nil, false
	}
	cs, ok := v.(kubernetes.Interface)
	if !ok || cs == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid clientset in context"})
		return nil, false
	}
	return cs, true
}

// k8sClientFromContext is declared in endpoints.go and reused here

// GetLogstashPipeline returns contents of the logstash pipeline ConfigMap.
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get logstash pipeline configmap", "details": err.Error()})
		return
	}
	if key != "" {
		if v, ok := cm.Data[key]; ok {
			c.JSON(http.StatusOK, gin.H{"namespace": namespace, "configMap": cmName, "key": key, "value": v})
			return
		}
		c.JSON(http.StatusNotFound, gin.H{"error": "key not found in configmap"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"namespace": namespace, "configMap": cmName, "data": cm.Data})
}

type updatePipelineRequest struct {
	Data map[string]string `json:"data"`
}

// UpdateLogstashPipeline updates or creates the logstash pipeline ConfigMap.
func UpdateLogstashPipeline(c *gin.Context) {
	clientset, ok := clientsetFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	cmName := c.DefaultQuery("configMap", "logstash-pipeline")
	var req updatePipelineRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body", "details": err.Error()})
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
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create pipeline configmap", "details": err2.Error()})
			return
		}
		c.JSON(http.StatusCreated, gin.H{"namespace": namespace, "configMap": cmName, "data": req.Data})
		return
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	for k, v := range req.Data {
		cm.Data[k] = v
	}
	if _, err := clientset.CoreV1().ConfigMaps(namespace).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update pipeline configmap", "details": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"namespace": namespace, "configMap": cmName, "data": cm.Data})
}

type esCertRequest struct {
	CACrt string `json:"caCrt"`
}

// GetElasticsearchCert returns info (and optionally content) of elastic-certs-public secret
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get secret", "details": err.Error()})
		return
	}
	v := string(sec.Data["ca.crt"])
	resp := gin.H{"namespace": namespace, "secret": secName, "hasCA": v != "", "size": len(v)}
	if include {
		resp["caCrt"] = v
	}
	c.JSON(http.StatusOK, resp)
}

// UpdateElasticsearchCert upserts elastic-certs-public secret's ca.crt
func UpdateElasticsearchCert(c *gin.Context) {
	clientset, ok := clientsetFromContext(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	secName := c.DefaultQuery("name", "elastic-certs-public")
	var req esCertRequest
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.CACrt) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid caCrt"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()
	sec, err := clientset.CoreV1().Secrets(namespace).Get(ctx, secName, metav1.GetOptions{})
	if err != nil {
		newSec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secName, Namespace: namespace}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{"ca.crt": []byte(req.CACrt)}}
		if _, err2 := clientset.CoreV1().Secrets(namespace).Create(ctx, newSec, metav1.CreateOptions{}); err2 != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create secret", "details": err2.Error()})
			return
		}
		c.JSON(http.StatusCreated, gin.H{"namespace": namespace, "secret": secName, "size": len(req.CACrt)})
		return
	}
	if sec.Data == nil {
		sec.Data = map[string][]byte{}
	}
	sec.Data["ca.crt"] = []byte(req.CACrt)
	if _, err := clientset.CoreV1().Secrets(namespace).Update(ctx, sec, metav1.UpdateOptions{}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update secret", "details": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"namespace": namespace, "secret": secName, "size": len(req.CACrt)})
}

// GetLogCollectorStatus aggregates Filebeat/Logstash readiness and output mode
func GetLogCollectorStatus(c *gin.Context) {
	k8sClient, ok := k8sClientFromContext(c)
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
	if collector, err := k8s.GetPolarDBXLogCollector(k8sClient, namespace, name); err == nil && collector != nil {
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
	c.JSON(http.StatusOK, resp)
}

// StreamLogstashLogs streams logs from a logstash pod
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
			c.JSON(http.StatusNotFound, gin.H{"error": "no logstash pod found"})
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to open log stream", "details": err.Error()})
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

// TestLogCollector performs simple checks for common misconfigurations
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
	c.JSON(http.StatusOK, gin.H{"checks": checks})
}

func joinValues(m map[string]string) string {
	b, _ := json.Marshal(m)
	return string(b)
}
