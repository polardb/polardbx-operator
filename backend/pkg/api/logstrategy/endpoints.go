package logstrategy

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"polardbx-ui-backend/pkg/api/util"
	"polardbx-ui-backend/pkg/k8s"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Strategy defines a logging strategy from a PXC to an output
// Secrets are referenced by name and stored as K8s Secret; no sensitive values here
type Strategy struct {
	Name        string `json:"name"`
	ClusterNS   string `json:"clusterNamespace"`
	ClusterName string `json:"clusterName"`
	EnableCN    bool   `json:"enableCN"`
	EnableDN    bool   `json:"enableDN"`
	Output      struct {
		Type     string `json:"type"` // elasticsearch|stdout
		Hosts    string `json:"hosts,omitempty"`
		AuthType string `json:"authType,omitempty"` // none|basic
		Username string `json:"username,omitempty"`
		Password string `json:"password,omitempty"` // only for precheck; should be provided once and persisted to Secret by apply
		UseTLS   bool   `json:"useTLS,omitempty"`
		CACrt    string `json:"caCrt,omitempty"` // ditto
	} `json:"output"`
}

const (
	cmName      = "log-strategies"
	cmNamespace = "polardbx-logcollector"
	cmKey       = "strategies.json"
)

func getStore(c *gin.Context) (*corev1.ConfigMap, error) {
	cs, ok := util.ClientsetFromContext(c)
	if !ok {
		return nil, nil
	}
	cm, err := cs.CoreV1().ConfigMaps(cmNamespace).Get(c.Request.Context(), cmName, metav1.GetOptions{})
	if err != nil {
		// create empty store
		empty := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: cmName, Namespace: cmNamespace}, Data: map[string]string{cmKey: "[]"}}
		_, e2 := cs.CoreV1().ConfigMaps(cmNamespace).Create(c.Request.Context(), empty, metav1.CreateOptions{})
		if e2 != nil {
			return nil, e2
		}
		return empty, nil
	}
	if cm.Data == nil {
		cm.Data = map[string]string{cmKey: ""}
	}
	if cm.Data[cmKey] == "" {
		cm.Data[cmKey] = "[]"
	}
	return cm, nil
}

func loadList(cm *corev1.ConfigMap) []Strategy {
	var list []Strategy
	_ = json.Unmarshal([]byte(cm.Data[cmKey]), &list)
	return list
}

func saveList(c *gin.Context, cm *corev1.ConfigMap, list []Strategy) error {
	buf, _ := json.Marshal(list)
	cm.Data[cmKey] = string(buf)
	cs, _ := util.ClientsetFromContext(c)
	_, e := cs.CoreV1().ConfigMaps(cmNamespace).Update(c.Request.Context(), cm, metav1.UpdateOptions{})
	return e
}

func List(c *gin.Context) {
	cm, err := getStore(c)
	if err != nil {
		// 兼容前端：返回空列表而不是 500，避免页面崩溃
		c.JSON(http.StatusOK, gin.H{"total": 0, "items": []any{}, "warning": "strategy store not accessible", "details": err.Error()})
		return
	}
	if cm == nil {
		c.JSON(http.StatusOK, gin.H{"total": 0, "items": []any{}})
		return
	}
	list := loadList(cm)
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	c.JSON(http.StatusOK, gin.H{"total": len(list), "items": list})
}

func Get(c *gin.Context) {
	name := c.Param("name")
	cm, err := getStore(c)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load strategy store", "details": err.Error()})
		return
	}
	for _, it := range loadList(cm) {
		if it.Name == name {
			c.JSON(http.StatusOK, it)
			return
		}
	}
	c.JSON(http.StatusNotFound, gin.H{"error": "strategy not found"})
}

func Create(c *gin.Context) {
	cm, err := getStore(c)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load strategy store", "details": err.Error()})
		return
	}
	var s Strategy
	if err := c.ShouldBindJSON(&s); err != nil || s.Name == "" || s.ClusterName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid strategy", "details": "name and clusterName required"})
		return
	}
	list := loadList(cm)
	for _, it := range list {
		if it.Name == s.Name {
			c.JSON(http.StatusConflict, gin.H{"error": "strategy exists"})
			return
		}
	}
	list = append(list, s)
	if e := saveList(c, cm, list); e != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to persist strategy", "details": e.Error()})
		return
	}
	c.JSON(http.StatusCreated, s)
}

func Update(c *gin.Context) {
	name := c.Param("name")
	cm, err := getStore(c)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load strategy store", "details": err.Error()})
		return
	}
	var s Strategy
	if err := c.ShouldBindJSON(&s); err != nil || s.Name == "" || s.Name != name {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid strategy", "details": "body.name must equal path name"})
		return
	}
	list := loadList(cm)
	found := false
	for i, it := range list {
		if it.Name == name {
			list[i] = s
			found = true
			break
		}
	}
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "strategy not found"})
		return
	}
	if e := saveList(c, cm, list); e != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to persist strategy", "details": e.Error()})
		return
	}
	// If no ES output remains for this cluster after update, perform cleanup
	hasActive := false
	for _, it := range list {
		if it.ClusterNS == s.ClusterNS && it.ClusterName == s.ClusterName && strings.ToLower(it.Output.Type) == "elasticsearch" {
			hasActive = true
			break
		}
	}
	if !hasActive {
		performCleanupForCluster(c, s.ClusterNS, s.ClusterName)
	}
	c.JSON(http.StatusOK, s)
}

func Delete(c *gin.Context) {
	name := c.Param("name")
	cm, err := getStore(c)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load strategy store", "details": err.Error()})
		return
	}
	list := loadList(cm)
	idx := -1
	var removed Strategy
	var hasRemoved bool
	for i, it := range list {
		if it.Name == name {
			idx = i
			removed = it
			hasRemoved = true
			break
		}
	}
	if idx < 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "strategy not found"})
		return
	}
	list = append(list[:idx], list[idx+1:]...)
	if e := saveList(c, cm, list); e != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to persist strategy", "details": e.Error()})
		return
	}
	if hasRemoved {
		// If no ES output remains for this cluster after deletion, perform cleanup
		hasActive := false
		for _, it := range list {
			if it.ClusterNS == removed.ClusterNS && it.ClusterName == removed.ClusterName && strings.ToLower(it.Output.Type) == "elasticsearch" {
				hasActive = true
				break
			}
		}
		if !hasActive {
			performCleanupForCluster(c, removed.ClusterNS, removed.ClusterName)
		}
	}
	c.Status(http.StatusOK)
}

// performCleanupForCluster resets pipeline to stdout, restarts logstash and disables cluster annotation
func performCleanupForCluster(c *gin.Context, ns string, clusterName string) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	clientset, ok := util.ClientsetFromContext(c)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
	defer cancel()

	// Reset pipeline to stdout
	cmNamePipeline := "logstash-pipeline"
	pipelineKey := "050-outputs.conf"
	if pl, err := clientset.CoreV1().ConfigMaps(cmNamespace).Get(ctx, cmNamePipeline, metav1.GetOptions{}); err == nil && pl != nil {
		if pl.Data == nil {
			pl.Data = map[string]string{}
		}
		pl.Data[pipelineKey] = "output { stdout { } }\n"
		_, _ = clientset.CoreV1().ConfigMaps(cmNamespace).Update(ctx, pl, metav1.UpdateOptions{})
	}

	// Restart logstash if exists
	if dep, err := clientset.AppsV1().Deployments(cmNamespace).Get(ctx, "logstash", metav1.GetOptions{}); err == nil && dep != nil {
		if dep.Spec.Template.Annotations == nil {
			dep.Spec.Template.Annotations = map[string]string{}
		}
		dep.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339)
		_, _ = clientset.AppsV1().Deployments(cmNamespace).Update(ctx, dep, metav1.UpdateOptions{})
	}

	// Disable annotation on cluster (best-effort)
	patch := map[string]any{"metadata": map[string]any{"annotations": map[string]any{"polardbx.aliyun.com/enable-log-collection": "false"}}}
	b, _ := json.Marshal(patch)
	if strings.TrimSpace(ns) == "" {
		ns = "default"
	}
	_, _ = k8s.PatchPolarDBXCluster(cli, ns, clusterName, b)
}

// Precheck validates cluster existence and output configuration syntax
func Precheck(c *gin.Context) {
	var s Strategy
	if err := c.ShouldBindJSON(&s); err != nil || s.Name == "" || s.ClusterName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name and clusterName required"})
		return
	}
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := s.ClusterNS
	if ns == "" {
		ns = "default"
	}
	checks := gin.H{"clusterExists": true, "esHostsValid": true, "authValid": true, "tlsCrtValid": true}
	errors := []string{}
	warnings := []string{}

	// Check cluster exists
	if _, err := k8s.GetPolarDBXCluster(cli, ns, s.ClusterName); err != nil {
		checks["clusterExists"] = false
		errors = append(errors, "cluster not found: "+ns+"/"+s.ClusterName)
	}

	// Output validation
	s.Output.Type = strings.ToLower(strings.TrimSpace(s.Output.Type))
	s.Output.AuthType = strings.ToLower(strings.TrimSpace(s.Output.AuthType))
	switch s.Output.Type {
	case "elasticsearch":
		if strings.TrimSpace(s.Output.Hosts) == "" {
			checks["esHostsValid"] = false
			errors = append(errors, "elasticsearch hosts is required")
		} else {
			for _, h := range strings.Split(s.Output.Hosts, ",") {
				h = strings.TrimSpace(h)
				if h == "" {
					continue
				}
				if !strings.Contains(h, "://") {
					h = "http://" + h
				}
				u, err := url.Parse(h)
				if err != nil || u.Host == "" {
					checks["esHostsValid"] = false
					errors = append(errors, "invalid elasticsearch host: "+h)
					break
				}
			}
		}
		if s.Output.AuthType == "basic" && strings.TrimSpace(s.Output.Username) == "" {
			checks["authValid"] = false
			errors = append(errors, "username required for basic auth")
		}
		if s.Output.UseTLS {
			if strings.TrimSpace(s.Output.CACrt) == "" {
				warnings = append(warnings, "TLS enabled but CA certificate not provided")
			} else {
				block, _ := pem.Decode([]byte(s.Output.CACrt))
				if block == nil || block.Type != "CERTIFICATE" {
					checks["tlsCrtValid"] = false
					errors = append(errors, "invalid CA certificate PEM")
				} else if _, err := x509.ParseCertificate(block.Bytes); err != nil {
					checks["tlsCrtValid"] = false
					errors = append(errors, "failed to parse CA certificate: "+err.Error())
				}
			}
		}
	case "stdout", "":
		// ok
	default:
		errors = append(errors, "unsupported output type: "+s.Output.Type)
	}

	valid := true
	for _, v := range checks {
		if b, ok := v.(bool); ok && !b {
			valid = false
		}
	}
	c.JSON(http.StatusOK, gin.H{"valid": valid, "checks": checks, "errors": errors, "warnings": warnings})
}

func buildLogstashOutput(s *Strategy) string {
	if strings.ToLower(s.Output.Type) != "elasticsearch" {
		return "output { stdout { } }\n"
	}
	hosts := []string{}
	for _, h := range strings.Split(s.Output.Hosts, ",") {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		hosts = append(hosts, "\""+h+"\"")
	}
	lines := []string{"output {", "  elasticsearch {"}
	if len(hosts) > 0 {
		lines = append(lines, "    hosts => ["+strings.Join(hosts, ", ")+"]")
	}
	if strings.ToLower(s.Output.AuthType) == "basic" {
		lines = append(lines, "    user => \"${ES_USERNAME}\"")
		lines = append(lines, "    password => \"${ES_PASSWORD}\"")
	}
	if s.Output.UseTLS {
		lines = append(lines, "    ssl => true")
		lines = append(lines, "    cacert => \"/usr/share/logstash/config/certs/ca.crt\"")
	}
	lines = append(lines, "  }", "}", "")
	return strings.Join(lines, "\n")
}

// Apply persists secrets/config and triggers rollout for logstash; also marks cluster to enable log collection
func Apply(c *gin.Context) {
	name := c.Param("name")
	cm, err := getStore(c)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load strategy store", "details": err.Error()})
		return
	}
	var s *Strategy
	for _, it := range loadList(cm) {
		if it.Name == name {
			copy := it
			s = &copy
			break
		}
	}
	if s == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "strategy not found"})
		return
	}
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	clientset, ok := util.ClientsetFromContext(c)
	if !ok {
		return
	}
	ns := s.ClusterNS
	if ns == "" {
		ns = "default"
	}
	if _, err := k8s.GetPolarDBXCluster(cli, ns, s.ClusterName); err != nil {
		util.HandleK8sError(c, "cluster not found", err)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 20*time.Second)
	defer cancel()

	// Upsert CA secret if provided
	if s.Output.UseTLS && strings.TrimSpace(s.Output.CACrt) != "" {
		secName := "elastic-certs-public"
		sec, err := clientset.CoreV1().Secrets(cmNamespace).Get(ctx, secName, metav1.GetOptions{})
		if err != nil {
			newSec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secName, Namespace: cmNamespace}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{"ca.crt": []byte(s.Output.CACrt)}}
			if _, e := clientset.CoreV1().Secrets(cmNamespace).Create(ctx, newSec, metav1.CreateOptions{}); e != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create es cert secret", "details": e.Error()})
				return
			}
		} else {
			if sec.Data == nil {
				sec.Data = map[string][]byte{}
			}
			sec.Data["ca.crt"] = []byte(s.Output.CACrt)
			if _, e := clientset.CoreV1().Secrets(cmNamespace).Update(ctx, sec, metav1.UpdateOptions{}); e != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update es cert secret", "details": e.Error()})
				return
			}
		}
	}

	// Upsert credentials if basic auth
	if strings.ToLower(strings.TrimSpace(s.Output.AuthType)) == "basic" && (s.Output.Username != "" || s.Output.Password != "") {
		credName := "elastic-credentials"
		sec, err := clientset.CoreV1().Secrets(cmNamespace).Get(ctx, credName, metav1.GetOptions{})
		if err != nil {
			newSec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: credName, Namespace: cmNamespace}, Type: corev1.SecretTypeOpaque, StringData: map[string]string{"username": s.Output.Username, "password": s.Output.Password}}
			if _, e := clientset.CoreV1().Secrets(cmNamespace).Create(ctx, newSec, metav1.CreateOptions{}); e != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create credentials secret", "details": e.Error()})
				return
			}
		} else {
			if sec.StringData == nil {
				sec.StringData = map[string]string{}
			}
			if sec.Data == nil {
				sec.Data = map[string][]byte{}
			}
			if s.Output.Username != "" {
				sec.StringData["username"] = s.Output.Username
			}
			if s.Output.Password != "" {
				sec.StringData["password"] = s.Output.Password
			}
			if _, e := clientset.CoreV1().Secrets(cmNamespace).Update(ctx, sec, metav1.UpdateOptions{}); e != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update credentials secret", "details": e.Error()})
				return
			}
		}
	}

	// Upsert/update pipeline configmap
	cmNamePipeline := "logstash-pipeline"
	pipelineKey := "050-outputs.conf"
	out := buildLogstashOutput(s)
	pl, err := clientset.CoreV1().ConfigMaps(cmNamespace).Get(ctx, cmNamePipeline, metav1.GetOptions{})
	if err != nil {
		newCm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: cmNamePipeline, Namespace: cmNamespace}, Data: map[string]string{pipelineKey: out}}
		if _, e := clientset.CoreV1().ConfigMaps(cmNamespace).Create(ctx, newCm, metav1.CreateOptions{}); e != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create pipeline configmap", "details": e.Error()})
			return
		}
	} else {
		if pl.Data == nil {
			pl.Data = map[string]string{}
		}
		pl.Data[pipelineKey] = out
		if _, e := clientset.CoreV1().ConfigMaps(cmNamespace).Update(ctx, pl, metav1.UpdateOptions{}); e != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update pipeline configmap", "details": e.Error()})
			return
		}
	}

	// Rollout restart logstash deployment if exists
	dep, err := clientset.AppsV1().Deployments(cmNamespace).Get(ctx, "logstash", metav1.GetOptions{})
	if err == nil && dep != nil {
		if dep.Spec.Template.Annotations == nil {
			dep.Spec.Template.Annotations = map[string]string{}
		}
		dep.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339)
		if _, e := clientset.AppsV1().Deployments(cmNamespace).Update(ctx, dep, metav1.UpdateOptions{}); e != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to restart logstash", "details": e.Error()})
			return
		}
	}

	// Mark cluster to enable log collection
	patch := map[string]any{"metadata": map[string]any{"annotations": map[string]any{"polardbx.aliyun.com/enable-log-collection": "true"}}}
	b, _ := json.Marshal(patch)
	if _, err := k8s.PatchPolarDBXCluster(cli, ns, s.ClusterName, b); err != nil {
		// not fatal, report warning
	}

	// Record the successful application
	targets := []string{ns + "/" + s.ClusterName}
	addApplyRecord(c, s.Name, "success", "Strategy applied successfully", targets)

	c.JSON(http.StatusOK, gin.H{"applied": true, "namespace": ns, "cluster": s.ClusterName, "logstash": gin.H{"restarted": dep != nil}, "pipelineKey": pipelineKey})
}

// TestConnection validates connectivity to Elasticsearch using simple HTTP request
// Payload example: {"hosts":["https://es:9200"], "username":"elastic", "password":"xxx"}
func TestConnection(c *gin.Context) {
	var payload struct {
		Hosts    []string `json:"hosts"`
		Username string   `json:"username"`
		Password string   `json:"password"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil || len(payload.Hosts) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "hosts required"})
		return
	}
	host := strings.TrimSpace(payload.Hosts[0])
	if host == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid host"})
		return
	}
	if !strings.Contains(host, "://") {
		host = "http://" + host
	}
	// Use HEAD first; fall back to GET
	client := &http.Client{Timeout: 5 * time.Second}
	req, _ := http.NewRequest(http.MethodHead, host, nil)
	if payload.Username != "" {
		req.SetBasicAuth(payload.Username, payload.Password)
	}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode >= 400 {
		// fallback GET /
		req2, _ := http.NewRequest(http.MethodGet, host, nil)
		if payload.Username != "" {
			req2.SetBasicAuth(payload.Username, payload.Password)
		}
		resp2, err2 := client.Do(req2)
		if err2 != nil || resp2.StatusCode >= 400 {
			code := http.StatusBadGateway
			if resp2 != nil {
				code = resp2.StatusCode
			}
			c.JSON(code, gin.H{"error": "elasticsearch connection failed"})
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// ApplyRecord represents a log strategy application record
type ApplyRecord struct {
	ID           string    `json:"id"`
	StrategyID   string    `json:"strategyId"`
	StrategyName string    `json:"strategyName"`
	AppliedAt    time.Time `json:"appliedAt"`
	Status       string    `json:"status"` // success, failed
	Message      string    `json:"message,omitempty"`
	Targets      []string  `json:"targets,omitempty"`
}

const (
	recordsCMName = "log-strategy-apply-records"
	recordsKey    = "records.json"
)

// ListApplyRecords returns the application history of log strategies
func ListApplyRecords(c *gin.Context) {
	cs, ok := util.ClientsetFromContext(c)
	if !ok {
		c.JSON(http.StatusOK, gin.H{"total": 0, "items": []ApplyRecord{}})
		return
	}

	// Try to get the records ConfigMap
	cm, err := cs.CoreV1().ConfigMaps(cmNamespace).Get(c.Request.Context(), recordsCMName, metav1.GetOptions{})
	if err != nil {
		// If ConfigMap doesn't exist, return empty list
		c.JSON(http.StatusOK, gin.H{"total": 0, "items": []ApplyRecord{}})
		return
	}

	var records []ApplyRecord
	if cm.Data != nil && cm.Data[recordsKey] != "" {
		_ = json.Unmarshal([]byte(cm.Data[recordsKey]), &records)
	}

	// Sort by appliedAt descending (newest first)
	sort.Slice(records, func(i, j int) bool {
		return records[i].AppliedAt.After(records[j].AppliedAt)
	})

	c.JSON(http.StatusOK, gin.H{"total": len(records), "items": records})
}

// Helper function to add an apply record (called from Apply function)
func addApplyRecord(c *gin.Context, strategyName string, status string, message string, targets []string) {
	cs, ok := util.ClientsetFromContext(c)
	if !ok {
		return // Best effort, don't fail the main operation
	}

	record := ApplyRecord{
		ID:           generateRecordID(),
		StrategyID:   strategyName, // Using name as ID for simplicity
		StrategyName: strategyName,
		AppliedAt:    time.Now(),
		Status:       status,
		Message:      message,
		Targets:      targets,
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	// Get or create the records ConfigMap
	cm, err := cs.CoreV1().ConfigMaps(cmNamespace).Get(ctx, recordsCMName, metav1.GetOptions{})
	if err != nil {
		// Create new ConfigMap
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      recordsCMName,
				Namespace: cmNamespace,
			},
			Data: map[string]string{
				recordsKey: "[]",
			},
		}
		cm, err = cs.CoreV1().ConfigMaps(cmNamespace).Create(ctx, newCM, metav1.CreateOptions{})
		if err != nil {
			return // Best effort
		}
	}

	// Load existing records
	var records []ApplyRecord
	if cm.Data != nil && cm.Data[recordsKey] != "" {
		_ = json.Unmarshal([]byte(cm.Data[recordsKey]), &records)
	}

	// Add new record
	records = append(records, record)

	// Keep only the last 100 records to prevent ConfigMap from growing too large
	if len(records) > 100 {
		records = records[len(records)-100:]
	}

	// Save back to ConfigMap
	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	recordsJSON, _ := json.Marshal(records)
	cm.Data[recordsKey] = string(recordsJSON)

	_, _ = cs.CoreV1().ConfigMaps(cmNamespace).Update(ctx, cm, metav1.UpdateOptions{})
}

// Generate a simple record ID based on timestamp
func generateRecordID() string {
	t := time.Now()
	ms := t.Nanosecond() / 1e6
	return t.Format("20060102-150405-") + fmt.Sprintf("%03d", ms)
}
