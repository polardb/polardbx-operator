package grafana

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Config struct {
	URL           string         `json:"url"`
	Auth          string         `json:"auth"` // sso|basic|token
	ApiKey        string         `json:"apiKey"`
	BasicUser     string         `json:"basicUser"`
	BasicPassword string         `json:"basicPassword"`
	DataSource    map[string]any `json:"dataSource"`
}

const (
	settingsNS      = "polardbx-operator-system"
	cmGrafanaConfig = "polardbx-grafana-config"
	cmDashboards    = "polardbx-grafana-dashboards"
	secGrafana      = "polardbx-grafana-secret"
)

func k8sClientFromContext(c *gin.Context) (client.Client, bool) {
	v, ok := c.Get("k8sClient")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "kubeconfig not provided or invalid"})
		return nil, false
	}
	cli, ok := v.(client.Client)
	if !ok || cli == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid kubernetes client in context"})
		return nil, false
	}
	return cli, true
}

func GetConfig(c *gin.Context) {
	cli, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	cm := corev1.ConfigMap{}
	_ = cli.Get(c.Request.Context(), client.ObjectKey{Namespace: settingsNS, Name: cmGrafanaConfig}, &cm)
	sec := corev1.Secret{}
	_ = cli.Get(c.Request.Context(), client.ObjectKey{Namespace: settingsNS, Name: secGrafana}, &sec)
	resp := Config{URL: cm.Data["url"], Auth: cm.Data["auth"]}
	if v, ok := sec.Data["apiKey"]; ok {
		resp.ApiKey = string(v)
	}
	if v, ok := sec.Data["basicUser"]; ok {
		resp.BasicUser = string(v)
	}
	if v, ok := sec.Data["basicPassword"]; ok {
		resp.BasicPassword = string(v)
	}
	if v, ok := cm.Data["datasource.json"]; ok {
		resp.DataSource = map[string]any{"raw": v}
	}
	c.JSON(http.StatusOK, resp)
}

func PutConfig(c *gin.Context) {
	cli, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	var req Config
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload", "details": err.Error()})
		return
	}
	cm := corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: settingsNS, Name: cmGrafanaConfig}
	err := cli.Get(c.Request.Context(), key, &cm)
	if err != nil {
		cm = corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: settingsNS, Name: cmGrafanaConfig}, Data: map[string]string{}}
		if err2 := cli.Create(c.Request.Context(), &cm); err2 != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create grafana config", "details": err2.Error()})
			return
		}
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data["url"] = req.URL
	cm.Data["auth"] = req.Auth
	if raw, ok := req.DataSource["raw"]; ok {
		cm.Data["datasource.json"] = fmt.Sprintf("%v", raw)
	}
	if err := cli.Update(c.Request.Context(), &cm); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update grafana config", "details": err.Error()})
		return
	}
	sec := corev1.Secret{}
	skey := client.ObjectKey{Namespace: settingsNS, Name: secGrafana}
	if err := cli.Get(c.Request.Context(), skey, &sec); err != nil {
		sec = corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: settingsNS, Name: secGrafana}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{}}
		if err2 := cli.Create(c.Request.Context(), &sec); err2 != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create grafana secret", "details": err2.Error()})
			return
		}
	}
	if sec.Data == nil {
		sec.Data = map[string][]byte{}
	}
	if req.ApiKey != "" {
		sec.Data["apiKey"] = []byte(req.ApiKey)
	}
	if req.BasicUser != "" {
		sec.Data["basicUser"] = []byte(req.BasicUser)
	}
	if req.BasicPassword != "" {
		sec.Data["basicPassword"] = []byte(req.BasicPassword)
	}
	if err := cli.Update(c.Request.Context(), &sec); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update grafana secret", "details": err.Error()})
		return
	}
	c.JSON(http.StatusOK, req)
}

// SyncDashboards stores dashboards JSON into a ConfigMap for provisioning sidecar to pick
// Body: { "dashboards": { "<name>": "<json>" }, "overwrite": true }
func SyncDashboards(c *gin.Context) {
	cli, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	var body struct {
		Dashboards map[string]string `json:"dashboards"`
		Overwrite  bool              `json:"overwrite"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload", "details": err.Error()})
		return
	}
	cm := corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: settingsNS, Name: cmDashboards}
	err := cli.Get(c.Request.Context(), key, &cm)
	if err != nil {
		cm = corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: settingsNS, Name: cmDashboards}, Data: map[string]string{}}
		if err2 := cli.Create(c.Request.Context(), &cm); err2 != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create dashboards", "details": err2.Error()})
			return
		}
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	for k, v := range body.Dashboards {
		if old, exists := cm.Data[k]; exists {
			if !body.Overwrite && old != "" {
				continue
			}
			if old != v {
				// version previous content
				next := nextVersion(cm.Data, k)
				cm.Data[fmt.Sprintf("%s@v%04d", k, next)] = old
			}
		}
		cm.Data[k] = v
	}
	if err := cli.Update(c.Request.Context(), &cm); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update dashboards", "details": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"message": "dashboards synced", "count": len(body.Dashboards)})
}

func nextVersion(data map[string]string, name string) int {
	maxv := 0
	prefix := name + "@v"
	for k := range data {
		if strings.HasPrefix(k, prefix) {
			if n, err := strconv.Atoi(strings.TrimPrefix(k, prefix)); err == nil && n > maxv {
				maxv = n
			}
		}
	}
	return maxv + 1
}

// ListDashboards returns dashboard names and version stats
func ListDashboards(c *gin.Context) {
	cli, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	cm := corev1.ConfigMap{}
	_ = cli.Get(c.Request.Context(), client.ObjectKey{Namespace: settingsNS, Name: cmDashboards}, &cm)
	stats := map[string]int{}
	for k := range cm.Data {
		if name, _, ok := splitVersionKey(k); ok {
			stats[name]++
		}
	}
	items := []gin.H{}
	seen := map[string]struct{}{}
	for k := range cm.Data {
		if name, _, ok := splitVersionKey(k); ok {
			if _, done := seen[name]; done {
				continue
			}
			seen[name] = struct{}{}
			items = append(items, gin.H{"name": name, "versions": stats[name]})
		} else {
			// treat as current
			if _, done := seen[k]; done {
				continue
			}
			seen[k] = struct{}{}
			items = append(items, gin.H{"name": k, "versions": stats[k]})
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i]["name"].(string) < items[j]["name"].(string) })
	c.JSON(http.StatusOK, gin.H{"items": items})
}

// ListDashboardVersions returns versions for a given dashboard name
func ListDashboardVersions(c *gin.Context) {
	cli, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	name := c.Param("name")
	cm := corev1.ConfigMap{}
	_ = cli.Get(c.Request.Context(), client.ObjectKey{Namespace: settingsNS, Name: cmDashboards}, &cm)
	vers := []int{}
	for k := range cm.Data {
		if n, v, ok := splitVersionKeyWithName(k, name); ok {
			vers = append(vers, v)
			_ = n
		}
	}
	sort.Sort(sort.Reverse(sort.IntSlice(vers)))
	c.JSON(http.StatusOK, gin.H{"name": name, "versions": vers})
}

// RollbackDashboard sets the current dashboard content to the specified version
func RollbackDashboard(c *gin.Context) {
	cli, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	name := c.Param("name")
	var body struct {
		Version int `json:"version"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || body.Version <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}
	cm := corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: settingsNS, Name: cmDashboards}
	if err := cli.Get(c.Request.Context(), key, &cm); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "dashboards not initialized"})
		return
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	targetKey := fmt.Sprintf("%s@v%04d", name, body.Version)
	target, ok2 := cm.Data[targetKey]
	if !ok2 {
		c.JSON(http.StatusNotFound, gin.H{"error": "version not found"})
		return
	}
	// backup current as new version before rollback
	if cur, ok3 := cm.Data[name]; ok3 {
		next := nextVersion(cm.Data, name)
		cm.Data[fmt.Sprintf("%s@v%04d", name, next)] = cur
	}
	cm.Data[name] = target
	if err := cli.Update(c.Request.Context(), &cm); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to rollback", "details": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "rolled back", "name": name, "version": body.Version})
}

func splitVersionKey(key string) (string, int, bool) {
	idx := strings.LastIndex(key, "@v")
	if idx <= 0 {
		return "", 0, false
	}
	name := key[:idx]
	vn := key[idx+2:]
	n, err := strconv.Atoi(vn)
	if err != nil {
		return "", 0, false
	}
	return name, n, true
}

func splitVersionKeyWithName(key, name string) (string, int, bool) {
	n, v, ok := splitVersionKey(key)
	if !ok || n != name {
		return "", 0, false
	}
	return n, v, true
}
