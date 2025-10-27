package alerts

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"

	"polardbx-ui-backend/pkg/api/util"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	settingsNS      = "polardbx-operator-system"
	cmAlertProfiles = "polardbx-alert-profiles"
	cmAlertRoutes   = "polardbx-alert-routes"
)

// Profiles stored as key -> YAML content in ConfigMap
func ListProfiles(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	cm := corev1.ConfigMap{}
	_ = cli.Get(c.Request.Context(), client.ObjectKey{Namespace: settingsNS, Name: cmAlertProfiles}, &cm)
	items := []gin.H{}
	for k := range cm.Data {
		items = append(items, gin.H{"name": k})
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}
func CreateProfile(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	var body struct {
		Name    string `json:"name"`
		Content string `json:"content"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || body.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}
	cm := corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: settingsNS, Name: cmAlertProfiles}
	if err := cli.Get(c.Request.Context(), key, &cm); err != nil {
		cm = corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: settingsNS, Name: cmAlertProfiles}, Data: map[string]string{}}
		if err2 := cli.Create(c.Request.Context(), &cm); err2 != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "create profiles cm", "details": err2.Error()})
			return
		}
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	if _, exists := cm.Data[body.Name]; exists {
		c.JSON(http.StatusConflict, gin.H{"error": "profile exists"})
		return
	}
	cm.Data[body.Name] = body.Content
	if err := cli.Update(c.Request.Context(), &cm); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "update profiles cm", "details": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"name": body.Name})
}
func GetProfile(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	name := c.Param("name")
	cm := corev1.ConfigMap{}
	if err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: settingsNS, Name: cmAlertProfiles}, &cm); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "profiles not found"})
		return
	}
	content, ok2 := cm.Data[name]
	if !ok2 {
		c.JSON(http.StatusNotFound, gin.H{"error": "profile not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"name": name, "content": content})
}
func UpdateProfile(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	name := c.Param("name")
	var body struct {
		Content string `json:"content"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}
	cm := corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: settingsNS, Name: cmAlertProfiles}
	if err := cli.Get(c.Request.Context(), key, &cm); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "profiles not found"})
		return
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[name] = body.Content
	if err := cli.Update(c.Request.Context(), &cm); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "update profiles cm", "details": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"name": name})
}
func DeleteProfile(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	name := c.Param("name")
	cm := corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: settingsNS, Name: cmAlertProfiles}
	if err := cli.Get(c.Request.Context(), key, &cm); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "profiles not found"})
		return
	}
	if cm.Data == nil || cm.Data[name] == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "profile not found"})
		return
	}
	delete(cm.Data, name)
	if err := cli.Update(c.Request.Context(), &cm); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "update profiles cm", "details": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": name})
}

// DryRunProfile validates Alertmanager YAML via promtool
func DryRunProfile(c *gin.Context) {
	_, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	var body struct {
		Content string `json:"content"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}
	// write to temp file
	f, err := os.CreateTemp("", "am-profile-*.yaml")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "tempfile"})
		return
	}
	defer os.Remove(f.Name())
	_, _ = f.Write([]byte(body.Content))
	_ = f.Close()
	cmd := exec.Command("promtool", "check", "rules", f.Name())
	out, err := cmd.CombinedOutput()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"valid": false, "details": string(out)})
		return
	}
	c.JSON(http.StatusOK, gin.H{"valid": true})
}

func GetRoutes(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	cm := corev1.ConfigMap{}
	_ = cli.Get(c.Request.Context(), client.ObjectKey{Namespace: settingsNS, Name: cmAlertRoutes}, &cm)
	c.JSON(http.StatusOK, gin.H{"content": cm.Data["config.yaml"]})
}
func PutRoutes(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	var body struct {
		Content string `json:"content"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}
	cm := corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: settingsNS, Name: cmAlertRoutes}
	if err := cli.Get(c.Request.Context(), key, &cm); err != nil {
		cm = corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: settingsNS, Name: cmAlertRoutes}, Data: map[string]string{"config.yaml": body.Content}}
		_ = cli.Create(c.Request.Context(), &cm)
	} else {
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data["config.yaml"] = body.Content
		_ = cli.Update(c.Request.Context(), &cm)
	}
	c.JSON(http.StatusOK, gin.H{"message": "routes updated"})
}

// Alertmanager API passthroughs (silences, test)
func ListSilences(c *gin.Context) {
	_, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	base := strings.TrimRight(c.DefaultQuery("alertmanager", ""), "/")
	if base == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "alertmanager required"})
		return
	}
	resp, err := http.Get(base + "/api/v2/silences")
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	defer resp.Body.Close()
	var out any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	c.JSON(resp.StatusCode, out)
}
func CreateSilence(c *gin.Context) {
	_, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	base := strings.TrimRight(c.DefaultQuery("alertmanager", ""), "/")
	if base == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "alertmanager required"})
		return
	}
	var body map[string]any
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}
	b, _ := json.Marshal(body)
	resp, err := http.Post(base+"/api/v2/silences", "application/json", bytes.NewReader(b))
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	defer resp.Body.Close()
	var out any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	c.JSON(resp.StatusCode, out)
}
func DeleteSilence(c *gin.Context) {
	_, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	base := strings.TrimRight(c.DefaultQuery("alertmanager", ""), "/")
	if base == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "alertmanager required"})
		return
	}
	id := c.Param("id")
	req, _ := http.NewRequest(http.MethodDelete, base+"/api/v2/silence/"+url.PathEscape(id), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	defer resp.Body.Close()
	var out any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	c.JSON(resp.StatusCode, out)
}
func TestAlert(c *gin.Context) {
	_, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	base := strings.TrimRight(c.DefaultQuery("alertmanager", ""), "/")
	if base == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "alertmanager required"})
		return
	}
	labels := c.DefaultQuery("labels", "severity=warning,service=test")
	var pairs []string
	for _, p := range strings.Split(labels, ",") {
		if strings.Contains(p, "=") {
			pairs = append(pairs, p)
		}
	}
	alert := []map[string]any{{"labels": map[string]string{}}}
	for _, kv := range pairs {
		parts := strings.SplitN(kv, "=", 2)
		alert[0]["labels"].(map[string]string)[parts[0]] = parts[1]
	}
	b, _ := json.Marshal(alert)
	resp, err := http.Post(base+"/api/v1/alerts", "application/json", bytes.NewReader(b))
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	defer resp.Body.Close()
	var out any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	c.JSON(resp.StatusCode, out)
}
