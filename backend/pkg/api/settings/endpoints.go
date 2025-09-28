package settings

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type BackupDashboardSettings struct {
	RPOThresholdSeconds                int     `json:"rpoThresholdSeconds"`
	ThroughputLowerBoundMBps           float64 `json:"throughputLowerBoundMBps"`
	DiagnosisRetentionDays             int     `json:"diagnosisRetentionDays"`
	AutoRebuildLagThresholdSeconds     int     `json:"autoRebuildLagThresholdSeconds"`
	AutoRebuildPreferredNodeLabelKey   string  `json:"autoRebuildPreferredNodeLabelKey"`
	AutoRebuildPreferredNodeLabelValue string  `json:"autoRebuildPreferredNodeLabelValue"`
}

const (
	settingsNamespace = "polardbx-operator-system"
	settingsConfigMap = "polardbx-ui-backend-config"
)

func k8sClientFromContext(c *gin.Context) (client.Client, bool) {
	v, ok := c.Get("k8sClient")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "kubernetes client not initialized"})
		return nil, false
	}
	cli, ok := v.(client.Client)
	if !ok || cli == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid kubernetes client in context"})
		return nil, false
	}
	return cli, true
}

func Get(c *gin.Context) {
	cli, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	cm := corev1.ConfigMap{}
	_ = cli.Get(c.Request.Context(), client.ObjectKey{Namespace: settingsNamespace, Name: settingsConfigMap}, &cm)
	c.JSON(http.StatusOK, cm.Data)
}

func Update(c *gin.Context) {
	cli, ok := k8sClientFromContext(c)
	if !ok {
		return
	}
	var body map[string]any
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload", "details": err.Error()})
		return
	}
	cm := corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: settingsNamespace, Name: settingsConfigMap}
	err := cli.Get(c.Request.Context(), key, &cm)
	if err != nil {
		cm = corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: settingsNamespace, Name: settingsConfigMap}, Data: map[string]string{}}
		if err2 := cli.Create(c.Request.Context(), &cm); err2 != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create settings", "details": err2.Error()})
			return
		}
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	for k, v := range body {
		cm.Data[k] = fmt.Sprintf("%v", v)
	}
	if err := cli.Update(c.Request.Context(), &cm); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update settings", "details": err.Error()})
		return
	}
	c.JSON(http.StatusOK, body)
}

func defaultSettings() BackupDashboardSettings {
	return BackupDashboardSettings{
		RPOThresholdSeconds:                3600,
		ThroughputLowerBoundMBps:           1.0,
		DiagnosisRetentionDays:             7,
		AutoRebuildLagThresholdSeconds:     0,
		AutoRebuildPreferredNodeLabelKey:   "",
		AutoRebuildPreferredNodeLabelValue: "",
	}
}

func parseInt(s string, def int) int {
	if v, err := strconv.Atoi(s); err == nil {
		return v
	}
	return def
}

func parseFloat(s string, def float64) float64 {
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return v
	}
	return def
}

// ReadDashboardSettings: typed 读取，供其他包使用
func ReadDashboardSettings(c *gin.Context, cli client.Client) (BackupDashboardSettings, error) {
	s := defaultSettings()
	var cm corev1.ConfigMap
	if err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: settingsNamespace, Name: settingsConfigMap}, &cm); err == nil {
		if cm.Data != nil {
			s.RPOThresholdSeconds = parseInt(cm.Data["rpoThresholdSeconds"], s.RPOThresholdSeconds)
			s.ThroughputLowerBoundMBps = parseFloat(cm.Data["throughputLowerBoundMBps"], s.ThroughputLowerBoundMBps)
			s.DiagnosisRetentionDays = parseInt(cm.Data["diagnosisRetentionDays"], s.DiagnosisRetentionDays)
			s.AutoRebuildLagThresholdSeconds = parseInt(cm.Data["autoRebuildLagThresholdSeconds"], s.AutoRebuildLagThresholdSeconds)
			if v, ok := cm.Data["autoRebuildPreferredNodeLabelKey"]; ok {
				s.AutoRebuildPreferredNodeLabelKey = v
			}
			if v, ok := cm.Data["autoRebuildPreferredNodeLabelValue"]; ok {
				s.AutoRebuildPreferredNodeLabelValue = v
			}
		}
	}
	return s, nil
}
