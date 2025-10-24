package services

import (
	"encoding/json"
	"net/http"
	"strings"

	"polardbx-ui-backend/pkg/api/util"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GetAlertsSummary 提供和旧 cluster 包相同的汇总逻辑，避免保留旧包。
func GetAlertsSummary(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := c.Param("namespace")
	name := c.Param("name")
	amURL := c.Query("alertmanager")
	summary := map[string]any{
		"namespace": ns,
		"name":      name,
		"critical":  0,
		"warning":   0,
		"info":      0,
		"total":     0,
		"source":    "none",
	}

	if amURL != "" {
		type amAlert struct {
			Labels map[string]string `json:"labels"`
			Status map[string]any    `json:"status"`
		}
		var alerts []amAlert
		if resp, err := http.Get(amURL + "/api/v2/alerts"); err == nil && resp.StatusCode == 200 {
			defer resp.Body.Close()
			if err := json.NewDecoder(resp.Body).Decode(&alerts); err == nil {
				for _, a := range alerts {
					if a.Labels["namespace"] != ns || a.Labels["cluster"] != name {
						continue
					}
					switch strings.ToLower(a.Labels["severity"]) {
					case "critical":
						summary["critical"] = summary["critical"].(int) + 1
					case "warning":
						summary["warning"] = summary["warning"].(int) + 1
					default:
						summary["info"] = summary["info"].(int) + 1
					}
					summary["total"] = summary["total"].(int) + 1
				}
				summary["source"] = "alertmanager"
				c.JSON(http.StatusOK, summary)
				return
			}
		}
	}

	// fallback: 根据 events 估算 warning
	var evList corev1.EventList
	if err := cli.List(c.Request.Context(), &evList, client.InNamespace(ns)); err == nil {
		warn := 0
		for _, ev := range evList.Items {
			if strings.Contains(ev.InvolvedObject.Name, name) && strings.EqualFold(ev.Type, "Warning") {
				warn++
			}
		}
		summary["warning"], summary["total"], summary["source"] = warn, warn, "events"
	}
	c.JSON(http.StatusOK, summary)
}
