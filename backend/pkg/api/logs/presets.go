package logs

import (
	"encoding/json"
	"net/http"
	"strings"

	"polardbx-ui-backend/pkg/api/util"

	"github.com/gin-gonic/gin"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LogPreset defines default aggregations for an index pattern
type LogPreset struct {
	IndexPattern string   `json:"indexPattern"`
	Facets       []string `json:"facets"`
	Histogram    struct {
		Field     string   `json:"field"`
		Intervals []string `json:"intervals"` // recommendation list: e.g. ["1m","5m","1h","1d"]
	} `json:"histogram"`
}

const presetsKey = "presets.json"

func defaultPresets() map[string]LogPreset {
	one := func(pattern string, facets []string) LogPreset {
		p := LogPreset{IndexPattern: pattern, Facets: facets}
		p.Histogram.Field = "@timestamp"
		p.Histogram.Intervals = []string{"1m", "5m", "1h", "1d"}
		return p
	}
	m := map[string]LogPreset{}
	m["cn_sql_log-*"] = one("cn_sql_log-*", []string{
		"fields.instance_id.keyword", "fields.pod_name.keyword", "fields.node_name.keyword",
		"message.schema.keyword", "message.user.keyword", "message.workload_type.keyword", "message.template_id.keyword",
	})
	m["cn_slow_log-*"] = one("cn_slow_log-*", []string{
		"fields.instance_id.keyword", "fields.pod_name.keyword", "fields.node_name.keyword",
		"message.schema.keyword", "message.user.keyword", "message.host.keyword",
	})
	m["cn_tddl_log-*"] = one("cn_tddl_log-*", []string{
		"fields.instance_id.keyword", "fields.pod_name.keyword", "fields.node_name.keyword",
		"loglevel.keyword", "logger.keyword",
	})
	m["dn_audit_log-*"] = one("dn_audit_log-*", []string{
		"fields.instance_id.keyword", "fields.dn_instance_id.keyword", "fields.pod_name.keyword",
		"host_or_ip.keyword", "user.keyword", "error_code",
	})
	m["dn_slow_log-*"] = one("dn_slow_log-*", []string{
		"fields.instance_id.keyword", "fields.dn_instance_id.keyword", "fields.pod_name.keyword",
		"db.keyword", "user_host.keyword",
	})
	m["dn_error_log-*"] = one("dn_error_log-*", []string{
		"fields.instance_id.keyword", "fields.dn_instance_id.keyword", "fields.pod_name.keyword",
		"label.keyword", "error_code.keyword", "subsystem.keyword",
	})
	return m
}

func loadPresets(c *gin.Context) map[string]LogPreset {
	// start from defaults and overlay user-defined presets if present
	presets := defaultPresets()
	cs, ok := util.ClientsetFromContext(c)
	if !ok {
		return presets
	}
	cm, err := cs.CoreV1().ConfigMaps(util.LogsSecurityConfigMapNamespace).Get(c.Request.Context(), util.LogsSecurityConfigMapName, metav1.GetOptions{})
	if err != nil || cm.Data == nil {
		return presets
	}
	raw := strings.TrimSpace(cm.Data[presetsKey])
	if raw == "" {
		return presets
	}
	var override map[string]LogPreset
	if err := json.Unmarshal([]byte(raw), &override); err != nil {
		return presets
	}
	for k, v := range override {
		// ensure histogram defaults if not set
		if v.Histogram.Field == "" {
			v.Histogram.Field = "@timestamp"
		}
		if len(v.Histogram.Intervals) == 0 {
			v.Histogram.Intervals = []string{"1m", "5m", "1h", "1d"}
		}
		presets[k] = v
	}
	return presets
}

// Presets returns all presets
func Presets(c *gin.Context) {
	m := loadPresets(c)
	list := make([]LogPreset, 0, len(m))
	for _, p := range m {
		list = append(list, p)
	}
	c.JSON(http.StatusOK, gin.H{"total": len(list), "items": list})
}

// PresetByPattern returns preset for a given pattern
func PresetByPattern(c *gin.Context) {
	pattern := c.Param("pattern")
	m := loadPresets(c)
	if p, ok := m[pattern]; ok {
		c.JSON(http.StatusOK, p)
		return
	}
	c.JSON(http.StatusNotFound, gin.H{"error": "preset not found"})
}
