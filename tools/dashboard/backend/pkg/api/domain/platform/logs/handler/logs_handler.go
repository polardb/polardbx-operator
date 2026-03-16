package handler

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
	"polardbx-dashboard-backend/pkg/config"

	"github.com/gin-gonic/gin"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ============================================================================
// Query Types
// ============================================================================

// QueryRequest represents log query parameters
type QueryRequest struct {
	Host      string            `json:"host"`
	Index     string            `json:"index"`
	Query     map[string]any    `json:"query"`
	Size      int               `json:"size"`
	From      int               `json:"from"`
	Sort      []map[string]any  `json:"sort"`
	Aggs      map[string]any    `json:"aggs"`
	TimeRange map[string]string `json:"timeRange"` // {"field":"@timestamp","from":"2024-01-01T00:00:00Z","to":"2024-01-02T00:00:00Z"}
	Facets    []FacetSpec       `json:"facets"`
	Histogram *HistogramSpec    `json:"histogram"`
	Normalize bool              `json:"normalize"`
}

// FacetSpec for aggregation
type FacetSpec struct {
	Name  string `json:"name"`
	Field string `json:"field"`
	Size  int    `json:"size"`
	Order string `json:"order"` // count|key
}

// HistogramSpec for date_histogram aggregation
type HistogramSpec struct {
	Name      string `json:"name"`
	Field     string `json:"field"`
	Interval  string `json:"interval"` // 1m,5m,1h
	MinDocCnt int    `json:"minDocCount"`
}

// FacetBucket represents a bucket in facet aggregation
type FacetBucket struct {
	Key   string `json:"key"`
	Count int64  `json:"count"`
}

// HistogramBucket represents a bucket in histogram aggregation
type HistogramBucket struct {
	Key   string `json:"key"`
	Count int64  `json:"count"`
}

// NormalizedResponse is the normalized query response
type NormalizedResponse struct {
	Total     int64                    `json:"total"`
	Items     []map[string]any         `json:"items"`
	Facets    map[string][]FacetBucket `json:"facets,omitempty"`
	Histogram []HistogramBucket        `json:"histogram,omitempty"`
}

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

// ============================================================================
// Query Handler
// ============================================================================

// Query handles log query requests
// @Summary Query logs
// @Description Queries logs from Elasticsearch/OpenSearch with support for aggregations, facets, and histograms
// @Tags logs
// @Accept json
// @Produce json
// @Param body body QueryRequest true "Log query request with host, index, query DSL, aggregations, etc."
// @Success 200 {object} NormalizedResponse "Query results with total count, items, facets, and histogram"
// @Failure 400 {object} map[string]any "Invalid request parameters"
// @Failure 403 {object} map[string]any "Target host not allowed"
// @Failure 500 {object} map[string]any "Internal server error or security bootstrap failed"
// @Router /api/v1/platform/logs/query [post]
func Query(c *gin.Context) {
	if err := util.EnsureLogsSecurityBootstrap(c); err != nil {
		apierr.AbortInternal(c, "security bootstrap failed: "+err.Error())
		return
	}
	var req QueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apierr.AbortValidation(c, "invalid request: "+err.Error())
		return
	}
	allowed, defHost, err := util.LoadLogsSecurityConfig(c)
	if err != nil {
		apierr.AbortInternal(c, "failed to load security config: "+err.Error())
		return
	}
	if req.Host == "" {
		req.Host = defHost
	}
	if req.Host == "" && len(allowed) == 0 && defHost == "" {
		apierr.AbortForbidden(c, "logs query disabled: configure logs-config ConfigMap (defaultHost/allowedHosts) or create an elasticsearch log strategy")
		return
	}
	if !util.IsHostAllowed(req.Host, allowed, defHost) {
		apierr.AbortForbidden(c, "target host not allowed (configure logs-config allowedHosts/defaultHost)")
		return
	}
	// host format validation
	if u, perr := url.Parse(req.Host); perr != nil || (u.Scheme != "http" && u.Scheme != "https") {
		apierr.AbortValidation(c, "invalid host, expect http(s) URL")
		return
	}

	if strings.TrimSpace(req.Index) == "" {
		apierr.AbortValidation(c, "index is required")
		return
	}
	if req.Size < 0 || req.From < 0 {
		apierr.AbortValidation(c, "size/from must be non-negative")
		return
	}

	// Build DSL
	body := map[string]any{
		"query": req.Query,
		"size":  defaultInt(req.Size, 50),
		"from":  defaultInt(req.From, 0),
	}
	if len(req.Sort) > 0 {
		body["sort"] = req.Sort
	}
	aggs := map[string]any{}
	if req.Aggs != nil {
		for k, v := range req.Aggs {
			aggs[k] = v
		}
	}
	// facets with keyword fallback
	for _, f := range req.Facets {
		if strings.TrimSpace(f.Name) == "" || strings.TrimSpace(f.Field) == "" {
			continue
		}
		size := f.Size
		if size <= 0 {
			size = 10
		}
		orderBy := "_count"
		if strings.ToLower(f.Order) == "key" {
			orderBy = "_key"
		}
		field := f.Field
		if !strings.HasSuffix(field, ".keyword") {
			field = field + ".keyword"
		}
		aggs[f.Name] = map[string]any{
			"terms": map[string]any{
				"field": field,
				"size":  size,
				"order": map[string]any{orderBy: "desc"},
			},
		}
	}
	// histogram with auto-interval
	if h := req.Histogram; h != nil && strings.TrimSpace(h.Name) != "" && strings.TrimSpace(h.Field) != "" {
		interval := strings.TrimSpace(h.Interval)
		if interval == "" {
			// try to compute from timeRange
			field := req.TimeRange["field"]
			if field == "" {
				field = "@timestamp"
			}
			fromStr := req.TimeRange["from"]
			toStr := req.TimeRange["to"]
			if fromStr != "" {
				if toStr == "" {
					toStr = time.Now().UTC().Format(time.RFC3339)
				}
				if iv, ok := chooseFixedInterval(fromStr, toStr); ok {
					interval = iv
				}
			}
			if interval == "" {
				interval = "1m"
			}
			h.Interval = interval
		}
		minDoc := h.MinDocCnt
		if minDoc < 0 {
			minDoc = 0
		}
		aggs[h.Name] = map[string]any{
			"date_histogram": map[string]any{
				"field":          h.Field,
				"fixed_interval": h.Interval,
				"min_doc_count":  minDoc,
			},
		}
	}
	if len(aggs) > 0 {
		body["aggs"] = aggs
	}
	// time range filter and basic validation
	if f, ok := req.TimeRange["from"]; ok && f != "" {
		field := req.TimeRange["field"]
		if field == "" {
			field = "@timestamp"
		}
		to := req.TimeRange["to"]
		// validate RFC3339 if provided
		if _, err := time.Parse(time.RFC3339, f); err != nil {
			apierr.AbortValidation(c, "invalid timeRange.from, expect RFC3339")
			return
		}
		if to != "" {
			if _, err := time.Parse(time.RFC3339, to); err != nil {
				apierr.AbortValidation(c, "invalid timeRange.to, expect RFC3339")
				return
			}
		}
		rangeQ := map[string]any{"range": map[string]any{field: map[string]any{"gte": f}}}
		if to != "" {
			rangeQ["range"].(map[string]any)[field].(map[string]any)["lte"] = to
		}
		if body["query"] == nil {
			body["query"] = map[string]any{"bool": map[string]any{"filter": []any{rangeQ}}}
		} else {
			// append to bool.filter if present, otherwise wrap
			if m, ok := body["query"].(map[string]any)["bool"].(map[string]any); ok {
				filters, _ := m["filter"].([]any)
				m["filter"] = append(filters, rangeQ)
			} else {
				body["query"] = map[string]any{"bool": map[string]any{"must": []any{body["query"]}, "filter": []any{rangeQ}}}
			}
		}
	}

	buf, _ := json.Marshal(body)
	endpoint := strings.TrimRight(req.Host, "/") + "/" + url.PathEscape(req.Index) + "/_search"

	// HTTP client with optional TLS
	httpClient := &http.Client{Timeout: 30 * time.Second}
	if pool, err := util.LoadESRootCAs(c); err == nil && pool != nil {
		httpClient.Transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}
	}

	httpReq, _ := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, endpoint, bytes.NewReader(buf))
	httpReq.Header.Set("Content-Type", "application/json")
	if user, pass, ok, _ := util.LoadESCredentials(c); ok && user != "" {
		httpReq.SetBasicAuth(user, pass)
	}

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		apierr.Abort(c, apierr.BadGateway("es query failed: "+err.Error()))
		return
	}
	defer resp.Body.Close()
	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		c.JSON(resp.StatusCode, gin.H{"error": "es error", "details": string(bodyBytes)})
		return
	}
	if !req.Normalize {
		var out any
		_ = json.Unmarshal(bodyBytes, &out)
		apierr.OK(c, out)
		return
	}
	// normalize
	var m map[string]any
	_ = json.Unmarshal(bodyBytes, &m)
	norm := NormalizedResponse{Items: make([]map[string]any, 0), Facets: map[string][]FacetBucket{}}
	if hits, ok := m["hits"].(map[string]any); ok {
		// total can be number or object with value
		if t, ok := hits["total"]; ok {
			norm.Total = toInt64(t)
		}
		if arr, ok := hits["hits"].([]any); ok {
			for _, it := range arr {
				if mm, ok := it.(map[string]any); ok {
					if src, ok := mm["_source"].(map[string]any); ok {
						norm.Items = append(norm.Items, src)
					}
				}
			}
		}
	}
	if aggs, ok := m["aggregations"].(map[string]any); ok {
		// histogram special handling if name matches
		hName := ""
		if req.Histogram != nil {
			hName = req.Histogram.Name
		}
		for name, v := range aggs {
			if mm, ok := v.(map[string]any); ok {
				if buckets, ok := mm["buckets"].([]any); ok {
					if name == hName {
						series := make([]HistogramBucket, 0, len(buckets))
						for _, b := range buckets {
							if bb, ok := b.(map[string]any); ok {
								key := fmt.Sprint(bb["key_as_string"]) // fall back to key
								if key == "" {
									key = fmt.Sprint(bb["key"])
								}
								series = append(series, HistogramBucket{Key: key, Count: toInt64(bb["doc_count"])})
							}
						}
						norm.Histogram = series
					} else {
						list := make([]FacetBucket, 0, len(buckets))
						for _, b := range buckets {
							if bb, ok := b.(map[string]any); ok {
								key := fmt.Sprint(bb["key"])
								list = append(list, FacetBucket{Key: key, Count: toInt64(bb["doc_count"])})
							}
						}
						norm.Facets[name] = list
					}
				}
			}
		}
	}
	// if no facets collected, set to nil to omit in JSON
	if len(norm.Facets) == 0 {
		norm.Facets = nil
	}
	apierr.OK(c, norm)
}

func defaultInt(v int, d int) int {
	if v <= 0 {
		return d
	}
	return v
}

// chooseFixedInterval decides a fixed_interval string based on time range aiming ~60 buckets
func chooseFixedInterval(fromRFC3339, toRFC3339 string) (string, bool) {
	from, err1 := time.Parse(time.RFC3339, fromRFC3339)
	to, err2 := time.Parse(time.RFC3339, toRFC3339)
	if err1 != nil || err2 != nil || !to.After(from) {
		return "", false
	}
	dur := to.Sub(from)
	totalSecs := dur.Seconds()
	// candidate intervals in seconds and their string forms
	candidates := []struct {
		sec float64
		str string
	}{
		{30, "30s"}, {60, "1m"}, {300, "5m"}, {600, "10m"}, {1800, "30m"},
		{3600, "1h"}, {10800, "3h"}, {21600, "6h"}, {43200, "12h"}, {86400, "1d"},
		{604800, "7d"}, {2592000, "30d"},
	}
	targetBucketsMin, targetBucketsMax := 48.0, 120.0
	best := candidates[1] // default 1m
	bestDiff := 1e18
	for _, c := range candidates {
		buckets := totalSecs / c.sec
		// prefer within range; otherwise choose closest
		var diff float64
		if buckets < targetBucketsMin {
			diff = targetBucketsMin - buckets
		} else if buckets > targetBucketsMax {
			diff = buckets - targetBucketsMax
		} else {
			diff = 0
		}
		if diff < bestDiff {
			best = c
			bestDiff = diff
		}
	}
	return best.str, true
}

func toInt64(v any) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case int64:
		return t
	case int:
		return int64(t)
	case map[string]any:
		if vv, ok := t["value"]; ok {
			return toInt64(vv)
		}
	}
	return 0
}

// ============================================================================
// Bootstrap Handler
// ============================================================================

// Bootstrap installs log collection stack
// @Summary Bootstrap log collection
// @Description Installs log collection stack (Filebeat/Logstash) via Helm chart
// @Tags logs
// @Accept json
// @Produce json
// @Param body body map[string]any true "Bootstrap request with mode, namespace, releaseName, enableFilebeat, enableLogstash, esHost, etc."
// @Success 200 {object} map[string]any "Bootstrap status and release information"
// @Failure 400 {object} map[string]any "Invalid request parameters"
// @Failure 500 {object} map[string]any "Internal server error"
// @Router /api/v1/platform/logs/bootstrap [post]
func Bootstrap(c *gin.Context) {
	type req struct {
		Mode           string `json:"mode"` // managed|assisted|byo
		Dry            bool   `json:"dryRun"`
		NS             string `json:"namespace"`
		Name           string `json:"releaseName"`
		EnableFilebeat bool   `json:"enableFilebeat"`
		EnableLogstash bool   `json:"enableLogstash"`
		ESHost         string `json:"esHost"`
		ESUser         string `json:"esUser"`
		ESPassword     string `json:"esPassword"`
		ESIndex        string `json:"esIndex"`
		DeploymentType string `json:"deploymentType"` // default|production|minimal|custom
	}
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	var r req
	_ = c.ShouldBindJSON(&r)
	r.NS = strings.TrimSpace(r.NS)
	if r.NS == "" {
		r.NS = "polardbx-logcollector"
	}
	if r.Name == "" {
		r.Name = "polardbx-logcollector"
	}
	targetNamespace := strings.TrimSpace(r.NS)

	// Check for existing ongoing bootstrap jobs (idempotent)
	if !r.Dry {
		existingJobs := &batchv1.JobList{}
		labelSelector := client.MatchingLabels{
			"app":       "polardbx-logs-bootstrap",
			"createdBy": "dashboard",
		}
		if err := cli.List(c.Request.Context(), existingJobs, client.InNamespace(r.NS), labelSelector); err == nil {
			// Look for non-completed jobs
			for _, job := range existingJobs.Items {
				isComplete := false
				isFailed := false
				for _, condition := range job.Status.Conditions {
					if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
						isComplete = true
						break
					}
					if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
						isFailed = true
						break
					}
				}
				// If job is still running, return existing job info
				if !isComplete && !isFailed {
					apierr.Accepted(c, gin.H{
						"message":      "logs bootstrap already in progress",
						"namespace":    targetNamespace,
						"targetNs":     targetNamespace,
						"mode":         r.Mode,
						"releaseName":  r.Name,
						"jobName":      job.Name,
						"instructions": "Use kubectl logs -n " + targetNamespace + " job/" + job.Name + " to see progress",
						"existing":     true,
					})
					return
				}
			}
		}
	}

	// Persist plan (idempotent)
	cm := corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: r.NS, Name: "polardbx-logs-plan"}
	if err := cli.Get(c.Request.Context(), key, &cm); err != nil {
		cm = corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: r.NS, Name: "polardbx-logs-plan"}, Data: map[string]string{}}
		_ = cli.Create(c.Request.Context(), &cm)
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data["mode"] = r.Mode
	cm.Data["releaseName"] = r.Name
	cm.Data["deploymentType"] = r.DeploymentType
	cm.Data["enableFilebeat"] = fmt.Sprintf("%t", r.EnableFilebeat)
	cm.Data["enableLogstash"] = fmt.Sprintf("%t", r.EnableLogstash)
	cm.Data["esHost"] = r.ESHost
	cm.Data["esIndex"] = r.ESIndex
	cm.Data["dryRun"] = map[bool]string{true: "true", false: "false"}[r.Dry]
	_ = cli.Update(c.Request.Context(), &cm)

	// If dry-run, just accept
	if r.Dry {
		apierr.Accepted(c, gin.H{
			"message":        "logs bootstrap accepted (dry-run)",
			"namespace":      targetNamespace,
			"mode":           r.Mode,
			"releaseName":    r.Name,
			"deploymentType": r.DeploymentType,
			"dryRun":         r.Dry,
		})
		return
	}

	// Ensure target logs namespace exists
	logsNS := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: targetNamespace}}
	if err := cli.Create(c.Request.Context(), logsNS); err != nil && !k8serrors.IsAlreadyExists(err) {
		apierr.AbortInternal(c, "failed to create namespace "+targetNamespace+": "+err.Error())
		return
	}

	// Create ConfigMap with manifests for the installer Job to apply
	manifestsCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.NS,
			Name:      "polardbx-logs-manifests",
		},
		Data: map[string]string{
			"manifests.yaml": getLogCollectorManifests(r.Name, r.DeploymentType),
		},
	}
	// Create or update manifests ConfigMap
	existingCM := &corev1.ConfigMap{}
	cmKey := client.ObjectKey{Namespace: r.NS, Name: "polardbx-logs-manifests"}
	if err := cli.Get(c.Request.Context(), cmKey, existingCM); err != nil {
		if err := cli.Create(c.Request.Context(), manifestsCM); err != nil {
			apierr.AbortInternal(c, "failed to create manifests ConfigMap: "+err.Error())
			return
		}
	} else {
		existingCM.Data = manifestsCM.Data
		if err := cli.Update(c.Request.Context(), existingCM); err != nil {
			apierr.AbortInternal(c, "failed to update manifests ConfigMap: "+err.Error())
			return
		}
	}

	// Create a short-lived Job to run log collection stack installation
	jobName := fmt.Sprintf("polardbx-logs-bootstrap-%d", time.Now().Unix())
	correlationId := fmt.Sprintf("logs-%d", time.Now().UnixNano())

	// Build Helm installation commands
	installCommands := []string{
		"set -e",
		"echo 'Starting PolarDB-X LogCollector installation...'",
		"echo 'Checking prerequisites...'",
		"helm version || (echo 'ERROR: helm not found' && exit 1)",

		// Add Helm repository
		"echo 'Adding Helm repository...'",
		"helm repo add polardbx https://polardbx-charts.oss-cn-beijing.aliyuncs.com || echo 'WARN: Failed to add repository'",
		"helm repo update || true",

		// Try to install from Helm repository
		fmt.Sprintf("echo 'Attempting to install %s from Helm repository...'", r.Name),
		fmt.Sprintf("helm upgrade --install %s polardbx/polardbx-logcollector --namespace %s --create-namespace --wait --timeout 300s || "+
			"(echo 'WARN: Chart not found in repository. Please install manually:' && "+
			"echo '  helm install %s ./charts/polardbx-logcollector -n %s --create-namespace' && "+
			"exit 1)", r.Name, targetNamespace, r.Name, targetNamespace),

		// Verify installation
		"echo 'Verifying installation...'",
		fmt.Sprintf("helm list -n %s", targetNamespace),

		"echo '========================================='",
		"echo 'Log collection stack installed successfully'",
		fmt.Sprintf("echo 'Namespace: %s'", targetNamespace),
		fmt.Sprintf("echo 'Release: %s'", r.Name),
		"echo 'Components: Filebeat DaemonSet + Logstash Deployment'",
		"echo '========================================='",
	}

	command := strings.Join(installCommands, " && ")

	// Create ServiceAccount for Helm installer Job
	installerSA := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "polardbx-logcollector-installer",
			Namespace: targetNamespace,
		},
	}
	if err := cli.Create(c.Request.Context(), installerSA); err != nil && !k8serrors.IsAlreadyExists(err) {
		apierr.AbortInternal(c, "failed to create installer ServiceAccount: "+err.Error())
		return
	}

	// Create ClusterRole with permissions for Helm installation
	installerRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: "polardbx-logcollector-installer-role",
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps", "secrets", "services", "serviceaccounts", "pods", "namespaces", "nodes"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			{
				APIGroups: []string{"apps"},
				Resources: []string{"deployments", "daemonsets", "replicasets", "statefulsets"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			{
				APIGroups: []string{"batch"},
				Resources: []string{"jobs", "cronjobs"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			{
				APIGroups: []string{"rbac.authorization.k8s.io"},
				Resources: []string{"roles", "rolebindings", "clusterroles", "clusterrolebindings"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			{
				APIGroups: []string{"polardbx.aliyun.com"},
				Resources: []string{"polardbxlogcollectors", "polardbxlogcollectors/status", "polardbxlogcollectors/finalizers"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
		},
	}
	if err := cli.Create(c.Request.Context(), installerRole); err != nil {
		if !k8serrors.IsAlreadyExists(err) {
			apierr.AbortInternal(c, "failed to create installer ClusterRole: "+err.Error())
			return
		}

		existingRole := &rbacv1.ClusterRole{}
		if getErr := cli.Get(c.Request.Context(), client.ObjectKey{Name: installerRole.Name}, existingRole); getErr != nil {
			apierr.AbortInternal(c, "failed to refresh installer ClusterRole: "+getErr.Error())
			return
		}

		existingRole.Rules = installerRole.Rules
		if updateErr := cli.Update(c.Request.Context(), existingRole); updateErr != nil {
			apierr.AbortInternal(c, "failed to update installer ClusterRole: "+updateErr.Error())
			return
		}
	}

	// Create ClusterRoleBinding
	installerBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: "polardbx-logcollector-installer-binding",
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      "polardbx-logcollector-installer",
				Namespace: targetNamespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "polardbx-logcollector-installer-role",
		},
	}
	if err := cli.Create(c.Request.Context(), installerBinding); err != nil {
		if !k8serrors.IsAlreadyExists(err) {
			apierr.AbortInternal(c, "failed to create installer ClusterRoleBinding: "+err.Error())
			return
		}

		existingBinding := &rbacv1.ClusterRoleBinding{}
		if getErr := cli.Get(c.Request.Context(), client.ObjectKey{Name: installerBinding.Name}, existingBinding); getErr != nil {
			apierr.AbortInternal(c, "failed to refresh installer ClusterRoleBinding: "+getErr.Error())
			return
		}

		existingBinding.Subjects = installerBinding.Subjects
		existingBinding.RoleRef = installerBinding.RoleRef
		if updateErr := cli.Update(c.Request.Context(), existingBinding); updateErr != nil {
			apierr.AbortInternal(c, "failed to update installer ClusterRoleBinding: "+updateErr.Error())
			return
		}
	}

	backoff := int32(0)
	ttl := int32(600)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: targetNamespace,
			Name:      jobName,
			Labels: map[string]string{
				"app":           "polardbx-logs-bootstrap",
				"createdBy":     "dashboard",
				"correlationId": correlationId,
				"targetType":    "logcollector",
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					ServiceAccountName:           "polardbx-logcollector-installer",
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: func(b bool) *bool { return &b }(true),
					Containers: []corev1.Container{{
						Name:            "logs-installer",
						Image:           config.GetGlobalConfig().GetHelmImage(),
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command:         []string{"sh", "-c", command},
						Env: []corev1.EnvVar{
							{Name: "ES_HOST", Value: r.ESHost},
							{Name: "ES_USER", Value: r.ESUser},
							{Name: "ES_INDEX", Value: r.ESIndex},
							{Name: "ENABLE_FILEBEAT", Value: fmt.Sprintf("%t", r.EnableFilebeat)},
							{Name: "ENABLE_LOGSTASH", Value: fmt.Sprintf("%t", r.EnableLogstash)},
							{Name: "DEPLOYMENT_TYPE", Value: r.DeploymentType},
						},
					}},
				},
			},
		},
	}

	// Add ES password as secret if provided
	if r.ESPassword != "" {
		job.Spec.Template.Spec.Containers[0].Env = append(job.Spec.Template.Spec.Containers[0].Env,
			corev1.EnvVar{Name: "ES_PASSWORD", Value: r.ESPassword})
	}

	if err := cli.Create(c.Request.Context(), job); err != nil {
		apierr.AbortInternal(c, "failed to create logs install job: "+err.Error())
		return
	}

	apierr.Accepted(c, gin.H{
		"message":        "logs bootstrap started",
		"namespace":      targetNamespace,
		"targetNs":       targetNamespace,
		"mode":           r.Mode,
		"releaseName":    r.Name,
		"deploymentType": r.DeploymentType,
		"jobName":        jobName,
		"correlationId":  correlationId,
		"instructions":   "Use kubectl logs -n " + targetNamespace + " job/" + jobName + " to see progress",
	})
}

// BootstrapStatus returns the status of a logs bootstrap job
// BootstrapStatus returns log collection bootstrap job status
// @Summary Get bootstrap status
// @Description Returns the status of log collection stack bootstrap job
// @Tags logs
// @Accept json
// @Produce json
// @Param namespace query string false "Namespace where bootstrap job is running"
// @Param jobName query string false "Bootstrap job name"
// @Success 200 {object} map[string]any "Bootstrap job status with conditions"
// @Failure 404 {object} map[string]any "Bootstrap job not found"
// @Failure 500 {object} map[string]any "Internal server error"
// @Router /api/v1/platform/logs/bootstrap/status [get]
func BootstrapStatus(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	jobName := c.Query("jobName")
	namespace := util.DefaultNamespace(c, "polardbx-logcollector")

	if jobName == "" {
		apierr.AbortValidation(c, "jobName parameter is required")
		return
	}

	// Get the job
	job := &batchv1.Job{}
	key := client.ObjectKey{Namespace: namespace, Name: jobName}
	if err := cli.Get(c.Request.Context(), key, job); err != nil {
		if k8serrors.IsNotFound(err) {
			apierr.AbortNotFound(c, "job", fmt.Sprintf("%s/%s", namespace, jobName))
			return
		}
		util.HandleK8sError(c, "failed to get job", err)
		return
	}

	// Determine job phase
	phase := "Running"
	var completionTime *metav1.Time
	var failureReason string

	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			phase = "Succeeded"
			completionTime = &condition.LastTransitionTime
			break
		}
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			phase = "Failed"
			failureReason = condition.Message
			completionTime = &condition.LastTransitionTime
			break
		}
	}

	// Check if job is still running by looking at active pods
	if phase == "Running" && job.Status.Active == 0 && job.Status.Succeeded == 0 && job.Status.Failed == 0 {
		phase = "Pending"
	}

	response := gin.H{
		"jobName":   jobName,
		"namespace": namespace,
		"phase":     phase,
		"startTime": job.Status.StartTime,
		"active":    job.Status.Active,
		"succeeded": job.Status.Succeeded,
		"failed":    job.Status.Failed,
	}

	if completionTime != nil {
		response["completionTime"] = completionTime
	}

	if failureReason != "" {
		response["failureReason"] = failureReason
	}

	// Add conditions for detailed status
	response["conditions"] = job.Status.Conditions

	apierr.OK(c, response)
}

// BootstrapLogs returns the logs of a logs bootstrap job
// BootstrapLogs retrieves logs from bootstrap job
// @Summary Get bootstrap logs
// @Description Retrieves logs from log collection bootstrap job
// @Tags logs
// @Accept json
// @Produce text/plain
// @Param namespace query string false "Namespace where bootstrap job is running"
// @Param jobName query string false "Bootstrap job name"
// @Param tailLines query int false "Number of lines to retrieve (default: 1000)"
// @Success 200 {string} string "Bootstrap job logs in plain text"
// @Failure 404 {object} map[string]any "Bootstrap job not found"
// @Failure 500 {object} map[string]any "Internal server error"
// @Router /api/v1/platform/logs/bootstrap/logs [get]
func BootstrapLogs(c *gin.Context) {
	cs, ok := util.ClientsetFromContext(c)
	if !ok {
		return
	}

	jobName := c.Query("jobName")
	namespace := util.DefaultNamespace(c, "polardbx-logcollector")

	// Security: Enforce upper limit for tailLines to prevent OOM
	const maxTailLines = int64(10000)
	tailLines := int64(100) // Default to last 100 lines

	if jobName == "" {
		apierr.AbortValidation(c, "jobName parameter is required")
		return
	}

	if tailParam := c.Query("tailLines"); tailParam != "" {
		if parsed, err := strconv.ParseInt(tailParam, 10, 64); err == nil && parsed > 0 {
			tailLines = parsed
			if tailLines > maxTailLines {
				tailLines = maxTailLines
			}
		}
	}

	// List pods created by this job
	pods, err := cs.CoreV1().Pods(namespace).List(c.Request.Context(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	if err != nil {
		util.HandleK8sError(c, "failed to list job pods", err)
		return
	}

	if len(pods.Items) == 0 {
		apierr.AbortNotFound(c, "pods for job", jobName)
		return
	}

	// Get logs from the first pod (usually there's only one for our jobs)
	pod := pods.Items[0]

	logOptions := &corev1.PodLogOptions{
		TailLines: &tailLines,
	}

	// If pod has multiple containers, get logs from the first one
	if len(pod.Spec.Containers) > 0 {
		logOptions.Container = pod.Spec.Containers[0].Name
	}

	logReq := cs.CoreV1().Pods(namespace).GetLogs(pod.Name, logOptions)
	rc, err := logReq.Stream(c.Request.Context())
	if err != nil {
		util.HandleK8sError(c, "failed to get pod logs", err)
		return
	}
	defer rc.Close()

	logs, err := io.ReadAll(rc)
	if err != nil {
		apierr.AbortInternal(c, "failed to read logs: "+err.Error())
		return
	}

	apierr.OK(c, gin.H{
		"jobName":   jobName,
		"namespace": namespace,
		"podName":   pod.Name,
		"logs":      string(logs),
		"tailLines": tailLines,
	})
}

// getLogCollectorManifests returns pre-rendered Kubernetes manifests for log collector components
func getLogCollectorManifests(releaseName, deploymentType string) string {
	return `# PolarDB-X LogCollector Manifests
# NOTE: This is a simplified placeholder manifest
# For full-featured deployment, use: helm template polardbx-logcollector ./charts/polardbx-logcollector

apiVersion: v1
kind: ConfigMap
metadata:
  name: placeholder-logcollector-info
  namespace: polardbx-logcollector
data:
  message: |
    PolarDB-X LogCollector installation requires Helm chart deployment.
    Please use one of the following methods:
    
    1. From Helm repository (recommended):
       helm repo add polardbx https://polardbx-charts.oss-cn-beijing.aliyuncs.com
       helm upgrade --install ` + releaseName + ` polardbx/polardbx-logcollector -n polardbx-logcollector --create-namespace
    
    2. From local chart:
       helm install ` + releaseName + ` ./charts/polardbx-logcollector -n polardbx-logcollector --create-namespace
    
    3. kubectl apply with pre-rendered manifests:
       helm template ` + releaseName + ` ./charts/polardbx-logcollector | kubectl apply -n polardbx-logcollector -f -
    
    Deployment Type: ` + deploymentType + `
`
}

// ============================================================================
// Presets Handler
// ============================================================================

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
// @Summary List log query presets
// @Description Returns all available log query presets with default aggregations and histogram configurations
// @Tags logs
// @Accept json
// @Produce json
// @Success 200 {object} map[string]any "List of log query presets"
// @Router /api/v1/platform/logs/presets [get]
func Presets(c *gin.Context) {
	m := loadPresets(c)
	list := make([]LogPreset, 0, len(m))
	for _, p := range m {
		list = append(list, p)
	}
	apierr.OK(c, gin.H{"total": len(list), "items": list})
}

// PresetByPattern returns preset for a given pattern
// @Summary Get log preset by pattern
// @Description Returns log query preset configuration for a specific index pattern
// @Tags logs
// @Accept json
// @Produce json
// @Param pattern path string true "Index pattern name"
// @Success 200 {object} LogPreset "Log preset configuration"
// @Failure 404 {object} map[string]any "Preset not found"
// @Router /api/v1/platform/logs/presets/{pattern} [get]
func PresetByPattern(c *gin.Context) {
	pattern := c.Param("pattern")
	m := loadPresets(c)
	if p, ok := m[pattern]; ok {
		apierr.OK(c, p)
		return
	}
	apierr.AbortNotFound(c, "preset", pattern)
}
