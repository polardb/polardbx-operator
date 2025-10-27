package logs

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"polardbx-ui-backend/pkg/api/util"

	"github.com/gin-gonic/gin"
)

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

type FacetSpec struct {
	Name  string `json:"name"`
	Field string `json:"field"`
	Size  int    `json:"size"`
	Order string `json:"order"` // count|key
}

type HistogramSpec struct {
	Name      string `json:"name"`
	Field     string `json:"field"`
	Interval  string `json:"interval"` // 1m,5m,1h
	MinDocCnt int    `json:"minDocCount"`
}

type FacetBucket struct {
	Key   string `json:"key"`
	Count int64  `json:"count"`
}

type HistogramBucket struct {
	Key   string `json:"key"`
	Count int64  `json:"count"`
}

type NormalizedResponse struct {
	Total     int64                    `json:"total"`
	Items     []map[string]any         `json:"items"`
	Facets    map[string][]FacetBucket `json:"facets,omitempty"`
	Histogram []HistogramBucket        `json:"histogram,omitempty"`
}

func Query(c *gin.Context) {
	if err := util.EnsureLogsSecurityBootstrap(c); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "security bootstrap failed", "details": err.Error()})
		return
	}
	var req QueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request", "details": err.Error()})
		return
	}
	allowed, defHost, err := util.LoadLogsSecurityConfig(c)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load security config", "details": err.Error()})
		return
	}
	if req.Host == "" {
		req.Host = defHost
	}
	if !util.IsHostAllowed(req.Host, allowed, defHost) {
		c.JSON(http.StatusForbidden, gin.H{"error": "target host not allowed"})
		return
	}
	// host format validation
	if u, perr := url.Parse(req.Host); perr != nil || (u.Scheme != "http" && u.Scheme != "https") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid host, expect http(s) URL"})
		return
	}

	if strings.TrimSpace(req.Index) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "index is required"})
		return
	}
	if req.Size < 0 || req.From < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "size/from must be non-negative"})
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
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid timeRange.from, expect RFC3339"})
			return
		}
		if to != "" {
			if _, err := time.Parse(time.RFC3339, to); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid timeRange.to, expect RFC3339"})
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
		c.JSON(http.StatusBadGateway, gin.H{"error": "es query failed", "details": err.Error()})
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
		c.JSON(http.StatusOK, out)
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
	c.JSON(http.StatusOK, norm)
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
