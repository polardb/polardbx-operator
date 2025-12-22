package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"polardbx-dashboard-backend/pkg/api/domain/platform/grafana/repository"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
)

// GrafanaHandler handles HTTP requests related to Grafana
type GrafanaHandler struct {
	repo repository.GrafanaRepository
}

// NewGrafanaHandler creates a new GrafanaHandler
func NewGrafanaHandler(repo repository.GrafanaRepository) *GrafanaHandler {
	return &GrafanaHandler{repo: repo}
}

// NewGrafanaHandlerFromContext creates handler from gin.Context
func NewGrafanaHandlerFromContext(c *gin.Context) (*GrafanaHandler, bool) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return nil, false
	}
	repo := repository.NewK8sGrafanaRepository(cli)
	return NewGrafanaHandler(repo), true
}

// GetConfig retrieves Grafana configuration.
// @Summary Get Grafana config
// @Description Get current Grafana configuration from the platform.
// @Tags platform, grafana
// @Produce json
// @Success 200 {object} map[string]any "Grafana configuration"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/grafana/config [get]
func GetConfig(c *gin.Context) {
	h, ok := NewGrafanaHandlerFromContext(c)
	if !ok {
		return
	}
	h.getConfig(c)
}

func (h *GrafanaHandler) getConfig(c *gin.Context) {
	config, err := h.repo.GetConfig(c.Request.Context())
	if err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("failed to get grafana config", err))
		return
	}
	apierr.OK(c, config)
}

// PutConfig updates Grafana configuration.
// @Summary Update Grafana config
// @Description Update Grafana configuration with the provided payload.
// @Tags platform, grafana
// @Accept json
// @Produce json
// @Param body body map[string]any true "Grafana configuration payload"
// @Success 200 {object} map[string]any "Updated Grafana configuration"
// @Failure 400 {object} apierr.ErrorResponse "Invalid payload"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/grafana/config [put]
func PutConfig(c *gin.Context) {
	h, ok := NewGrafanaHandlerFromContext(c)
	if !ok {
		return
	}
	h.putConfig(c)
}

func (h *GrafanaHandler) putConfig(c *gin.Context) {
	var req repository.GrafanaConfig
	if err := c.ShouldBindJSON(&req); err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	if err := h.repo.SaveConfig(c.Request.Context(), &req); err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("failed to save grafana config", err))
		return
	}
	apierr.OK(c, req)
}

// SyncDashboards synchronizes Grafana dashboards to the platform.
// @Summary Sync Grafana dashboards
// @Description Synchronize Grafana dashboards with optional overwrite flag.
// @Tags platform, grafana
// @Accept json
// @Produce json
// @Param body body map[string]any true "Dashboards payload (dashboards map and overwrite flag)"
// @Success 202 {object} map[string]any "Dashboards synced confirmation"
// @Failure 400 {object} apierr.ErrorResponse "Invalid payload"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/grafana/dashboards/sync [post]
func SyncDashboards(c *gin.Context) {
	h, ok := NewGrafanaHandlerFromContext(c)
	if !ok {
		return
	}
	h.syncDashboards(c)
}

func (h *GrafanaHandler) syncDashboards(c *gin.Context) {
	var body struct {
		Dashboards map[string]string `json:"dashboards"`
		Overwrite  bool              `json:"overwrite"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	count, err := h.repo.SaveDashboards(c.Request.Context(), body.Dashboards, body.Overwrite)
	if err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("failed to save dashboards", err))
		return
	}
	apierr.Accepted(c, gin.H{"message": "dashboards synced", "count": count})
}

// ListDashboards lists all Grafana dashboards.
// @Summary List Grafana dashboards
// @Description List all Grafana dashboards stored in the platform.
// @Tags platform, grafana
// @Produce json
// @Success 200 {object} map[string]any "Dashboard list with names and version counts"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/grafana/dashboards [get]
func ListDashboards(c *gin.Context) {
	h, ok := NewGrafanaHandlerFromContext(c)
	if !ok {
		return
	}
	h.listDashboards(c)
}

func (h *GrafanaHandler) listDashboards(c *gin.Context) {
	data, err := h.repo.GetDashboards(c.Request.Context())
	if err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("failed to get dashboards", err))
		return
	}

	stats := map[string]int{}
	for k := range data {
		if name, _, ok := repository.SplitVersionKey(k); ok {
			stats[name]++
		}
	}

	items := []gin.H{}
	seen := map[string]struct{}{}
	for k := range data {
		if name, _, ok := repository.SplitVersionKey(k); ok {
			if _, done := seen[name]; done {
				continue
			}
			seen[name] = struct{}{}
			items = append(items, gin.H{"name": name, "versions": stats[name]})
		} else {
			if _, done := seen[k]; done {
				continue
			}
			seen[k] = struct{}{}
			items = append(items, gin.H{"name": k, "versions": stats[k]})
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i]["name"].(string) < items[j]["name"].(string) })
	apierr.OK(c, gin.H{"items": items})
}

// ListDashboardVersions lists all versions of a specific dashboard.
// @Summary List dashboard versions
// @Description List all stored versions of a specific Grafana dashboard.
// @Tags platform, grafana
// @Produce json
// @Param name path string true "Dashboard name"
// @Success 200 {object} map[string]any "Dashboard name and version list"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/grafana/dashboards/{name}/versions [get]
func ListDashboardVersions(c *gin.Context) {
	h, ok := NewGrafanaHandlerFromContext(c)
	if !ok {
		return
	}
	h.listDashboardVersions(c)
}

func (h *GrafanaHandler) listDashboardVersions(c *gin.Context) {
	name := c.Param("name")
	data, err := h.repo.GetDashboards(c.Request.Context())
	if err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("failed to get dashboards", err))
		return
	}

	vers := []int{}
	for k := range data {
		if n, v, ok := repository.SplitVersionKey(k); ok && n == name {
			vers = append(vers, v)
		}
	}
	sort.Sort(sort.Reverse(sort.IntSlice(vers)))
	apierr.OK(c, gin.H{"name": name, "versions": vers})
}

// RollbackDashboard rolls back a dashboard to a previous version.
// @Summary Rollback dashboard
// @Description Rollback a Grafana dashboard to a specific previous version.
// @Tags platform, grafana
// @Accept json
// @Produce json
// @Param name path string true "Dashboard name"
// @Param body body map[string]any true "Version payload (version number)"
// @Success 200 {object} map[string]any "Rollback confirmation"
// @Failure 400 {object} apierr.ErrorResponse "Invalid payload"
// @Failure 404 {object} apierr.ErrorResponse "Dashboard version not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/grafana/dashboards/{name}/rollback [post]
func RollbackDashboard(c *gin.Context) {
	h, ok := NewGrafanaHandlerFromContext(c)
	if !ok {
		return
	}
	h.rollbackDashboard(c)
}

func (h *GrafanaHandler) rollbackDashboard(c *gin.Context) {
	name := c.Param("name")
	var body struct {
		Version int `json:"version"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	if body.Version <= 0 {
		apierr.AbortWithError(c, apierr.ValidationError("invalid payload", nil))
		return
	}

	data, err := h.repo.GetDashboards(c.Request.Context())
	if err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("failed to get dashboards", err))
		return
	}

	targetKey := fmt.Sprintf("%s@v%04d", name, body.Version)
	target, ok := data[targetKey]
	if !ok {
		apierr.AbortWithError(c, apierr.NotFoundError("dashboard version", targetKey))
		return
	}

	// Save current version and rollback
	rollback := map[string]string{name: target}
	if _, err := h.repo.SaveDashboards(c.Request.Context(), rollback, true); err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("failed to rollback dashboard", err))
		return
	}

	apierr.OK(c, gin.H{"message": "rolled back", "name": name, "version": body.Version})
}

// ============ Template related ============

const (
	templatesDirEnv     = "POLARDBX_GRAFANA_TEMPLATES_DIR"
	defaultTemplatePath = "dashboard"
)

// DashboardTemplateSummary dashboard template summary
type DashboardTemplateSummary struct {
	Name        string   `json:"name"`
	Title       string   `json:"title"`
	Description string   `json:"description"`
	Tags        []string `json:"tags,omitempty"`
	File        string   `json:"file"`
	Source      string   `json:"source"`
	Size        int64    `json:"size"`
	UpdatedAt   string   `json:"updatedAt"`
}

// DashboardTemplateDetail dashboard template detail
type DashboardTemplateDetail struct {
	DashboardTemplateSummary
	Content json.RawMessage `json:"content"`
}

// ListTemplates lists all available Grafana dashboard templates.
// @Summary List Grafana dashboard templates
// @Description List all available Grafana dashboard templates from the templates directory.
// @Tags platform, grafana
// @Produce json
// @Success 200 {object} map[string]any "Template list and directory"
// @Failure 404 {object} apierr.ErrorResponse "Templates directory not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/grafana/templates [get]
func ListTemplates(c *gin.Context) {
	dir, err := ResolveTemplatesDir()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			apierr.AbortWithError(c, apierr.NotFoundError("grafana dashboard templates directory", ""))
		} else {
			apierr.AbortWithError(c, apierr.InternalServiceError("grafana dashboard templates directory not found", err))
		}
		return
	}

	items, err := LoadTemplateSummaries(dir)
	if err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("failed to load grafana templates", err))
		return
	}

	apierr.OK(c, gin.H{
		"items":     items,
		"directory": dir,
	})
}

// GetTemplate retrieves a specific Grafana dashboard template by name.
// @Summary Get Grafana dashboard template
// @Description Get a specific Grafana dashboard template including its content.
// @Tags platform, grafana
// @Produce json
// @Param name path string true "Template name"
// @Success 200 {object} map[string]any "Template details including content"
// @Failure 400 {object} apierr.ErrorResponse "Template name required"
// @Failure 404 {object} apierr.ErrorResponse "Template not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/platform/grafana/templates/{name} [get]
func GetTemplate(c *gin.Context) {
	name := c.Param("name")
	if name == "" {
		apierr.AbortWithError(c, apierr.ValidationError("template name is required", nil))
		return
	}

	dir, err := ResolveTemplatesDir()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			apierr.AbortWithError(c, apierr.NotFoundError("grafana dashboard templates directory", ""))
		} else {
			apierr.AbortWithError(c, apierr.InternalServiceError("grafana dashboard templates directory not found", err))
		}
		return
	}

	detail, err := LoadTemplateDetail(dir, name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			apierr.AbortWithError(c, apierr.NotFoundError("template", name))
			return
		}
		apierr.AbortWithError(c, apierr.InternalServiceError("failed to load template", err))
		return
	}

	apierr.OK(c, detail)
}

func ResolveTemplatesDir() (string, error) {
	if env := strings.TrimSpace(os.Getenv(templatesDirEnv)); env != "" {
		absEnv, err := filepath.Abs(env)
		if err != nil {
			return "", fmt.Errorf("resolve %s: %w", templatesDirEnv, err)
		}
		if info, err := os.Stat(absEnv); err == nil && info.IsDir() {
			return absEnv, nil
		}
	}

	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("getwd: %w", err)
	}

	searchPaths := []string{
		filepath.Join("charts", "polardbx-monitor", defaultTemplatePath),
		filepath.Join("charts", "polardbx-monitoring", defaultTemplatePath),
		filepath.Join(defaultTemplatePath),
	}

	visited := map[string]struct{}{}
	dir := cwd
	for i := 0; i < 8; i++ {
		for _, searchPath := range searchPaths {
			candidate := filepath.Join(dir, searchPath)
			if _, seen := visited[candidate]; seen {
				continue
			}
			visited[candidate] = struct{}{}
			if info, err := os.Stat(candidate); err == nil && info.IsDir() {
				return candidate, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return "", fmt.Errorf("grafana templates directory not found in search paths: %v (set %s env to override)", searchPaths, templatesDirEnv)
}

func LoadTemplateSummaries(dir string) ([]DashboardTemplateSummary, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	summaries := make([]DashboardTemplateSummary, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		summary, err := readTemplateSummary(dir, name)
		if err != nil {
			return nil, err
		}
		summaries = append(summaries, summary)
	}

	sort.SliceStable(summaries, func(i, j int) bool {
		if summaries[i].Title == summaries[j].Title {
			return summaries[i].Name < summaries[j].Name
		}
		return summaries[i].Title < summaries[j].Title
	})

	return summaries, nil
}

func LoadTemplateDetail(dir, name string) (DashboardTemplateDetail, error) {
	fileName := name
	if !strings.HasSuffix(fileName, ".json") {
		fileName = name + ".json"
	}
	path := filepath.Join(dir, fileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return DashboardTemplateDetail{}, os.ErrNotExist
		}
		return DashboardTemplateDetail{}, err
	}

	summary, err := readTemplateSummaryWithData(dir, fileName, data)
	if err != nil {
		return DashboardTemplateDetail{}, err
	}

	return DashboardTemplateDetail{
		DashboardTemplateSummary: summary,
		Content:                  json.RawMessage(data),
	}, nil
}

func readTemplateSummary(dir, fileName string) (DashboardTemplateSummary, error) {
	data, err := os.ReadFile(filepath.Join(dir, fileName))
	if err != nil {
		return DashboardTemplateSummary{}, err
	}
	return readTemplateSummaryWithData(dir, fileName, data)
}

func readTemplateSummaryWithData(dir, fileName string, data []byte) (DashboardTemplateSummary, error) {
	info, err := os.Stat(filepath.Join(dir, fileName))
	if err != nil {
		return DashboardTemplateSummary{}, err
	}

	meta, err := parseTemplateMeta(data)
	if err != nil {
		return DashboardTemplateSummary{}, err
	}
	if meta.Title == "" {
		meta.Title = fallbackTitle(strings.TrimSuffix(fileName, ".json"))
	}

	rel := filepath.ToSlash(filepath.Join(
		filepath.Base(filepath.Dir(filepath.Dir(dir))),
		filepath.Base(filepath.Dir(dir)),
		filepath.Base(dir),
		fileName,
	))

	name := strings.TrimSuffix(fileName, ".json")
	return DashboardTemplateSummary{
		Name:        name,
		Title:       meta.Title,
		Description: meta.Description,
		Tags:        meta.Tags,
		File:        fileName,
		Source:      rel,
		Size:        info.Size(),
		UpdatedAt:   info.ModTime().UTC().Format(time.RFC3339),
	}, nil
}

type templateMetadata struct {
	Title       string   `json:"title"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
}

func parseTemplateMeta(data []byte) (templateMetadata, error) {
	var meta templateMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return templateMetadata{}, fmt.Errorf("parse grafana template: %w", err)
	}
	meta.Title = strings.TrimSpace(meta.Title)
	meta.Description = strings.TrimSpace(meta.Description)
	meta.Tags = normalizeTags(meta.Tags)
	return meta, nil
}

func normalizeTags(tags []string) []string {
	set := make([]string, 0, len(tags))
	for _, tag := range tags {
		t := strings.TrimSpace(tag)
		if t == "" {
			continue
		}
		set = append(set, t)
	}
	return set
}

func fallbackTitle(name string) string {
	parts := strings.FieldsFunc(name, func(r rune) bool {
		return r == '-' || r == '_' || r == '.'
	})
	for i, part := range parts {
		if part == "" {
			continue
		}
		lower := strings.ToLower(part)
		parts[i] = strings.ToUpper(lower[:1]) + lower[1:]
	}
	title := strings.Join(parts, " ")
	if title == "" {
		return name
	}
	return title
}

// Keep these variables to avoid unused import warnings
var _ = strconv.Atoi
