package grafana

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	templatesDirEnv     = "POLARDBX_GRAFANA_TEMPLATES_DIR"
	defaultTemplatePath = "dashboard" // Remove hardcoded full path
)

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

type DashboardTemplateDetail struct {
	DashboardTemplateSummary
	Content json.RawMessage `json:"content"`
}

func ListTemplates(c *gin.Context) {
	dir, err := resolveTemplatesDir()
	if err != nil {
		status := http.StatusNotFound
		if !errors.Is(err, os.ErrNotExist) {
			status = http.StatusInternalServerError
		}
		c.JSON(status, gin.H{"error": "grafana dashboard templates directory not found", "details": err.Error()})
		return
	}

	items, err := loadTemplateSummaries(dir)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load grafana templates", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"items":     items,
		"directory": dir,
	})
}

func GetTemplate(c *gin.Context) {
	name := c.Param("name")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "template name is required"})
		return
	}

	dir, err := resolveTemplatesDir()
	if err != nil {
		status := http.StatusNotFound
		if !errors.Is(err, os.ErrNotExist) {
			status = http.StatusInternalServerError
		}
		c.JSON(status, gin.H{"error": "grafana dashboard templates directory not found", "details": err.Error()})
		return
	}

	detail, err := loadTemplateDetail(dir, name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			c.JSON(http.StatusNotFound, gin.H{"error": "template not found", "name": name})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load template", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, detail)
}

func resolveTemplatesDir() (string, error) {
	// Priority 1: Environment variable
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

	// Priority 2: Search for charts/*/dashboard (not hardcoded to polardbx-monitor)
	searchPaths := []string{
		filepath.Join("charts", "polardbx-monitor", defaultTemplatePath),
		filepath.Join("charts", "polardbx-monitoring", defaultTemplatePath),
		filepath.Join(defaultTemplatePath), // Direct dashboard folder
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

func loadTemplateSummaries(dir string) ([]DashboardTemplateSummary, error) {
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

func loadTemplateDetail(dir, name string) (DashboardTemplateDetail, error) {
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
