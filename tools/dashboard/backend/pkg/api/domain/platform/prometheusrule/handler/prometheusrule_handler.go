// Package handler provides HTTP handlers for PrometheusRule alert rule management
// Follows Clean Architecture design pattern
package handler

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"

	"github.com/gin-gonic/gin"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/yaml"
)

// ======================== Types ========================

// PrometheusRule represents alert rule resource
type PrometheusRule struct {
	Name           string            `json:"name"`
	Namespace      string            `json:"namespace"`
	Labels         map[string]string `json:"labels,omitempty"`
	GroupsCount    int               `json:"groupsCount"`
	RulesCount     int               `json:"rulesCount"`
	AlertsCount    int               `json:"alertsCount"`
	RecordingCount int               `json:"recordingCount"`
	Groups         []interface{}     `json:"groups,omitempty"`
}

// AlertRuleTemplateSummary represents alert rule template summary information
type AlertRuleTemplateSummary struct {
	Name            string                  `json:"name"`
	DisplayName     string                  `json:"displayName"`
	Title           string                  `json:"title"` // used by frontend
	Description     string                  `json:"description"`
	Category        string                  `json:"category"`
	Categories      []string                `json:"categories,omitempty"`
	PrimarySeverity string                  `json:"primarySeverity,omitempty"`
	Groups          []AlertRuleGroupSummary `json:"groups,omitempty"`
	Labels          map[string]string       `json:"labels,omitempty"`
	Annotations     map[string]string       `json:"annotations,omitempty"`
	Source          string                  `json:"source"`    // chart or embedded
	File            string                  `json:"file"`      // source filename
	Size            int64                   `json:"size"`      // file size
	UpdatedAt       string                  `json:"updatedAt"` // update time
}

// AlertRuleGroupSummary represents alert rule group summary information
// Field names match frontend AlertRuleGroupSummary interface
type AlertRuleGroupSummary struct {
	Name       string   `json:"name"`
	Rules      int      `json:"rules"`                // frontend uses rules instead of rulesCount
	Interval   string   `json:"interval,omitempty"`   // rule evaluation interval
	Severities []string `json:"severities,omitempty"` // list of severity levels within the group
}

// AlertRuleTemplateDetail represents alert rule template detailed information
type AlertRuleTemplateDetail struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
	Description string `json:"description"`
	Category    string `json:"category"`
	Content     string `json:"content"`
}

// ruleValidationOutcome represents rule validation result
type ruleValidationOutcome struct {
	Success  bool
	Message  string
	Details  []map[string]string
	Errors   []string
	Warnings []string
}

// ======================== Embedded Templates ========================

//go:embed templates/*.yaml
var embeddedTemplates embed.FS

// ======================== Constants ========================

const (
	// Template directory relative path in chart
	// Note: PrometheusRule templates are directly in templates directory, not alertrules subdirectory
	alertRulesDir = "charts/polardbx-monitor/templates"
)

var (
	// prometheusRuleGVR defines PrometheusRule resource GroupVersionResource
	prometheusRuleGVR = schema.GroupVersionResource{
		Group:    "monitoring.coreos.com",
		Version:  "v1",
		Resource: "prometheusrules",
	}

	// template directory cache
	templateDirCache    string
	templateDirCacheMu  sync.RWMutex
	templateDirResolved bool
)

// ======================== Handler ========================

// PrometheusRuleHandler handles alert rule related requests
type PrometheusRuleHandler struct {
	dynamicClient dynamic.Interface
}

// NewPrometheusRuleHandler creates new PrometheusRuleHandler instance
func NewPrometheusRuleHandler(dynamicClient dynamic.Interface) *PrometheusRuleHandler {
	return &PrometheusRuleHandler{
		dynamicClient: dynamicClient,
	}
}

// ======================== PrometheusRule CRUD ========================

// List gets PrometheusRule list
func (h *PrometheusRuleHandler) List(c *gin.Context) {
	namespace := c.Query("namespace")

	ctx := c.Request.Context()

	var list *unstructured.UnstructuredList
	var err error

	if namespace != "" {
		list, err = h.dynamicClient.Resource(prometheusRuleGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	} else {
		list, err = h.dynamicClient.Resource(prometheusRuleGVR).List(ctx, metav1.ListOptions{})
	}

	if err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("Failed to list PrometheusRules", err))
		return
	}

	rules := make([]PrometheusRule, 0, len(list.Items))
	for _, item := range list.Items {
		rule := parsePrometheusRule(item)
		rules = append(rules, rule)
	}

	apierr.OK(c, rules)
}

// GetYAML gets PrometheusRule YAML content
func (h *PrometheusRuleHandler) GetYAML(c *gin.Context) {
	namespace := c.Param("namespace")
	name := c.Param("name")

	if namespace == "" || name == "" {
		apierr.AbortWithError(c, apierr.ValidationError("namespace and name are required", nil))
		return
	}

	ctx := c.Request.Context()

	obj, err := h.dynamicClient.Resource(prometheusRuleGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("Failed to get PrometheusRule", err))
		return
	}

	// Remove managedFields for cleaner output
	unstructured.RemoveNestedField(obj.Object, "metadata", "managedFields")
	unstructured.RemoveNestedField(obj.Object, "metadata", "resourceVersion")
	unstructured.RemoveNestedField(obj.Object, "metadata", "uid")
	unstructured.RemoveNestedField(obj.Object, "metadata", "creationTimestamp")
	unstructured.RemoveNestedField(obj.Object, "metadata", "generation")

	yamlBytes, err := yaml.Marshal(obj.Object)
	if err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("Failed to marshal to YAML", err))
		return
	}

	apierr.OK(c, gin.H{
		"yaml": string(yamlBytes),
	})
}

// ValidateRule validates PrometheusRule YAML content
func (h *PrometheusRuleHandler) ValidateRule(c *gin.Context) {
	var body struct {
		YAML string `json:"yaml" binding:"required"`
	}

	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, apierr.ValidationError("Invalid request body: yaml field is required", nil))
		return
	}

	_, outcome := runRuleValidation(body.YAML)

	apierr.OK(c, gin.H{
		"success":  outcome.Success,
		"message":  outcome.Message,
		"details":  outcome.Details,
		"errors":   outcome.Errors,
		"warnings": outcome.Warnings,
	})
}

// ======================== Template Management ========================

// ListTemplates List all alert rule templates
func (h *PrometheusRuleHandler) ListTemplates(c *gin.Context) {
	templates, err := loadAllTemplates()
	if err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("Failed to load templates", err))
		return
	}

	// Get template directory path for frontend display
	directory := ""
	if dir, err := resolveAlertTemplateDir(); err == nil {
		directory = dir
	}

	// Return format expected by frontend: { items: [...], directory: "..." }
	apierr.OK(c, gin.H{
		"items":     templates,
		"directory": directory,
	})
}

// GetTemplate Get detailed information of specified template
func (h *PrometheusRuleHandler) GetTemplate(c *gin.Context) {
	templateName := c.Param("name")
	if templateName == "" {
		apierr.AbortWithError(c, apierr.ValidationError("template name is required", nil))
		return
	}

	template, err := loadTemplateDetail(templateName)
	if err != nil {
		apierr.AbortWithError(c, apierr.NotFoundError("template", templateName))
		return
	}

	apierr.OK(c, template)
}

// ApplyTemplate Apply template to cluster
func (h *PrometheusRuleHandler) ApplyTemplate(c *gin.Context) {
	var body struct {
		TemplateName string `json:"templateName" binding:"required"`
		Namespace    string `json:"namespace" binding:"required"`
		Name         string `json:"name"`
	}

	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	// Get template details
	template, err := loadTemplateDetail(body.TemplateName)
	if err != nil {
		apierr.AbortWithError(c, apierr.NotFoundError("template", body.TemplateName))
		return
	}

	// Parse YAML content
	var obj unstructured.Unstructured
	if err := yaml.Unmarshal([]byte(template.Content), &obj.Object); err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("Failed to parse template", err))
		return
	}

	// Set namespace
	obj.SetNamespace(body.Namespace)

	// If custom name is provided, use it
	if body.Name != "" {
		obj.SetName(body.Name)
	}

	ctx := c.Request.Context()

	// Try to create or update
	existing, err := h.dynamicClient.Resource(prometheusRuleGVR).Namespace(body.Namespace).Get(ctx, obj.GetName(), metav1.GetOptions{})
	if err == nil {
		// Resource already exists, perform update
		obj.SetResourceVersion(existing.GetResourceVersion())
		_, err = h.dynamicClient.Resource(prometheusRuleGVR).Namespace(body.Namespace).Update(ctx, &obj, metav1.UpdateOptions{})
		if err != nil {
			apierr.AbortWithError(c, apierr.InternalServiceError("Failed to update PrometheusRule", err))
			return
		}
		apierr.OK(c, gin.H{
			"message": "PrometheusRule updated successfully",
			"name":    obj.GetName(),
		})
	} else {
		// Create new resource
		_, err = h.dynamicClient.Resource(prometheusRuleGVR).Namespace(body.Namespace).Create(ctx, &obj, metav1.CreateOptions{})
		if err != nil {
			apierr.AbortWithError(c, apierr.InternalServiceError("Failed to create PrometheusRule", err))
			return
		}
		apierr.OK(c, gin.H{
			"message": "PrometheusRule created successfully",
			"name":    obj.GetName(),
		})
	}
}

// ======================== Helper Functions ========================

// parsePrometheusRule parses PrometheusRule from unstructured object
func parsePrometheusRule(item unstructured.Unstructured) PrometheusRule {
	rule := PrometheusRule{
		Name:      item.GetName(),
		Namespace: item.GetNamespace(),
		Labels:    item.GetLabels(),
	}

	// Parse groups
	groups, found, _ := unstructured.NestedSlice(item.Object, "spec", "groups")
	if found {
		rule.GroupsCount = len(groups)
		rule.Groups = groups

		for _, g := range groups {
			groupMap, ok := g.(map[string]interface{})
			if !ok {
				continue
			}

			rules, ok := groupMap["rules"].([]interface{})
			if !ok {
				continue
			}

			rule.RulesCount += len(rules)

			for _, r := range rules {
				ruleMap, ok := r.(map[string]interface{})
				if !ok {
					continue
				}

				if _, hasAlert := ruleMap["alert"]; hasAlert {
					rule.AlertsCount++
				}
				if _, hasRecord := ruleMap["record"]; hasRecord {
					rule.RecordingCount++
				}
			}
		}
	}

	return rule
}

// runRuleValidation validates PrometheusRule YAML content
func runRuleValidation(yamlContent string) (*unstructured.Unstructured, ruleValidationOutcome) {
	trimmed := strings.TrimSpace(yamlContent)
	if trimmed == "" {
		return nil, ruleValidationOutcome{
			Success: false,
			Message: "YAML content is empty",
			Details: []map[string]string{{
				"level":   "error",
				"message": "YAML content is required",
			}},
			Errors: []string{"YAML content is required"},
		}
	}

	var obj unstructured.Unstructured
	if err := yaml.Unmarshal([]byte(trimmed), &obj.Object); err != nil {
		errMsg := fmt.Sprintf("Invalid YAML format: %v", err)
		return nil, ruleValidationOutcome{
			Success: false,
			Message: errMsg,
			Details: []map[string]string{{
				"level":   "error",
				"message": errMsg,
			}},
			Errors: []string{errMsg},
		}
	}

	errors := []string{}
	warnings := []string{}
	details := []map[string]string{}

	// Validate apiVersion and kind
	if obj.GetAPIVersion() != "monitoring.coreos.com/v1" {
		errors = append(errors, "apiVersion should be 'monitoring.coreos.com/v1'")
	}
	if obj.GetKind() != "PrometheusRule" {
		errors = append(errors, "kind should be 'PrometheusRule'")
	}
	if obj.GetName() == "" {
		errors = append(errors, "metadata.name is required")
	}

	// Validate groups
	groups, found, _ := unstructured.NestedSlice(obj.Object, "spec", "groups")
	if !found {
		errors = append(errors, "spec.groups is required")
	} else if len(groups) == 0 {
		warnings = append(warnings, "No rule groups defined")
	}

	for i, groupInterface := range groups {
		groupMap, ok := groupInterface.(map[string]interface{})
		if !ok {
			errors = append(errors, fmt.Sprintf("Group %d: invalid structure", i))
			continue
		}

		name, hasName := groupMap["name"].(string)
		if !hasName || strings.TrimSpace(name) == "" {
			errors = append(errors, fmt.Sprintf("Group %d: name is required", i))
			continue
		}

		rulesInterface, ok := groupMap["rules"].([]interface{})
		if !ok {
			errors = append(errors, fmt.Sprintf("Group '%s': rules field must be an array", name))
			continue
		}
		if len(rulesInterface) == 0 {
			warnings = append(warnings, fmt.Sprintf("Group '%s': No rules defined", name))
		}

		for j, ruleInterface := range rulesInterface {
			ruleMap, ok := ruleInterface.(map[string]interface{})
			if !ok {
				errors = append(errors, fmt.Sprintf("Group '%s', Rule %d: invalid structure", name, j))
				continue
			}

			expr, hasExpr := ruleMap["expr"].(string)
			if !hasExpr || strings.TrimSpace(expr) == "" {
				errors = append(errors, fmt.Sprintf("Group '%s', Rule %d: expr is required", name, j))
			}

			_, hasAlert := ruleMap["alert"].(string)
			_, hasRecord := ruleMap["record"].(string)
			if !hasAlert && !hasRecord {
				errors = append(errors, fmt.Sprintf("Group '%s', Rule %d: either 'alert' or 'record' must be specified", name, j))
			}
			if hasAlert && hasRecord {
				errors = append(errors, fmt.Sprintf("Group '%s', Rule %d: cannot have both 'alert' and 'record'", name, j))
			}

			if hasExpr && strings.TrimSpace(expr) != "" && !IsValidPromQLBasic(expr) {
				warnings = append(warnings, fmt.Sprintf("Group '%s', Rule %d: potentially invalid PromQL expression", name, j))
			}
		}
	}

	// Build details
	for _, err := range errors {
		details = append(details, map[string]string{
			"level":   "error",
			"message": err,
		})
	}
	for _, warn := range warnings {
		details = append(details, map[string]string{
			"level":   "warning",
			"message": warn,
		})
	}

	success := len(errors) == 0
	message := "Validation passed"
	if !success {
		message = "Validation failed"
	} else if len(warnings) > 0 {
		message = "Validation passed with warnings"
	}

	return &obj, ruleValidationOutcome{
		Success:  success,
		Message:  message,
		Details:  details,
		Errors:   errors,
		Warnings: warnings,
	}
}

// IsValidPromQLBasic performs basic PromQL syntax check (exported for test compatibility)
func IsValidPromQLBasic(expr string) bool {
	// Basic check: bracket balance
	parenCount := 0
	braceCount := 0
	bracketCount := 0

	for _, ch := range expr {
		switch ch {
		case '(':
			parenCount++
		case ')':
			parenCount--
		case '{':
			braceCount++
		case '}':
			braceCount--
		case '[':
			bracketCount++
		case ']':
			bracketCount--
		}

		if parenCount < 0 || braceCount < 0 || bracketCount < 0 {
			return false
		}
	}

	return parenCount == 0 && braceCount == 0 && bracketCount == 0
}

// ======================== Template Loading ========================

// loadAllTemplates loads summary information of all templates
func loadAllTemplates() ([]AlertRuleTemplateSummary, error) {
	// First try to load chart templates from filesystem
	chartTemplates, err := LoadChartTemplates()
	if err == nil && len(chartTemplates) > 0 {
		return chartTemplates, nil
	}

	// Fallback to embedded templates
	return loadEmbeddedTemplates()
}

// LoadChartTemplates loads templates from chart directory (exported for test compatibility)
func LoadChartTemplates() ([]AlertRuleTemplateSummary, error) {
	templateDir, err := resolveAlertTemplateDir()
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(templateDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read template directory: %w", err)
	}

	templates := []AlertRuleTemplateSummary{}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}

		filePath := filepath.Join(templateDir, entry.Name())

		// Get file information
		fileInfo, err := os.Stat(filePath)
		var fileSize int64
		var updatedAt string
		if err == nil {
			fileSize = fileInfo.Size()
			updatedAt = fileInfo.ModTime().Format("2006-01-02T15:04:05Z")
		}

		content, err := os.ReadFile(filePath)
		if err != nil {
			continue
		}

		// Process Helm template content
		cleanContent := sanitizeHelmPlaceholders(string(content))

		// Parse YAML that may contain multiple documents
		docs := splitYAMLDocuments(cleanContent)

		for _, doc := range docs {
			template, err := parseTemplateFromYAMLWithInfo(doc, entry.Name(), "chart", fileSize, updatedAt)
			if err != nil {
				continue
			}
			templates = append(templates, template)
		}
	}

	// Sort by name
	sort.Slice(templates, func(i, j int) bool {
		return templates[i].Name < templates[j].Name
	})

	return templates, nil
}

// loadEmbeddedTemplates loads templates from embedded resources
func loadEmbeddedTemplates() ([]AlertRuleTemplateSummary, error) {
	templates := []AlertRuleTemplateSummary{}

	err := fs.WalkDir(embeddedTemplates, "templates", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".yaml") {
			return nil
		}

		content, err := embeddedTemplates.ReadFile(path)
		if err != nil {
			return nil
		}

		// Get embedded file information
		fileInfo, _ := d.Info()
		var fileSize int64
		var updatedAt string
		if fileInfo != nil {
			fileSize = fileInfo.Size()
			updatedAt = fileInfo.ModTime().Format("2006-01-02T15:04:05Z")
		}

		template, err := parseTemplateFromYAMLWithInfo(string(content), d.Name(), "embedded", fileSize, updatedAt)
		if err != nil {
			return nil
		}

		templates = append(templates, template)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return templates, nil
}

// loadTemplateDetail loads template detailed information
func loadTemplateDetail(templateName string) (*AlertRuleTemplateDetail, error) {
	// First try to load from chart
	templateDir, err := resolveAlertTemplateDir()
	if err == nil {
		entries, err := os.ReadDir(templateDir)
		if err == nil {
			for _, entry := range entries {
				if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
					continue
				}

				filePath := filepath.Join(templateDir, entry.Name())
				content, err := os.ReadFile(filePath)
				if err != nil {
					continue
				}

				cleanContent := sanitizeHelmPlaceholders(string(content))
				docs := splitYAMLDocuments(cleanContent)

				for _, doc := range docs {
					var obj map[string]interface{}
					if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
						continue
					}

					metadata, _ := obj["metadata"].(map[string]interface{})
					name, _ := metadata["name"].(string)

					if name == templateName {
						return &AlertRuleTemplateDetail{
							Name:        name,
							DisplayName: formatDisplayName(name),
							Description: extractDescription(obj),
							Category:    extractCategory(entry.Name()),
							Content:     doc,
						}, nil
					}
				}
			}
		}
	}

	// Fallback to embedded template
	return loadEmbeddedTemplateDetail(templateName)
}

// loadEmbeddedTemplateDetail loads template details from embedded resources
func loadEmbeddedTemplateDetail(templateName string) (*AlertRuleTemplateDetail, error) {
	var result *AlertRuleTemplateDetail

	err := fs.WalkDir(embeddedTemplates, "templates", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".yaml") {
			return nil
		}

		content, err := embeddedTemplates.ReadFile(path)
		if err != nil {
			return nil
		}

		var obj map[string]interface{}
		if err := yaml.Unmarshal(content, &obj); err != nil {
			return nil
		}

		metadata, _ := obj["metadata"].(map[string]interface{})
		name, _ := metadata["name"].(string)

		if name == templateName {
			result = &AlertRuleTemplateDetail{
				Name:        name,
				DisplayName: formatDisplayName(name),
				Description: extractDescription(obj),
				Category:    extractCategory(d.Name()),
				Content:     string(content),
			}
			return fs.SkipAll
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	if result == nil {
		return nil, fmt.Errorf("template not found: %s", templateName)
	}

	return result, nil
}

// resolveAlertTemplateDir resolves alert template directory path
func resolveAlertTemplateDir() (string, error) {
	templateDirCacheMu.RLock()
	if templateDirResolved {
		dir := templateDirCache
		templateDirCacheMu.RUnlock()
		if dir == "" {
			return "", fmt.Errorf("template directory not found")
		}
		return dir, nil
	}
	templateDirCacheMu.RUnlock()

	templateDirCacheMu.Lock()
	defer templateDirCacheMu.Unlock()

	// Double-check
	if templateDirResolved {
		if templateDirCache == "" {
			return "", fmt.Errorf("template directory not found")
		}
		return templateDirCache, nil
	}

	// Try different paths
	possiblePaths := []string{
		alertRulesDir,
		filepath.Join("..", alertRulesDir),
		filepath.Join("..", "..", alertRulesDir),
		"/app/charts/polardbx-monitor/templates",
	}

	// Add paths based on executable location
	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		possiblePaths = append(possiblePaths,
			filepath.Join(exeDir, alertRulesDir),
			filepath.Join(exeDir, "..", alertRulesDir),
		)
	}

	for _, path := range possiblePaths {
		if info, err := os.Stat(path); err == nil && info.IsDir() {
			templateDirCache = path
			templateDirResolved = true
			return path, nil
		}
	}

	templateDirResolved = true
	templateDirCache = ""
	return "", fmt.Errorf("template directory not found in any of the expected locations")
}

// splitYAMLDocuments splits multi-document YAML
func splitYAMLDocuments(content string) []string {
	docs := []string{}
	separator := regexp.MustCompile(`(?m)^---\s*$`)
	parts := separator.Split(content, -1)

	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			docs = append(docs, trimmed)
		}
	}

	return docs
}

// sanitizeHelmPlaceholders cleans Helm template placeholders
func sanitizeHelmPlaceholders(content string) string {
	// Process conditional blocks - remove entire if/else/end structures
	ifPattern := regexp.MustCompile(`\{\{-?\s*if[^}]*\}\}`)
	elsePattern := regexp.MustCompile(`\{\{-?\s*else[^}]*\}\}`)
	endPattern := regexp.MustCompile(`\{\{-?\s*end\s*-?\}\}`)
	rangePattern := regexp.MustCompile(`\{\{-?\s*range[^}]*\}\}`)
	withPattern := regexp.MustCompile(`\{\{-?\s*with[^}]*\}\}`)

	result := content
	result = ifPattern.ReplaceAllString(result, "")
	result = elsePattern.ReplaceAllString(result, "")
	result = endPattern.ReplaceAllString(result, "")
	result = rangePattern.ReplaceAllString(result, "")
	result = withPattern.ReplaceAllString(result, "")

	// Process include statements
	includePattern := regexp.MustCompile(`\{\{-?\s*include\s+"[^"]*"\s*\.\s*\|\s*nindent\s+\d+\s*-?\}\}`)
	result = includePattern.ReplaceAllString(result, "")

	// Process simple value replacements {{ .Values.xxx }}
	valuePattern := regexp.MustCompile(`\{\{[^}]+\}\}`)
	result = valuePattern.ReplaceAllStringFunc(result, func(match string) string {
		// Keep some common default values
		if strings.Contains(match, ".Release.Namespace") {
			return "default"
		}
		if strings.Contains(match, ".Release.Name") {
			return "polardbx-monitor"
		}
		return ""
	})

	return result
}

// parseTemplateFromYAML parses template summary from YAML content
func parseTemplateFromYAML(content string, filename string) (AlertRuleTemplateSummary, error) {
	return parseTemplateFromYAMLWithInfo(content, filename, "chart", 0, "")
}

// parseTemplateFromYAMLWithInfo parses template summary from YAML content (with file info)
func parseTemplateFromYAMLWithInfo(content string, filename string, source string, size int64, updatedAt string) (AlertRuleTemplateSummary, error) {
	var obj map[string]interface{}
	if err := yaml.Unmarshal([]byte(content), &obj); err != nil {
		return AlertRuleTemplateSummary{}, err
	}

	// Validate it's a PrometheusRule
	kind, _ := obj["kind"].(string)
	if kind != "PrometheusRule" {
		return AlertRuleTemplateSummary{}, fmt.Errorf("not a PrometheusRule")
	}

	metadata, _ := obj["metadata"].(map[string]interface{})
	name, _ := metadata["name"].(string)
	if name == "" {
		return AlertRuleTemplateSummary{}, fmt.Errorf("missing name")
	}

	displayName := formatDisplayName(name)
	category := extractCategory(filename)

	// Extract labels and annotations
	labels := make(map[string]string)
	if labelsRaw, ok := metadata["labels"].(map[string]interface{}); ok {
		for k, v := range labelsRaw {
			if s, ok := v.(string); ok {
				labels[k] = s
			}
		}
	}
	annotations := make(map[string]string)
	if annotationsRaw, ok := metadata["annotations"].(map[string]interface{}); ok {
		for k, v := range annotationsRaw {
			if s, ok := v.(string); ok {
				annotations[k] = s
			}
		}
	}

	template := AlertRuleTemplateSummary{
		Name:            name,
		DisplayName:     displayName,
		Title:           displayName, // Frontend uses title field
		Description:     extractDescription(obj),
		Category:        category,
		Categories:      []string{category},
		PrimarySeverity: extractPrimarySeverity(obj),
		Groups:          []AlertRuleGroupSummary{},
		Labels:          labels,
		Annotations:     annotations,
		Source:          source,
		File:            filename,
		Size:            size,
		UpdatedAt:       updatedAt,
	}

	// Parse groups
	spec, _ := obj["spec"].(map[string]interface{})
	groups, _ := spec["groups"].([]interface{})

	for _, g := range groups {
		groupMap, ok := g.(map[string]interface{})
		if !ok {
			continue
		}

		groupName, _ := groupMap["name"].(string)
		interval, _ := groupMap["interval"].(string)
		rules, _ := groupMap["rules"].([]interface{})

		// Collect all severity levels within this group
		severitySet := make(map[string]bool)
		for _, r := range rules {
			ruleMap, ok := r.(map[string]interface{})
			if !ok {
				continue
			}
			ruleLabels, _ := ruleMap["labels"].(map[string]interface{})
			if sev, ok := ruleLabels["severity"].(string); ok && sev != "" {
				severitySet[sev] = true
			}
		}
		severities := make([]string, 0, len(severitySet))
		for sev := range severitySet {
			severities = append(severities, sev)
		}

		template.Groups = append(template.Groups, AlertRuleGroupSummary{
			Name:       groupName,
			Rules:      len(rules),
			Interval:   interval,
			Severities: severities,
		})
	}

	return template, nil
}

// extractPrimarySeverity extracts primary severity level from object
func extractPrimarySeverity(obj map[string]interface{}) string {
	spec, _ := obj["spec"].(map[string]interface{})
	groups, _ := spec["groups"].([]interface{})

	severityCounts := make(map[string]int)
	for _, g := range groups {
		groupMap, ok := g.(map[string]interface{})
		if !ok {
			continue
		}
		rules, _ := groupMap["rules"].([]interface{})
		for _, r := range rules {
			ruleMap, ok := r.(map[string]interface{})
			if !ok {
				continue
			}
			labels, _ := ruleMap["labels"].(map[string]interface{})
			if sev, ok := labels["severity"].(string); ok {
				severityCounts[sev]++
			}
		}
	}

	// Return the most common severity level
	maxCount := 0
	primarySev := ""
	for sev, count := range severityCounts {
		if count > maxCount {
			maxCount = count
			primarySev = sev
		}
	}
	return primarySev
}

// formatDisplayName formats display name
func formatDisplayName(name string) string {
	// Remove common prefixes
	name = strings.TrimPrefix(name, "polardbx-")
	name = strings.TrimPrefix(name, "pxc-")

	// Convert to title format
	parts := strings.Split(name, "-")
	for i, part := range parts {
		if len(part) > 0 {
			parts[i] = strings.ToUpper(part[:1]) + part[1:]
		}
	}

	return strings.Join(parts, " ")
}

// extractDescription extracts description from object
func extractDescription(obj map[string]interface{}) string {
	metadata, _ := obj["metadata"].(map[string]interface{})
	annotations, _ := metadata["annotations"].(map[string]interface{})

	if desc, ok := annotations["description"].(string); ok {
		return desc
	}

	// Try to generate description from name
	name, _ := metadata["name"].(string)
	return fmt.Sprintf("Alert rules for %s", formatDisplayName(name))
}

// extractCategory extracts category from filename
func extractCategory(filename string) string {
	// Infer category based on filename
	lower := strings.ToLower(filename)

	categories := map[string]string{
		"cn":      "CN (Compute Node)",
		"dn":      "DN (Data Node)",
		"gms":     "GMS (Global Meta Service)",
		"cdc":     "CDC (Change Data Capture)",
		"storage": "Storage",
		"cluster": "Cluster",
	}

	for key, category := range categories {
		if strings.Contains(lower, key) {
			return category
		}
	}

	return "General"
}

// ======================== Context-based Functions ========================

// ListWithContext uses context to get PrometheusRule list (for internal calls)
func (h *PrometheusRuleHandler) ListWithContext(ctx context.Context, namespace string) ([]PrometheusRule, error) {
	var list *unstructured.UnstructuredList
	var err error

	if namespace != "" {
		list, err = h.dynamicClient.Resource(prometheusRuleGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	} else {
		list, err = h.dynamicClient.Resource(prometheusRuleGVR).List(ctx, metav1.ListOptions{})
	}

	if err != nil {
		return nil, err
	}

	rules := make([]PrometheusRule, 0, len(list.Items))
	for _, item := range list.Items {
		rule := parsePrometheusRule(item)
		rules = append(rules, rule)
	}

	return rules, nil
}

// GetYAMLWithContext uses context to get YAML (for internal calls)
func (h *PrometheusRuleHandler) GetYAMLWithContext(ctx context.Context, namespace, name string) (string, error) {
	obj, err := h.dynamicClient.Resource(prometheusRuleGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}

	unstructured.RemoveNestedField(obj.Object, "metadata", "managedFields")
	unstructured.RemoveNestedField(obj.Object, "metadata", "resourceVersion")
	unstructured.RemoveNestedField(obj.Object, "metadata", "uid")
	unstructured.RemoveNestedField(obj.Object, "metadata", "creationTimestamp")
	unstructured.RemoveNestedField(obj.Object, "metadata", "generation")

	yamlBytes, err := yaml.Marshal(obj.Object)
	if err != nil {
		return "", err
	}

	return string(yamlBytes), nil
}

// ValidateWithContext uses context to validate rules (for internal calls)
func (h *PrometheusRuleHandler) ValidateWithContext(yamlContent string) ruleValidationOutcome {
	_, outcome := runRuleValidation(yamlContent)
	return outcome
}

// ApplyTemplateWithContext uses context to apply template (for internal calls)
func (h *PrometheusRuleHandler) ApplyTemplateWithContext(ctx context.Context, templateName, namespace, name string) error {
	template, err := loadTemplateDetail(templateName)
	if err != nil {
		return err
	}

	var obj unstructured.Unstructured
	if err := yaml.Unmarshal([]byte(template.Content), &obj.Object); err != nil {
		return err
	}

	obj.SetNamespace(namespace)
	if name != "" {
		obj.SetName(name)
	}

	existing, err := h.dynamicClient.Resource(prometheusRuleGVR).Namespace(namespace).Get(ctx, obj.GetName(), metav1.GetOptions{})
	if err == nil {
		obj.SetResourceVersion(existing.GetResourceVersion())
		_, err = h.dynamicClient.Resource(prometheusRuleGVR).Namespace(namespace).Update(ctx, &obj, metav1.UpdateOptions{})
	} else {
		_, err = h.dynamicClient.Resource(prometheusRuleGVR).Namespace(namespace).Create(ctx, &obj, metav1.CreateOptions{})
	}

	return err
}

// ======================== Package-level Functions (for main.go) ========================

// getHandler gets handler from context
func getHandler(c *gin.Context) (*PrometheusRuleHandler, bool) {
	dynClient, ok := util.DynamicClientFromContext(c)
	if !ok {
		return nil, false
	}
	return NewPrometheusRuleHandler(dynClient), true
}

// List package-level function for main.go route registration
// List lists all PrometheusRule resources
// @Summary List PrometheusRules
// @Description Lists all PrometheusRule custom resources, optionally filtered by namespace
// @Tags prometheus-rules
// @Accept json
// @Produce json
// @Param namespace query string false "Kubernetes namespace filter (optional, lists all namespaces if not specified)"
// @Success 200 {array} PrometheusRule "List of PrometheusRule resources"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/prometheus-rules [get]
func List(c *gin.Context) {
	h, ok := getHandler(c)
	if !ok {
		return
	}
	h.List(c)
}

// GetYAML package-level function for main.go route registration
// @Summary Get PrometheusRule YAML
// @Description Retrieves YAML content of a specific PrometheusRule resource
// @Tags prometheus-rules
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace"
// @Param name path string true "Name of the PrometheusRule"
// @Success 200 {object} map[string]string "YAML content of the PrometheusRule"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request parameters"
// @Failure 404 {object} apierr.ErrorResponse "PrometheusRule not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/prometheus-rules/{namespace}/{name}/yaml [get]
func GetYAML(c *gin.Context) {
	h, ok := getHandler(c)
	if !ok {
		return
	}
	h.GetYAML(c)
}

// ValidateRule package-level function for main.go route registration
// @Summary Validate PrometheusRule
// @Description Validates PrometheusRule YAML content for syntax and PromQL correctness
// @Tags prometheus-rules
// @Accept json
// @Produce json
// @Param body body map[string]string true "YAML content to validate (key: 'yaml')"
// @Success 200 {object} map[string]any "Validation result with success status, message, details, errors, and warnings"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request or YAML syntax error"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/prometheus-rules/validate [post]
func ValidateRule(c *gin.Context) {
	h, ok := getHandler(c)
	if !ok {
		return
	}
	h.ValidateRule(c)
}

// ListTemplates package-level function for main.go route registration
// @Summary List alert rule templates
// @Description Lists all available PrometheusRule alert rule templates from embedded files and Helm chart
// @Tags prometheus-rules
// @Accept json
// @Produce json
// @Success 200 {array} AlertRuleTemplateSummary "List of alert rule templates with metadata"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/prometheus-rules/templates [get]
func ListTemplates(c *gin.Context) {
	h, ok := getHandler(c)
	if !ok {
		return
	}
	h.ListTemplates(c)
}

// GetTemplate package-level function for main.go route registration
// @Summary Get alert rule template
// @Description Retrieves detailed content of a specific alert rule template
// @Tags prometheus-rules
// @Accept json
// @Produce json
// @Param name path string true "Name of the alert rule template"
// @Success 200 {object} AlertRuleTemplateDetail "Template details with content"
// @Failure 404 {object} apierr.ErrorResponse "Template not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/prometheus-rules/templates/{name} [get]
func GetTemplate(c *gin.Context) {
	h, ok := getHandler(c)
	if !ok {
		return
	}
	h.GetTemplate(c)
}

// ApplyTemplate package-level function for main.go route registration
// @Summary Apply alert rule template
// @Description Applies an alert rule template to create or update PrometheusRule resources
// @Tags prometheus-rules
// @Accept json
// @Produce json
// @Param body body map[string]any true "Apply request with template name, namespace, and optional overrides"
// @Success 200 {object} map[string]any "Apply result with created/updated resources"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request or template not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/prometheus-rules/templates/apply [post]
func ApplyTemplate(c *gin.Context) {
	h, ok := getHandler(c)
	if !ok {
		return
	}
	h.ApplyTemplate(c)
}
