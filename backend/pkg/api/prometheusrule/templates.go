package prometheusrule

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	"polardbx-ui-backend/pkg/api/util"

	"github.com/gin-gonic/gin"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

const alertTemplateDirEnv = "POLARDBX_ALERT_RULE_TEMPLATES_DIR"

var helmPlaceholderReplacer = strings.NewReplacer(
	"{{ .Release.Name }}", "polardbx-monitor",
	"{{.Release.Name}}", "polardbx-monitor",
	"{{- .Release.Name }}", "polardbx-monitor",
	"{{ .Release.Name -}}", "polardbx-monitor",
	"{{- .Release.Name -}}", "polardbx-monitor",
	"{{ .Release.Namespace }}", "polardbx-monitor",
	"{{.Release.Namespace}}", "polardbx-monitor",
	"{{- .Release.Namespace }}", "polardbx-monitor",
	"{{ .Release.Namespace -}}", "polardbx-monitor",
	"{{- .Release.Namespace -}}", "polardbx-monitor",
	"{{ .Release.Service }}", "Helm",
	"{{.Release.Service}}", "Helm",
	"{{- .Release.Service }}", "Helm",
	"{{ .Release.Service -}}", "Helm",
	"{{- .Release.Service -}}", "Helm",
)

func sanitizeHelmPlaceholders(data []byte) []byte {
	if len(data) == 0 {
		return data
	}
	sanitized := helmPlaceholderReplacer.Replace(string(data))
	return []byte(sanitized)
}

// AlertRuleTemplateSummary represents metadata for a rule template.
type AlertRuleTemplateSummary struct {
	Name            string                  `json:"name"`
	Title           string                  `json:"title"`
	Description     string                  `json:"description"`
	Categories      []string                `json:"categories,omitempty"`
	PrimarySeverity string                  `json:"primarySeverity,omitempty"`
	Groups          []AlertRuleGroupSummary `json:"groups,omitempty"`
	Labels          map[string]string       `json:"labels,omitempty"`
	Annotations     map[string]string       `json:"annotations,omitempty"`
	Source          string                  `json:"source"`
	File            string                  `json:"file"`
	Size            int64                   `json:"size"`
	UpdatedAt       string                  `json:"updatedAt"`
}

// AlertRuleGroupSummary describes basic information about a Prometheus rule group.
type AlertRuleGroupSummary struct {
	Name       string   `json:"name"`
	Rules      int      `json:"rules"`
	Interval   string   `json:"interval,omitempty"`
	Severities []string `json:"severities,omitempty"`
}

// AlertRuleTemplateDetail includes full YAML content of a template.
type AlertRuleTemplateDetail struct {
	AlertRuleTemplateSummary
	Content string `json:"content"`
}

// ListTemplates returns available PrometheusRule templates.
func ListTemplates(c *gin.Context) {
	if dir, err := resolveAlertTemplateDir(); err == nil {
		summaries, err := loadAlertTemplateSummaries(dir)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load alert rule templates", "details": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"items": summaries, "directory": dir})
		return
	} else if !errors.Is(err, os.ErrNotExist) {
		status := http.StatusInternalServerError
		if errors.Is(err, os.ErrPermission) {
			status = http.StatusForbidden
		}
		c.JSON(status, gin.H{"error": "failed to resolve alert template directory", "details": err.Error()})
		return
	}

	templates, baseDir, err := loadChartTemplates()
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, os.ErrNotExist) {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"error": "failed to load built-in alert templates", "details": err.Error()})
		return
	}

	summaries := make([]AlertRuleTemplateSummary, 0, len(templates))
	for _, detail := range templates {
		summaries = append(summaries, detail.AlertRuleTemplateSummary)
	}

	sort.SliceStable(summaries, func(i, j int) bool {
		if summaries[i].Title == summaries[j].Title {
			return summaries[i].Name < summaries[j].Name
		}
		return summaries[i].Title < summaries[j].Title
	})

	c.JSON(http.StatusOK, gin.H{
		"items":     summaries,
		"directory": baseDir,
	})
}

// GetTemplate returns detail of a specific PrometheusRule template.
func GetTemplate(c *gin.Context) {
	name := c.Param("name")
	if strings.TrimSpace(name) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "template name is required"})
		return
	}

	if dir, err := resolveAlertTemplateDir(); err == nil {
		detail, err := loadAlertTemplateDetail(dir, name)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				c.JSON(http.StatusNotFound, gin.H{"error": "template not found", "name": name})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load template", "details": err.Error()})
			return
		}
		c.JSON(http.StatusOK, detail)
		return
	} else if !errors.Is(err, os.ErrNotExist) {
		status := http.StatusInternalServerError
		if errors.Is(err, os.ErrPermission) {
			status = http.StatusForbidden
		}
		c.JSON(status, gin.H{"error": "failed to resolve alert template directory", "details": err.Error()})
		return
	}

	templates, _, err := loadChartTemplates()
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, os.ErrNotExist) {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"error": "failed to load built-in alert templates", "details": err.Error()})
		return
	}

	normalized := normalizeTemplateKey(name)
	if detail, ok := templates[normalized]; ok {
		c.JSON(http.StatusOK, detail)
		return
	}

	c.JSON(http.StatusNotFound, gin.H{"error": "template not found", "name": name})
}

// ApplyTemplate creates or updates a PrometheusRule from template content.
func ApplyTemplate(c *gin.Context) {
	var payload struct {
		Template    string            `json:"template"`
		Namespace   string            `json:"namespace"`
		Name        string            `json:"name"`
		Content     string            `json:"content"`
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
		Overwrite   bool              `json:"overwrite"`
		DryRun      bool              `json:"dryRun"`
	}

	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload", "details": err.Error()})
		return
	}

	sourceContent := strings.TrimSpace(payload.Content)
	if sourceContent == "" {
		if strings.TrimSpace(payload.Template) == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "either template or content must be provided"})
			return
		}

		if dir, err := resolveAlertTemplateDir(); err == nil {
			fileName := ensureTemplateFileName(payload.Template)
			data, err := os.ReadFile(filepath.Join(dir, fileName))
			if err != nil {
				if os.IsNotExist(err) {
					c.JSON(http.StatusNotFound, gin.H{"error": "template not found", "name": payload.Template})
					return
				}
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read template", "details": err.Error()})
				return
			}
			sourceContent = string(data)
		} else if errors.Is(err, os.ErrNotExist) {
			templates, _, loadErr := loadChartTemplates()
			if loadErr != nil {
				status := http.StatusInternalServerError
				if errors.Is(loadErr, os.ErrNotExist) {
					status = http.StatusNotFound
				}
				c.JSON(status, gin.H{"error": "failed to load built-in alert templates", "details": loadErr.Error()})
				return
			}

			key := normalizeTemplateKey(payload.Template)
			detail, ok := templates[key]
			if !ok {
				c.JSON(http.StatusNotFound, gin.H{"error": "template not found", "name": payload.Template})
				return
			}
			sourceContent = detail.Content
		} else {
			status := http.StatusInternalServerError
			if errors.Is(err, os.ErrPermission) {
				status = http.StatusForbidden
			}
			c.JSON(status, gin.H{"error": "failed to resolve alert template directory", "details": err.Error()})
			return
		}
	}

	obj, validation := runRuleValidation(sourceContent)
	if !validation.Success {
		c.JSON(http.StatusBadRequest, gin.H{"error": "validation failed", "details": validation.Details})
		return
	}

	if obj == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unable to parse template content"})
		return
	}

	if payload.Name != "" {
		obj.SetName(payload.Name)
	}
	if obj.GetName() == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "metadata.name is required"})
		return
	}

	namespace := payload.Namespace
	if strings.TrimSpace(namespace) == "" {
		namespace = util.DefaultNamespace(c, "polardbx-monitor")
	}
	obj.SetNamespace(namespace)

	if len(payload.Labels) > 0 {
		labels := obj.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		for k, v := range payload.Labels {
			labels[k] = v
		}
		obj.SetLabels(labels)
	}

	if len(payload.Annotations) > 0 {
		ann := obj.GetAnnotations()
		if ann == nil {
			ann = map[string]string{}
		}
		for k, v := range payload.Annotations {
			ann[k] = v
		}
		obj.SetAnnotations(ann)
	}

	if payload.DryRun {
		resp := gin.H{
			"name":      obj.GetName(),
			"namespace": obj.GetNamespace(),
			"overwrite": payload.Overwrite,
			"validation": gin.H{
				"success":  validation.Success,
				"message":  validation.Message,
				"details":  validation.Details,
				"warnings": validation.Warnings,
			},
		}
		c.JSON(http.StatusOK, resp)
		return
	}

	dynClient, ok := util.DynamicClientFromContext(c)
	if !ok {
		return
	}

	ctx, cancel := util.CrudCtx(c)
	defer cancel()

	resource := dynClient.Resource(prometheusRuleGVR).Namespace(obj.GetNamespace())

	if payload.Overwrite {
		existing, err := resource.Get(ctx, obj.GetName(), metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				if _, err = resource.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
					util.HandleK8sError(c, "create PrometheusRule", err)
					return
				}
			} else {
				util.HandleK8sError(c, "get PrometheusRule", err)
				return
			}
		} else {
			obj.SetResourceVersion(existing.GetResourceVersion())
			if _, err = resource.Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
				util.HandleK8sError(c, "update PrometheusRule", err)
				return
			}
		}
	} else {
		if _, err := resource.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
			if apierrors.IsAlreadyExists(err) {
				c.JSON(http.StatusConflict, gin.H{"error": "PrometheusRule already exists", "name": obj.GetName(), "namespace": obj.GetNamespace()})
				return
			}
			util.HandleK8sError(c, "create PrometheusRule", err)
			return
		}
	}

	response := gin.H{
		"name":      obj.GetName(),
		"namespace": obj.GetNamespace(),
		"message":   "PrometheusRule applied",
		"overwrite": payload.Overwrite,
	}
	if len(validation.Warnings) > 0 {
		response["warnings"] = validation.Warnings
	}

	c.JSON(http.StatusOK, response)
}

func resolveAlertTemplateDir() (string, error) {
	if env := strings.TrimSpace(os.Getenv(alertTemplateDirEnv)); env != "" {
		absEnv, err := filepath.Abs(env)
		if err != nil {
			return "", fmt.Errorf("resolve %s: %w", alertTemplateDirEnv, err)
		}
		if info, err := os.Stat(absEnv); err == nil && info.IsDir() {
			return absEnv, nil
		}
	}

	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("getwd: %w", err)
	}

	target := filepath.Join("charts", "polardbx-monitor", "alert-templates")
	dir := cwd
	for i := 0; i < 8; i++ {
		candidate := filepath.Join(dir, target)
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return "", os.ErrNotExist
}

func loadAlertTemplateSummaries(dir string) ([]AlertRuleTemplateSummary, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	summaries := make([]AlertRuleTemplateSummary, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !isYAMLFile(name) {
			continue
		}
		summary, err := readAlertTemplateSummary(dir, name)
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

func loadAlertTemplateDetail(dir, name string) (AlertRuleTemplateDetail, error) {
	fileName := ensureTemplateFileName(name)
	path := filepath.Join(dir, fileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return AlertRuleTemplateDetail{}, os.ErrNotExist
		}
		return AlertRuleTemplateDetail{}, err
	}

	summary, err := readAlertTemplateSummaryWithData(dir, fileName, data)
	if err != nil {
		return AlertRuleTemplateDetail{}, err
	}

	return AlertRuleTemplateDetail{
		AlertRuleTemplateSummary: summary,
		Content:                  string(data),
	}, nil
}

func readAlertTemplateSummary(dir, fileName string) (AlertRuleTemplateSummary, error) {
	data, err := os.ReadFile(filepath.Join(dir, fileName))
	if err != nil {
		return AlertRuleTemplateSummary{}, err
	}
	return readAlertTemplateSummaryWithData(dir, fileName, data)
}

func readAlertTemplateSummaryWithData(dir, fileName string, data []byte) (AlertRuleTemplateSummary, error) {
	info, err := os.Stat(filepath.Join(dir, fileName))
	if err != nil {
		return AlertRuleTemplateSummary{}, err
	}

	var obj unstructured.Unstructured
	if err := yaml.Unmarshal(data, &obj.Object); err != nil {
		return AlertRuleTemplateSummary{}, err
	}

	title := strings.TrimSpace(obj.GetAnnotations()["polardbx.com/template-title"])
	if title == "" {
		title = obj.GetName()
	}
	if title == "" {
		title = fallbackTitle(strings.TrimSuffix(fileName, filepath.Ext(fileName)))
	}

	description := strings.TrimSpace(obj.GetAnnotations()["polardbx.com/template-description"])
	if description == "" {
		description = guessDescriptionFromRules(&obj)
	}

	categories := parseCategories(obj.GetLabels()["polardbx.com/template-category"])
	primarySeverity := firstSeverity(&obj)
	groups := summarizeGroups(&obj)

	rel := filepath.ToSlash(filepath.Join(
		filepath.Base(filepath.Dir(filepath.Dir(dir))),
		filepath.Base(filepath.Dir(dir)),
		filepath.Base(dir),
		fileName,
	))

	return AlertRuleTemplateSummary{
		Name:            strings.TrimSuffix(fileName, filepath.Ext(fileName)),
		Title:           title,
		Description:     description,
		Categories:      categories,
		PrimarySeverity: primarySeverity,
		Groups:          groups,
		Labels:          obj.GetLabels(),
		Annotations:     obj.GetAnnotations(),
		Source:          rel,
		File:            fileName,
		Size:            info.Size(),
		UpdatedAt:       info.ModTime().UTC().Format(time.RFC3339),
	}, nil
}

func splitYAMLDocuments(data []byte) ([][]byte, error) {
	reader := yamlutil.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
	var docs [][]byte
	for {
		doc, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

func loadChartTemplates() (map[string]AlertRuleTemplateDetail, string, error) {
	dir, err := resolveChartTemplatesDir()
	if err != nil {
		return nil, "", err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, "", err
	}

	templates := make(map[string]AlertRuleTemplateDetail)
	nameCounts := make(map[string]int)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, "", err
		}

		data = sanitizeHelmPlaceholders(data)

		docs, err := splitYAMLDocuments(data)
		if err != nil {
			return nil, "", err
		}

		info, err := entry.Info()
		if err != nil {
			return nil, "", err
		}

		relSource := filepath.ToSlash(filepath.Join(
			filepath.Base(filepath.Dir(filepath.Dir(dir))),
			filepath.Base(filepath.Dir(dir)),
			filepath.Base(dir),
			entry.Name(),
		))

		for _, doc := range docs {
			if len(bytes.TrimSpace(doc)) == 0 {
				continue
			}

			var obj unstructured.Unstructured
			if err := yaml.Unmarshal(doc, &obj.Object); err != nil {
				continue
			}

			if !strings.EqualFold(obj.GetKind(), "PrometheusRule") {
				continue
			}

			groups, found, _ := unstructured.NestedSlice(obj.Object, "spec", "groups")
			if !found || len(groups) == 0 {
				continue
			}

			for _, groupInterface := range groups {
				groupMap, ok := groupInterface.(map[string]interface{})
				if !ok {
					continue
				}

				groupName, _ := groupMap["name"].(string)
				if strings.TrimSpace(groupName) == "" {
					groupName = obj.GetName()
				}

				groupObj := obj.DeepCopy()
				if err := unstructured.SetNestedSlice(groupObj.Object, []interface{}{groupMap}, "spec", "groups"); err != nil {
					continue
				}

				templateName := buildTemplateName(obj.GetName(), groupName, nameCounts)
				groupObj.SetName(templateName)

				annotations := groupObj.GetAnnotations()
				if annotations == nil {
					annotations = map[string]string{}
				}
				annotations["polardbx.com/template-source"] = relSource + "#" + groupName
				groupObj.SetAnnotations(annotations)

				content, err := yaml.Marshal(groupObj.Object)
				if err != nil {
					continue
				}

				summary := AlertRuleTemplateSummary{
					Name:            templateName,
					Title:           deriveTemplateTitle(groupName, templateName, groupObj),
					Description:     guessDescriptionFromRules(groupObj),
					Categories:      deriveCategoriesFromGroup(groupName),
					PrimarySeverity: firstSeverity(groupObj),
					Groups:          summarizeGroups(groupObj),
					Labels:          groupObj.GetLabels(),
					Annotations:     groupObj.GetAnnotations(),
					Source:          relSource + "#" + groupName,
					File:            entry.Name(),
					Size:            int64(len(content)),
					UpdatedAt:       info.ModTime().UTC().Format(time.RFC3339),
				}

				templates[templateName] = AlertRuleTemplateDetail{
					AlertRuleTemplateSummary: summary,
					Content:                  string(content),
				}
			}
		}
	}

	if len(templates) == 0 {
		return nil, dir, os.ErrNotExist
	}

	return templates, dir, nil
}

func resolveChartTemplatesDir() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("getwd: %w", err)
	}

	target := filepath.Join("charts", "polardbx-monitor", "templates")
	dir := cwd
	for i := 0; i < 8; i++ {
		candidate := filepath.Join(dir, target)
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", os.ErrNotExist
}

func buildTemplateName(baseName, groupName string, counts map[string]int) string {
	base := sanitizeSegment(baseName)
	group := sanitizeSegment(groupName)
	if base == "" {
		base = "prometheusrule"
	}
	if group == "" {
		group = "group"
	}
	name := strings.Trim(strings.Join([]string{base, group}, "-"), "-")
	if name == "" {
		name = "prometheusrule-group"
	}

	if counts[name] == 0 {
		counts[name] = 1
		return name
	}

	counts[name]++
	return fmt.Sprintf("%s-%d", name, counts[name])
}

func sanitizeSegment(input string) string {
	input = strings.TrimSpace(strings.ToLower(input))
	if input == "" {
		return ""
	}

	var b strings.Builder
	lastDash := false
	for _, r := range input {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastDash = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		case r == '-' || r == '_' || r == '.' || unicode.IsSpace(r):
			if !lastDash {
				b.WriteRune('-')
				lastDash = true
			}
		default:
			// skip other characters
		}
	}

	res := strings.Trim(b.String(), "-")
	return res
}

func deriveTemplateTitle(groupName, templateName string, obj *unstructured.Unstructured) string {
	annotations := obj.GetAnnotations()
	if annotations != nil {
		if val := strings.TrimSpace(annotations["polardbx.com/template-title"]); val != "" {
			return val
		}
	}
	if strings.TrimSpace(groupName) != "" {
		return fallbackTitle(groupName)
	}
	return fallbackTitle(templateName)
}

func deriveCategoriesFromGroup(groupName string) []string {
	trimmed := strings.TrimSpace(groupName)
	if trimmed == "" {
		return nil
	}
	parts := strings.Split(trimmed, ".")
	categories := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		s := strings.TrimSpace(part)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		categories = append(categories, s)
	}
	if len(categories) == 0 {
		categories = append(categories, trimmed)
	}
	return categories
}

func normalizeTemplateKey(name string) string {
	trimmed := strings.TrimSpace(name)
	trimmed = strings.TrimSuffix(trimmed, ".yaml")
	trimmed = strings.TrimSuffix(trimmed, ".yml")
	return trimmed
}

func summarizeGroups(obj *unstructured.Unstructured) []AlertRuleGroupSummary {
	groups, found, _ := unstructured.NestedSlice(obj.Object, "spec", "groups")
	if !found || len(groups) == 0 {
		return nil
	}

	summaries := make([]AlertRuleGroupSummary, 0, len(groups))
	for _, groupInterface := range groups {
		groupMap, ok := groupInterface.(map[string]interface{})
		if !ok {
			continue
		}

		var summary AlertRuleGroupSummary
		if name, ok := groupMap["name"].(string); ok {
			summary.Name = name
		}
		if interval, ok := groupMap["interval"].(string); ok {
			summary.Interval = interval
		}

		severities := map[string]struct{}{}
		if rulesInterface, ok := groupMap["rules"].([]interface{}); ok {
			summary.Rules = len(rulesInterface)
			for _, ruleInterface := range rulesInterface {
				ruleMap, ok := ruleInterface.(map[string]interface{})
				if !ok {
					continue
				}
				if labelsMap, ok := ruleMap["labels"].(map[string]interface{}); ok {
					if sev, ok := labelsMap["severity"].(string); ok && strings.TrimSpace(sev) != "" {
						severities[strings.TrimSpace(sev)] = struct{}{}
					}
				}
			}
		}

		for sev := range severities {
			summary.Severities = append(summary.Severities, sev)
		}
		sort.Strings(summary.Severities)

		summaries = append(summaries, summary)
	}

	return summaries
}

func firstSeverity(obj *unstructured.Unstructured) string {
	groups, found, _ := unstructured.NestedSlice(obj.Object, "spec", "groups")
	if !found {
		return ""
	}
	for _, groupInterface := range groups {
		groupMap, ok := groupInterface.(map[string]interface{})
		if !ok {
			continue
		}
		if rulesInterface, ok := groupMap["rules"].([]interface{}); ok {
			for _, ruleInterface := range rulesInterface {
				ruleMap, ok := ruleInterface.(map[string]interface{})
				if !ok {
					continue
				}
				if labelsMap, ok := ruleMap["labels"].(map[string]interface{}); ok {
					if sev, ok := labelsMap["severity"].(string); ok {
						return strings.TrimSpace(sev)
					}
				}
			}
		}
	}
	return ""
}

func guessDescriptionFromRules(obj *unstructured.Unstructured) string {
	groups, found, _ := unstructured.NestedSlice(obj.Object, "spec", "groups")
	if !found {
		return ""
	}
	for _, groupInterface := range groups {
		groupMap, ok := groupInterface.(map[string]interface{})
		if !ok {
			continue
		}
		if rulesInterface, ok := groupMap["rules"].([]interface{}); ok {
			for _, ruleInterface := range rulesInterface {
				ruleMap, ok := ruleInterface.(map[string]interface{})
				if !ok {
					continue
				}
				if annotationsMap, ok := ruleMap["annotations"].(map[string]interface{}); ok {
					if summary, ok := annotationsMap["summary"].(string); ok && strings.TrimSpace(summary) != "" {
						return strings.TrimSpace(summary)
					}
				}
				if expr, ok := ruleMap["expr"].(string); ok && strings.TrimSpace(expr) != "" {
					return strings.TrimSpace(expr)
				}
			}
		}
	}
	return ""
}

func parseCategories(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	cats := make([]string, 0, len(parts))
	for _, part := range parts {
		p := strings.TrimSpace(part)
		if p != "" {
			cats = append(cats, p)
		}
	}
	return cats
}

func ensureTemplateFileName(name string) string {
	if strings.HasSuffix(name, ".yaml") || strings.HasSuffix(name, ".yml") {
		return name
	}
	return name + ".yaml"
}

func isYAMLFile(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".yaml") || strings.HasSuffix(lower, ".yml")
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
