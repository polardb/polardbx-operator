package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"

	"polardbx-dashboard-backend/pkg/api/domain/platform/alerts/repository"
	"polardbx-dashboard-backend/pkg/api/domain/platform/alerts/service"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
)

// AlertsHandler handles alert-related HTTP requests
type AlertsHandler struct {
	service *service.AlertsService
}

// NewAlertsHandler creates new AlertsHandler
func NewAlertsHandler(svc *service.AlertsService) *AlertsHandler {
	return &AlertsHandler{service: svc}
}

// NewAlertsHandlerFromContext creates complete handler chain from gin.Context
func NewAlertsHandlerFromContext(c *gin.Context) (*AlertsHandler, bool) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return nil, false
	}
	repo := repository.NewK8sAlertsRepository(cli)
	svc := service.NewAlertsService(repo)
	return NewAlertsHandler(svc), true
}

// ListProfiles lists all alertmanager configuration profiles.
// @Summary List alert profiles
// @Description List all stored Alertmanager configuration profiles.
// @Tags platform, alerts
// @Produce json
// @Success 200 {object} map[string]any "Profiles list (items)"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func ListProfiles(c *gin.Context) {
	h, ok := NewAlertsHandlerFromContext(c)
	if !ok {
		return
	}
	items, _ := h.service.ListProfiles(c.Request.Context())
	apierr.OK(c, gin.H{"items": items})
}

// CreateProfile creates a new alertmanager configuration profile.
// @Summary Create alert profile
// @Description Create a new Alertmanager configuration profile with the given name and content.
// @Tags platform, alerts
// @Accept json
// @Produce json
// @Param body body map[string]any true "Profile payload (expects fields: name, content)"
// @Success 201 {object} map[string]any "Created profile name"
// @Failure 400 {object} apierr.ErrorResponse "Invalid payload"
// @Failure 409 {object} apierr.ErrorResponse "Profile already exists"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func CreateProfile(c *gin.Context) {
	h, ok := NewAlertsHandlerFromContext(c)
	if !ok {
		return
	}
	var body struct {
		Name    string `json:"name"`
		Content string `json:"content"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	if body.Name == "" {
		apierr.AbortWithError(c, apierr.ValidationError("invalid payload", nil))
		return
	}
	if err := h.service.CreateProfile(c.Request.Context(), body.Name, body.Content); err != nil {
		if err == service.ErrProfileExists {
			apierr.Abort(c, apierr.Conflict("profile exists"))
			return
		}
		apierr.AbortWithError(c, apierr.InternalServiceError("create profiles cm", err))
		return
	}
	apierr.Created(c, gin.H{"name": body.Name})
}

// GetProfile gets a single alertmanager configuration profile.
// @Summary Get alert profile
// @Description Get a specific Alertmanager configuration profile by name.
// @Tags platform, alerts
// @Produce json
// @Param name path string true "Profile name"
// @Success 200 {object} map[string]any "Profile name and content"
// @Failure 404 {object} apierr.ErrorResponse "Profile not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func GetProfile(c *gin.Context) {
	h, ok := NewAlertsHandlerFromContext(c)
	if !ok {
		return
	}
	name := c.Param("name")
	profile, err := h.service.GetProfile(c.Request.Context(), name)
	if err != nil {
		apierr.AbortWithError(c, apierr.NotFoundError("profile", name))
		return
	}
	apierr.OK(c, gin.H{"name": profile.Name, "content": profile.Content})
}

// UpdateProfile updates an existing alertmanager configuration profile.
// @Summary Update alert profile
// @Description Update content of an existing Alertmanager configuration profile.
// @Tags platform, alerts
// @Accept json
// @Produce json
// @Param name path string true "Profile name"
// @Param body body map[string]any true "Updated profile content (expects field: content)"
// @Success 200 {object} map[string]any "Updated profile name"
// @Failure 400 {object} apierr.ErrorResponse "Invalid payload"
// @Failure 404 {object} apierr.ErrorResponse "Profile not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func UpdateProfile(c *gin.Context) {
	h, ok := NewAlertsHandlerFromContext(c)
	if !ok {
		return
	}
	name := c.Param("name")
	var body struct {
		Content string `json:"content"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	if name == "" {
		apierr.AbortWithError(c, apierr.ValidationError("invalid payload", nil))
		return
	}
	if err := h.service.UpdateProfile(c.Request.Context(), name, body.Content); err != nil {
		if err == service.ErrProfileNotFound {
			apierr.AbortWithError(c, apierr.NotFoundError("profile", name))
			return
		}
		apierr.AbortWithError(c, apierr.InternalServiceError("update profiles cm", err))
		return
	}
	apierr.OK(c, gin.H{"name": name})
}

// DeleteProfile deletes an alertmanager configuration profile.
// @Summary Delete alert profile
// @Description Delete an Alertmanager configuration profile by name.
// @Tags platform, alerts
// @Produce json
// @Param name path string true "Profile name"
// @Success 200 {object} map[string]any "Deleted profile name"
// @Failure 404 {object} apierr.ErrorResponse "Profile not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func DeleteProfile(c *gin.Context) {
	h, ok := NewAlertsHandlerFromContext(c)
	if !ok {
		return
	}
	name := c.Param("name")
	if err := h.service.DeleteProfile(c.Request.Context(), name); err != nil {
		if err == service.ErrProfileNotFound {
			apierr.AbortWithError(c, apierr.NotFoundError("profile", name))
			return
		}
		apierr.AbortWithError(c, apierr.InternalServiceError("update profiles cm", err))
		return
	}
	apierr.OK(c, gin.H{"deleted": name})
}

// DryRunProfile validates Alertmanager configuration content without persisting it.
// @Summary Dry-run alert profile
// @Description Validate Alertmanager YAML content and return validation result without saving.
// @Tags platform, alerts
// @Accept json
// @Produce json
// @Param body body map[string]any true "Profile content to validate (expects field: content)"
// @Success 200 {object} map[string]any "Validation result (valid: true)"
// @Failure 400 {object} apierr.ErrorResponse "Invalid payload or invalid configuration"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func DryRunProfile(c *gin.Context) {
	h, ok := NewAlertsHandlerFromContext(c)
	if !ok {
		return
	}
	var body struct {
		Content string `json:"content"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	result, err := h.service.DryRunProfile(body.Content)
	if err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("tempfile error", err))
		return
	}
	if !result.Valid {
		apierr.AbortWithError(c, apierr.ValidationError(result.Details, nil))
		return
	}
	apierr.OK(c, gin.H{"valid": true})
}

// GetRoutes gets Alertmanager routing configuration.
// @Summary Get alert routes
// @Description Get Alertmanager route configuration content.
// @Tags platform, alerts
// @Produce json
// @Success 200 {object} map[string]any "Routing configuration content"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func GetRoutes(c *gin.Context) {
	h, ok := NewAlertsHandlerFromContext(c)
	if !ok {
		return
	}
	content, alertmanagerURL, _ := h.service.GetRoutes(c.Request.Context())
	resp := gin.H{"content": content}
	if strings.TrimSpace(alertmanagerURL) != "" {
		resp["alertmanagerUrl"] = strings.TrimSpace(alertmanagerURL)
	}
	apierr.OK(c, resp)
}

// PutRoutes updates Alertmanager routing configuration.
// @Summary Update alert routes
// @Description Update Alertmanager route configuration.
// @Tags platform, alerts
// @Accept json
// @Produce json
// @Param body body map[string]any true "Routing configuration content (expects field: content)"
// @Success 200 {object} map[string]any "Update confirmation"
// @Failure 400 {object} apierr.ErrorResponse "Invalid payload"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func PutRoutes(c *gin.Context) {
	h, ok := NewAlertsHandlerFromContext(c)
	if !ok {
		return
	}
	var body struct {
		Content         string `json:"content"`
		AlertmanagerURL string `json:"alertmanagerUrl"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	_ = h.service.PutRoutes(c.Request.Context(), body.Content, body.AlertmanagerURL)
	apierr.OK(c, gin.H{"message": "routes updated"})
}

// List returns an aggregated list of alerts.
// @Summary List alerts
// @Description List alerts filtered by namespace, cluster, and optional Alertmanager base URL.
// @Tags platform, alerts
// @Produce json
// @Param namespace query string false "Kubernetes namespace filter"
// @Param cluster query string false "Cluster name filter"
// @Param alertmanager query string false "Base URL of Alertmanager; defaults to in-cluster instance"
// @Success 200 {object} map[string]any "Alerts list (items)"
// @Failure 500 {object} apierr.ErrorResponse "Internal or upstream error"
func List(c *gin.Context) {
	h, ok := NewAlertsHandlerFromContext(c)
	if !ok {
		return
	}
	namespace := c.DefaultQuery("namespace", "")
	cluster := c.DefaultQuery("cluster", "")
	alertmanagerURL := c.Query("alertmanager")
	items, _ := h.service.ListAlerts(c.Request.Context(), namespace, cluster, alertmanagerURL)
	apierr.OK(c, gin.H{"items": items})
}

// ListSilences proxies Alertmanager silence list.
// @Summary List alert silences
// @Description Proxy request to Alertmanager /api/v2/silences and return response as-is.
// @Tags platform, alerts
// @Produce json
// @Param alertmanager query string true "Base URL of Alertmanager"
// @Success 200 {array} map[string]any "List of silences (status 200 from Alertmanager)"
// @Failure 400 {object} apierr.ErrorResponse "Missing Alertmanager base URL"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Alertmanager error"
func ListSilences(c *gin.Context) {
	base, ok := resolveAlertmanagerBase(c)
	if !ok {
		return
	}
	resp, err := http.Get(base + "/api/v2/silences")
	if err != nil {
		apierr.Abort(c, apierr.BadGateway(err.Error()))
		return
	}
	defer resp.Body.Close()
	var out any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	c.JSON(resp.StatusCode, out)
}

// CreateSilence creates an Alertmanager silence.
// @Summary Create alert silence
// @Description Proxy request to Alertmanager /api/v2/silences to create a silence.
// @Tags platform, alerts
// @Accept json
// @Produce json
// @Param alertmanager query string true "Base URL of Alertmanager"
// @Param body body map[string]any true "Silence payload (Alertmanager format)"
// @Success 200 {object} map[string]any "Silence created (status propagated from Alertmanager)"
// @Failure 400 {object} apierr.ErrorResponse "Missing Alertmanager base URL or invalid payload"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Alertmanager error"
func CreateSilence(c *gin.Context) {
	base, ok := resolveAlertmanagerBase(c)
	if !ok {
		return
	}
	var body map[string]any
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	b, _ := json.Marshal(body)
	resp, err := http.Post(base+"/api/v2/silences", "application/json", bytes.NewReader(b))
	if err != nil {
		apierr.Abort(c, apierr.BadGateway(err.Error()))
		return
	}
	defer resp.Body.Close()
	var out any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	c.JSON(resp.StatusCode, out)
}

// DeleteSilence deletes an Alertmanager silence.
// @Summary Delete alert silence
// @Description Proxy request to Alertmanager /api/v2/silence/{id} to delete a silence.
// @Tags platform, alerts
// @Produce json
// @Param alertmanager query string true "Base URL of Alertmanager"
// @Param id path string true "Silence ID"
// @Success 200 {object} map[string]any "Silence deleted (status propagated from Alertmanager)"
// @Failure 400 {object} apierr.ErrorResponse "Missing Alertmanager base URL"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Alertmanager error"
func DeleteSilence(c *gin.Context) {
	base, ok := resolveAlertmanagerBase(c)
	if !ok {
		return
	}
	id := c.Param("id")
	req, _ := http.NewRequest(http.MethodDelete, base+"/api/v2/silence/"+url.PathEscape(id), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		apierr.Abort(c, apierr.BadGateway(err.Error()))
		return
	}
	defer resp.Body.Close()
	var out any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	c.JSON(resp.StatusCode, out)
}

// TestAlert sends a test alert to Alertmanager.
// @Summary Send test alert
// @Description Send a synthetic test alert to Alertmanager with configurable labels.
// @Tags platform, alerts
// @Produce json
// @Param alertmanager query string true "Base URL of Alertmanager"
// @Param labels query string false "Comma-separated key=value label pairs (e.g., severity=warning,service=test)"
// @Success 200 {object} map[string]any "Test alert result (status propagated from Alertmanager)"
// @Failure 400 {object} apierr.ErrorResponse "Missing Alertmanager base URL"
// @Failure 502 {object} apierr.ErrorResponse "Upstream Alertmanager error"
func TestAlert(c *gin.Context) {
	base, ok := resolveAlertmanagerBase(c)
	if !ok {
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
		apierr.Abort(c, apierr.BadGateway(err.Error()))
		return
	}
	defer resp.Body.Close()
	var out any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	c.JSON(resp.StatusCode, out)
}

func resolveAlertmanagerBase(c *gin.Context) (string, bool) {
	_, ok := util.K8sClientFromContext(c)
	if !ok {
		return "", false
	}
	base := strings.TrimSpace(c.Query("alertmanager"))
	if base != "" {
		return strings.TrimRight(base, "/"), true
	}

	h, ok := NewAlertsHandlerFromContext(c)
	if !ok {
		return "", false
	}
	_, stored, _ := h.service.GetRoutes(c.Request.Context())
	base = strings.TrimSpace(stored)
	if base == "" {
		apierr.AbortWithError(c, apierr.ValidationError("alertmanager required", nil))
		return "", false
	}
	return strings.TrimRight(base, "/"), true
}
