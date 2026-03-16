package handler

import (
	"context"

	"github.com/gin-gonic/gin"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-dashboard-backend/pkg/api/domain/platform/settings/repository"
	"polardbx-dashboard-backend/pkg/api/domain/platform/settings/service"
	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/api/util"
)

// SettingsHandler handles HTTP requests related to platform settings.
type SettingsHandler struct {
	service *service.SettingsService
}

// NewSettingsHandler creates a new SettingsHandler
func NewSettingsHandler(svc *service.SettingsService) *SettingsHandler {
	return &SettingsHandler{service: svc}
}

// NewSettingsHandlerFromContext creates complete handler chain from gin.Context
func NewSettingsHandlerFromContext(c *gin.Context) (*SettingsHandler, bool) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return nil, false
	}
	repo := repository.NewK8sSettingsRepository(cli)
	svc := service.NewSettingsService(repo)
	return NewSettingsHandler(svc), true
}

// NewSettingsServiceFromClient creates SettingsService (for use by other packages)
func NewSettingsServiceFromClient(cli client.Client) *service.SettingsService {
	repo := repository.NewK8sSettingsRepository(cli)
	return service.NewSettingsService(repo)
}

// Get gets all backup dashboard settings.
// @Summary Get backup dashboard settings
// @Description Get current backup dashboard settings for the platform.
// @Tags platform, settings
// @Produce json
// @Success 200 {object} map[string]any "Settings payload"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func Get(c *gin.Context) {
	h, ok := NewSettingsHandlerFromContext(c)
	if !ok {
		return
	}
	data, _ := h.service.Get(c.Request.Context())
	apierr.OK(c, data)
}

// Update updates backup dashboard settings.
// @Summary Update backup dashboard settings
// @Description Update backup dashboard settings with the provided key-value map.
// @Tags platform, settings
// @Accept json
// @Produce json
// @Param body body map[string]any true "Settings payload"
// @Success 200 {object} map[string]any "Updated settings"
// @Failure 400 {object} apierr.ErrorResponse "Invalid payload"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func Update(c *gin.Context) {
	h, ok := NewSettingsHandlerFromContext(c)
	if !ok {
		return
	}
	var body map[string]any
	if err := c.ShouldBindJSON(&body); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	if err := h.service.Update(c.Request.Context(), body); err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("failed to update settings", err))
		return
	}
	apierr.OK(c, body)
}

// ReadDashboardSettings reads backup dashboard settings (for use by other packages)
func ReadDashboardSettings(ctx context.Context, cli client.Client) service.BackupDashboardSettings {
	svc := NewSettingsServiceFromClient(cli)
	return svc.GetDashboardSettings(ctx)
}

// GetImageRegistryConfig gets image registry configuration.
// @Summary Get image registry configuration
// @Description Get current image registry configuration used by the platform.
// @Tags platform, settings
// @Produce json
// @Success 200 {object} map[string]any "Image registry configuration"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func GetImageRegistryConfig(c *gin.Context) {
	h, ok := NewSettingsHandlerFromContext(c)
	if !ok {
		return
	}
	apierr.OK(c, gin.H{
		"success": true,
		"data":    h.service.GetImageRegistryConfig(),
	})
}

// UpdateImageRegistryRequest update image registry request
type UpdateImageRegistryRequest struct {
	Registry        string   `json:"registry"`
	CustomRegistry  string   `json:"customRegistry"`
	DefaultRegistry string   `json:"defaultRegistry"`
	Mirrors         []string `json:"mirrors"`
}

// UpdateImageRegistryConfig updates image registry configuration.
// @Summary Update image registry configuration
// @Description Update image registry configuration such as registry presets and defaults.
// @Tags platform, settings
// @Accept json
// @Produce json
// @Param body body UpdateImageRegistryRequest true "Image registry configuration payload"
// @Success 200 {object} map[string]any "Updated image registry configuration"
// @Failure 400 {object} apierr.ErrorResponse "Invalid payload or validation error"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func UpdateImageRegistryConfig(c *gin.Context) {
	h, ok := NewSettingsHandlerFromContext(c)
	if !ok {
		return
	}
	var req UpdateImageRegistryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	data, err := h.service.UpdateImageRegistryConfig(req.Registry, req.CustomRegistry, req.DefaultRegistry)
	if err != nil {
		apierr.AbortWithError(c, apierr.ValidationError(err.Error(), nil))
		return
	}

	apierr.OK(c, gin.H{
		"success": true,
		"message": "Image registry configuration updated",
		"data":    data,
	})
}

// GetAvailableRegistries gets available image registry presets.
// @Summary List available image registries
// @Description List available image registry presets that can be chosen.
// @Tags platform, settings
// @Produce json
// @Success 200 {object} map[string]any "Available registry presets"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
func GetAvailableRegistries(c *gin.Context) {
	h, ok := NewSettingsHandlerFromContext(c)
	if !ok {
		return
	}
	apierr.OK(c, gin.H{
		"success": true,
		"data":    h.service.GetAvailableRegistries(),
	})
}

// TestImageRegistryRequest test image registry request
type TestImageRegistryRequest struct {
	Registry string `json:"registry" binding:"required"`
}

// TestImageRegistry tests image registry connectivity (placeholder implementation).
// @Summary Test image registry connectivity
// @Description Test connectivity to the given image registry (currently a stub implementation).
// @Tags platform, settings
// @Accept json
// @Produce json
// @Param body body TestImageRegistryRequest true "Registry test request"
// @Success 200 {object} map[string]any "Test result (always reachable in current implementation)"
// @Failure 400 {object} apierr.ErrorResponse "Invalid payload"
func TestImageRegistry(c *gin.Context) {
	var req TestImageRegistryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apierr.AbortWithError(c, err)
		return
	}

	apierr.OK(c, gin.H{
		"success":   true,
		"message":   "Registry test not yet implemented",
		"registry":  req.Registry,
		"reachable": true,
	})
}
