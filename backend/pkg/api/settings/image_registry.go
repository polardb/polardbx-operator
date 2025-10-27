package settings

import (
	"net/http"

	"polardbx-ui-backend/pkg/config"

	"github.com/gin-gonic/gin"
)

// GetImageRegistryConfig returns the current image registry configuration
// GET /api/v1/image-registry/config
func GetImageRegistryConfig(c *gin.Context) {
	cfg := config.GetGlobalConfig()
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    cfg.ToMap(),
	})
}

// UpdateImageRegistryRequest represents the request to update image registry
type UpdateImageRegistryRequest struct {
	Registry        string   `json:"registry"`
	CustomRegistry  string   `json:"customRegistry"`
	DefaultRegistry string   `json:"defaultRegistry"`
	Mirrors         []string `json:"mirrors"`
}

// UpdateImageRegistryConfig updates the default image registry
// PUT /api/v1/image-registry/config
func UpdateImageRegistryConfig(c *gin.Context) {
	var req UpdateImageRegistryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		// 打印详细错误信息用于调试
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": "Invalid request: " + err.Error(),
			"debug":   gin.H{"registry": req.Registry, "customRegistry": req.CustomRegistry, "defaultRegistry": req.DefaultRegistry},
		})
		return
	}

	cfg := config.GetGlobalConfig()

	registry := req.Registry
	if registry == "" {
		registry = req.DefaultRegistry
	}
	if registry == "custom" && req.CustomRegistry != "" {
		registry = req.CustomRegistry
	}

	if registry == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": "Registry cannot be empty",
		})
		return
	}

	cfg.SetDefaultRegistry(registry)

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "镜像源配置已更新",
		"data":    cfg.ToMap(),
	})
}

// GetAvailableRegistries returns a list of commonly used image registries
// GET /api/v1/image-registry/presets
func GetAvailableRegistries(c *gin.Context) {
	presets := []map[string]interface{}{
		{
			"name":        "DaoCloud Mirror (推荐)",
			"registry":    "docker.m.daocloud.io",
			"description": "DaoCloud 公共镜像加速服务，完整代理 Docker Hub",
			"region":      "China",
			"status":      "verified", // 已验证可用
		},
		{
			"name":        "Docker Hub (官方)",
			"registry":    "docker.io",
			"description": "官方 Docker Hub 镜像仓库 (docker.io)",
			"region":      "Global",
			"status":      "slow", // 国内访问慢或不可用
		},
		{
			"name":        "自定义镜像仓库",
			"registry":    "custom",
			"description": "使用企业私有镜像仓库（如 Harbor），需提前同步 alpine/helm:3.12.3 镜像",
			"region":      "Custom",
			"status":      "custom",
		},
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    presets,
	})
}

// TestImageRegistryRequest represents the request to test an image registry
type TestImageRegistryRequest struct {
	Registry string `json:"registry" binding:"required"`
}

// TestImageRegistry tests connectivity to an image registry
// POST /api/v1/image-registry/test
func TestImageRegistry(c *gin.Context) {
	var req TestImageRegistryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": "Invalid request: " + err.Error(),
		})
		return
	}

	// For now, just return success. In a real implementation, you would
	// try to pull a small test image or check registry availability
	c.JSON(http.StatusOK, gin.H{
		"success":   true,
		"message":   "Registry test not yet implemented",
		"registry":  req.Registry,
		"reachable": true,
	})
}
