package config

import (
	"log"
	"os"
	"strings"
	"sync"

	"polardbx-dashboard-backend/pkg/logger"
)

// ImageRegistryConfig holds image registry configuration
type ImageRegistryConfig struct {
	mu sync.RWMutex

	// DefaultRegistry is the default image registry to use
	// If empty, defaults to docker.io (Docker Hub)
	DefaultRegistry string

	// HelmImage is the full helm image reference (registry + repository + tag)
	// Used for bootstrap jobs
	HelmImage string

	// Mirrors is a list of alternative registries for fallback
	Mirrors []string
}

var (
	globalConfig     *ImageRegistryConfig
	globalConfigOnce sync.Once
)

var (
	autoFixOverlayConfig     *AutoFixOverlayConfig
	autoFixOverlayConfigOnce sync.Once
)

// GetGlobalConfig returns the global image registry configuration
func GetGlobalConfig() *ImageRegistryConfig {
	globalConfigOnce.Do(func() {
		globalConfig = &ImageRegistryConfig{
			DefaultRegistry: getDefaultRegistry(),
			HelmImage:       getDefaultHelmImage(),
			Mirrors:         getDefaultMirrors(),
		}
		// Use logger if available, otherwise fallback to standard log
		if logger.L() != nil {
			logger.Info("ImageRegistryConfig initialized",
				"defaultRegistry", globalConfig.DefaultRegistry,
				"helmImage", globalConfig.HelmImage,
				"mirrors", globalConfig.Mirrors)
		} else {
			log.Printf("ImageRegistryConfig initialized: DefaultRegistry=%s, HelmImage=%s, Mirrors=%v",
				globalConfig.DefaultRegistry, globalConfig.HelmImage, globalConfig.Mirrors)
		}
	})
	return globalConfig
}

// GetDefaultRegistry returns the default registry, respecting environment variables
func getDefaultRegistry() string {
	if reg := os.Getenv("DEFAULT_IMAGE_REGISTRY"); reg != "" {
		if logger.L() != nil {
			logger.Info("Using image registry from env", "env", "DEFAULT_IMAGE_REGISTRY", "registry", reg)
		} else {
			log.Printf("Using image registry from env DEFAULT_IMAGE_REGISTRY: %s", reg)
		}
		return reg
	}

	// Default to DaoCloud mirror for better accessibility in China
	// Users can override this via environment variable or API
	return "docker.m.daocloud.io"
}

// GetDefaultHelmImage returns the default helm image
func getDefaultHelmImage() string {
	if img := os.Getenv("HELM_IMAGE"); img != "" {
		if logger.L() != nil {
			logger.Info("Using helm image from env", "env", "HELM_IMAGE", "image", img)
		} else {
			log.Printf("Using helm image from env HELM_IMAGE: %s", img)
		}
		return img
	}

	registry := getDefaultRegistry()
	if registry == "" {
		return "alpine/helm:3.12.3"
	}
	return registry + "/alpine/helm:3.12.3"
}

// GetDefaultMirrors returns a list of default mirror registries
func getDefaultMirrors() []string {
	mirrors := []string{
		"docker.m.daocloud.io",
		"docker.mirrors.ustc.edu.cn",
		"registry.cn-hangzhou.aliyuncs.com",
	}

	// Allow override via environment variable (comma-separated)
	if mirrorsEnv := os.Getenv("IMAGE_REGISTRY_MIRRORS"); mirrorsEnv != "" {
		if logger.L() != nil {
			logger.Info("Using image registry mirrors from env", "env", "IMAGE_REGISTRY_MIRRORS", "mirrors", mirrorsEnv)
		} else {
			log.Printf("Using image registry mirrors from env: %s", mirrorsEnv)
		}
		// Parse comma-separated list
		// For simplicity, just use the env value as-is
		return []string{mirrorsEnv}
	}

	return mirrors
}

// SetDefaultRegistry updates the default registry at runtime
func (c *ImageRegistryConfig) SetDefaultRegistry(registry string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.DefaultRegistry = registry
	// Update helm image accordingly
	if registry == "" {
		c.HelmImage = "alpine/helm:3.12.3"
	} else {
		c.HelmImage = registry + "/alpine/helm:3.12.3"
	}

	if logger.L() != nil {
		logger.Info("Updated default registry",
			"defaultRegistry", c.DefaultRegistry,
			"helmImage", c.HelmImage)
	} else {
		log.Printf("Updated default registry to: %s, HelmImage: %s", c.DefaultRegistry, c.HelmImage)
	}
}

// GetHelmImage returns the current helm image (thread-safe)
func (c *ImageRegistryConfig) GetHelmImage() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.HelmImage
}

// GetDefaultRegistry returns the current default registry (thread-safe)
func (c *ImageRegistryConfig) GetDefaultRegistry() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.DefaultRegistry
}

// GetMirrors returns the list of mirror registries (thread-safe)
func (c *ImageRegistryConfig) GetMirrors() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]string{}, c.Mirrors...) // Return a copy
}

// ToMap converts the config to a map for API responses
func (c *ImageRegistryConfig) ToMap() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return map[string]interface{}{
		"defaultRegistry": c.DefaultRegistry,
		"helmImage":       c.HelmImage,
		"mirrors":         c.Mirrors,
	}
}

// AutoFixOverlayConfig controls runtime patching of monitoring resources to enable auto-fix hooks
// and RBAC without modifying the upstream Helm chart.
type AutoFixOverlayConfig struct {
	Enabled             bool
	Namespace           string
	ServiceAccount      string
	ClusterRoleName     string
	ClusterRoleBinding  string
	AnnotationKey       string
	PrometheusTargets   []string
	GrafanaTargets      []string
	AlertmanagerTargets []string
}

// GetAutoFixOverlayConfig lazily initializes and returns the auto-fix overlay config.
func GetAutoFixOverlayConfig() *AutoFixOverlayConfig {
	autoFixOverlayConfigOnce.Do(func() {
		autoFixOverlayConfig = &AutoFixOverlayConfig{
			Enabled:             parseBoolEnv("MONITORING_AUTOFIX_OVERLAY_ENABLED"),
			Namespace:           firstNonEmpty(os.Getenv("MONITORING_AUTOFIX_OVERLAY_NAMESPACE"), "polardbx-monitor"),
			ServiceAccount:      os.Getenv("MONITORING_AUTOFIX_OVERLAY_SERVICE_ACCOUNT"),
			ClusterRoleName:     firstNonEmpty(os.Getenv("MONITORING_AUTOFIX_OVERLAY_CLUSTER_ROLE"), "polardbx-monitor-autofix"),
			ClusterRoleBinding:  firstNonEmpty(os.Getenv("MONITORING_AUTOFIX_OVERLAY_CLUSTER_ROLEBINDING"), "polardbx-monitor-autofix"),
			AnnotationKey:       firstNonEmpty(os.Getenv("MONITORING_AUTOFIX_ANNOTATION_KEY"), "monitoring.polardbx.com/auto-fix-ids"),
			PrometheusTargets:   parseListEnv("MONITORING_AUTOFIX_PROMETHEUS", []string{"k8s", "kube-prometheus-stack-prometheus"}),
			GrafanaTargets:      parseListEnv("MONITORING_AUTOFIX_GRAFANA", []string{"grafana", "kube-prometheus-stack-grafana"}),
			AlertmanagerTargets: parseListEnv("MONITORING_AUTOFIX_ALERTMANAGER", []string{"main", "kube-prometheus-stack-alertmanager"}),
		}
		if logger.L() != nil {
			logger.Info("AutoFixOverlayConfig initialized",
				"enabled", autoFixOverlayConfig.Enabled,
				"namespace", autoFixOverlayConfig.Namespace,
				"serviceAccount", autoFixOverlayConfig.ServiceAccount)
		} else {
			log.Printf("AutoFixOverlayConfig initialized: enabled=%v, namespace=%s, serviceAccount=%s", autoFixOverlayConfig.Enabled, autoFixOverlayConfig.Namespace, autoFixOverlayConfig.ServiceAccount)
		}
	})
	return autoFixOverlayConfig
}

func parseBoolEnv(key string) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return false
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func parseListEnv(key string, fallback []string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return append([]string{}, fallback...)
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	if len(out) == 0 {
		return append([]string{}, fallback...)
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
