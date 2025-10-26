package config

import (
	"log"
	"os"
	"sync"
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

// GetGlobalConfig returns the global image registry configuration
func GetGlobalConfig() *ImageRegistryConfig {
	globalConfigOnce.Do(func() {
		globalConfig = &ImageRegistryConfig{
			DefaultRegistry: getDefaultRegistry(),
			HelmImage:       getDefaultHelmImage(),
			Mirrors:         getDefaultMirrors(),
		}
		log.Printf("ImageRegistryConfig initialized: DefaultRegistry=%s, HelmImage=%s, Mirrors=%v",
			globalConfig.DefaultRegistry, globalConfig.HelmImage, globalConfig.Mirrors)
	})
	return globalConfig
}

// GetDefaultRegistry returns the default registry, respecting environment variables
func getDefaultRegistry() string {
	if reg := os.Getenv("DEFAULT_IMAGE_REGISTRY"); reg != "" {
		log.Printf("Using image registry from env DEFAULT_IMAGE_REGISTRY: %s", reg)
		return reg
	}

	// Default to DaoCloud mirror for better accessibility in China
	// Users can override this via environment variable or API
	return "docker.m.daocloud.io"
}

// GetDefaultHelmImage returns the default helm image
func getDefaultHelmImage() string {
	if img := os.Getenv("HELM_IMAGE"); img != "" {
		log.Printf("Using helm image from env HELM_IMAGE: %s", img)
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
		log.Printf("Using image registry mirrors from env: %s", mirrorsEnv)
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

	log.Printf("Updated default registry to: %s, HelmImage: %s", c.DefaultRegistry, c.HelmImage)
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
