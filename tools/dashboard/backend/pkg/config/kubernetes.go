package config

import (
	"log"
	"os"
	"sync"

	"polardbx-dashboard-backend/pkg/logger"
)

// KubeConfig holds Kubernetes client configuration
type KubeConfig struct {
	// Kubeconfig file path (empty = use in-cluster config)
	ConfigPath string

	// Namespace to operate in (empty = all namespaces)
	DefaultNamespace string

	// Connection settings
	QPS   float32 // Queries per second limit
	Burst int     // Burst limit for API calls

	// Cache settings
	EnableCache     bool
	CacheSyncPeriod int // seconds
}

var (
	kubeConfig     *KubeConfig
	kubeConfigOnce sync.Once
)

// GetKubeConfig returns the global Kubernetes configuration (singleton)
func GetKubeConfig() *KubeConfig {
	kubeConfigOnce.Do(func() {
		kubeConfig = loadKubeConfig()
		configSource := "in-cluster"
		if kubeConfig.ConfigPath != "" {
			configSource = kubeConfig.ConfigPath
		}
		// Use logger if available, otherwise fallback to standard log
		if logger.L() != nil {
			logger.Info("KubeConfig initialized",
				"source", configSource,
				"defaultNamespace", kubeConfig.DefaultNamespace)
		} else {
			log.Printf("KubeConfig initialized: Source=%s, DefaultNamespace=%s",
				configSource, kubeConfig.DefaultNamespace)
		}
	})
	return kubeConfig
}

// loadKubeConfig loads Kubernetes configuration from environment
func loadKubeConfig() *KubeConfig {
	return &KubeConfig{
		ConfigPath:       getEnvString("KUBECONFIG", ""),
		DefaultNamespace: getEnvString("DEFAULT_NAMESPACE", "default"),
		QPS:              float32(getEnvInt("KUBE_QPS", 50)),
		Burst:            getEnvInt("KUBE_BURST", 100),
		EnableCache:      getEnvBool("KUBE_ENABLE_CACHE", true),
		CacheSyncPeriod:  getEnvInt("KUBE_CACHE_SYNC_PERIOD", 30),
	}
}

// IsInCluster returns true if using in-cluster config
func (c *KubeConfig) IsInCluster() bool {
	return c.ConfigPath == ""
}

// GetKubeconfigPath returns the kubeconfig path, checking multiple sources
func (c *KubeConfig) GetKubeconfigPath() string {
	if c.ConfigPath != "" {
		return c.ConfigPath
	}

	// Check common kubeconfig locations
	if home := os.Getenv("HOME"); home != "" {
		defaultPath := home + "/.kube/config"
		if _, err := os.Stat(defaultPath); err == nil {
			return defaultPath
		}
	}

	return ""
}
