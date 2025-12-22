package config

import (
	"log"
	"sync"

	"polardbx-dashboard-backend/pkg/logger"
)

// AppConfig is the unified application configuration
// It provides a single entry point to all configuration
type AppConfig struct {
	Server     *ServerConfig
	Kubernetes *KubeConfig
	Image      *ImageRegistryConfig
	AutoFix    *AutoFixOverlayConfig
}

var (
	appConfig     *AppConfig
	appConfigOnce sync.Once
)

// GetAppConfig returns the unified application configuration (singleton)
func GetAppConfig() *AppConfig {
	appConfigOnce.Do(func() {
		appConfig = &AppConfig{
			Server:     GetServerConfig(),
			Kubernetes: GetKubeConfig(),
			Image:      GetGlobalConfig(),
			AutoFix:    GetAutoFixOverlayConfig(),
		}
		// Use logger if available, otherwise fallback to standard log
		if logger.L() != nil {
			logger.Info("AppConfig initialized: All configuration modules loaded")
		} else {
			log.Println("AppConfig initialized: All configuration modules loaded")
		}
	})
	return appConfig
}

// PrintConfig prints the current configuration (for debugging)
func (c *AppConfig) PrintConfig() {
	// Use logger if available, otherwise fallback to standard log
	useLogger := logger.L() != nil

	if useLogger {
		logger.Info("=== Application Configuration ===")
		logger.Info("Server configuration",
			"port", c.Server.Port,
			"mode", c.Server.Mode,
			"logLevel", c.Server.LogLevel,
			"jwtSecret", maskSecret(c.Server.JWTSecret))
	} else {
		log.Println("=== Application Configuration ===")
		log.Printf("Server: Port=%d, Mode=%s, LogLevel=%s, JWTSecret=%s",
			c.Server.Port, c.Server.Mode, c.Server.LogLevel, maskSecret(c.Server.JWTSecret))
	}

	configSource := "in-cluster"
	if !c.Kubernetes.IsInCluster() {
		configSource = c.Kubernetes.ConfigPath
	}

	if useLogger {
		logger.Info("Kubernetes configuration",
			"source", configSource,
			"defaultNamespace", c.Kubernetes.DefaultNamespace)
		logger.Info("Image configuration",
			"defaultRegistry", c.Image.GetDefaultRegistry(),
			"mirrors", c.Image.GetMirrors())
		logger.Info("AutoFix configuration",
			"enabled", c.AutoFix.Enabled,
			"namespace", c.AutoFix.Namespace)
		logger.Info("=================================")
	} else {
		log.Printf("Kubernetes: Source=%s, DefaultNamespace=%s",
			configSource, c.Kubernetes.DefaultNamespace)
		log.Printf("Image: DefaultRegistry=%s, Mirrors=%v",
			c.Image.GetDefaultRegistry(), c.Image.GetMirrors())
		log.Printf("AutoFix: Enabled=%t, Namespace=%s",
			c.AutoFix.Enabled, c.AutoFix.Namespace)
		log.Println("=================================")
	}
}

func maskSecret(v string) string {
	if v == "" {
		return "<empty>"
	}
	if len(v) <= 4 {
		return "***"
	}
	return v[:2] + "***" + v[len(v)-2:]
}

// Validate validates the configuration and returns any errors
func (c *AppConfig) Validate() []string {
	var errors []string

	// Validate server config
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		errors = append(errors, "Invalid server port")
	}

	if c.Server.Mode != "debug" && c.Server.Mode != "release" && c.Server.Mode != "test" {
		errors = append(errors, "Invalid GIN_MODE, must be 'debug', 'release', or 'test'")
	}

	// Validate log level
	validLogLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}
	if !validLogLevels[c.Server.LogLevel] {
		errors = append(errors, "Invalid LOG_LEVEL, must be 'debug', 'info', 'warn', or 'error'")
	}

	return errors
}
