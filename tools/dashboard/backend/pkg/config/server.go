package config

import (
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"polardbx-dashboard-backend/pkg/logger"
)

// ServerConfig holds all server configuration
type ServerConfig struct {
	// Server settings
	Port            int
	Host            string
	Mode            string // "debug", "release", "test"
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration

	// CORS settings
	CORSAllowOrigins     []string
	CORSAllowMethods     []string
	CORSAllowHeaders     []string
	CORSExposeHeaders    []string
	CORSAllowCredentials bool
	CORSMaxAge           int // Preflight cache duration in seconds

	// Logging settings
	LogLevel           string // "debug", "info", "warn", "error"
	LogRequestBody     bool
	LogResponseBody    bool
	LogMaxBodySize     int
	LogSkipPaths       []string
	LogSensitiveFields []string

	// Security settings
	JWTSecret     string
	JWTExpiration time.Duration

	// Feature flags
	EnableMetrics   bool
	EnableProfiling bool
	EnableSwagger   bool
}

var (
	serverConfig     *ServerConfig
	serverConfigOnce sync.Once
)

// GetServerConfig returns the global server configuration (singleton)
func GetServerConfig() *ServerConfig {
	serverConfigOnce.Do(func() {
		serverConfig = loadServerConfig()
		// Use logger if available, otherwise fallback to standard log
		if logger.L() != nil {
			logger.Info("ServerConfig initialized",
				"port", serverConfig.Port,
				"mode", serverConfig.Mode,
				"logLevel", serverConfig.LogLevel)
		} else {
			log.Printf("ServerConfig initialized: Port=%d, Mode=%s, LogLevel=%s",
				serverConfig.Port, serverConfig.Mode, serverConfig.LogLevel)
		}
	})
	return serverConfig
}

// loadServerConfig loads configuration from environment variables
func loadServerConfig() *ServerConfig {
	return &ServerConfig{
		// Server settings
		Port:            getEnvInt("SERVER_PORT", 8080),
		Host:            getEnvString("SERVER_HOST", ""),
		Mode:            getEnvString("GIN_MODE", "debug"),
		ReadTimeout:     getEnvDuration("SERVER_READ_TIMEOUT", 30*time.Second),
		WriteTimeout:    getEnvDuration("SERVER_WRITE_TIMEOUT", 60*time.Second),
		ShutdownTimeout: getEnvDuration("SERVER_SHUTDOWN_TIMEOUT", 10*time.Second),

		// CORS settings
		CORSAllowOrigins:     parseListEnv("CORS_ALLOW_ORIGINS", []string{"*"}),
		CORSAllowMethods:     parseListEnv("CORS_ALLOW_METHODS", []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}),
		CORSAllowHeaders:     parseListEnv("CORS_ALLOW_HEADERS", []string{"Content-Type", "Content-Length", "Accept-Encoding", "X-CSRF-Token", "Authorization", "accept", "origin", "Cache-Control", "X-Requested-With", "X-Kubeconfig-B64"}),
		CORSExposeHeaders:    parseListEnv("CORS_EXPOSE_HEADERS", []string{"Content-Length"}),
		CORSAllowCredentials: parseBoolEnv("CORS_ALLOW_CREDENTIALS"),
		CORSMaxAge:           getEnvInt("CORS_MAX_AGE", 86400),

		// Logging settings
		LogLevel:           getEnvString("LOG_LEVEL", "info"),
		LogRequestBody:     getEnvBool("LOG_REQUEST_BODY", false),
		LogResponseBody:    getEnvBool("LOG_RESPONSE_BODY", false),
		LogMaxBodySize:     getEnvInt("LOG_MAX_BODY_SIZE", 4096),
		LogSkipPaths:       parseListEnv("LOG_SKIP_PATHS", []string{"/ping", "/health", "/ready", "/metrics", "/api/v1/connect", "/api/v1/auth/login"}),
		LogSensitiveFields: parseListEnv("LOG_SENSITIVE_FIELDS", []string{"password", "token", "secret", "kubeconfig", "authorization", "client-key-data", "client-certificate-data", "certificate-authority-data"}),

		// Security settings
		JWTSecret:     getEnvString("JWT_SECRET", ""),
		JWTExpiration: getEnvDuration("JWT_EXPIRATION", 24*time.Hour),

		// Feature flags
		EnableMetrics:   getEnvBool("ENABLE_METRICS", false),
		EnableProfiling: getEnvBool("ENABLE_PROFILING", false),
		EnableSwagger:   getEnvBool("ENABLE_SWAGGER", false),
	}
}

// Helper functions for environment variable parsing

func getEnvString(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
		// Use logger if available, otherwise fallback to standard log
		if logger.L() != nil {
			logger.Warn("Invalid integer value, using default",
				"key", key,
				"value", value,
				"default", defaultValue)
		} else {
			log.Printf("Warning: invalid integer value for %s: %s, using default: %d", key, value, defaultValue)
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		return parseBoolEnv(key)
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
		// Use logger if available, otherwise fallback to standard log
		if logger.L() != nil {
			logger.Warn("Invalid duration value, using default",
				"key", key,
				"value", value,
				"default", defaultValue)
		} else {
			log.Printf("Warning: invalid duration value for %s: %s, using default: %v", key, value, defaultValue)
		}
	}
	return defaultValue
}

// Address returns the server address in host:port format
func (c *ServerConfig) Address() string {
	return c.Host + ":" + strconv.Itoa(c.Port)
}

// IsProduction returns true if running in release mode
func (c *ServerConfig) IsProduction() bool {
	return c.Mode == "release"
}

// IsDebug returns true if running in debug mode
func (c *ServerConfig) IsDebug() bool {
	return c.Mode == "debug"
}
