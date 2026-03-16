package logger

import (
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	globalLogger *zap.Logger
	sugar        *zap.SugaredLogger
	once         sync.Once
)

// Config holds logger configuration
type Config struct {
	Level       string // "debug", "info", "warn", "error"
	Development bool   // Enable development mode (more verbose)
	Encoding    string // "json" or "console"
	OutputPaths []string
}

// DefaultConfig returns the default logger configuration
func DefaultConfig() Config {
	return Config{
		Level:       getEnvOrDefault("LOG_LEVEL", "info"),
		Development: getEnvOrDefault("LOG_DEV_MODE", "false") == "true",
		Encoding:    getEnvOrDefault("LOG_ENCODING", "console"),
		OutputPaths: []string{"stdout"},
	}
}

// Init initializes the global logger with the given configuration
func Init(cfg Config) error {
	var err error
	once.Do(func() {
		globalLogger, err = buildLogger(cfg)
		if err == nil {
			sugar = globalLogger.Sugar()
		}
	})
	return err
}

// InitDefault initializes the global logger with default configuration
func InitDefault() error {
	return Init(DefaultConfig())
}

// buildLogger creates a zap logger from configuration
func buildLogger(cfg Config) (*zap.Logger, error) {
	level := parseLevel(cfg.Level)

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalColorLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	if cfg.Encoding == "json" {
		encoderConfig.EncodeLevel = zapcore.LowercaseLevelEncoder
	}

	zapCfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(level),
		Development:      cfg.Development,
		Encoding:         cfg.Encoding,
		EncoderConfig:    encoderConfig,
		OutputPaths:      cfg.OutputPaths,
		ErrorOutputPaths: []string{"stderr"},
	}

	return zapCfg.Build(zap.AddCallerSkip(1))
}

// parseLevel converts string level to zapcore.Level
func parseLevel(level string) zapcore.Level {
	switch level {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

// L returns the global logger
func L() *zap.Logger {
	if globalLogger == nil {
		_ = InitDefault()
	}
	return globalLogger
}

// S returns the global sugared logger
func S() *zap.SugaredLogger {
	if sugar == nil {
		_ = InitDefault()
	}
	return sugar
}

// Named creates a named child logger
func Named(name string) *zap.Logger {
	return L().Named(name)
}

// With creates a child logger with additional fields
func With(fields ...zap.Field) *zap.Logger {
	return L().With(fields...)
}

// Convenience methods that use the global sugared logger

// Debug logs a debug message
func Debug(msg string, keysAndValues ...interface{}) {
	S().Debugw(msg, keysAndValues...)
}

// Info logs an info message
func Info(msg string, keysAndValues ...interface{}) {
	S().Infow(msg, keysAndValues...)
}

// Warn logs a warning message
func Warn(msg string, keysAndValues ...interface{}) {
	S().Warnw(msg, keysAndValues...)
}

// Error logs an error message
func Error(msg string, keysAndValues ...interface{}) {
	S().Errorw(msg, keysAndValues...)
}

// Fatal logs a fatal message and exits
func Fatal(msg string, keysAndValues ...interface{}) {
	S().Fatalw(msg, keysAndValues...)
}

// Debugf logs a formatted debug message
func Debugf(template string, args ...interface{}) {
	S().Debugf(template, args...)
}

// Infof logs a formatted info message
func Infof(template string, args ...interface{}) {
	S().Infof(template, args...)
}

// Warnf logs a formatted warning message
func Warnf(template string, args ...interface{}) {
	S().Warnf(template, args...)
}

// Errorf logs a formatted error message
func Errorf(template string, args ...interface{}) {
	S().Errorf(template, args...)
}

// Fatalf logs a formatted fatal message and exits
func Fatalf(template string, args ...interface{}) {
	S().Fatalf(template, args...)
}

// Sync flushes any buffered log entries
func Sync() error {
	if globalLogger != nil {
		return globalLogger.Sync()
	}
	return nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
