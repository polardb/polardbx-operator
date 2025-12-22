package middleware

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"polardbx-dashboard-backend/pkg/logger"
)

// RequestLogConfig is the configuration for request logging
type RequestLogConfig struct {
	// LogRequestBody indicates whether to log request body
	LogRequestBody bool
	// LogResponseBody indicates whether to log response body
	LogResponseBody bool
	// MaxBodyLogSize is the maximum size of body to log
	MaxBodyLogSize int
	// SkipPaths are paths to skip logging
	SkipPaths []string
	// SensitiveFields are fields that need to be masked
	SensitiveFields []string
}

// DefaultLogConfig is the default logging configuration
var DefaultLogConfig = RequestLogConfig{
	LogRequestBody:  false,
	LogResponseBody: false, // Response body is usually large, not logged by default
	MaxBodyLogSize:  4096,
	SkipPaths:       []string{"/ping", "/health", "/ready", "/metrics", "/api/v1/connect", "/api/v1/auth/login"},
	SensitiveFields: []string{"password", "token", "secret", "kubeconfig", "authorization", "client-key-data", "client-certificate-data", "certificate-authority-data"},
}

// RequestIDKey is the context key for request ID
const RequestIDKey = "requestId"

// bodyLogWriter is used to capture response body
// (only used when LogResponseBody is enabled)
type bodyLogWriter struct {
	gin.ResponseWriter
	body *bytes.Buffer
}

func (w bodyLogWriter) Write(b []byte) (int, error) {
	w.body.Write(b)
	return w.ResponseWriter.Write(b)
}

// RequestLogger is a request logging middleware (zap structured)
func RequestLogger(config ...RequestLogConfig) gin.HandlerFunc {
	cfg := DefaultLogConfig
	if len(config) > 0 {
		cfg = config[0]
	}

	return func(c *gin.Context) {
		path := c.Request.URL.Path
		for _, skip := range cfg.SkipPaths {
			if strings.HasPrefix(path, skip) {
				c.Next()
				return
			}
		}

		requestID := c.GetString(RequestIDKey)
		if requestID == "" {
			requestID = uuid.New().String()[:8]
			c.Set(RequestIDKey, requestID)
		}

		startTime := time.Now()
		method := c.Request.Method
		clientIP := c.ClientIP()
		userAgent := truncateString(c.Request.UserAgent(), 80)

		l := logger.L().With(
			zap.String("requestId", requestID),
			zap.String("method", method),
			zap.String("path", path),
			zap.String("clientIP", clientIP),
			zap.String("userAgent", userAgent),
		)

		// Read request body (if needed). Default is disabled; when enabled, only log JSON bodies.
		if cfg.LogRequestBody && c.Request.Body != nil && c.Request.Body != http.NoBody {
			contentType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
			if err != nil {
				contentType = c.GetHeader("Content-Type")
			}
			if !(contentType == "application/json" || strings.HasSuffix(contentType, "+json")) {
				l.Info("request received", zap.String("requestBody", "(omitted: non-JSON content-type)"))
			} else {
				max := cfg.MaxBodyLogSize
				if max <= 0 {
					max = DefaultLogConfig.MaxBodyLogSize
				}

				// If Content-Length is known and too large, avoid reading it into memory just for logging.
				if c.Request.ContentLength > int64(max) && c.Request.ContentLength != -1 {
					l.Info("request received", zap.String("requestBody", "(omitted: too large)"))
				} else {
					peek, _ := io.ReadAll(io.LimitReader(c.Request.Body, int64(max)+1))
					c.Request.Body = io.NopCloser(io.MultiReader(bytes.NewReader(peek), c.Request.Body))

					body := string(peek)
					if len(peek) > max {
						body = body[:max] + "...(truncated)"
					}
					body = maskSensitiveFieldsJSON(body, cfg.SensitiveFields)
					l.Info("request received", zap.String("requestBody", body))
				}
			}
		} else {
			l.Info("request received")
		}

		// Capture response body (if needed)
		var blw *bodyLogWriter
		if cfg.LogResponseBody {
			blw = &bodyLogWriter{body: bytes.NewBufferString(""), ResponseWriter: c.Writer}
			c.Writer = blw
		}

		c.Next()

		latency := time.Since(startTime)
		statusCode := c.Writer.Status()
		user := c.GetString("k8sUser")
		k8sContext := c.GetString("k8sContext")

		fields := []zap.Field{
			zap.Int("status", statusCode),
			zap.Duration("latency", latency),
		}
		if user != "" {
			fields = append(fields, zap.String("user", user))
		}
		if k8sContext != "" {
			fields = append(fields, zap.String("k8sContext", k8sContext))
		}
		if len(c.Errors) > 0 {
			fields = append(fields, zap.String("errors", c.Errors.String()))
		}

		if cfg.LogResponseBody && blw != nil && statusCode >= 400 {
			responseBody := blw.body.String()
			if len(responseBody) > cfg.MaxBodyLogSize {
				responseBody = responseBody[:cfg.MaxBodyLogSize] + "...(truncated)"
			}
			fields = append(fields, zap.String("responseBody", responseBody))
		}

		switch {
		case statusCode >= 500:
			l.With(fields...).Error("request completed")
		case statusCode >= 400:
			l.With(fields...).Warn("request completed")
		default:
			l.With(fields...).Info("request completed")
		}
	}
}

// K8sOperationLogger is a helper function for logging K8s operations
func K8sOperationLogger(c *gin.Context, operation, resource, namespace, name string) func(err error) {
	requestID := c.GetString(RequestIDKey)
	startTime := time.Now()
	l := logger.L().With(
		zap.String("requestId", requestID),
		zap.String("operation", operation),
		zap.String("resource", resource),
		zap.String("namespace", namespace),
		zap.String("name", name),
	)

	l.Info("k8s operation starting")

	return func(err error) {
		latency := time.Since(startTime)
		if err != nil {
			l.Error("k8s operation failed", zap.Duration("latency", latency), zap.Error(err))
		} else {
			l.Info("k8s operation completed", zap.Duration("latency", latency))
		}
	}
}

// BusinessLogger is a business log recorder
// Maintains compatibility with old interface while outputting structured fields.
type BusinessLogger struct {
	logger *zap.SugaredLogger
}

// NewBusinessLogger creates a business logger from context
func NewBusinessLogger(c *gin.Context, component string) *BusinessLogger {
	requestID := c.GetString(RequestIDKey)
	if requestID == "" {
		requestID = "no-req-id"
	}
	l := logger.L().With(
		zap.String("requestId", requestID),
		zap.String("component", component),
	).Sugar()
	return &BusinessLogger{logger: l}
}

// Info logs an info message
func (l *BusinessLogger) Info(format string, args ...interface{}) {
	l.logger.Infof(format, args...)
}

// Warn logs a warning message
func (l *BusinessLogger) Warn(format string, args ...interface{}) {
	l.logger.Warnf(format, args...)
}

// Error logs an error message
func (l *BusinessLogger) Error(err error, format string, args ...interface{}) {
	if err != nil {
		args = append(args, err)
		l.logger.With("error", err).Errorf(format, args...)
	} else {
		l.logger.Errorf(format, args...)
	}
}

// Debug logs a debug message
func (l *BusinessLogger) Debug(format string, args ...interface{}) {
	l.logger.Debugf(format, args...)
}

// WithField adds an extra field
func (l *BusinessLogger) WithField(key string, value interface{}) *BusinessLogger {
	return &BusinessLogger{logger: l.logger.With(key, value)}
}

// StructuredLog is structured log
// (reserved for scenarios requiring JSON format)
type StructuredLog struct {
	Timestamp   string                 `json:"timestamp"`
	Level       string                 `json:"level"`
	RequestID   string                 `json:"request_id,omitempty"`
	Component   string                 `json:"component,omitempty"`
	Message     string                 `json:"message"`
	Method      string                 `json:"method,omitempty"`
	Path        string                 `json:"path,omitempty"`
	StatusCode  int                    `json:"status_code,omitempty"`
	Latency     string                 `json:"latency,omitempty"`
	ClientIP    string                 `json:"client_ip,omitempty"`
	User        string                 `json:"user,omitempty"`
	Context     string                 `json:"context,omitempty"`
	Error       string                 `json:"error,omitempty"`
	ExtraFields map[string]interface{} `json:"extra,omitempty"`
}

// LogJSON outputs log in JSON format
func LogJSON(l StructuredLog) {
	l.Timestamp = time.Now().UTC().Format(time.RFC3339)
	if data, err := json.Marshal(l); err == nil {
		logger.L().Info(string(data))
	}
}

func normalizeSensitiveKey(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, "_", "")
	return s
}

func buildSensitiveKeySet(fields []string) map[string]struct{} {
	set := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		n := normalizeSensitiveKey(f)
		if n == "" {
			continue
		}
		set[n] = struct{}{}
	}
	return set
}

func maskJSONValue(v interface{}, sensitive map[string]struct{}) {
	switch vv := v.(type) {
	case map[string]interface{}:
		for k, val := range vv {
			if _, ok := sensitive[normalizeSensitiveKey(k)]; ok {
				vv[k] = "***MASKED***"
				continue
			}
			maskJSONValue(val, sensitive)
		}
	case []interface{}:
		for i := range vv {
			maskJSONValue(vv[i], sensitive)
		}
	}
}

// maskSensitiveFieldsJSON masks configured fields from a JSON body string (best-effort).
func maskSensitiveFieldsJSON(body string, fields []string) string {
	if len(fields) == 0 {
		return body
	}
	trimmed := strings.TrimSpace(body)
	if trimmed == "" {
		return body
	}

	var v interface{}
	if err := json.Unmarshal([]byte(trimmed), &v); err != nil {
		return body
	}
	maskJSONValue(v, buildSensitiveKeySet(fields))
	b, err := json.Marshal(v)
	if err != nil {
		return body
	}
	return string(b)
}

// truncateString truncates a string
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
