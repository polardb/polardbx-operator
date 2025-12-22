package errors

import (
	"crypto/rand"
	"log"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"polardbx-dashboard-backend/pkg/logger"
)

// ============================================================================
// Global Error Handler Middleware
// ============================================================================

// Handler returns a Gin handler that properly formats API errors
func Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		// Check for errors after handler execution
		if len(c.Errors) > 0 {
			err := c.Errors.Last().Err
			HandleError(c, err)
		}
	}
}

// RecoveryHandler returns a recovery middleware that converts panics to APIError
func RecoveryHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				// Log the panic with stack trace (internal only)
				log.Printf("PANIC recovered: %v\nStack trace:\n%s", r, debug.Stack())

				// Return generic error to client (no internal details)
				apiErr := &APIError{
					Code:       ErrInternal,
					Message:    "An unexpected error occurred",
					HTTPStatus: http.StatusInternalServerError,
				}
				c.AbortWithStatusJSON(apiErr.HTTPStatusCode(), apiErr.ToResponse())
			}
		}()
		c.Next()
	}
}

// ============================================================================
// Error Handling Functions
// ============================================================================

// HandleError handles an error and sends appropriate response
// It performs the following:
// 1. Converts known error types to APIError
// 2. Logs internal details (not exposed to client)
// 3. Returns sanitized response to client
func HandleError(c *gin.Context, err error) {
	if err == nil {
		return
	}

	apiErr := ConvertServiceError(err)

	// Log the full error internally (with context)
	logError(c, apiErr)

	// Send sanitized response to client
	sendErrorResponse(c, apiErr)
}

// logError logs the error with request context for debugging
func logError(c *gin.Context, apiErr *APIError) {
	// Build log context
	user := c.GetString("k8sUser")
	if user == "" {
		user = "anonymous"
	}

	requestID := c.GetString("requestId")
	if requestID == "" && c.Request != nil {
		requestID = c.GetHeader("X-Request-ID")
	}

	// Log with context (internal details) using structured logger
	method := ""
	path := ""
	if c.Request != nil {
		method = c.Request.Method
		path = c.Request.URL.Path
	}

	logFields := []interface{}{
		"code", apiErr.Code,
		"user", user,
		"requestId", requestID,
		"method", method,
		"path", path,
		"message", apiErr.Message,
	}

	if apiErr.Cause != nil {
		logFields = append(logFields, "cause", apiErr.Cause)
	}

	logger.Error("API error occurred", logFields...)
}

// sendErrorResponse sends sanitized error response to client
func sendErrorResponse(c *gin.Context, apiErr *APIError) {
	// Build response (sanitized, no internal details)
	resp := apiErr.ToResponse()

	// Add retry headers if applicable
	if details, ok := apiErr.Details.(map[string]interface{}); ok {
		if retryAfter, ok := details["retry_after_seconds"].(int); ok && retryAfter > 0 {
			c.Header("Retry-After", strconv.Itoa(retryAfter))
		}
	}

	c.JSON(apiErr.HTTPStatusCode(), resp)
}

// ============================================================================
// Abort Functions (stop request processing)
// ============================================================================

// Abort aborts the request with an API error
func Abort(c *gin.Context, apiErr *APIError) {
	logError(c, apiErr)
	c.AbortWithStatusJSON(apiErr.HTTPStatusCode(), apiErr.ToResponse())
}

// AbortWithError aborts with any error (converts to APIError first)
func AbortWithError(c *gin.Context, err error) {
	apiErr := ConvertServiceError(err)
	Abort(c, apiErr)
}

// AbortNotFound aborts with a not found error
func AbortNotFound(c *gin.Context, resource, name string) {
	Abort(c, NotFound(resource, name))
}

// AbortValidation aborts with a validation error
func AbortValidation(c *gin.Context, message string) {
	Abort(c, Validation(message))
}

// AbortUnauthorized aborts with an unauthorized error
func AbortUnauthorized(c *gin.Context, message string) {
	Abort(c, Unauthorized(message))
}

// AbortForbidden aborts with a forbidden error
func AbortForbidden(c *gin.Context, message string) {
	Abort(c, Forbidden(message))
}

// AbortInternal aborts with an internal server error
func AbortInternal(c *gin.Context, message string) {
	Abort(c, Internal(message))
}

// AbortK8sError aborts with a Kubernetes error (sanitized for client)
func AbortK8sError(c *gin.Context, operation string, err error) {
	apiErr := FromK8sError(err)
	if apiErr == nil {
		apiErr = Internal("Kubernetes operation failed")
	}

	// Override message to be more user-friendly
	switch {
	case k8serrors.IsNotFound(err):
		apiErr.Message = "Resource not found"
	case k8serrors.IsForbidden(err):
		apiErr.Message = "Permission denied for this operation"
	case k8serrors.IsConflict(err):
		apiErr.Message = "Resource conflict - please retry"
	case k8serrors.IsTimeout(err), k8serrors.IsServerTimeout(err):
		apiErr.Message = "Operation timed out - please retry"
	default:
		apiErr.Message = "Failed to " + operation
	}

	apiErr.Cause = err
	Abort(c, apiErr)
}

// ============================================================================
// Response Helpers
// ============================================================================

// Response sends a success response
func Response(c *gin.Context, status int, data interface{}) {
	c.JSON(status, data)
}

// OK sends a 200 OK response
func OK(c *gin.Context, data interface{}) {
	Response(c, http.StatusOK, data)
}

// Created sends a 201 Created response
func Created(c *gin.Context, data interface{}) {
	Response(c, http.StatusCreated, data)
}

// NoContent sends a 204 No Content response
func NoContent(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

// Accepted sends a 202 Accepted response (for async operations)
func Accepted(c *gin.Context, data interface{}) {
	Response(c, http.StatusAccepted, data)
}

// ============================================================================
// Request ID Middleware
// ============================================================================

// RequestIDMiddleware adds a request ID to the context for tracing
func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			requestID = generateRequestID()
		}
		c.Set("requestId", requestID)
		c.Header("X-Request-ID", requestID)
		c.Next()
	}
}

// generateRequestID generates a cryptographically secure request ID
func generateRequestID() string {
	// Use timestamp prefix for readability, then add secure random suffix
	timestamp := time.Now().Format("20060102150405")
	randomSuffix := randomString(8)
	return timestamp + "-" + randomSuffix
}

// randomString generates a cryptographically secure random string of given length
// Uses crypto/rand for security-critical identifiers like request IDs
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	// Read random bytes
	if _, err := rand.Read(b); err != nil {
		// Fallback to timestamp-based (not ideal, but better than panic)
		// This should rarely happen in practice
		for i := range b {
			b[i] = letters[time.Now().UnixNano()%int64(len(letters))]
			time.Sleep(time.Nanosecond)
		}
		return string(b)
	}
	// Map random bytes to letters
	for i := range b {
		b[i] = letters[b[i]%byte(len(letters))]
	}
	return string(b)
}
