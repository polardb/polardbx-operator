package errors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/go-playground/validator/v10"
)

// ============================================================================
// Context-aware error handling
// ============================================================================

// FromContextError converts context errors to APIError
func FromContextError(err error) *APIError {
	if err == nil {
		return nil
	}

	switch {
	case err == context.DeadlineExceeded:
		return &APIError{
			Code:       ErrTimeout,
			Message:    "Request timeout - please try again",
			HTTPStatus: http.StatusGatewayTimeout,
			Cause:      err,
			Details: map[string]interface{}{
				"retryable": true,
				"reason":    "deadline_exceeded",
			},
		}
	case err == context.Canceled:
		return &APIError{
			Code:       ErrTimeout,
			Message:    "Request was cancelled",
			HTTPStatus: http.StatusGatewayTimeout,
			Cause:      err,
			Details: map[string]interface{}{
				"retryable": true,
				"reason":    "cancelled",
			},
		}
	default:
		return nil
	}
}

// IsContextError checks if the error is a context-related error
func IsContextError(err error) bool {
	return err == context.DeadlineExceeded || err == context.Canceled
}

// ============================================================================
// Validation error handling (for gin binding)
// ============================================================================

// FieldError represents a single field validation error
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
	Tag     string `json:"tag,omitempty"`
	Value   string `json:"value,omitempty"`
}

// FromBindError converts binding/validation errors to APIError with field-level details
func FromBindError(err error) *APIError {
	if err == nil {
		return nil
	}

	// Check if it's a validator error
	if validationErrors, ok := err.(validator.ValidationErrors); ok {
		fieldErrors := make([]FieldError, 0, len(validationErrors))
		for _, fe := range validationErrors {
			fieldErrors = append(fieldErrors, FieldError{
				Field:   toSnakeCase(fe.Field()),
				Message: formatValidationMessage(fe),
				Tag:     fe.Tag(),
			})
		}
		return &APIError{
			Code:       ErrValidation,
			Message:    "Validation failed",
			HTTPStatus: http.StatusBadRequest,
			Cause:      err,
			Details: map[string]interface{}{
				"fields": fieldErrors,
			},
		}
	}

	// Only treat known decoder/binding failures as "invalid format".
	// This must NOT match arbitrary business/service errors.
	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return &APIError{Code: ErrInvalidFormat, Message: "Invalid request format", HTTPStatus: http.StatusBadRequest, Cause: err}
	}
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return &APIError{Code: ErrInvalidFormat, Message: "Invalid request format", HTTPStatus: http.StatusBadRequest, Cause: err}
	}
	var invalidUnmarshal *json.InvalidUnmarshalError
	if errors.As(err, &invalidUnmarshal) {
		return &APIError{Code: ErrInvalidFormat, Message: "Invalid request format", HTTPStatus: http.StatusBadRequest, Cause: err}
	}
	if errors.Is(err, io.EOF) {
		return &APIError{Code: ErrInvalidFormat, Message: "Invalid request format", HTTPStatus: http.StatusBadRequest, Cause: err}
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return &APIError{Code: ErrInvalidFormat, Message: "Invalid request format", HTTPStatus: http.StatusBadRequest, Cause: err}
	}
	var numErr *strconv.NumError
	if errors.As(err, &numErr) {
		return &APIError{Code: ErrInvalidFormat, Message: "Invalid request format", HTTPStatus: http.StatusBadRequest, Cause: err}
	}

	return nil
}

// formatValidationMessage creates a user-friendly validation message
func formatValidationMessage(fe validator.FieldError) string {
	switch fe.Tag() {
	case "required":
		return fmt.Sprintf("%s is required", toSnakeCase(fe.Field()))
	case "min":
		return fmt.Sprintf("%s must be at least %s", toSnakeCase(fe.Field()), fe.Param())
	case "max":
		return fmt.Sprintf("%s must be at most %s", toSnakeCase(fe.Field()), fe.Param())
	case "email":
		return fmt.Sprintf("%s must be a valid email address", toSnakeCase(fe.Field()))
	case "url":
		return fmt.Sprintf("%s must be a valid URL", toSnakeCase(fe.Field()))
	case "oneof":
		return fmt.Sprintf("%s must be one of: %s", toSnakeCase(fe.Field()), fe.Param())
	case "gt":
		return fmt.Sprintf("%s must be greater than %s", toSnakeCase(fe.Field()), fe.Param())
	case "gte":
		return fmt.Sprintf("%s must be greater than or equal to %s", toSnakeCase(fe.Field()), fe.Param())
	case "lt":
		return fmt.Sprintf("%s must be less than %s", toSnakeCase(fe.Field()), fe.Param())
	case "lte":
		return fmt.Sprintf("%s must be less than or equal to %s", toSnakeCase(fe.Field()), fe.Param())
	default:
		return fmt.Sprintf("%s failed validation: %s", toSnakeCase(fe.Field()), fe.Tag())
	}
}

// toSnakeCase converts CamelCase to snake_case
func toSnakeCase(s string) string {
	var result []byte
	for i, c := range s {
		if c >= 'A' && c <= 'Z' {
			if i > 0 {
				result = append(result, '_')
			}
			result = append(result, byte(c-'A'+'a'))
		} else {
			result = append(result, byte(c))
		}
	}
	return string(result)
}

// ============================================================================
// Retry-aware errors
// ============================================================================

// RetryInfo contains retry guidance for clients
type RetryInfo struct {
	Retryable  bool          `json:"retryable"`
	RetryAfter time.Duration `json:"-"`
	Reason     string        `json:"reason,omitempty"`
}

// WithRetry adds retry information to an error
func (e *APIError) WithRetry(retryable bool, retryAfter time.Duration) *APIError {
	if e.Details == nil {
		e.Details = make(map[string]interface{})
	}
	if details, ok := e.Details.(map[string]interface{}); ok {
		details["retryable"] = retryable
		if retryAfter > 0 {
			details["retry_after_seconds"] = int(retryAfter.Seconds())
		}
	}
	return e
}

// IsRetryable checks if the error indicates the operation can be retried
func IsRetryable(err error) bool {
	if apiErr, ok := err.(*APIError); ok {
		if details, ok := apiErr.Details.(map[string]interface{}); ok {
			if retryable, ok := details["retryable"].(bool); ok {
				return retryable
			}
		}
		// Default retryable codes
		switch apiErr.Code {
		case ErrTimeout, ErrServiceUnavail, ErrRateLimited, ErrK8sTimeout:
			return true
		}
	}
	return false
}

// Timeout creates a timeout error with retry info
func Timeout(message string) *APIError {
	return &APIError{
		Code:       ErrTimeout,
		Message:    message,
		HTTPStatus: http.StatusGatewayTimeout,
		Details: map[string]interface{}{
			"retryable": true,
		},
	}
}

// RateLimited creates a rate limit error with retry-after
func RateLimited(retryAfter time.Duration) *APIError {
	return &APIError{
		Code:       ErrRateLimited,
		Message:    "Too many requests - please slow down",
		HTTPStatus: http.StatusTooManyRequests,
		Details: map[string]interface{}{
			"retryable":           true,
			"retry_after_seconds": int(retryAfter.Seconds()),
		},
	}
}

// ServiceUnavailable creates a service unavailable error
func ServiceUnavailable(message string, retryAfter time.Duration) *APIError {
	err := &APIError{
		Code:       ErrServiceUnavail,
		Message:    message,
		HTTPStatus: http.StatusServiceUnavailable,
		Details: map[string]interface{}{
			"retryable": true,
		},
	}
	if retryAfter > 0 {
		err.Details.(map[string]interface{})["retry_after_seconds"] = int(retryAfter.Seconds())
	}
	return err
}
