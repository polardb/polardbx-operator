package errors

import (
	"fmt"
	"strings"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

// New creates a new APIError
func New(code ErrorCode, message string) *APIError {
	return &APIError{
		Code:    code,
		Message: message,
	}
}

// Newf creates a new APIError with formatted message
func Newf(code ErrorCode, format string, args ...interface{}) *APIError {
	return &APIError{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
	}
}

// Wrap wraps an error with an APIError
func Wrap(code ErrorCode, message string, err error) *APIError {
	return &APIError{
		Code:    code,
		Message: message,
		Cause:   err,
	}
}

// Common error constructors

// Internal creates an internal server error
func Internal(message string) *APIError {
	return New(ErrInternal, message)
}

// NotFound creates a not found error
func NotFound(resource, name string) *APIError {
	return Newf(ErrNotFound, "%s '%s' not found", resource, name)
}

// AlreadyExists creates an already exists error
func AlreadyExists(resource, name string) *APIError {
	return Newf(ErrAlreadyExists, "%s '%s' already exists", resource, name)
}

// Conflict creates a conflict error (e.g., resource version conflict)
func Conflict(message string) *APIError {
	return New(ErrK8sConflict, message)
}

// Validation creates a validation error
func Validation(message string) *APIError {
	return New(ErrValidation, message)
}

// ValidationWithDetails creates a validation error with field details
func ValidationWithDetails(message string, details map[string]string) *APIError {
	return New(ErrValidation, message).WithDetails(details)
}

// Unauthorized creates an unauthorized error
func Unauthorized(message string) *APIError {
	if message == "" {
		message = "Authentication required"
	}
	return New(ErrUnauthorized, message)
}

// Forbidden creates a forbidden error
func Forbidden(message string) *APIError {
	if message == "" {
		message = "Access denied"
	}
	return New(ErrForbidden, message)
}

// InvalidParam creates an invalid parameter error
func InvalidParam(param, reason string) *APIError {
	return Newf(ErrInvalidParam, "Invalid parameter '%s': %s", param, reason)
}

// MissingParam creates a missing parameter error
func MissingParam(param string) *APIError {
	return Newf(ErrMissingParam, "Required parameter '%s' is missing", param)
}

// FromK8sError converts a Kubernetes error to APIError
func FromK8sError(err error) *APIError {
	if err == nil {
		return nil
	}

	// Check for specific Kubernetes error types
	switch {
	case k8serrors.IsNotFound(err):
		return Wrap(ErrK8sNotFound, extractK8sResourceInfo(err), err)
	case k8serrors.IsAlreadyExists(err):
		return Wrap(ErrAlreadyExists, extractK8sResourceInfo(err), err)
	case k8serrors.IsConflict(err):
		return Wrap(ErrK8sConflict, "Resource conflict", err)
	case k8serrors.IsForbidden(err):
		return Wrap(ErrK8sPermission, "Insufficient Kubernetes permissions", err)
	case k8serrors.IsUnauthorized(err):
		return Wrap(ErrKubeconfigReq, "Kubernetes authentication failed", err)
	case k8serrors.IsTimeout(err):
		return Wrap(ErrK8sTimeout, "Kubernetes operation timed out", err)
	case k8serrors.IsServerTimeout(err):
		return Wrap(ErrK8sTimeout, "Kubernetes server timeout", err)
	default:
		return Wrap(ErrK8sConnection, "Kubernetes operation failed", err)
	}
}

// extractK8sResourceInfo extracts resource information from K8s error
func extractK8sResourceInfo(err error) string {
	if statusErr, ok := err.(*k8serrors.StatusError); ok {
		status := statusErr.Status()
		if status.Details != nil {
			return fmt.Sprintf("%s/%s not found", status.Details.Kind, status.Details.Name)
		}
	}
	return "Resource not found"
}

// BadGateway creates a bad gateway error (502)
func BadGateway(message string) *APIError {
	return New(ErrBadGateway, message)
}

// IsNotFound checks if error is a not found error
func IsNotFound(err error) bool {
	if apiErr, ok := err.(*APIError); ok {
		return apiErr.Code == ErrNotFound || apiErr.Code == ErrK8sNotFound
	}
	return false
}

// IsValidation checks if error is a validation error
func IsValidation(err error) bool {
	if apiErr, ok := err.(*APIError); ok {
		return strings.HasPrefix(string(apiErr.Code), "VAL_")
	}
	return false
}

// IsK8sError checks if error is a Kubernetes error
func IsK8sError(err error) bool {
	if apiErr, ok := err.(*APIError); ok {
		return strings.HasPrefix(string(apiErr.Code), "K8S_")
	}
	return false
}

// GetCode extracts the error code from an error
func GetCode(err error) ErrorCode {
	if apiErr, ok := err.(*APIError); ok {
		return apiErr.Code
	}
	return ErrInternal
}
