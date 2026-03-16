package errors

import (
	"fmt"
)

// ErrorCategory represents a high-level category for service-layer errors.
// It is intentionally coarse-grained so that HTTP and logging behavior can be
// driven consistently across domains.
type ErrorCategory string

const (
	CategoryValidation   ErrorCategory = "validation"
	CategoryNotFound     ErrorCategory = "not_found"
	CategoryConflict     ErrorCategory = "conflict"
	CategoryUnauthorized ErrorCategory = "unauthorized"
	CategoryForbidden    ErrorCategory = "forbidden"
	CategoryK8s          ErrorCategory = "k8s"
	CategoryInternal     ErrorCategory = "internal"
)

// ServiceError is a domain/service-layer error that is independent of HTTP / gin.
// Handlers are responsible for converting ServiceError to APIError.
type ServiceError struct {
	Category ErrorCategory
	Code     ErrorCode
	Message  string
	Details  interface{}
	Cause    error
}

// Error implements error.
func (e *ServiceError) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

// Unwrap returns the underlying cause.
func (e *ServiceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// NewServiceError builds a new ServiceError.
func NewServiceError(cat ErrorCategory, code ErrorCode, message string, cause error) *ServiceError {
	return &ServiceError{
		Category: cat,
		Code:     code,
		Message:  message,
		Cause:    cause,
	}
}

// Helper constructors for common categories – these can be gradually adopted by services.

func ValidationError(message string, details interface{}) *ServiceError {
	return &ServiceError{
		Category: CategoryValidation,
		Code:     ErrValidation,
		Message:  message,
		Details:  details,
	}
}

func NotFoundError(resource, name string) *ServiceError {
	return &ServiceError{
		Category: CategoryNotFound,
		Code:     ErrNotFound,
		Message:  fmt.Sprintf("%s '%s' not found", resource, name),
	}
}

func ConflictError(message string) *ServiceError {
	return &ServiceError{
		Category: CategoryConflict,
		Code:     ErrConflict,
		Message:  message,
	}
}

func UnauthorizedError(message string) *ServiceError {
	return &ServiceError{
		Category: CategoryUnauthorized,
		Code:     ErrUnauthorized,
		Message:  message,
	}
}

func ForbiddenError(message string) *ServiceError {
	return &ServiceError{
		Category: CategoryForbidden,
		Code:     ErrForbidden,
		Message:  message,
	}
}

func InternalServiceError(message string, cause error) *ServiceError {
	return &ServiceError{
		Category: CategoryInternal,
		Code:     ErrInternal,
		Message:  message,
		Cause:    cause,
	}
}

// ConvertServiceError converts an arbitrary error (including ServiceError)
// into an APIError. This is the primary bridge between service-layer errors
// and HTTP responses.
func ConvertServiceError(err error) *APIError {
	if err == nil {
		return nil
	}

	// Already an APIError
	if apiErr, ok := err.(*APIError); ok {
		return apiErr
	}

	// ServiceError – map by category/code to APIError
	if se, ok := err.(*ServiceError); ok {
		code := se.Code
		if code == "" {
			// Fallback to default codes by category
			switch se.Category {
			case CategoryValidation:
				code = ErrValidation
			case CategoryNotFound:
				code = ErrNotFound
			case CategoryConflict:
				code = ErrConflict
			case CategoryUnauthorized:
				code = ErrUnauthorized
			case CategoryForbidden:
				code = ErrForbidden
			case CategoryK8s:
				code = ErrK8sConnection
			default:
				code = ErrInternal
			}
		}

		apiErr := &APIError{
			Code:    code,
			Message: se.Message,
			Details: se.Details,
			Cause:   se.Cause,
		}

		// If HTTPStatus is not set, HTTPStatusCode() will derive from Code.
		return apiErr
	}

	// Fallback to existing conversion logic:
	// 1. Context errors
	if ctxErr := FromContextError(err); ctxErr != nil {
		return ctxErr
	}
	// 2. K8s errors (except connection errors which are mapped to generic gateway errors)
	if k8sErr := FromK8sError(err); k8sErr != nil && k8sErr.Code != ErrK8sConnection {
		return k8sErr
	}
	// 3. Binding / validation errors from gin
	if bindErr := FromBindError(err); bindErr != nil {
		return bindErr
	}

	// 4. Generic internal error fallback
	apiErr := Internal("An error occurred while processing your request")
	apiErr.Cause = err
	return apiErr
}
