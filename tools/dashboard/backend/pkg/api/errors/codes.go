package errors

import (
	"fmt"
	"net/http"
)

// ErrorCode represents a unique error code
type ErrorCode string

// Error code constants - organized by category
const (
	// System errors (1xxx)
	ErrInternal        ErrorCode = "SYS_1001" // Internal server error
	ErrServiceUnavail  ErrorCode = "SYS_1002" // Service temporarily unavailable
	ErrTimeout         ErrorCode = "SYS_1003" // Request timeout
	ErrRateLimited     ErrorCode = "SYS_1004" // Rate limit exceeded
	ErrMaintenanceMode ErrorCode = "SYS_1005" // System in maintenance mode

	// Authentication errors (2xxx)
	ErrUnauthorized    ErrorCode = "AUTH_2001" // Not authenticated
	ErrForbidden       ErrorCode = "AUTH_2002" // Not authorized
	ErrInvalidToken    ErrorCode = "AUTH_2003" // Invalid or expired token
	ErrKubeconfigReq   ErrorCode = "AUTH_2004" // Kubeconfig required
	ErrInvalidKubeconf ErrorCode = "AUTH_2005" // Invalid kubeconfig

	// Validation errors (3xxx)
	ErrValidation    ErrorCode = "VAL_3001" // Validation failed
	ErrInvalidParam  ErrorCode = "VAL_3002" // Invalid parameter
	ErrMissingParam  ErrorCode = "VAL_3003" // Required parameter missing
	ErrInvalidFormat ErrorCode = "VAL_3004" // Invalid format
	ErrInvalidYAML   ErrorCode = "VAL_3005" // Invalid YAML

	// Resource errors (4xxx)
	ErrNotFound      ErrorCode = "RES_4001" // Resource not found
	ErrAlreadyExists ErrorCode = "RES_4002" // Resource already exists
	ErrConflict      ErrorCode = "RES_4003" // Resource conflict
	ErrQuotaExceeded ErrorCode = "RES_4004" // Resource quota exceeded
	ErrInvalidState  ErrorCode = "RES_4005" // Invalid resource state

	// Kubernetes errors (5xxx)
	ErrK8sConnection ErrorCode = "K8S_5001" // Failed to connect to cluster
	ErrK8sPermission ErrorCode = "K8S_5002" // Insufficient permissions
	ErrK8sNotFound   ErrorCode = "K8S_5003" // Kubernetes resource not found
	ErrK8sConflict   ErrorCode = "K8S_5004" // Kubernetes resource conflict
	ErrK8sTimeout    ErrorCode = "K8S_5005" // Kubernetes operation timeout
	ErrCRDNotFound   ErrorCode = "K8S_5006" // CRD not installed

	// Cluster operations (6xxx)
	ErrClusterNotReady ErrorCode = "CLU_6001" // Cluster not ready
	ErrClusterLocked   ErrorCode = "CLU_6002" // Cluster operation in progress
	ErrScaleFailed     ErrorCode = "CLU_6003" // Scale operation failed
	ErrUpgradeFailed   ErrorCode = "CLU_6004" // Upgrade operation failed
	ErrDeleteFailed    ErrorCode = "CLU_6005" // Delete operation failed

	// Backup operations (7xxx)
	ErrBackupFailed     ErrorCode = "BAK_7001" // Backup operation failed
	ErrRestoreFailed    ErrorCode = "BAK_7002" // Restore operation failed
	ErrBackupNotFound   ErrorCode = "BAK_7003" // Backup not found
	ErrBackupInProgress ErrorCode = "BAK_7004" // Backup already in progress
	ErrInvalidSink      ErrorCode = "BAK_7005" // Invalid backup sink

	// Monitoring errors (8xxx)
	ErrMonitorNotInst   ErrorCode = "MON_8001" // Monitoring not installed
	ErrGrafanaFailed    ErrorCode = "MON_8002" // Grafana operation failed
	ErrPrometheusFailed ErrorCode = "MON_8003" // Prometheus operation failed
	ErrHelmFailed       ErrorCode = "MON_8004" // Helm operation failed
	ErrInstallFailed    ErrorCode = "MON_8005" // Installation failed

	// Gateway errors (9xxx)
	ErrBadGateway         ErrorCode = "GW_9001" // Bad gateway (upstream error)
	ErrGatewayTimeout     ErrorCode = "GW_9002" // Gateway timeout
	ErrServiceUnavailable ErrorCode = "GW_9003" // Service unavailable
)

// APIError represents a structured API error
type APIError struct {
	Code       ErrorCode   `json:"code"`
	Message    string      `json:"message"`
	Details    interface{} `json:"details,omitempty"`
	HTTPStatus int         `json:"-"`
	Cause      error       `json:"-"`
}

// Error implements the error interface
func (e *APIError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// Unwrap returns the underlying error
func (e *APIError) Unwrap() error {
	return e.Cause
}

// WithDetails adds additional details to the error
func (e *APIError) WithDetails(details interface{}) *APIError {
	e.Details = details
	return e
}

// WithCause adds the underlying cause
func (e *APIError) WithCause(err error) *APIError {
	e.Cause = err
	return e
}

// ToResponse converts the error to an API response
func (e *APIError) ToResponse() map[string]interface{} {
	resp := map[string]interface{}{
		"error": map[string]interface{}{
			"code":    e.Code,
			"message": e.Message,
		},
	}
	if e.Details != nil {
		resp["error"].(map[string]interface{})["details"] = e.Details
	}
	return resp
}

// HTTPStatusCode returns the HTTP status code for this error
func (e *APIError) HTTPStatusCode() int {
	if e.HTTPStatus != 0 {
		return e.HTTPStatus
	}
	return codeToHTTPStatus[e.Code]
}

// codeToHTTPStatus maps error codes to HTTP status codes
var codeToHTTPStatus = map[ErrorCode]int{
	// System errors
	ErrInternal:        http.StatusInternalServerError,
	ErrServiceUnavail:  http.StatusServiceUnavailable,
	ErrTimeout:         http.StatusGatewayTimeout,
	ErrRateLimited:     http.StatusTooManyRequests,
	ErrMaintenanceMode: http.StatusServiceUnavailable,

	// Auth errors
	ErrUnauthorized:    http.StatusUnauthorized,
	ErrForbidden:       http.StatusForbidden,
	ErrInvalidToken:    http.StatusUnauthorized,
	ErrKubeconfigReq:   http.StatusUnauthorized,
	ErrInvalidKubeconf: http.StatusUnauthorized,

	// Validation errors
	ErrValidation:    http.StatusBadRequest,
	ErrInvalidParam:  http.StatusBadRequest,
	ErrMissingParam:  http.StatusBadRequest,
	ErrInvalidFormat: http.StatusBadRequest,
	ErrInvalidYAML:   http.StatusBadRequest,

	// Resource errors
	ErrNotFound:      http.StatusNotFound,
	ErrAlreadyExists: http.StatusConflict,
	ErrConflict:      http.StatusConflict,
	ErrQuotaExceeded: http.StatusForbidden,
	ErrInvalidState:  http.StatusConflict,

	// Kubernetes errors
	ErrK8sConnection: http.StatusBadGateway,
	ErrK8sPermission: http.StatusForbidden,
	ErrK8sNotFound:   http.StatusNotFound,
	ErrK8sConflict:   http.StatusConflict,
	ErrK8sTimeout:    http.StatusGatewayTimeout,
	ErrCRDNotFound:   http.StatusNotFound,

	// Cluster errors
	ErrClusterNotReady: http.StatusPreconditionFailed,
	ErrClusterLocked:   http.StatusConflict,
	ErrScaleFailed:     http.StatusInternalServerError,
	ErrUpgradeFailed:   http.StatusInternalServerError,
	ErrDeleteFailed:    http.StatusInternalServerError,

	// Backup errors
	ErrBackupFailed:     http.StatusInternalServerError,
	ErrRestoreFailed:    http.StatusInternalServerError,
	ErrBackupNotFound:   http.StatusNotFound,
	ErrBackupInProgress: http.StatusConflict,
	ErrInvalidSink:      http.StatusBadRequest,

	// Monitoring errors
	ErrMonitorNotInst:   http.StatusNotFound,
	ErrGrafanaFailed:    http.StatusInternalServerError,
	ErrPrometheusFailed: http.StatusInternalServerError,
	ErrHelmFailed:       http.StatusInternalServerError,
	ErrInstallFailed:    http.StatusInternalServerError,

	// Gateway errors
	ErrBadGateway:         http.StatusBadGateway,
	ErrGatewayTimeout:     http.StatusGatewayTimeout,
	ErrServiceUnavailable: http.StatusServiceUnavailable,
}
