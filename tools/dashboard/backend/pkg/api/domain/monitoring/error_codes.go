package domain_monitoring

// ErrorCode represents a stable identifier for errors returned by the
// monitoring installation APIs. Codes are shared with the Angular frontend so
// messaging can be localized consistently.
type ErrorCode string

const (
	ErrorCodeDetectionUnavailable ErrorCode = "monitoring/detect-unavailable"
	ErrorCodeDetectionFailed      ErrorCode = "monitoring/detect-failed"

	ErrorCodePlanInvalid    ErrorCode = "monitoring/plan-invalid"
	ErrorCodePlanEmpty      ErrorCode = "monitoring/install-plan-empty"
	ErrorCodeInstallInvalid ErrorCode = "monitoring/install-invalid-request"

	ErrorCodeKubernetesClientMissing ErrorCode = "monitoring/k8s-client-missing"

	ErrorCodeSessionCreateFailed  ErrorCode = "monitoring/session-create-failed"
	ErrorCodeSessionRequired      ErrorCode = "monitoring/session-required"
	ErrorCodeSessionPersistFailed ErrorCode = "monitoring/session-persist-failed"
	ErrorCodeSessionRestoreFailed ErrorCode = "monitoring/session-restore-failed"
	ErrorCodeSessionNotFound      ErrorCode = "monitoring/session-not-found"

	ErrorCodeRetryInvalidRequest ErrorCode = "monitoring/retry-invalid-request"
	ErrorCodeRetryInvalidMode    ErrorCode = "monitoring/retry-invalid-mode"
	ErrorCodeRetryActivateFailed ErrorCode = "monitoring/retry-activate-failed"
	ErrorCodeRetryPersistFailed  ErrorCode = "monitoring/retry-persist-failed"
	ErrorCodeRetryUnavailable    ErrorCode = "monitoring/retry-unavailable"
	ErrorCodeRetryLimitReached   ErrorCode = "monitoring/retry-limit-reached"
	ErrorCodeRetryStartFailed    ErrorCode = "monitoring/retry-start-failed"

	ErrorCodeDiagnoseInvalid       ErrorCode = "monitoring/diagnose-invalid"
	ErrorCodeDiagnoseContextFailed ErrorCode = "monitoring/diagnose-context-failed"
	ErrorCodeDiagnoseFailed        ErrorCode = "monitoring/diagnose-failed"

	ErrorCodeAutoFixInvalid       ErrorCode = "monitoring/autofix-invalid"
	ErrorCodeAutoFixContextFailed ErrorCode = "monitoring/autofix-context-failed"
	ErrorCodeAutoFixFailed        ErrorCode = "monitoring/autofix-failed"

	ErrorCodeInstallFailed   ErrorCode = "monitoring/install-failed"
	ErrorCodeUninstallFailed ErrorCode = "monitoring/uninstall-failed"

	ErrorCodeUnknown ErrorCode = "monitoring/unknown-error"
)
