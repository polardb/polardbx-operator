package service

import (
	"context"

	spec "polardbx-dashboard-backend/pkg/api/domain/monitoring/spec"
)

// SuggestedFixType identifies the form of recommended fixes, used to distinguish between automatic execution, scripted, or manual operation steps.
type SuggestedFixType string

const (
	// SuggestedFixTypeOperatorHook indicates fix actions that can be automatically triggered through Operator/Helm hooks.
	SuggestedFixTypeOperatorHook SuggestedFixType = "operator-hook"
	// SuggestedFixTypeManualProcedure indicates fix actions that require users to manually execute step by step.
	SuggestedFixTypeManualProcedure SuggestedFixType = "manual-procedure"
	// SuggestedFixTypeKnowledgeBase indicates auxiliary information that redirects to knowledge base or external documentation.
	SuggestedFixTypeKnowledgeBase SuggestedFixType = "knowledge-base"
)

// SuggestedFix describes the structure of diagnostic suggestions, which will later be mapped to AutoFix/Fix related models in OpenAPI.
type SuggestedFix struct {
	// ID needs to be unique within the same diagnostic result, facilitating UI to trigger automatic fixes or track events.
	ID string
	// Title is a short title for users.
	Title string
	// Description provides more detailed steps or execution background.
	Description string
	// Type is used to distinguish fix methods, such as automated hooks, manual operations, or knowledge base links.
	Type SuggestedFixType
	// Automated indicates whether this fix can be automatically executed by the backend.
	Automated bool
	// Steps is the structured step description for manual solutions.
	Steps []string
	// Verification provides suggested verification methods after the fix is completed.
	Verification string
	// RelatedComponents are the associated monitoring components, facilitating UI highlighting.
	RelatedComponents []spec.ComponentName
	// Metadata reserves additional information, such as the name of the Operator Hook to be called, RBAC prerequisites, etc.
	Metadata map[string]string
}

// DiagnosticFinding represents a single diagnostic result from a probe.
type DiagnosticFinding struct {
	// ProbeID is the identifier of the probe that produced this conclusion.
	ProbeID string
	// Summary is the user-facing diagnostic summary.
	Summary string
	// Category corresponds to FailureCategory in OpenAPI, facilitating consistency with server-side enums.
	Category spec.FailureCategory
	// Severity corresponds to DiagnosisSeverity, indicating severity level.
	Severity spec.DiagnosisSeverity
	// PossibleCauses are candidate root causes.
	PossibleCauses []string
	// SuggestedFixes is the list of suggested fix solutions.
	SuggestedFixes []SuggestedFix
	// Evidence records screenshots, log snippets, etc. as supporting information.
	Evidence []string
	// Details are additional key-value explanations for UI to expose more context.
	Details map[string]string
}

// ProbeInput provides context information for diagnostic probes, including cluster namespace, session information, and existing installation status.
type ProbeInput struct {
	Namespace string
	SessionID string
	// Checkpoint is the most recent persisted installation checkpoint, can be nil.
	Checkpoint *spec.Checkpoint
	// Status is the installation execution status snapshot, including component progress and error lists.
	Status *spec.InstallStatusResponse
	// Plan is the current installation plan, used for comparing expected actions.
	Plan *spec.InstallationPlan
	// Context is additional diagnostic context, such as manually collected log paths.
	Context map[string]string
}

// DiagnosticProbe defines the minimum interface that diagnostic probes need to implement.
type DiagnosticProbe interface {
	// ID returns a globally unique probe identifier, such as "prometheus-health".
	ID() string
	// Description provides a brief human-readable description.
	Description() string
	// Run executes diagnostic logic and returns a set of diagnostic results; can return empty slice if no issues.
	Run(ctx context.Context, input ProbeInput) ([]DiagnosticFinding, error)
}
