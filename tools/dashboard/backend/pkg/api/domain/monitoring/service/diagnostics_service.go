package service

import (
	"context"
	"fmt"
	"sort"
	"strings"

	spec "polardbx-dashboard-backend/pkg/api/domain/monitoring/spec"
)

// FailureDiagnosticService runs registered probes and aggregates their findings into an API response.
type FailureDiagnosticService interface {
	Diagnose(ctx context.Context, req spec.DiagnoseRequest, input ProbeInput) (spec.DiagnoseResponse, error)
}

type failureDiagnosticService struct {
	probeIDs []string
}

// NewFailureDiagnosticService constructs a diagnostic service using the default probe registry.
func NewFailureDiagnosticService() FailureDiagnosticService {
	ids := ListDefaultDiagnosticProbes()
	return &failureDiagnosticService{probeIDs: ids}
}

func (s *failureDiagnosticService) Diagnose(ctx context.Context, req spec.DiagnoseRequest, input ProbeInput) (spec.DiagnoseResponse, error) {
	base := baseDiagnosis(req)

	findings := make([]DiagnosticFinding, 0)
	for _, id := range s.probeIDs {
		probe := ResolveDiagnosticProbe(id)
		if probe == nil {
			continue
		}
		result, err := probe.Run(ctx, input)
		if err != nil {
			findings = append(findings, DiagnosticFinding{
				ProbeID:  id,
				Summary:  fmt.Sprintf("Probe %s execution failed", id),
				Category: spec.Unknown,
				Severity: spec.Minor,
				Details:  map[string]string{"error": err.Error()},
			})
			continue
		}
		if len(result) > 0 {
			findings = append(findings, result...)
		}
	}

	diagnosis, autoFixes := aggregateFindings(base, findings)
	resp := spec.DiagnoseResponse{Diagnosis: diagnosis}
	if len(autoFixes) > 0 {
		resp.AutoFixes = &autoFixes
	}
	return resp, nil
}

func baseDiagnosis(req spec.DiagnoseRequest) spec.Diagnosis {
	component := "unknown"
	if req.Error.Component != nil {
		component = string(*req.Error.Component)
	}
	summary := strings.TrimSpace(req.Error.Message)
	if summary == "" {
		summary = fmt.Sprintf("Detected %s issue for component %s", req.Error.Category, component)
	}

	severity := inferSeverity(req.Error.Category)
	possible := []string{}
	if ctx := req.Error.Context; ctx != nil {
		if session, ok := (*ctx)["sessionId"]; ok && session != "" {
			possible = append(possible, fmt.Sprintf("Session %s encountered an error", session))
		}
	}
	possible = append(possible, "Inspect installer logs for detailed error output")

	diagnosis := spec.Diagnosis{
		Category: req.Error.Category,
		Severity: severity,
		Summary:  summary,
	}
	if len(possible) > 0 {
		diagnosis.PossibleCauses = &possible
	}
	return diagnosis
}

func aggregateFindings(base spec.Diagnosis, findings []DiagnosticFinding) (spec.Diagnosis, []spec.AutoFix) {
	if len(findings) == 0 {
		// No probe provided extra details; keep base diagnosis.
		suggested := []string{"Review cluster connectivity and retry the installation"}
		base.SuggestedFixes = &suggested
		return base, nil
	}

	merged := base
	severityOrder := map[spec.DiagnosisSeverity]int{
		spec.Minor:    1,
		spec.Major:    2,
		spec.Critical: 3,
	}

	var possibleCauses set[string]
	var suggestedFixSummaries set[string]
	autoFixes := map[string]spec.AutoFix{}

	appendIf := func(collection *set[string], value string) {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return
		}
		if *collection == nil {
			*collection = make(set[string])
		}
		(*collection)[trimmed] = struct{}{}
	}

	for _, finding := range findings {
		if finding.Category != "" {
			merged.Category = finding.Category
		}
		if severityOrder[finding.Severity] > severityOrder[merged.Severity] {
			merged.Severity = finding.Severity
		}
		if strings.TrimSpace(finding.Summary) != "" {
			merged.Summary = finding.Summary
		}
		for _, cause := range finding.PossibleCauses {
			appendIf(&possibleCauses, cause)
		}
		for _, fix := range finding.SuggestedFixes {
			appendIf(&suggestedFixSummaries, summarizeSuggestedFix(fix))
			if fix.ID == "" {
				continue
			}
			if _, exists := autoFixes[fix.ID]; exists {
				continue
			}
			autoFixes[fix.ID] = toAutoFix(fix)
		}
	}

	if len(possibleCauses) > 0 {
		merged.PossibleCauses = toSortedSlice(possibleCauses)
	}
	if len(suggestedFixSummaries) > 0 {
		merged.SuggestedFixes = toSortedSlice(suggestedFixSummaries)
	}

	fixes := make([]spec.AutoFix, 0, len(autoFixes))
	for _, fix := range autoFixes {
		fixes = append(fixes, fix)
	}
	sort.Slice(fixes, func(i, j int) bool { return fixes[i].Id < fixes[j].Id })
	return merged, fixes
}

type set[T comparable] map[T]struct{}

func toSortedSlice(values set[string]) *[]string {
	if len(values) == 0 {
		return nil
	}
	list := make([]string, 0, len(values))
	for v := range values {
		list = append(list, v)
	}
	sort.Strings(list)
	return &list
}

func summarizeSuggestedFix(f SuggestedFix) string {
	title := strings.TrimSpace(f.Title)
	description := strings.TrimSpace(f.Description)
	switch {
	case title != "" && description != "":
		return fmt.Sprintf("%s — %s", title, description)
	case title != "":
		if len(f.Steps) > 0 {
			return fmt.Sprintf("%s — %s", title, strings.Join(f.Steps, "; "))
		}
		return title
	case description != "":
		return description
	default:
		if len(f.Steps) > 0 {
			return strings.Join(f.Steps, "; ")
		}
	}
	return ""
}

func toAutoFix(f SuggestedFix) spec.AutoFix {
	autoFix := spec.AutoFix{
		Id:        f.ID,
		Title:     f.Title,
		Automated: f.Automated,
	}
	if f.Description != "" {
		desc := f.Description
		autoFix.Description = &desc
	}
	if f.Verification != "" {
		verify := f.Verification
		autoFix.Verification = &verify
	}
	return autoFix
}

func inferSeverity(category spec.FailureCategory) spec.DiagnosisSeverity {
	switch category {
	case spec.Timeout, spec.RBAC, spec.ResourceQuota, spec.Dependency, spec.HelmChart, spec.NetworkPolicy:
		return spec.Major
	case spec.Configuration, spec.StorageClass:
		return spec.Major
	case spec.Unknown:
		fallthrough
	default:
		return spec.Minor
	}
}
