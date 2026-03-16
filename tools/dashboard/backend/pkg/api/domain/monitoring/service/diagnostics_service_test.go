package service

import (
	"context"
	"testing"

	spec "polardbx-dashboard-backend/pkg/api/domain/monitoring/spec"
)

type stubProbe struct {
	id       string
	findings []DiagnosticFinding
	err      error
}

func (p stubProbe) ID() string { return p.id }

func (p stubProbe) Description() string { return "stub" }

func (p stubProbe) Run(_ context.Context, _ ProbeInput) ([]DiagnosticFinding, error) {
	return p.findings, p.err
}

func withDefaultProbes(t *testing.T, probe DiagnosticProbe) {
	original := defaultDiagnosticProbes
	defaultDiagnosticProbes = map[string]struct {
		Description string
		Factory     DiagnosticProbeFactory
	}{
		probe.ID(): {
			Description: probe.Description(),
			Factory:     func() DiagnosticProbe { return probe },
		},
	}
	t.Cleanup(func() {
		defaultDiagnosticProbes = original
	})
}

func TestDiagnoseReturnsBaseDiagnosisWhenNoFindings(t *testing.T) {
	probe := stubProbe{id: "noop"}
	withDefaultProbes(t, probe)

	svc := NewFailureDiagnosticService()
	req := spec.DiagnoseRequest{
		Error: spec.InstallError{
			Category:  spec.Unknown,
			Message:   "Provisioning failed",
			Component: func() *spec.ComponentName { c := spec.Grafana; return &c }(),
			Context: &map[string]string{
				"sessionId": "sess-123",
			},
		},
	}

	resp, err := svc.Diagnose(context.Background(), req, ProbeInput{})
	if err != nil {
		t.Fatalf("diagnose returned error: %v", err)
	}

	diagnosis := resp.Diagnosis
	if diagnosis.Category != spec.Unknown {
		t.Fatalf("unexpected category: %s", diagnosis.Category)
	}
	if diagnosis.Severity != spec.Minor {
		t.Fatalf("unexpected severity: %s", diagnosis.Severity)
	}
	if diagnosis.Summary != "Provisioning failed" {
		t.Fatalf("unexpected summary: %s", diagnosis.Summary)
	}
	if resp.AutoFixes != nil {
		t.Fatalf("expected no auto fixes when probes return nothing")
	}
	if diagnosis.SuggestedFixes == nil || len(*diagnosis.SuggestedFixes) != 1 {
		t.Fatalf("expected default suggested fix, got %#v", diagnosis.SuggestedFixes)
	}
	if (*diagnosis.SuggestedFixes)[0] != "Review cluster connectivity and retry the installation" {
		t.Fatalf("unexpected suggested fix text: %s", (*diagnosis.SuggestedFixes)[0])
	}
}

func TestDiagnoseAggregatesProbeFindings(t *testing.T) {
	findings := []DiagnosticFinding{
		{
			ProbeID:  "probe-1",
			Summary:  "Prometheus StatefulSet pending",
			Category: spec.Dependency,
			Severity: spec.Critical,
			PossibleCauses: []string{
				"StatefulSet waiting for PVC",
				"StatefulSet waiting for PVC",
			},
			SuggestedFixes: []SuggestedFix{
				{
					ID:          "monitoring::prometheus::restart",
					Title:       "Restart Prometheus",
					Description: "Roll the StatefulSet to fetch new PVCs",
					Automated:   true,
				},
			},
		},
		{
			ProbeID:  "probe-2",
			Severity: spec.Major,
			SuggestedFixes: []SuggestedFix{
				{
					ID:          "monitoring::prometheus::restart",
					Title:       "Restart Prometheus",
					Description: "Roll the StatefulSet to fetch new PVCs",
					Automated:   true,
				},
				{
					ID:        "monitoring::grafana::secret",
					Title:     "Recreate Grafana admin secret",
					Automated: false,
					Steps: []string{
						"kubectl create secret generic grafana-admin",
					},
				},
			},
		},
	}
	probe := stubProbe{id: "aggregate", findings: findings}
	withDefaultProbes(t, probe)

	svc := NewFailureDiagnosticService()
	req := spec.DiagnoseRequest{
		Error: spec.InstallError{
			Category: spec.Timeout,
			Message:  "Installer timed out",
		},
	}

	resp, err := svc.Diagnose(context.Background(), req, ProbeInput{})
	if err != nil {
		t.Fatalf("diagnose returned error: %v", err)
	}

	diagnosis := resp.Diagnosis
	if diagnosis.Category != spec.Dependency {
		t.Fatalf("expected category from finding, got %s", diagnosis.Category)
	}
	if diagnosis.Severity != spec.Critical {
		t.Fatalf("expected escalated severity critical, got %s", diagnosis.Severity)
	}
	if diagnosis.Summary != "Prometheus StatefulSet pending" {
		t.Fatalf("unexpected summary: %s", diagnosis.Summary)
	}

	if diagnosis.PossibleCauses == nil || len(*diagnosis.PossibleCauses) != 1 {
		t.Fatalf("expected deduplicated possible cause list, got %#v", diagnosis.PossibleCauses)
	}
	if (*diagnosis.PossibleCauses)[0] != "StatefulSet waiting for PVC" {
		t.Fatalf("unexpected possible cause: %s", (*diagnosis.PossibleCauses)[0])
	}

	if diagnosis.SuggestedFixes == nil || len(*diagnosis.SuggestedFixes) != 2 {
		t.Fatalf("expected two suggested fix summaries, got %#v", diagnosis.SuggestedFixes)
	}

	if resp.AutoFixes == nil {
		t.Fatalf("expected auto fixes in response")
	}
	autoFixes := *resp.AutoFixes
	if len(autoFixes) != 2 {
		t.Fatalf("expected two unique auto fixes, got %d", len(autoFixes))
	}
	if autoFixes[0].Id != "monitoring::grafana::secret" || autoFixes[1].Id != "monitoring::prometheus::restart" {
		t.Fatalf("unexpected auto fix ordering: %+v", autoFixes)
	}
}
