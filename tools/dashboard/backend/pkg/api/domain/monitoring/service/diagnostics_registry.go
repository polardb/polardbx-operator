package service

import (
	"context"
	"sort"
)

// DiagnosticProbeFactory is used for delayed construction of probe instances, facilitating testing and dependency injection.
type DiagnosticProbeFactory func() DiagnosticProbe

// registry contains probes defined during the design phase by default, which can be replaced with specific implementations in subsequent phases.
var defaultDiagnosticProbes = map[string]struct {
	Description string
	Factory     DiagnosticProbeFactory
}{
	"prometheus-health": {
		Description: "Check Prometheus StatefulSet, Service, and core metrics availability",
		Factory:     func() DiagnosticProbe { return newPlaceholderProbe("prometheus-health") },
	},
	"grafana-connectivity": {
		Description: "Verify Grafana Pod and data source connectivity",
		Factory:     func() DiagnosticProbe { return newPlaceholderProbe("grafana-connectivity") },
	},
	"operator-events": {
		Description: "Analyze monitoring Operator events and logs to discover tuning failures",
		Factory:     func() DiagnosticProbe { return newPlaceholderProbe("operator-events") },
	},
	"k8s-events": {
		Description: "Aggregate Warning/Error events at the namespace level",
		Factory:     func() DiagnosticProbe { return newPlaceholderProbe("k8s-events") },
	},
}

// ListDefaultDiagnosticProbes returns a sorted list of probe IDs for UI or log display.
func ListDefaultDiagnosticProbes() []string {
	ids := make([]string, 0, len(defaultDiagnosticProbes))
	for id := range defaultDiagnosticProbes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// ResolveDiagnosticProbe constructs a probe instance based on ID; returns nil if not found.
func ResolveDiagnosticProbe(id string) DiagnosticProbe {
	if meta, ok := defaultDiagnosticProbes[id]; ok && meta.Factory != nil {
		return meta.Factory()
	}
	return nil
}

// newPlaceholderProbe provides a phase placeholder implementation, which will be replaced with real logic in Phase 3.
func newPlaceholderProbe(id string) DiagnosticProbe {
	return placeholderProbe{id: id}
}

// placeholderProbe satisfies the DiagnosticProbe interface but performs no diagnosis.
type placeholderProbe struct {
	id string
}

func (p placeholderProbe) ID() string { return p.id }

func (p placeholderProbe) Description() string { return "placeholder" }

func (p placeholderProbe) Run(_ context.Context, _ ProbeInput) ([]DiagnosticFinding, error) {
	return nil, nil
}
