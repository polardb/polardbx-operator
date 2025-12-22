package service

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"polardbx-dashboard-backend/pkg/api/domain/platform/alerts/repository"
)

const (
	SettingsNS           = "polardbx-operator-system"
	CMAlertProfiles      = "polardbx-alert-profiles"
	CMAlertRoutes        = "polardbx-alert-routes"
	cmAlertmanagerURLKey = "alertmanagerUrl"
	cmRoutesContentKey   = "config.yaml"
)

// ProfileInfo configuration file information
type ProfileInfo struct {
	Name string `json:"name"`
}

// ProfileDetail configuration file details
type ProfileDetail struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

// AlertItem alert item
type AlertItem struct {
	Source    string            `json:"source"`
	Severity  string            `json:"severity"`
	Labels    map[string]string `json:"labels"`
	Message   string            `json:"message"`
	Timestamp string            `json:"timestamp"`
	Time      string            `json:"time,omitempty"`
}

// DryRunResult dry run result
type DryRunResult struct {
	Valid   bool   `json:"valid"`
	Details string `json:"details,omitempty"`
}

// AlertsService defines alert business logic layer
type AlertsService struct {
	repo repository.AlertsRepository
}

// NewAlertsService creates new AlertsService
func NewAlertsService(repo repository.AlertsRepository) *AlertsService {
	return &AlertsService{repo: repo}
}

// ListProfiles lists all configuration files
func (s *AlertsService) ListProfiles(ctx context.Context) ([]ProfileInfo, error) {
	cm, err := s.repo.GetConfigMap(ctx, SettingsNS, CMAlertProfiles)
	if err != nil {
		return []ProfileInfo{}, nil
	}
	items := make([]ProfileInfo, 0, len(cm.Data))
	for k := range cm.Data {
		items = append(items, ProfileInfo{Name: k})
	}
	return items, nil
}

// CreateProfile creates configuration file
func (s *AlertsService) CreateProfile(ctx context.Context, name, content string) error {
	cm, err := s.repo.GetConfigMap(ctx, SettingsNS, CMAlertProfiles)
	if err != nil {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: SettingsNS, Name: CMAlertProfiles},
			Data:       map[string]string{},
		}
		if err2 := s.repo.CreateConfigMap(ctx, cm); err2 != nil {
			return err2
		}
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	if _, exists := cm.Data[name]; exists {
		return ErrProfileExists
	}
	cm.Data[name] = content
	return s.repo.UpdateConfigMap(ctx, cm)
}

// GetProfile gets configuration file
func (s *AlertsService) GetProfile(ctx context.Context, name string) (*ProfileDetail, error) {
	cm, err := s.repo.GetConfigMap(ctx, SettingsNS, CMAlertProfiles)
	if err != nil {
		return nil, ErrProfileNotFound
	}
	content, ok := cm.Data[name]
	if !ok {
		return nil, ErrProfileNotFound
	}
	return &ProfileDetail{Name: name, Content: content}, nil
}

// UpdateProfile updates configuration file
func (s *AlertsService) UpdateProfile(ctx context.Context, name, content string) error {
	cm, err := s.repo.GetConfigMap(ctx, SettingsNS, CMAlertProfiles)
	if err != nil {
		return ErrProfileNotFound
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[name] = content
	return s.repo.UpdateConfigMap(ctx, cm)
}

// DeleteProfile deletes configuration file
func (s *AlertsService) DeleteProfile(ctx context.Context, name string) error {
	cm, err := s.repo.GetConfigMap(ctx, SettingsNS, CMAlertProfiles)
	if err != nil {
		return ErrProfileNotFound
	}
	if cm.Data == nil || cm.Data[name] == "" {
		return ErrProfileNotFound
	}
	delete(cm.Data, name)
	return s.repo.UpdateConfigMap(ctx, cm)
}

// DryRunProfile validates Alertmanager YAML
func (s *AlertsService) DryRunProfile(content string) (*DryRunResult, error) {
	f, err := os.CreateTemp("", "am-profile-*.yaml")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	_, _ = f.Write([]byte(content))
	_ = f.Close()
	cmd := exec.Command("promtool", "check", "rules", f.Name())
	out, err := cmd.CombinedOutput()
	if err != nil {
		return &DryRunResult{Valid: false, Details: string(out)}, nil
	}
	return &DryRunResult{Valid: true}, nil
}

// GetRoutes gets routing configuration
func (s *AlertsService) GetRoutes(ctx context.Context) (string, string, error) {
	cm, _ := s.repo.GetConfigMap(ctx, SettingsNS, CMAlertRoutes)
	if cm == nil {
		return "", "", nil
	}
	content := ""
	alertmanagerURL := ""
	if cm.Data != nil {
		content = cm.Data[cmRoutesContentKey]
		alertmanagerURL = cm.Data[cmAlertmanagerURLKey]
	}
	return content, alertmanagerURL, nil
}

// PutRoutes updates routing configuration
func (s *AlertsService) PutRoutes(ctx context.Context, content, alertmanagerURL string) error {
	cm, err := s.repo.GetConfigMap(ctx, SettingsNS, CMAlertRoutes)
	if err != nil {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: SettingsNS, Name: CMAlertRoutes},
			Data:       map[string]string{},
		}
		cm.Data[cmRoutesContentKey] = content
		if strings.TrimSpace(alertmanagerURL) != "" {
			cm.Data[cmAlertmanagerURLKey] = strings.TrimSpace(alertmanagerURL)
		}
		return s.repo.CreateConfigMap(ctx, cm)
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[cmRoutesContentKey] = content
	if strings.TrimSpace(alertmanagerURL) != "" {
		cm.Data[cmAlertmanagerURLKey] = strings.TrimSpace(alertmanagerURL)
	} else {
		delete(cm.Data, cmAlertmanagerURLKey)
	}
	return s.repo.UpdateConfigMap(ctx, cm)
}

// ListAlerts aggregates alert list from Alertmanager and K8s events
func (s *AlertsService) ListAlerts(ctx context.Context, namespace, cluster, alertmanagerURL string) ([]AlertItem, error) {
	items := make([]AlertItem, 0)

	// Fetch from Alertmanager
	if alertmanagerURL != "" {
		items = append(items, s.fetchAlertsFromAlertmanager(alertmanagerURL, namespace, cluster)...)
	}

	// Fetch from K8s events
	events, err := s.repo.ListEvents(ctx, namespace)
	if err == nil {
		for _, ev := range events {
			if cluster != "" && !strings.Contains(ev.InvolvedObject.Name, cluster) {
				continue
			}
			sev := "info"
			if ev.Type == corev1.EventTypeWarning {
				sev = "warning"
			}
			labels := map[string]string{
				"namespace":      ev.Namespace,
				"reason":         ev.Reason,
				"involvedObject": ev.InvolvedObject.Name,
			}
			if cluster != "" {
				labels["cluster"] = cluster
			}
			ts := ev.LastTimestamp.Time
			if ts.IsZero() && !ev.EventTime.IsZero() {
				ts = ev.EventTime.Time
			}
			if ts.IsZero() {
				ts = ev.ObjectMeta.CreationTimestamp.Time
			}
			tsStr := ts.Format(time.RFC3339)
			items = append(items, AlertItem{
				Source:    "k8s-event",
				Severity:  sev,
				Message:   ev.Message,
				Labels:    labels,
				Time:      tsStr,
				Timestamp: tsStr,
			})
		}
	}

	return items, nil
}

func (s *AlertsService) fetchAlertsFromAlertmanager(url, namespace, cluster string) []AlertItem {
	type amAlert struct {
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
		StartsAt    string            `json:"startsAt"`
	}
	items := make([]AlertItem, 0)
	resp, err := http.Get(url + "/api/v2/alerts")
	if err != nil || resp.StatusCode != 200 {
		return items
	}
	defer resp.Body.Close()
	var alerts []amAlert
	if err := json.NewDecoder(resp.Body).Decode(&alerts); err != nil {
		return items
	}
	for _, a := range alerts {
		if (namespace == "" || a.Labels["namespace"] == namespace) && (cluster == "" || a.Labels["cluster"] == cluster) {
			msg := a.Annotations["summary"]
			if msg == "" {
				msg = a.Annotations["description"]
			}
			if msg == "" {
				msg = a.Labels["alertname"]
			}
			items = append(items, AlertItem{
				Source:    "alertmanager",
				Severity:  a.Labels["severity"],
				Labels:    a.Labels,
				Message:   msg,
				Timestamp: a.StartsAt,
			})
		}
	}
	return items
}

// Error definitions
type AlertsError string

func (e AlertsError) Error() string { return string(e) }

const (
	ErrProfileExists   AlertsError = "profile already exists"
	ErrProfileNotFound AlertsError = "profile not found"
)
