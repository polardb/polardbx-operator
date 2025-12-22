package router

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/cache"
	"polardbx-dashboard-backend/pkg/config"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	readinessTimeout = 2 * time.Second
	kubeProbeTimeout = 1500 * time.Millisecond
)

var (
	startTime = time.Now()

	// Probe functions are overridable in tests.
	configProbe = defaultConfigProbe
	cacheProbe  = defaultCacheProbe
	kubeProbe   = defaultKubeProbe
	buildProbe  = defaultBuildProbe
)

// HealthResponse represents the health check response
type HealthResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// ComponentStatus describes individual dependency state.
type ComponentStatus struct {
	Status string `json:"status"`           // ok | warn | error
	Detail string `json:"detail,omitempty"` // optional human-readable detail
}

// ReadyResponse represents the readiness check response
type ReadyResponse struct {
	Status     string                     `json:"status"` // ready | degraded | not_ready
	Timestamp  string                     `json:"timestamp"`
	Components map[string]ComponentStatus `json:"components,omitempty"`
}

// VersionInfo holds build version information
type VersionInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"buildDate"`
	GoVersion string `json:"goVersion"`
	Uptime    string `json:"uptime"`
}

// Build information - set via ldflags
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
	GoVersion = "unknown"
)

// RegisterHealthRoutes registers health check endpoints.
// It sets up the following routes:
//   - GET /health, /healthz - Liveness probe
//   - GET /ready, /readyz - Readiness probe with dependency checks
//   - GET /version - Version information endpoint
func RegisterHealthRoutes(r *gin.Engine) {
	// Liveness probe - simple check that the service is running
	r.GET("/health", healthHandler)
	r.GET("/healthz", healthHandler)

	// Readiness probe - checks if service is ready to accept traffic
	r.GET("/ready", readyHandler)
	r.GET("/readyz", readyHandler)

	// Version info endpoint
	r.GET("/version", versionHandler)
}

// healthHandler handles liveness probes.
// Returns a simple health status indicating the service is running.
func healthHandler(c *gin.Context) {
	apierr.OK(c, HealthResponse{
		Status:    "healthy",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
}

// readyHandler handles readiness probes with dependency checks and timeouts.
// Checks the status of config, cache, kubernetes, and build components.
// Returns "ready", "degraded", or "not_ready" status based on component health.
func readyHandler(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), readinessTimeout)
	defer cancel()

	components := map[string]ComponentStatus{
		"config":     configProbe(ctx),
		"cache":      cacheProbe(ctx),
		"kubernetes": kubeProbe(ctx),
		"build":      buildProbe(ctx),
	}

	status := "ready"
	statusCode := http.StatusOK

	for _, cs := range components {
		if cs.Status == "error" {
			status = "not_ready"
			statusCode = http.StatusServiceUnavailable
			break
		}
		if cs.Status == "warn" && status == "ready" {
			status = "degraded"
		}
	}

	resp := ReadyResponse{
		Status:     status,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		Components: components,
	}

	if statusCode == http.StatusOK {
		apierr.OK(c, resp)
		return
	}

	// return detailed components for troubleshooting instead of a generic APIError
	c.JSON(statusCode, resp)
}

// versionHandler returns build version information including version, commit, build date, Go version, and uptime.
func versionHandler(c *gin.Context) {
	uptime := time.Since(startTime).Round(time.Second).String()

	apierr.OK(c, VersionInfo{
		Version:   Version,
		Commit:    Commit,
		BuildDate: BuildDate,
		GoVersion: GoVersion,
		Uptime:    uptime,
	})
}

// BuildInfoStatus exposes build metadata status for startup validation.
func BuildInfoStatus() ComponentStatus {
	return defaultBuildProbe(context.Background())
}

func defaultConfigProbe(_ context.Context) ComponentStatus {
	cfg := config.GetAppConfig()
	if cfg == nil {
		return ComponentStatus{Status: "error", Detail: "app config is nil"}
	}
	if errs := cfg.Validate(); len(errs) > 0 {
		return ComponentStatus{Status: "error", Detail: strings.Join(errs, "; ")}
	}
	return ComponentStatus{Status: "ok"}
}

func defaultCacheProbe(_ context.Context) ComponentStatus {
	c := cache.GetGlobalCache()
	if c == nil {
		return ComponentStatus{Status: "error", Detail: "global cache not initialized"}
	}
	stats := c.Stats()
	return ComponentStatus{
		Status: "ok",
		Detail: fmt.Sprintf("items=%v active=%v", stats["total"], stats["active"]),
	}
}

func defaultBuildProbe(_ context.Context) ComponentStatus {
	missing := make([]string, 0, 4)
	if Version == "" || Version == "dev" {
		missing = append(missing, "version")
	}
	if Commit == "" || Commit == "unknown" {
		missing = append(missing, "commit")
	}
	if BuildDate == "" || BuildDate == "unknown" {
		missing = append(missing, "buildDate")
	}
	if GoVersion == "" || GoVersion == "unknown" {
		missing = append(missing, "goVersion")
	}

	if len(missing) > 0 {
		return ComponentStatus{
			Status: "warn",
			Detail: "build metadata not injected: " + strings.Join(missing, ","),
		}
	}

	return ComponentStatus{Status: "ok"}
}

func defaultKubeProbe(ctx context.Context) ComponentStatus {
	restCfg, source, err := resolveRestConfig()
	if err != nil {
		return ComponentStatus{Status: "error", Detail: err.Error()}
	}

	restCfg.Timeout = kubeProbeTimeout
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return ComponentStatus{Status: "error", Detail: err.Error()}
	}

	healthCtx, cancel := context.WithTimeout(ctx, kubeProbeTimeout)
	defer cancel()

	result := clientset.Discovery().RESTClient().Get().AbsPath("/readyz").Do(healthCtx)
	raw, err := result.Raw()
	if err != nil {
		return ComponentStatus{Status: "error", Detail: err.Error()}
	}

	detail := strings.TrimSpace(string(bytes.TrimSpace(raw)))
	if detail == "" {
		detail = "ok"
	}

	return ComponentStatus{
		Status: "ok",
		Detail: fmt.Sprintf("%s: %s", source, detail),
	}
}

func resolveRestConfig() (*rest.Config, string, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, "in-cluster", nil
	}

	kubeCfg := config.GetKubeConfig()
	if kubeCfg != nil {
		if path := kubeCfg.GetKubeconfigPath(); path != "" {
			data, err := os.ReadFile(path)
			if err != nil {
				return nil, "", fmt.Errorf("read kubeconfig %s: %w", path, err)
			}
			restCfg, err := clientcmd.RESTConfigFromKubeConfig(data)
			if err != nil {
				return nil, "", fmt.Errorf("parse kubeconfig %s: %w", path, err)
			}
			return restCfg, path, nil
		}
	}

	return nil, "", fmt.Errorf("no in-cluster or kubeconfig found")
}
