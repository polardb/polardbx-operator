package config

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"
)

func TestMaskSecret(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"", "<empty>"},
		{"abc", "***"},
		{"abcd", "***"},
		{"abcdef", "ab***ef"},
	}

	for _, tt := range tests {
		if got := maskSecret(tt.in); got != tt.want {
			t.Fatalf("maskSecret(%s) = %s, want %s", tt.in, got, tt.want)
		}
	}
}

func TestPrintConfigRedactsSecrets(t *testing.T) {
	appCfg := &AppConfig{
		Server: &ServerConfig{
			Port:          8080,
			Mode:          "debug",
			LogLevel:      "info",
			JWTSecret:     "super-secret-token",
			JWTExpiration: 24 * time.Hour,
		},
		Kubernetes: &KubeConfig{ConfigPath: "/tmp/kube", DefaultNamespace: "default"},
		Image:      &ImageRegistryConfig{DefaultRegistry: "docker.m.daocloud.io"},
		AutoFix:    &AutoFixOverlayConfig{Enabled: true, Namespace: "ns"},
	}

	// Capture stdout/stderr because PrintConfig uses our zap-based logger by default.
	// IMPORTANT: redirect before calling PrintConfig so logger initializes with the redirected fds.
	origStdout := os.Stdout
	origStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	os.Stderr = w
	defer func() {
		_ = w.Close()
		_ = r.Close()
		os.Stdout = origStdout
		os.Stderr = origStderr
	}()

	appCfg.PrintConfig()

	_ = w.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	out := buf.String()

	if strings.Contains(out, "super-secret-token") {
		t.Fatalf("PrintConfig leaked sensitive value: %s", out)
	}
	// Accept either stdlog format or structured logger format.
	if !(strings.Contains(out, "JWTSecret=su***en") || strings.Contains(out, "jwtSecret") && strings.Contains(out, "su***en")) {
		t.Fatalf("masked secret not present: %s", out)
	}
}
