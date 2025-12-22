package services

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	hpfsconfig "github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	corev1 "k8s.io/api/core/v1"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"

	svcerr "polardbx-dashboard-backend/pkg/api/errors"
)

// normalizeSinkType maps aliases to canonical types for robustness.
func normalizeSinkType(t string) string {
	lt := strings.ToLower(strings.TrimSpace(t))
	switch lt {
	case "minio", "s3-compatible", "s3compat", "aws-s3":
		return "s3"
	case "aliyun-oss", "alibaba-oss", "alioss":
		return "oss"
	default:
		return lt
	}
}

// ListHpfsSinks returns sinks configured in HPFS ConfigMap
type HpfsSinksResponse struct {
	Namespace string    `json:"namespace"`
	ConfigMap string    `json:"configMap"`
	Sinks     []SinkDTO `json:"sinks"`
}

type SinkDTO struct {
	Name             string `json:"name"`
	Type             string `json:"type"`
	Endpoint         string `json:"endpoint,omitempty"`
	Bucket           string `json:"bucket,omitempty"`
	BucketLookupType string `json:"bucketLookupType,omitempty"`
	Host             string `json:"host,omitempty"`
	Port             int    `json:"port,omitempty"`
	RootPath         string `json:"rootPath,omitempty"`
}

type HpfsSinkValidationResult struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

// ListHpfsSinks returns sinks configured in HPFS ConfigMap.
func (s *BackupService) ListHpfsSinks(ctx context.Context, cli client.Client, systemNS string) (*HpfsSinksResponse, error) {
	if systemNS == "" {
		systemNS = "polardbx-operator-system"
	}
	cm, err := getHpfsConfigMap(ctx, cli, systemNS)
	if err != nil {
		return nil, err
	}
	if _, exists := cm.Data["config.yaml"]; !exists {
		return nil, svcerr.InternalServiceError("config.yaml not found in ConfigMap", nil)
	}
	cfg, err := decodeHpfsConfig(cm)
	if err != nil {
		return nil, svcerr.InternalServiceError("failed to parse config.yaml", err)
	}
	sinks := make([]SinkDTO, 0, len(cfg.Sinks))
	for _, s := range cfg.Sinks {
		dto := SinkDTO{
			Name:             s.Name,
			Type:             normalizeSinkType(s.Type),
			Endpoint:         s.Endpoint,
			Bucket:           s.Bucket,
			BucketLookupType: s.MinioSink.BucketLookupType,
			Host:             s.SftpSink.Host,
			Port:             s.SftpSink.Port,
			RootPath:         s.SftpSink.RootPath,
		}
		sinks = append(sinks, dto)
	}
	return &HpfsSinksResponse{Namespace: systemNS, ConfigMap: "polardbx-hpfs-config", Sinks: sinks}, nil
}

// ValidateHpfsSink validates that a given sink (name+type) exists in HPFS config
// ValidateHpfsSink validates that a given sink (name+type) exists in HPFS config.
func (s *BackupService) ValidateHpfsSink(ctx context.Context, cli client.Client, systemNS, name, sinkType string) (*HpfsSinkValidationResult, error) {
	if name == "" {
		return nil, svcerr.ValidationError("name is required", nil)
	}
	if sinkType == "" {
		return nil, svcerr.ValidationError("type is required", nil)
	}
	if systemNS == "" {
		systemNS = "polardbx-operator-system"
	}
	cm, err := getHpfsConfigMap(ctx, cli, systemNS)
	if err != nil {
		return nil, err
	}
	if _, exists := cm.Data["config.yaml"]; !exists {
		return nil, svcerr.InternalServiceError("config.yaml not found in ConfigMap", nil)
	}
	cfg, err := decodeHpfsConfig(cm)
	if err != nil {
		return nil, svcerr.InternalServiceError("failed to parse config.yaml", err)
	}
	reqType := normalizeSinkType(sinkType)
	reqName := strings.TrimSpace(name)
	status := "not_found"
	message := "sink not found"
	for _, s := range cfg.Sinks {
		if s.Name == reqName && normalizeSinkType(s.Type) == reqType {
			status = "ok"
			message = "sink exists"
			break
		}
	}
	return &HpfsSinkValidationResult{Name: reqName, Type: reqType, Status: status, Message: message}, nil
}

// helpers adapted from original
func getHpfsConfigMap(ctx context.Context, cli client.Client, namespace string) (*corev1.ConfigMap, error) {
	var cm corev1.ConfigMap
	if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: "polardbx-hpfs-config"}, &cm); err != nil {
		return nil, err
	}
	return &cm, nil
}

func decodeHpfsConfig(cm *corev1.ConfigMap) (hpfsconfig.Config, error) {
	var cfg hpfsconfig.Config
	data, ok := cm.Data["config.yaml"]
	if !ok {
		return cfg, nil
	}
	dec := yamlutil.NewYAMLOrJSONDecoder(bytes.NewBufferString(data), 4096)
	if err := dec.Decode(&cfg); err != nil {
		return hpfsconfig.Config{}, err
	}
	return cfg, nil
}

// EvaluateStorageConnectivity performs lightweight connectivity probe on HPFS sinks (TCP direct connection), returns (status, detail)
func (s *BackupService) EvaluateStorageConnectivity(ctx context.Context, cli client.Client, systemNS string) (string, string) {
	if systemNS == "" {
		systemNS = "polardbx-operator-system"
	}
	cm, err := getHpfsConfigMap(ctx, cli, systemNS)
	if err != nil {
		// Treat missing ConfigMap as "unknown" instead of a hard error to avoid noise in environments without HPFS configured.
		return "unknown", "hpfs_config_not_found"
	}
	cfg, err := decodeHpfsConfig(cm)
	if err != nil {
		// Treat config parse failure as "unknown" and surface the specific reason via the detail field.
		return "unknown", "hpfs_config_parse_error"
	}
	if len(cfg.Sinks) == 0 {
		// ConfigMap exists but contains no sink definitions.
		return "unknown", "no_sinks_configured"
	}
	for _, s := range cfg.Sinks {
		kind := normalizeSinkType(s.Type)
		switch kind {
		case "s3", "oss":
			addr := parseHostPort(s.Endpoint)
			if addr != "" && tcpReachable(addr, 3*time.Second) {
				return "ok", fmt.Sprintf("%s:%s reachable", kind, addr)
			}
		case "sftp":
			port := s.SftpSink.Port
			if port <= 0 {
				port = 22
			}
			addr := fmt.Sprintf("%s:%d", strings.TrimSpace(s.SftpSink.Host), port)
			if tcpReachable(addr, 3*time.Second) {
				return "ok", fmt.Sprintf("sftp:%s reachable", addr)
			}
		}
	}
	return "error", "no_sink_reachable"
}

func parseHostPort(endpoint string) string {
	e := strings.TrimSpace(endpoint)
	if e == "" {
		return ""
	}
	if !strings.Contains(e, "://") {
		e = "http://" + e
	}
	u, err := url.Parse(e)
	if err != nil {
		return ""
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		if strings.EqualFold(u.Scheme, "https") {
			port = "443"
		} else {
			port = "80"
		}
	}
	if host == "" || port == "" {
		return ""
	}
	return host + ":" + port
}

func tcpReachable(addr string, timeout time.Duration) bool {
	if strings.TrimSpace(addr) == "" {
		return false
	}
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
