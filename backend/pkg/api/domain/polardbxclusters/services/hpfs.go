package services

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	hpfsconfig "github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-ui-backend/pkg/api/util"
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
func (s *BackupService) ListHpfsSinks(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	// Allow override of system namespace via query for tests and flexibility
	systemNS := c.DefaultQuery("systemNamespace", "polardbx-operator-system")
	cm, err := getHpfsConfigMap(c, cli, systemNS)
	if err != nil {
		util.HandleK8sError(c, "failed to get HPFS config ConfigMap", err)
		return
	}
	if _, exists := cm.Data["config.yaml"]; !exists {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "config.yaml not found in ConfigMap"})
		return
	}
	cfg, err := decodeHpfsConfig(cm)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to parse config.yaml", "details": err.Error()})
		return
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
	c.JSON(http.StatusOK, gin.H{"namespace": systemNS, "configMap": "polardbx-hpfs-config", "sinks": sinks})
}

// ValidateHpfsSink validates that a given sink (name+type) exists in HPFS config
func (s *BackupService) ValidateHpfsSink(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	var req struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	systemNS := c.DefaultQuery("systemNamespace", "polardbx-operator-system")
	cm, err := getHpfsConfigMap(c, cli, systemNS)
	if err != nil {
		util.HandleK8sError(c, "failed to get HPFS config ConfigMap", err)
		return
	}
	if _, exists := cm.Data["config.yaml"]; !exists {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "config.yaml not found in ConfigMap"})
		return
	}
	cfg, err := decodeHpfsConfig(cm)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to parse config.yaml", "details": err.Error()})
		return
	}
	reqType := normalizeSinkType(req.Type)
	reqName := strings.TrimSpace(req.Name)
	status := "not_found"
	message := "sink not found"
	for _, s := range cfg.Sinks {
		if s.Name == reqName && normalizeSinkType(s.Type) == reqType {
			status = "ok"
			message = "sink exists"
			break
		}
	}
	c.JSON(http.StatusOK, gin.H{"name": reqName, "type": reqType, "status": status, "message": message})
}

// helpers adapted from original
func getHpfsConfigMap(c *gin.Context, cli client.Client, namespace string) (*corev1.ConfigMap, error) {
	var cm corev1.ConfigMap
	if err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: namespace, Name: "polardbx-hpfs-config"}, &cm); err != nil {
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

// evaluateStorageConnectivity 对 HPFS sinks 做轻量连通性探测（TCP 直连），返回 (status, detail)
func (s *BackupService) evaluateStorageConnectivity(c *gin.Context) (string, string) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return "unknown", "no_k8s_client"
	}
	systemNS := c.DefaultQuery("systemNamespace", "polardbx-operator-system")
	cm, err := getHpfsConfigMap(c, cli, systemNS)
	if err != nil {
		return "error", "hpfs_config_not_found"
	}
	cfg, err := decodeHpfsConfig(cm)
	if err != nil {
		return "error", "hpfs_config_parse_error"
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
