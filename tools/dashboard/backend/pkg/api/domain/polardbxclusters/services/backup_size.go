package services

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	hpfsconfig "github.com/alibaba/polardbx-operator/pkg/hpfs/config"

	"polardbx-dashboard-backend/pkg/cache"
)

const (
	backupSizeCacheKeyPrefix = "backup:size:"
	defaultBackupSizeTTL     = 30 * time.Minute
	backupSizeTTLEnv         = "BACKUP_SIZE_CACHE_TTL"
	defaultBackupSizeTimeout = 12 * time.Second
	backupSizeTimeoutEnv     = "BACKUP_SIZE_ESTIMATE_TIMEOUT"
	defaultHPFSSystemNS      = "polardbx-operator-system"
	hpfsSystemNSEnv          = "HPFS_SYSTEM_NAMESPACE"
)

type backupSizeCacheEntry struct {
	Bytes        int64     `json:"bytes"`
	Estimated    bool      `json:"estimated"`
	CalculatedAt time.Time `json:"calculatedAt"`
}

func backupSizeTTL() time.Duration {
	raw := strings.TrimSpace(os.Getenv(backupSizeTTLEnv))
	if raw == "" {
		return defaultBackupSizeTTL
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return defaultBackupSizeTTL
	}
	return d
}

func backupSizeTimeout() time.Duration {
	raw := strings.TrimSpace(os.Getenv(backupSizeTimeoutEnv))
	if raw == "" {
		return defaultBackupSizeTimeout
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return defaultBackupSizeTimeout
	}
	return d
}

func hpfsSystemNamespace() string {
	if v := strings.TrimSpace(os.Getenv(hpfsSystemNSEnv)); v != "" {
		return v
	}
	return defaultHPFSSystemNS
}

func backupSizeCacheKey(namespace, name, storageType, sink, rootPath string) string {
	root := strings.TrimSpace(rootPath)
	return fmt.Sprintf("%s%s/%s:%s:%s:%s", backupSizeCacheKeyPrefix, namespace, name, storageType, sink, root)
}

func shouldEstimateBackupSize(phase string) bool {
	switch strings.ToLower(strings.TrimSpace(phase)) {
	case "finished", "completed", "succeeded":
		return true
	default:
		return false
	}
}

func (s *BackupService) estimateBackupSizeBytes(ctx context.Context, cli client.Client, backup *polardbxv1.PolarDBXBackup) (*int64, bool) {
	if backup == nil {
		return nil, false
	}
	phase := strings.ToLower(string(backup.Status.Phase))
	if !shouldEstimateBackupSize(phase) {
		return nil, false
	}

	rootPath := strings.TrimSpace(backup.Status.BackupRootPath)
	if rootPath == "" {
		return nil, false
	}

	storageType := strings.TrimSpace(string(backup.Spec.StorageProvider.StorageName))
	if storageType == "" {
		storageType = strings.TrimSpace(string(backup.Status.StorageName))
	}
	storageType = normalizeSinkType(storageType)
	if storageType == "" {
		return nil, false
	}
	sinkName := strings.TrimSpace(backup.Spec.StorageProvider.Sink)
	if sinkName == "" {
		sinkName = "default"
	}

	cacheKey := backupSizeCacheKey(backup.Namespace, backup.Name, storageType, sinkName, rootPath)
	c := cache.GetGlobalCache()
	if v, ok := c.Get(cacheKey); ok {
		if e, ok2 := v.(backupSizeCacheEntry); ok2 {
			out := e.Bytes
			return &out, e.Estimated
		}
		if e, ok2 := v.(*backupSizeCacheEntry); ok2 && e != nil {
			out := e.Bytes
			return &out, e.Estimated
		}
		c.Delete(cacheKey)
	}

	timeout := backupSizeTimeout()
	ctx2, cancel := withDefaultTimeout(ctx, timeout)
	defer cancel()

	cfg, err := s.loadHPFSConfig(ctx2, cli)
	if err != nil {
		return nil, false
	}
	sink, ok := findSink(cfg, sinkName, storageType)
	if !ok {
		return nil, false
	}

	prefix := strings.TrimLeft(rootPath, "/")
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var bytes int64
	switch storageType {
	case "s3":
		bytes, err = estimateS3PrefixBytes(ctx2, sink, prefix)
	case "oss":
		bytes, err = estimateOSSPrefixBytes(ctx2, sink, prefix)
	case "sftp":
		bytes, err = estimateSFTPPrefixBytes(ctx2, sink, prefix)
	default:
		return nil, false
	}
	if err != nil || bytes <= 0 {
		return nil, false
	}

	entry := backupSizeCacheEntry{Bytes: bytes, Estimated: true, CalculatedAt: time.Now()}
	c.SetWithExpiration(cacheKey, entry, backupSizeTTL())
	out := bytes
	return &out, entry.Estimated
}

func withDefaultTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		return context.WithTimeout(context.Background(), d)
	}
	if deadline, ok := parent.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.WithCancel(parent)
		}
		if remaining < d {
			d = remaining
		}
	}
	return context.WithTimeout(parent, d)
}

func (s *BackupService) loadHPFSConfig(ctx context.Context, cli client.Client) (hpfsconfig.Config, error) {
	var cm corev1.ConfigMap
	if err := cli.Get(ctx, client.ObjectKey{Namespace: hpfsSystemNamespace(), Name: "polardbx-hpfs-config"}, &cm); err != nil {
		return hpfsconfig.Config{}, err
	}
	return decodeHpfsConfig(&cm)
}

func findSink(cfg hpfsconfig.Config, name, sinkType string) (hpfsconfig.Sink, bool) {
	wantName := strings.TrimSpace(name)
	wantType := normalizeSinkType(sinkType)
	for _, s := range cfg.Sinks {
		if strings.TrimSpace(s.Name) == wantName && normalizeSinkType(s.Type) == wantType {
			return s, true
		}
	}
	return hpfsconfig.Sink{}, false
}

func parseEndpointHost(endpoint string) (host string, secure bool) {
	e := strings.TrimSpace(endpoint)
	if e == "" {
		return "", false
	}
	if strings.Contains(e, "://") {
		if u, err := url.Parse(e); err == nil {
			if u.Host != "" {
				return u.Host, strings.EqualFold(u.Scheme, "https")
			}
		}
	}
	return e, false
}

func estimateS3PrefixBytes(ctx context.Context, sink hpfsconfig.Sink, prefix string) (int64, error) {
	host, secureFromURL := parseEndpointHost(sink.Endpoint)
	secure := sink.MinioSink.UseSSL || secureFromURL
	if host == "" || sink.Bucket == "" || sink.AccessKey == "" || sink.AccessSecret == "" {
		return 0, fmt.Errorf("incomplete s3 sink config")
	}
	cli, err := minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(sink.AccessKey, sink.AccessSecret, ""),
		Secure: secure,
	})
	if err != nil {
		return 0, err
	}

	var total int64
	for obj := range cli.ListObjects(ctx, sink.Bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return 0, obj.Err
		}
		// Skip directory placeholders when present.
		if strings.HasSuffix(obj.Key, "/") && obj.Size == 0 {
			continue
		}
		total += obj.Size
	}
	return total, nil
}

func estimateOSSPrefixBytes(ctx context.Context, sink hpfsconfig.Sink, prefix string) (int64, error) {
	_ = ctx
	if sink.Endpoint == "" || sink.Bucket == "" || sink.AccessKey == "" || sink.AccessSecret == "" {
		return 0, fmt.Errorf("incomplete oss sink config")
	}
	cli, err := oss.New(strings.TrimSpace(sink.Endpoint), sink.AccessKey, sink.AccessSecret)
	if err != nil {
		return 0, err
	}
	bk, err := cli.Bucket(strings.TrimSpace(sink.Bucket))
	if err != nil {
		return 0, err
	}

	marker := ""
	var total int64
	for {
		res, err := bk.ListObjects(oss.Prefix(prefix), oss.Marker(marker), oss.MaxKeys(1000))
		if err != nil {
			return 0, err
		}
		for _, obj := range res.Objects {
			total += obj.Size
		}
		if !res.IsTruncated {
			break
		}
		marker = res.NextMarker
	}
	return total, nil
}

func estimateSFTPPrefixBytes(ctx context.Context, sink hpfsconfig.Sink, prefix string) (int64, error) {
	if strings.TrimSpace(sink.SftpSink.Host) == "" || strings.TrimSpace(sink.SftpSink.User) == "" {
		return 0, fmt.Errorf("incomplete sftp sink config")
	}
	port := sink.SftpSink.Port
	if port <= 0 {
		port = 22
	}
	timeout := 8 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d > 0 && d < timeout {
			timeout = d
		}
	}

	cfg := &ssh.ClientConfig{
		User:            sink.SftpSink.User,
		Auth:            []ssh.AuthMethod{ssh.Password(sink.SftpSink.Password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         timeout,
	}
	addr := fmt.Sprintf("%s:%d", strings.TrimSpace(sink.SftpSink.Host), port)
	conn, err := ssh.Dial("tcp", addr, cfg)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	client, err := sftp.NewClient(conn)
	if err != nil {
		return 0, err
	}
	defer client.Close()

	root := strings.TrimSpace(sink.SftpSink.RootPath)
	start := prefix
	if root != "" {
		start = path.Join(root, prefix)
	}

	var total int64
	queue := []string{start}
	for len(queue) > 0 {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		cur := queue[0]
		queue = queue[1:]
		entries, err := client.ReadDir(cur)
		if err != nil {
			// If directory doesn't exist, treat as empty.
			if strings.Contains(strings.ToLower(err.Error()), "no such file") {
				continue
			}
			return 0, err
		}
		for _, fi := range entries {
			p := path.Join(cur, fi.Name())
			if fi.IsDir() {
				queue = append(queue, p)
				continue
			}
			total += fi.Size()
		}
	}
	return total, nil
}
