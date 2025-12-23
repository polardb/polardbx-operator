package services

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	hpfsconfig "github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"sigs.k8s.io/controller-runtime/pkg/client"

	svcerr "polardbx-dashboard-backend/pkg/api/errors"
)

const (
	defaultBackupDownloadTimeout = 30 * time.Minute
	backupDownloadTimeoutEnv     = "BACKUP_DOWNLOAD_TIMEOUT"
)

type BackupDownloadInfo struct {
	Namespace      string         `json:"namespace"`
	Name           string         `json:"name"`
	Phase          string         `json:"phase"`
	BackupRootPath string         `json:"backupRootPath"`
	Storage        string         `json:"storage"`
	Sink           string         `json:"sink"`
	SinkConfig     map[string]any `json:"sinkConfig,omitempty"`
	URL            string         `json:"url"`
	Filename       string         `json:"filename"`
	Message        string         `json:"message"`
	Command        string         `json:"command,omitempty"`
}

func backupDownloadTimeout() time.Duration {
	raw := strings.TrimSpace(os.Getenv(backupDownloadTimeoutEnv))
	if raw == "" {
		return defaultBackupDownloadTimeout
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return defaultBackupDownloadTimeout
	}
	return d
}

func shouldAllowBackupDownload(phase string) bool {
	switch strings.ToLower(strings.TrimSpace(phase)) {
	case "finished", "completed", "succeeded":
		return true
	default:
		return false
	}
}

var k8sNameRe = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

func validateK8sName(v, field string) error {
	if v == "" {
		return svcerr.ValidationError(field+" is required", nil)
	}
	if !k8sNameRe.MatchString(v) {
		return svcerr.ValidationError("invalid "+field, nil)
	}
	return nil
}

func validateBackupRootPath(p string) error {
	raw := strings.TrimSpace(p)
	if raw == "" {
		return svcerr.ValidationError("backupRootPath is empty", nil)
	}
	if strings.Contains(raw, "\\") {
		return svcerr.ValidationError("invalid backupRootPath", nil)
	}
	if strings.HasPrefix(raw, "/") {
		return svcerr.ValidationError("backupRootPath must be relative", nil)
	}
	clean := path.Clean(raw)
	if clean == "." || clean == "/" || strings.HasPrefix(clean, "../") || clean == ".." || strings.Contains(clean, "/../") {
		return svcerr.ValidationError("invalid backupRootPath", nil)
	}
	return nil
}

// GetBackupDownloadInfo returns a best-effort payload telling the client how to download a backup.
func (s *BackupService) GetBackupDownloadInfo(ctx context.Context, cli client.Client, namespace, name string) (*BackupDownloadInfo, error) {
	if err := validateK8sName(namespace, "namespace"); err != nil {
		return nil, err
	}
	if err := validateK8sName(name, "name"); err != nil {
		return nil, err
	}

	var backup polardbxv1.PolarDBXBackup
	if err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &backup); err != nil {
		return nil, err
	}

	root := strings.TrimSpace(backup.Status.BackupRootPath)
	if err := validateBackupRootPath(root); err != nil {
		return nil, err
	}

	storage := strings.TrimSpace(string(backup.Spec.StorageProvider.StorageName))
	if storage == "" {
		storage = strings.TrimSpace(string(backup.Status.StorageName))
	}
	storage = normalizeSinkType(storage)
	if storage == "" {
		return nil, svcerr.ValidationError("storage is empty", nil)
	}
	sinkName := strings.TrimSpace(backup.Spec.StorageProvider.Sink)
	if sinkName == "" {
		sinkName = "default"
	}

	// Load sink config for "how to download" guidance.
	cfg, err := s.loadHPFSConfig(ctx, cli)
	if err != nil {
		// Still return minimal info (client may have other means).
		cfg = hpfsconfig.Config{}
	}
	sink, ok := findSink(cfg, sinkName, storage)
	sinkCfg := map[string]any{}
	if ok {
		sinkCfg = sinkConfigSummary(sink)
	}

	filename := fmt.Sprintf("%s-%s.tar.gz", namespace, name)
	url := fmt.Sprintf("/api/v1/backups/%s/%s/file", namespace, name)

	info := &BackupDownloadInfo{
		Namespace:      namespace,
		Name:           name,
		Phase:          string(backup.Status.Phase),
		BackupRootPath: root,
		Storage:        storage,
		Sink:           sinkName,
		SinkConfig:     sinkCfg,
		URL:            url,
		Filename:       filename,
		Message:        "backup download info",
	}
	info.Command = buildSuggestedDownloadCommand(storage, sink, root, namespace, name)
	return info, nil
}

func sinkConfigSummary(s hpfsconfig.Sink) map[string]any {
	out := map[string]any{
		"name": s.Name,
		"type": normalizeSinkType(s.Type),
	}
	switch normalizeSinkType(s.Type) {
	case "oss":
		out["endpoint"] = strings.TrimSpace(s.Endpoint)
		out["bucket"] = strings.TrimSpace(s.Bucket)
	case "s3":
		out["endpoint"] = strings.TrimSpace(s.Endpoint)
		out["bucket"] = strings.TrimSpace(s.Bucket)
		out["useSSL"] = s.MinioSink.UseSSL
		if v := strings.TrimSpace(s.MinioSink.BucketLookupType); v != "" {
			out["bucketLookupType"] = v
		}
	case "sftp":
		out["host"] = strings.TrimSpace(s.SftpSink.Host)
		out["port"] = s.SftpSink.Port
		out["user"] = strings.TrimSpace(s.SftpSink.User)
		out["rootPath"] = strings.TrimSpace(s.SftpSink.RootPath)
	}
	return out
}

func buildSuggestedDownloadCommand(storage string, sink hpfsconfig.Sink, root, namespace, name string) string {
	// These are best-effort hints; credentials are not included.
	switch normalizeSinkType(storage) {
	case "oss":
		if strings.TrimSpace(sink.Bucket) != "" {
			return fmt.Sprintf("ossutil cp -r oss://%s/%s ./%s/", strings.TrimSpace(sink.Bucket), strings.TrimLeft(root, "/"), name)
		}
	case "s3":
		if strings.TrimSpace(sink.Bucket) != "" {
			return fmt.Sprintf("aws s3 sync s3://%s/%s ./%s/", strings.TrimSpace(sink.Bucket), strings.TrimLeft(root, "/"), name)
		}
	case "sftp":
		host := strings.TrimSpace(sink.SftpSink.Host)
		user := strings.TrimSpace(sink.SftpSink.User)
		if host != "" && user != "" {
			port := sink.SftpSink.Port
			if port <= 0 {
				port = 22
			}
			base := path.Join(strings.TrimSpace(sink.SftpSink.RootPath), strings.TrimLeft(root, "/"))
			return fmt.Sprintf("sftp -P %d %s@%s <<< $'get -r %s ./%s'", port, user, host, base, name)
		}
	}
	// Fallback: suggest using the API (works when dashboard can reach the storage).
	return fmt.Sprintf("curl -L -o %s.tar.gz '<dashboard>/api/v1/backups/%s/%s/file'", name, namespace, name)
}

// StreamBackupAsTarGz streams backup contents as tar.gz to the provided writer.
// It uses HPFS sink config (ConfigMap: polardbx-hpfs-config) to access storage.
func (s *BackupService) StreamBackupAsTarGz(ctx context.Context, cli client.Client, namespace, name string, w io.Writer) error {
	if err := validateK8sName(namespace, "namespace"); err != nil {
		return err
	}
	if err := validateK8sName(name, "name"); err != nil {
		return err
	}
	if w == nil {
		return svcerr.ValidationError("writer is required", nil)
	}

	ctx2, cancel := withDefaultTimeout(ctx, backupDownloadTimeout())
	defer cancel()

	var backup polardbxv1.PolarDBXBackup
	if err := cli.Get(ctx2, client.ObjectKey{Namespace: namespace, Name: name}, &backup); err != nil {
		return err
	}
	if !shouldAllowBackupDownload(string(backup.Status.Phase)) {
		return svcerr.Conflict("backup not ready for download").WithDetails(map[string]any{
			"phase":   string(backup.Status.Phase),
			"message": backup.Status.Message,
		})
	}

	root := strings.TrimSpace(backup.Status.BackupRootPath)
	if err := validateBackupRootPath(root); err != nil {
		return err
	}

	storage := strings.TrimSpace(string(backup.Spec.StorageProvider.StorageName))
	if storage == "" {
		storage = strings.TrimSpace(string(backup.Status.StorageName))
	}
	storage = normalizeSinkType(storage)
	if storage == "" {
		return svcerr.ValidationError("storage is empty", nil)
	}
	sinkName := strings.TrimSpace(backup.Spec.StorageProvider.Sink)
	if sinkName == "" {
		sinkName = "default"
	}

	cfg, err := s.loadHPFSConfig(ctx2, cli)
	if err != nil {
		return err
	}
	sink, ok := findSink(cfg, sinkName, storage)
	if !ok {
		return svcerr.NotFoundError("hpfs sink", sinkName)
	}

	prefix := strings.TrimLeft(root, "/")
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	gw := gzip.NewWriter(w)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	switch storage {
	case "oss":
		return streamOssPrefixToTar(ctx2, sink, prefix, tw)
	case "s3":
		return streamS3PrefixToTar(ctx2, sink, prefix, tw)
	case "sftp":
		return streamSftpPrefixToTar(ctx2, sink, prefix, tw)
	default:
		return svcerr.ValidationError("unsupported storage type", nil)
	}
}

func streamOssPrefixToTar(ctx context.Context, sink hpfsconfig.Sink, prefix string, tw *tar.Writer) error {
	if sink.Endpoint == "" || sink.Bucket == "" || sink.AccessKey == "" || sink.AccessSecret == "" {
		return svcerr.ValidationError("incomplete oss sink config", nil)
	}
	cli, err := oss.New(strings.TrimSpace(sink.Endpoint), sink.AccessKey, sink.AccessSecret)
	if err != nil {
		return err
	}
	bk, err := cli.Bucket(strings.TrimSpace(sink.Bucket))
	if err != nil {
		return err
	}

	marker := ""
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		res, err := bk.ListObjects(oss.Prefix(prefix), oss.Marker(marker), oss.MaxKeys(1000))
		if err != nil {
			return err
		}
		for _, obj := range res.Objects {
			if err := writeOssObjectToTar(ctx, bk, obj, tw); err != nil {
				return err
			}
		}
		if !res.IsTruncated {
			break
		}
		marker = res.NextMarker
	}
	return nil
}

func writeOssObjectToTar(ctx context.Context, bk *oss.Bucket, obj oss.ObjectProperties, tw *tar.Writer) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	key := obj.Key
	if key == "" {
		return nil
	}
	if strings.HasSuffix(key, "/") && obj.Size == 0 {
		// Directory placeholder.
		hdr := &tar.Header{Name: strings.TrimSuffix(key, "/") + "/", Mode: 0o755, Typeflag: tar.TypeDir, ModTime: obj.LastModified}
		return tw.WriteHeader(hdr)
	}

	r, err := bk.GetObject(key)
	if err != nil {
		return err
	}
	defer r.Close()

	hdr := &tar.Header{
		Name:    key,
		Mode:    0o644,
		Size:    obj.Size,
		ModTime: obj.LastModified,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	_, err = io.Copy(tw, r)
	return err
}

func streamS3PrefixToTar(ctx context.Context, sink hpfsconfig.Sink, prefix string, tw *tar.Writer) error {
	host, secureFromURL := parseEndpointHost(sink.Endpoint)
	secure := sink.MinioSink.UseSSL || secureFromURL
	if host == "" || sink.Bucket == "" || sink.AccessKey == "" || sink.AccessSecret == "" {
		return svcerr.ValidationError("incomplete s3 sink config", nil)
	}
	cli, err := minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(sink.AccessKey, sink.AccessSecret, ""),
		Secure: secure,
	})
	if err != nil {
		return err
	}

	for obj := range cli.ListObjects(ctx, sink.Bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return obj.Err
		}
		if err := writeS3ObjectToTar(ctx, cli, sink.Bucket, obj, tw); err != nil {
			return err
		}
	}
	return nil
}

func writeS3ObjectToTar(ctx context.Context, cli *minio.Client, bucket string, obj minio.ObjectInfo, tw *tar.Writer) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	key := obj.Key
	if key == "" {
		return nil
	}
	if strings.HasSuffix(key, "/") && obj.Size == 0 {
		hdr := &tar.Header{Name: key, Mode: 0o755, Typeflag: tar.TypeDir, ModTime: obj.LastModified}
		return tw.WriteHeader(hdr)
	}

	r, err := cli.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer r.Close()

	hdr := &tar.Header{
		Name:    key,
		Mode:    0o644,
		Size:    obj.Size,
		ModTime: obj.LastModified,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	_, err = io.Copy(tw, r)
	return err
}

func streamSftpPrefixToTar(ctx context.Context, sink hpfsconfig.Sink, prefix string, tw *tar.Writer) error {
	host := strings.TrimSpace(sink.SftpSink.Host)
	user := strings.TrimSpace(sink.SftpSink.User)
	if host == "" || user == "" {
		return svcerr.ValidationError("incomplete sftp sink config", nil)
	}
	port := sink.SftpSink.Port
	if port <= 0 {
		port = 22
	}

	timeout := 10 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d > 0 && d < timeout {
			timeout = d
		}
	}
	sshCfg := &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{ssh.Password(sink.SftpSink.Password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         timeout,
	}
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := ssh.Dial("tcp", addr, sshCfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	sftpCli, err := sftp.NewClient(conn)
	if err != nil {
		return err
	}
	defer sftpCli.Close()

	root := strings.TrimSpace(sink.SftpSink.RootPath)
	base := prefix
	if root != "" {
		base = path.Join(root, prefix)
	}
	base = path.Clean(base)

	walker := sftpCli.Walk(base)
	for walker.Step() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := walker.Err(); err != nil {
			// If path disappears mid-walk, continue.
			if strings.Contains(strings.ToLower(err.Error()), "no such file") {
				continue
			}
			return err
		}
		fi := walker.Stat()
		remotePath := walker.Path()
		if fi == nil {
			continue
		}
		rel, err := filepath.Rel(filepath.FromSlash(base), filepath.FromSlash(remotePath))
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "." || strings.HasPrefix(rel, "../") {
			continue
		}
		tarName := path.Join(strings.TrimSuffix(prefix, "/"), rel)
		tarName = strings.TrimPrefix(tarName, "/")

		if fi.IsDir() {
			hdr := &tar.Header{Name: tarName + "/", Mode: 0o755, Typeflag: tar.TypeDir, ModTime: fi.ModTime()}
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
			continue
		}

		f, err := sftpCli.Open(remotePath)
		if err != nil {
			return err
		}
		hdr := &tar.Header{
			Name:    tarName,
			Mode:    0o644,
			Size:    fi.Size(),
			ModTime: fi.ModTime(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			_ = f.Close()
			return err
		}
		_, copyErr := io.Copy(tw, f)
		_ = f.Close()
		if copyErr != nil {
			return copyErr
		}
	}

	if err := walker.Err(); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}
