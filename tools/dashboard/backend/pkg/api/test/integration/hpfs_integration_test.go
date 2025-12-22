package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	domain_pxc "polardbx-dashboard-backend/pkg/api/domain/polardbxclusters"
)

// setupHPFSRouter sets up a test router with HPFS routes
func setupHPFSRouter(t *testing.T, objs ...runtime.Object) (*gin.Engine, client.Client) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")

	// Build scheme with all required types
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	ctrlClient := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(objs...).
		Build()

	v1.Use(func(c *gin.Context) {
		c.Set("k8sClient", ctrlClient)
		c.Set("k8sDefaultNamespace", "default")
		c.Next()
	})

	// Register HPFS routes
	v1.GET("/hpfs/sinks", domain_pxc.ListHpfsSinks)
	v1.POST("/hpfs/sinks/validate", domain_pxc.ValidateHpfsSink)

	return r, ctrlClient
}

// ==================== ListHpfsSinks Integration Tests ====================

func TestIntegration_ListHpfsSinks_Success(t *testing.T) {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "polardbx-hpfs-config",
			Namespace: "polardbx-operator-system",
		},
		Data: map[string]string{
			"config.yaml": `
sinks:
  - name: s3-sink
    type: s3
    endpoint: https://s3.amazonaws.com
    bucket: test-bucket
  - name: oss-sink
    type: oss
    endpoint: https://oss-cn-hangzhou.aliyuncs.com
    bucket: test-oss-bucket
  - name: sftp-sink
    type: sftp
    host: sftp.example.com
    port: 22
    rootPath: /backup
`,
		},
	}

	router, _ := setupHPFSRouter(t, configMap)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/hpfs/sinks?systemNamespace=polardbx-operator-system", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Contains(t, resp, "sinks")
	assert.Contains(t, resp, "namespace")
	assert.Contains(t, resp, "configMap")

	if sinks, ok := resp["sinks"].([]interface{}); ok {
		assert.Greater(t, len(sinks), 0)
	}
}

func TestIntegration_ListHpfsSinks_ConfigMapNotFound(t *testing.T) {
	router, _ := setupHPFSRouter(t)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/hpfs/sinks?systemNamespace=polardbx-operator-system", nil)
	router.ServeHTTP(w, req)

	assert.Contains(t, []int{http.StatusNotFound, http.StatusInternalServerError}, w.Code)
}

func TestIntegration_ListHpfsSinks_MissingConfigYAML(t *testing.T) {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "polardbx-hpfs-config",
			Namespace: "polardbx-operator-system",
		},
		Data: map[string]string{
			// Missing config.yaml
		},
	}

	router, _ := setupHPFSRouter(t, configMap)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/hpfs/sinks?systemNamespace=polardbx-operator-system", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestIntegration_ListHpfsSinks_InvalidYAML(t *testing.T) {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "polardbx-hpfs-config",
			Namespace: "polardbx-operator-system",
		},
		Data: map[string]string{
			"config.yaml": `invalid: yaml: content`,
		},
	}

	router, _ := setupHPFSRouter(t, configMap)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/hpfs/sinks?systemNamespace=polardbx-operator-system", nil)
	router.ServeHTTP(w, req)

	// May return 200 with empty sinks or 500 error
	assert.Contains(t, []int{http.StatusOK, http.StatusInternalServerError}, w.Code)
}

func TestIntegration_ListHpfsSinks_MultipleSinkTypes(t *testing.T) {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "polardbx-hpfs-config",
			Namespace: "polardbx-operator-system",
		},
		Data: map[string]string{
			"config.yaml": `
sinks:
  - name: minio-sink
    type: minio
    endpoint: http://minio:9000
    bucket: minio-bucket
  - name: aliyun-oss-sink
    type: aliyun-oss
    endpoint: https://oss-cn-hangzhou.aliyuncs.com
    bucket: oss-bucket
  - name: sftp-sink
    type: sftp
    host: sftp.example.com
    port: 2222
    rootPath: /data/backup
`,
		},
	}

	router, _ := setupHPFSRouter(t, configMap)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/hpfs/sinks?systemNamespace=polardbx-operator-system", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)

	if sinks, ok := resp["sinks"].([]interface{}); ok {
		assert.Equal(t, 3, len(sinks))
		// Verify type normalization (minio -> s3, aliyun-oss -> oss)
		sinkMap := make(map[string]interface{})
		for _, s := range sinks {
			if sink, ok := s.(map[string]interface{}); ok {
				name := sink["name"].(string)
				sinkMap[name] = sink
			}
		}
		if minioSink, ok := sinkMap["minio-sink"].(map[string]interface{}); ok {
			assert.Equal(t, "s3", minioSink["type"])
		}
		if ossSink, ok := sinkMap["aliyun-oss-sink"].(map[string]interface{}); ok {
			assert.Equal(t, "oss", ossSink["type"])
		}
	}
}

// ==================== ValidateHpfsSink Integration Tests ====================

func TestIntegration_ValidateHpfsSink_Success(t *testing.T) {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "polardbx-hpfs-config",
			Namespace: "polardbx-operator-system",
		},
		Data: map[string]string{
			"config.yaml": `
sinks:
  - name: test-sink
    type: s3
    endpoint: https://s3.amazonaws.com
    bucket: test-bucket
`,
		},
	}

	router, _ := setupHPFSRouter(t, configMap)

	reqBody := map[string]interface{}{
		"name": "test-sink",
		"type": "s3",
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/hpfs/sinks/validate?systemNamespace=polardbx-operator-system", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "ok", resp["status"])
	assert.Equal(t, "sink exists", resp["message"])
}

func TestIntegration_ValidateHpfsSink_NotFound(t *testing.T) {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "polardbx-hpfs-config",
			Namespace: "polardbx-operator-system",
		},
		Data: map[string]string{
			"config.yaml": `
sinks:
  - name: test-sink
    type: s3
    endpoint: https://s3.amazonaws.com
    bucket: test-bucket
`,
		},
	}

	router, _ := setupHPFSRouter(t, configMap)

	reqBody := map[string]interface{}{
		"name": "nonexistent-sink",
		"type": "s3",
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/hpfs/sinks/validate?systemNamespace=polardbx-operator-system", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "not_found", resp["status"])
	assert.Equal(t, "sink not found", resp["message"])
}

func TestIntegration_ValidateHpfsSink_TypeMismatch(t *testing.T) {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "polardbx-hpfs-config",
			Namespace: "polardbx-operator-system",
		},
		Data: map[string]string{
			"config.yaml": `
sinks:
  - name: test-sink
    type: s3
    endpoint: https://s3.amazonaws.com
    bucket: test-bucket
`,
		},
	}

	router, _ := setupHPFSRouter(t, configMap)

	reqBody := map[string]interface{}{
		"name": "test-sink",
		"type": "oss", // Wrong type
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/hpfs/sinks/validate?systemNamespace=polardbx-operator-system", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "not_found", resp["status"])
}

func TestIntegration_ValidateHpfsSink_TypeAlias(t *testing.T) {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "polardbx-hpfs-config",
			Namespace: "polardbx-operator-system",
		},
		Data: map[string]string{
			"config.yaml": `
sinks:
  - name: minio-sink
    type: minio
    endpoint: http://minio:9000
    bucket: test-bucket
`,
		},
	}

	router, _ := setupHPFSRouter(t, configMap)

	reqBody := map[string]interface{}{
		"name": "minio-sink",
		"type": "s3", // minio should normalize to s3
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/hpfs/sinks/validate?systemNamespace=polardbx-operator-system", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "ok", resp["status"])
}

func TestIntegration_ValidateHpfsSink_InvalidRequest(t *testing.T) {
	router, _ := setupHPFSRouter(t)

	body := bytes.NewBufferString(`{invalid json}`)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/hpfs/sinks/validate", body)
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestIntegration_ValidateHpfsSink_ConfigMapNotFound(t *testing.T) {
	router, _ := setupHPFSRouter(t)

	reqBody := map[string]interface{}{
		"name": "test-sink",
		"type": "s3",
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/hpfs/sinks/validate?systemNamespace=polardbx-operator-system", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Contains(t, []int{http.StatusNotFound, http.StatusInternalServerError}, w.Code)
}

