package prechange

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"

	"github.com/gin-gonic/gin"
)

// ------- ValidatePrecheckToken -------

func TestValidatePrecheckToken(t *testing.T) {
	ns, name := "default", "pxc"
	nowTs := time.Now().UnixNano()
	token := fmt.Sprintf("%d:%s:%s/%s", nowTs, "scale", ns, name)
	if !ValidatePrecheckToken(token, "scale", ns, name, 10*time.Minute) {
		t.Fatalf("expected valid token")
	}

	oldTs := time.Now().Add(-11 * time.Minute).UnixNano()
	expired := fmt.Sprintf("%d:%s:%s/%s", oldTs, "scale", ns, name)
	if ValidatePrecheckToken(expired, "scale", ns, name, 10*time.Minute) {
		t.Fatalf("expected expired token to be invalid")
	}

	wrongOp := fmt.Sprintf("%d:%s:%s/%s", nowTs, "upgrade", ns, name)
	if ValidatePrecheckToken(wrongOp, "scale", ns, name, 10*time.Minute) {
		t.Fatalf("expected wrong op token to be invalid")
	}

	wrongId := fmt.Sprintf("%d:%s:%s/%s", nowTs, "scale", ns, "another")
	if ValidatePrecheckToken(wrongId, "scale", ns, name, 10*time.Minute) {
		t.Fatalf("expected wrong ns/name token to be invalid")
	}
}

// ------- HMAC sign/verify -------

func TestHMACSignAndVerify(t *testing.T) {
	secret := "s3cr3t"
	plain := fmt.Sprintf("%d:%s:%s/%s", time.Now().UnixNano(), "scale", "ns", "name")
	sig := signPrecheckToken(secret, plain)
	if sig == "" {
		t.Fatalf("expected non-empty signature")
	}
	if !verifyPrecheckToken(secret, plain, sig) {
		t.Fatalf("expected signature to verify")
	}
	if verifyPrecheckToken(secret, plain, "deadbeef") {
		t.Fatalf("expected wrong signature to fail")
	}
}

// ------- parseVersion & evaluateVersionCompat -------

func TestParseVersion(t *testing.T) {
	if mj, mn, pt, ok := parseVersion("8.0.18"); !ok || mj != 8 || mn != 0 || pt != 18 {
		t.Fatalf("unexpected parse 8.0.18 => %v.%v.%v ok=%v", mj, mn, pt, ok)
	}
	if mj, mn, pt, ok := parseVersion("8.0.18-foo"); !ok || mj != 8 || mn != 0 || pt != 18 {
		t.Fatalf("unexpected parse 8.0.18-foo => %v.%v.%v ok=%v", mj, mn, pt, ok)
	}
	if _, _, _, ok := parseVersion("8"); ok {
		t.Fatalf("expected parse failure for '8'")
	}
}

func TestEvaluateVersionCompat(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = polardbxv1.AddToScheme(scheme)

	ns, name := "ns", "pxc"
	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       polardbxv1.PolarDBXClusterSpec{Topology: polardbx.Topology{Version: "8.0.18"}},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)

	cur, tgt, ok, msg := evaluateVersionCompat(c, cli, ns, name, "upgrade", map[string]any{"targetVersion": "8.0.19"})
	if !ok || cur != "8.0.18" || tgt != "8.0.19" {
		t.Fatalf("expected ok upgrade: cur=%s tgt=%s ok=%v msg=%s", cur, tgt, ok, msg)
	}

	// cross-major not allowed
	_, _, ok2, _ := evaluateVersionCompat(c, cli, ns, name, "upgrade", map[string]any{"targetVersion": "9.0.1"})
	if ok2 {
		t.Fatalf("expected cross-major to be not ok")
	}
}

// ------- Precheck handler basic path -------

func TestPrecheckHandler_Basic(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = polardbxv1.AddToScheme(scheme)

	ns, name := "ns", "pxc"
	now := time.Now()

	// Cluster with ClusterReady=true
	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       polardbxv1.PolarDBXClusterSpec{Topology: polardbx.Topology{Version: "8.0.18"}},
		Status: polardbxv1.PolarDBXClusterStatus{
			Phase: polardbx.PhaseRunning,
			Conditions: []polardbx.Condition{{
				Type:   polardbx.ClusterReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}

	// Pod ready
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pxc-cn-0", Namespace: ns, Labels: map[string]string{"polardbx/name": name}},
		Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}

	// One recent finished backup
	start := metav1.NewTime(now.Add(-30 * time.Minute))
	lrt := metav1.NewTime(now.Add(-5 * time.Minute))
	backup := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "b1", Namespace: ns},
		Spec:       polardbxv1.PolarDBXBackupSpec{Cluster: polardbxv1.PolarDBXClusterReference{Name: name}},
		Status: polardbxv1.PolarDBXBackupStatus{
			StartTime:                  &start,
			Phase:                      polardbxv1.BackupFinished,
			LatestRecoverableTimestamp: &lrt,
		},
	}

	// HPFS configured
	hpfs := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "polardbx-hpfs-config", Namespace: "polardbx-operator-system"}}

	// Backend settings with precheck secret
	settings := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "polardbx-ui-backend-config", Namespace: "polardbx-operator-system"}, Data: map[string]string{"precheck.secret": "testsecret"}}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, pod, backup, hpfs, settings).Build()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	// Build request
	reqBody, _ := json.Marshal(PrecheckRequest{Operation: "scale", TargetSpec: map[string]any{}})
	c.Request = httptest.NewRequest("POST", "/api/v1/clusters/ns/pxc/precheck", bytes.NewReader(reqBody))
	c.Params = gin.Params{{Key: "namespace", Value: ns}, {Key: "name", Value: name}}
	c.Set("k8sClient", cli)

	Precheck(c)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["token"] == "" {
		t.Fatalf("expected token in response")
	}
	if resp["tokenSig"] == "" {
		t.Fatalf("expected tokenSig in response")
	}
}

func TestPrecheckHandler_UpgradeStrict(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = polardbxv1.AddToScheme(scheme)

	ns, name := "ns", "pxc"

	// Cluster without ClusterReady
	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       polardbxv1.PolarDBXClusterSpec{Topology: polardbx.Topology{Version: "8.0.18"}},
	}

	// Backend settings (optional secret)
	settings := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "polardbx-ui-backend-config", Namespace: "polardbx-operator-system"}}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, settings).Build()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	reqBody, _ := json.Marshal(PrecheckRequest{Operation: "upgrade", TargetSpec: map[string]any{"targetVersion": "8.0.18"}})
	c.Request = httptest.NewRequest("POST", "/api/v1/clusters/ns/pxc/precheck", bytes.NewReader(reqBody))
	c.Params = gin.Params{{Key: "namespace", Value: ns}, {Key: "name", Value: name}}
	c.Set("k8sClient", cli)

	Precheck(c)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp struct {
		Pass   bool             `json:"pass"`
		Plan   []map[string]any `json:"plan"`
		Errors []string         `json:"errors"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Pass {
		t.Fatalf("expected pass=false for strict upgrade without HPFS/backup/ready")
	}
	// assert version plan item is error
	found := false
	for _, p := range resp.Plan {
		if p["id"] == "checkVersionCompat" {
			if state, ok := p["state"].(string); !ok || state != "error" {
				t.Fatalf("expected checkVersionCompat state=error, got %v", p["state"])
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected checkVersionCompat plan item")
	}
}
