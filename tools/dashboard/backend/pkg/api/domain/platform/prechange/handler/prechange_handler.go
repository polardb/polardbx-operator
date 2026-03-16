// Package handler provides HTTP handlers for pre-change checks
// Follows Clean Architecture design pattern
package handler

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	systemtask "github.com/alibaba/polardbx-operator/api/v1/systemtask"
	"github.com/gin-gonic/gin"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apierr "polardbx-dashboard-backend/pkg/api/errors"
)

// ======================== Types ========================

// PrecheckRequest precheck request
type PrecheckRequest struct {
	Operation  string         `json:"operation"`  // scale|upgrade|config
	TargetSpec map[string]any `json:"targetSpec"` // optional, reserved
}

// Checklist summarizes key results of pre-change checks
type Checklist struct {
	HasRecentBackup     bool   `json:"hasRecentBackup"`
	RpoLagSeconds       int    `json:"rpoLagSeconds"`
	StorageConnectivity string `json:"storageConnectivity"`
	RpoOk               bool   `json:"rpoOk"`
}

// ======================== Helper Functions ========================

func getK8sClient(c *gin.Context) (client.Client, bool) {
	v, ok := c.Get("k8sClient")
	if !ok {
		apierr.AbortForbidden(c, "kubernetes client not initialized")
		return nil, false
	}
	cli, ok := v.(client.Client)
	if !ok || cli == nil {
		apierr.AbortForbidden(c, "invalid kubernetes client in context")
		return nil, false
	}
	return cli, true
}

func ternary[T any](cond bool, a T, b T) T {
	if cond {
		return a
	}
	return b
}

// ======================== Core Logic ========================

func computePrechangeChecklist(c *gin.Context, k8sClient client.Client, namespace, name string, windowHours int, now time.Time) (hasRecent bool, lastBackupTime *time.Time, rpoLagSeconds int, storageConnectivity string, rpoOk bool, err error) {
	var backupList polardbxv1.PolarDBXBackupList
	if err = k8sClient.List(c.Request.Context(), &backupList, client.InNamespace(namespace)); err != nil {
		return
	}
	windowStart := now.Add(-time.Duration(windowHours) * time.Hour)
	var maxLrt time.Time
	for _, b := range backupList.Items {
		if b.Spec.Cluster.Name != name {
			continue
		}
		if b.Status.StartTime != nil {
			st := b.Status.StartTime.Time
			if lastBackupTime == nil || st.After(*lastBackupTime) {
				lb := st
				lastBackupTime = &lb
			}
			if st.After(windowStart) && b.Status.Phase == polardbxv1.BackupFinished {
				hasRecent = true
			}
		}
		if b.Status.LatestRecoverableTimestamp != nil {
			lrt := b.Status.LatestRecoverableTimestamp.Time
			if lrt.After(maxLrt) {
				maxLrt = lrt
			}
		}
	}
	rpoLagSeconds = -1
	if !maxLrt.IsZero() {
		rpoLagSeconds = int(now.Sub(maxLrt).Seconds())
	}
	storageConnectivity = "unknown"
	var hpfsCM corev1.ConfigMap
	if e := k8sClient.Get(c.Request.Context(), client.ObjectKey{Namespace: "polardbx-operator-system", Name: "polardbx-hpfs-config"}, &hpfsCM); e == nil {
		storageConnectivity = "configured"
	}
	rpoThreshold := 3600
	var settingsCM corev1.ConfigMap
	if e := k8sClient.Get(c.Request.Context(), client.ObjectKey{Namespace: "polardbx-operator-system", Name: "polardbx-dashboard-backend-config"}, &settingsCM); e == nil {
		if settingsCM.Data != nil {
			if v := settingsCM.Data["rpoThresholdSeconds"]; v != "" {
				if n, err := strconv.Atoi(v); err == nil {
					rpoThreshold = n
				}
			}
		}
	}
	rpoOk = rpoLagSeconds >= 0 && rpoLagSeconds <= rpoThreshold
	return
}

// evaluateClusterState checks cluster CR status and underlying workloads/pods readiness
func evaluateClusterState(c *gin.Context, k8sClient client.Client, namespace, name string) (phase string, clusterReady bool, controllersReady bool, podsReady bool, err error) {
	var cluster polardbxv1.PolarDBXCluster
	if e := k8sClient.Get(c.Request.Context(), client.ObjectKey{Namespace: namespace, Name: name}, &cluster); e != nil {
		err = e
		return
	}
	phase = string(cluster.Status.Phase)
	for _, cond := range cluster.Status.Conditions {
		if string(cond.Type) == "ClusterReady" && cond.Status == corev1.ConditionTrue {
			clusterReady = true
			break
		}
	}

	controllersReady = true
	var depList appsv1.DeploymentList
	if e := k8sClient.List(c.Request.Context(), &depList, client.InNamespace(namespace), client.MatchingLabels(map[string]string{"polardbx/name": name})); e == nil {
		for _, d := range depList.Items {
			desired := d.Status.Replicas
			ready := d.Status.ReadyReplicas
			if desired > 0 && ready < desired {
				controllersReady = false
				break
			}
		}
	}
	if controllersReady {
		var stsList appsv1.StatefulSetList
		if e := k8sClient.List(c.Request.Context(), &stsList, client.InNamespace(namespace), client.MatchingLabels(map[string]string{"polardbx/name": name})); e == nil {
			for _, s := range stsList.Items {
				var desired int32 = 0
				if s.Spec.Replicas != nil {
					desired = *s.Spec.Replicas
				}
				ready := s.Status.ReadyReplicas
				if desired > 0 && ready < desired {
					controllersReady = false
					break
				}
			}
		}
	}

	podsReady = true
	var podList corev1.PodList
	if e := k8sClient.List(c.Request.Context(), &podList, client.InNamespace(namespace), client.MatchingLabels(map[string]string{"polardbx/name": name})); e == nil {
		for _, p := range podList.Items {
			isReady := false
			for _, cs := range p.Status.Conditions {
				if cs.Type == corev1.PodReady && cs.Status == corev1.ConditionTrue {
					isReady = true
					break
				}
			}
			if !isReady {
				podsReady = false
				break
			}
		}
	}
	return
}

// scanConflicts scans ongoing/conflicting jobs related to the cluster
func scanConflicts(c *gin.Context, k8sClient client.Client, namespace, name string) (conflicts []map[string]any) {
	conflicts = []map[string]any{}
	ctx := c.Request.Context()

	var backupList polardbxv1.PolarDBXBackupList
	if err := k8sClient.List(ctx, &backupList, client.InNamespace(namespace)); err == nil {
		for _, b := range backupList.Items {
			if b.Spec.Cluster.Name != name {
				continue
			}
			ph := string(b.Status.Phase)
			if ph != string(polardbxv1.BackupFinished) && ph != string(polardbxv1.BackupFailed) && ph != string(polardbxv1.BackupDeleting) {
				conflicts = append(conflicts, map[string]any{"kind": "Backup", "name": b.Name, "phase": ph})
			}
		}
	}

	var taskList polardbxv1.SystemTaskList
	if err := k8sClient.List(ctx, &taskList, client.InNamespace(namespace)); err == nil {
		for _, t := range taskList.Items {
			if t.Labels["polardbx/name"] != name {
				continue
			}
			if string(t.Status.Phase) != string(systemtask.SuccessPhase) {
				conflicts = append(conflicts, map[string]any{"kind": "SystemTask", "name": t.Name, "phase": string(t.Status.Phase)})
			}
		}
	}

	var cluster polardbxv1.PolarDBXCluster
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &cluster); err == nil {
		if cluster.Status.Phase == polardbx.PhaseUpgrading {
			conflicts = append(conflicts, map[string]any{"kind": "Cluster", "name": cluster.Name, "phase": string(cluster.Status.Phase)})
		}
	}
	return
}

// evaluateCapacityAndDeps inspects scheduling capacity, node disk pressure, resource quotas,
// binlog availability and basic RBAC/CRD presence by probing list calls.
func evaluateCapacityAndDeps(c *gin.Context, k8sClient client.Client, namespace, name string) (unschedulablePods bool, nodeDiskPressure bool, quotaOk bool, binlogOk bool, rbacOk bool) {
	ctx := c.Request.Context()

	var podList corev1.PodList
	if err := k8sClient.List(ctx, &podList, client.InNamespace(namespace), client.MatchingLabels(map[string]string{"polardbx/name": name})); err == nil {
		for _, p := range podList.Items {
			for _, cond := range p.Status.Conditions {
				if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse && cond.Reason == "Unschedulable" {
					unschedulablePods = true
					break
				}
			}
			if unschedulablePods {
				break
			}
		}
	}

	var nodes corev1.NodeList
	if err := k8sClient.List(ctx, &nodes); err == nil {
		for _, n := range nodes.Items {
			for _, cond := range n.Status.Conditions {
				if cond.Type == corev1.NodeDiskPressure && cond.Status == corev1.ConditionTrue {
					nodeDiskPressure = true
					break
				}
			}
			if nodeDiskPressure {
				break
			}
		}
	}

	quotaOk = true
	var rqList corev1.ResourceQuotaList
	if err := k8sClient.List(ctx, &rqList, client.InNamespace(namespace)); err == nil {
		for _, rq := range rqList.Items {
			for resName, hard := range rq.Status.Hard {
				used := rq.Status.Used[resName]
				var hardF float64
				var usedF float64
				if hard.Format == "DecimalSI" || hard.Format == "BinarySI" || hard.Format == "DecimalExponent" {
					hardF = float64(hard.Value())
					usedF = float64(used.Value())
				} else {
					hardF = hard.AsApproximateFloat64()
					usedF = used.AsApproximateFloat64()
				}
				if hardF > 0 && usedF/hardF >= 0.95 {
					quotaOk = false
					break
				}
			}
			if !quotaOk {
				break
			}
		}
	}

	binlogOk = false
	var binlogList polardbxv1.PolarDBXBackupBinlogList
	if err := k8sClient.List(ctx, &binlogList, client.InNamespace(namespace)); err == nil {
		for _, b := range binlogList.Items {
			if b.Spec.PxcName == name {
				binlogOk = true
				break
			}
		}
	}

	rbacOk = true
	{
		var x polardbxv1.PolarDBXBackupList
		if err := k8sClient.List(ctx, &x, client.InNamespace(namespace)); err != nil {
			rbacOk = false
		}
	}
	{
		var x polardbxv1.PolarDBXBackupScheduleList
		if err := k8sClient.List(ctx, &x, client.InNamespace(namespace)); err != nil {
			rbacOk = false
		}
	}
	{
		var x polardbxv1.SystemTaskList
		if err := k8sClient.List(ctx, &x, client.InNamespace(namespace)); err != nil {
			rbacOk = false
		}
	}

	return
}

// evaluateVersionCompat validates upgrade target version compatibility
func evaluateVersionCompat(c *gin.Context, k8sClient client.Client, namespace, name, op string, targetSpec map[string]any) (currentVersion string, targetVersion string, versionOk bool, msg string) {
	ctx := c.Request.Context()
	var cluster polardbxv1.PolarDBXCluster
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &cluster); err != nil {
		return "", "", true, "unable to get cluster, skipping version check"
	}
	currentVersion = cluster.Spec.Topology.Version
	versionOk = true
	if op != "upgrade" {
		return currentVersion, "", true, "not an upgrade operation"
	}
	if v, ok := targetSpec["targetVersion"].(string); ok {
		targetVersion = v
	}
	if targetVersion == "" {
		return currentVersion, "", false, "upgrade target version not provided"
	}
	cmj, cmn, cpt, cok := parseVersion(currentVersion)
	tmj, tmn, tpt, tok := parseVersion(targetVersion)
	if !cok || !tok {
		return currentVersion, targetVersion, true, "version number cannot be parsed, skipping strict validation (recommended to use x.y.z semantic version)"
	}
	if tmj != cmj {
		return currentVersion, targetVersion, false, "cross-major version upgrade not supported"
	}
	if tmn == cmn && tpt == cpt {
		return currentVersion, targetVersion, false, "target version is the same as current version, no upgrade needed"
	}
	if tmn < cmn || (tmn == cmn && tpt < cpt) {
		return currentVersion, targetVersion, false, "downgrade to lower version not supported"
	}
	return currentVersion, targetVersion, true, "version path validation passed"
}

// parseVersion accepts strings like "8.0.18" or "8.0.18-xxx" and returns major/minor/patch
func parseVersion(s string) (major int, minor int, patch int, ok bool) {
	parts := strings.Split(s, ".")
	if len(parts) < 2 {
		return 0, 0, 0, false
	}
	num := func(tok string) (int, bool) {
		val := 0
		got := false
		for i := 0; i < len(tok); i++ {
			ch := tok[i]
			if ch < '0' || ch > '9' {
				break
			}
			val = val*10 + int(ch-'0')
			got = true
		}
		return val, got
	}
	mj, ok1 := num(parts[0])
	mn, ok2 := 0, false
	pt, ok3 := 0, false
	if len(parts) >= 2 {
		mn, ok2 = num(parts[1])
	}
	if len(parts) >= 3 {
		pt, ok3 = num(parts[2])
	} else {
		ok3 = true
	}
	if !(ok1 && ok2 && ok3) {
		return 0, 0, 0, false
	}
	return mj, mn, pt, true
}

// ======================== Token Functions ========================

func getPrecheckSecret(c *gin.Context, cli client.Client) string {
	cm := corev1.ConfigMap{}
	if err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: "polardbx-operator-system", Name: "polardbx-dashboard-backend-config"}, &cm); err == nil {
		if cm.Data != nil && cm.Data["precheck.secret"] != "" {
			return cm.Data["precheck.secret"]
		}
	}
	return ""
}

func signPrecheckToken(secret, plain string) string {
	if secret == "" {
		return ""
	}
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(plain))
	mac := h.Sum(nil)
	return hex.EncodeToString(mac)
}

func verifyPrecheckToken(secret, plain, sig string) bool {
	if secret == "" || sig == "" {
		return false
	}
	expected := signPrecheckToken(secret, plain)
	return hmac.Equal([]byte(expected), []byte(sig))
}

// ======================== HTTP Handlers ========================

// GetPrechangeChecklist gets pre-change checklist
// @Summary Get pre-change checklist
// @Description Retrieves a pre-change checklist for cluster operations (backup status, RPO, storage connectivity, etc.)
// @Tags prechange
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the cluster"
// @Param name path string true "Name of the PolarDB-X cluster"
// @Param windowHours query int false "Time window in hours for recent backup check (default: 24)"
// @Param now query string false "Reference time in RFC3339 format (default: current time)"
// @Success 200 {object} map[string]any "Pre-change checklist with backup status, RPO, storage connectivity"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request parameters"
// @Failure 404 {object} apierr.ErrorResponse "Cluster not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/polardbxclusters/{namespace}/{name}/prechange-check [get]
func GetPrechangeChecklist(c *gin.Context) {
	k8sClient, ok := getK8sClient(c)
	if !ok {
		return
	}
	namespace := c.Param("namespace")
	name := c.Param("name")
	windowHours := 24
	if v := c.DefaultQuery("windowHours", "24"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			windowHours = n
		}
	}
	nowStr := c.DefaultQuery("now", "")
	var now time.Time
	if nowStr != "" {
		if t, err := time.Parse(time.RFC3339, nowStr); err == nil {
			now = t
		}
	}
	if now.IsZero() {
		now = time.Now()
	}

	hasRecent, lastBackupTime, rpoLagSeconds, storageConnectivity, rpoOk, err := computePrechangeChecklist(c, k8sClient, namespace, name, windowHours, now)
	if err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("failed to list backups", err))
		return
	}

	plan := []map[string]any{
		{"id": "checkStorage", "state": ternary(storageConnectivity == "configured", "ok", "warn"), "message": "HPFS configuration exists"},
		{"id": "checkRecentBackup", "state": ternary(hasRecent, "ok", "warn"), "message": "Successful full backup exists within recent window"},
		{"id": "checkRPO", "state": ternary(rpoOk, "ok", "warn"), "message": "RPO has not exceeded threshold"},
	}
	resp := gin.H{"namespace": namespace, "name": name, "generatedAt": now.Format(time.RFC3339), "checks": gin.H{
		"hasRecentBackup":     hasRecent,
		"lastBackupTime":      ternary(lastBackupTime != nil, lastBackupTime.UTC().Format(time.RFC3339), ""),
		"rpoLagSeconds":       rpoLagSeconds,
		"storageConnectivity": storageConnectivity,
	}, "plan": plan}
	apierr.OK(c, resp)
}

// Precheck performs precheck and returns pass/warnings/errors and lightweight token
// @Summary Perform pre-change check
// @Description Performs comprehensive pre-change validation for cluster operations (scale/upgrade/config), returns pass status, warnings, errors, and a validation token
// @Tags prechange
// @Accept json
// @Produce json
// @Param namespace path string true "Kubernetes namespace of the cluster"
// @Param name path string true "Name of the PolarDB-X cluster"
// @Param body body PrecheckRequest true "Precheck request with operation type and optional target spec"
// @Success 200 {object} map[string]any "Precheck result with pass status, warnings, errors, checks, and validation token"
// @Failure 400 {object} apierr.ErrorResponse "Invalid request or unsupported operation"
// @Failure 404 {object} apierr.ErrorResponse "Cluster not found"
// @Failure 500 {object} apierr.ErrorResponse "Internal server error"
// @Router /api/v1/polardbxclusters/{namespace}/{name}/precheck [post]
func Precheck(c *gin.Context) {
	k8sClient, ok := getK8sClient(c)
	if !ok {
		return
	}

	ns := c.Param("namespace")
	name := c.Param("name")

	var req PrecheckRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apierr.AbortWithError(c, err)
		return
	}
	op := req.Operation
	switch op {
	case "", "scale", "upgrade", "config":
		if op == "" {
			op = "config"
		}
	default:
		apierr.AbortWithError(c, apierr.ValidationError("unsupported operation: "+op, nil))
		return
	}

	now := time.Now()
	hasRecent, lastBackupTime, rpoLagSeconds, storageConnectivity, rpoOk, err := computePrechangeChecklist(c, k8sClient, ns, name, 24, now)
	if err != nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("failed to compute checklist", err))
		return
	}

	warnings := []string{}
	errorsArr := []string{}

	phase, clusterReady, controllersReady, podsReady, _ := evaluateClusterState(c, k8sClient, ns, name)
	unsched, diskPressure, quotaOk, binlogOk, rbacOk := evaluateCapacityAndDeps(c, k8sClient, ns, name)
	curVer, tgtVer, verOk, verMsg := evaluateVersionCompat(c, k8sClient, ns, name, op, req.TargetSpec)
	conflicts := scanConflicts(c, k8sClient, ns, name)

	if len(conflicts) > 0 {
		warnings = append(warnings, fmt.Sprintf("found %d potentially conflicting jobs", len(conflicts)))
	}
	if unsched {
		warnings = append(warnings, "unschedulable pods exist, resources or affinity may be insufficient")
	}
	if diskPressure {
		warnings = append(warnings, "some nodes have disk pressure (NodeDiskPressure)")
	}
	if !quotaOk {
		warnings = append(warnings, "namespace resource quota is nearly exhausted (≥95%)")
	}
	if !binlogOk {
		warnings = append(warnings, "Binlog backup configuration not detected, PITR capability may be unavailable")
	}
	if !rbacOk {
		warnings = append(warnings, "RBAC/CRD dependency validation may fail (please check CRD and permissions)")
	}
	if !clusterReady {
		if op == "upgrade" {
			errorsArr = append(errorsArr, "cluster is not ready, must reach ClusterReady before upgrade")
		} else {
			warnings = append(warnings, "cluster is not ready, recommend checking status first")
		}
	}
	if !controllersReady {
		if op == "upgrade" {
			errorsArr = append(errorsArr, "not all controllers are ready (Deployment/StatefulSet)")
		} else {
			warnings = append(warnings, "not all controllers are ready (Deployment/StatefulSet)")
		}
	}
	if !podsReady {
		warnings = append(warnings, "some pods are not ready, please pay attention")
	}
	if storageConnectivity != "configured" {
		warnings = append(warnings, "HPFS configuration not detected, recommend configuring to ensure rollback point")
	}
	if !hasRecent {
		warnings = append(warnings, "no successful full backup in the last 24 hours")
	}
	if !rpoOk {
		warnings = append(warnings, fmt.Sprintf("RPO lag %d seconds exceeds threshold", rpoLagSeconds))
	}

	if op == "upgrade" {
		if storageConnectivity != "configured" {
			errorsArr = append(errorsArr, "HPFS must be configured before upgrade to ensure rollback capability")
		}
		if !hasRecent {
			errorsArr = append(errorsArr, "recent successful full backup must exist before upgrade")
		}
	}

	tStr := func(t *time.Time) string {
		if t == nil {
			return ""
		}
		return t.UTC().Format(time.RFC3339)
	}
	plan := []map[string]any{
		{"id": "checkClusterReady", "state": ternary(clusterReady, "ok", ternary(op == "upgrade", "error", "warn")), "message": "ClusterReady condition is True"},
		{"id": "checkControllersReady", "state": ternary(controllersReady, "ok", ternary(op == "upgrade", "error", "warn")), "message": "Deployment/StatefulSet replicas ready"},
		{"id": "checkPodsReady", "state": ternary(podsReady, "ok", "warn"), "message": "All pods are Ready"},
		{"id": "checkStorage", "state": ternary(storageConnectivity == "configured", "ok", ternary(op == "upgrade", "error", "warn")), "message": "HPFS configuration exists"},
		{"id": "checkRecentBackup", "state": ternary(hasRecent, "ok", ternary(op == "upgrade", "error", "warn")), "message": "Successful full backup exists within recent window"},
		{"id": "checkRPO", "state": ternary(rpoOk, "ok", "warn"), "message": "RPO has not exceeded threshold"},
		{"id": "checkConflicts", "state": ternary(len(conflicts) == 0, "ok", "warn"), "message": "Conflict job scan"},
		{"id": "checkScheduling", "state": ternary(!unsched, "ok", "warn"), "message": "Unschedulable pod check"},
		{"id": "checkNodeDiskPressure", "state": ternary(!diskPressure, "ok", "warn"), "message": "Node disk pressure"},
		{"id": "checkResourceQuota", "state": ternary(quotaOk, "ok", "warn"), "message": "Namespace quota healthy"},
		{"id": "checkBinlogAvailable", "state": ternary(binlogOk, "ok", ternary(op == "upgrade", "warn", "warn")), "message": "PITR Binlog availability"},
		{"id": "checkRBAC", "state": ternary(rbacOk, "ok", "warn"), "message": "RBAC/CRD dependencies available"},
		{"id": "checkVersionCompat", "state": ternary(verOk, "ok", ternary(op == "upgrade", "error", "warn")), "message": verMsg},
	}

	pass := len(errorsArr) == 0
	plain := fmt.Sprintf("%d:%s:%s/%s", time.Now().UnixNano(), op, ns, name)
	token := plain
	secret := getPrecheckSecret(c, k8sClient)
	sig := signPrecheckToken(secret, plain)

	apierr.OK(c, gin.H{
		"pass":      pass,
		"operation": op,
		"warnings":  warnings,
		"errors":    errorsArr,
		"plan":      plan,
		"checks": gin.H{
			"hasRecentBackup":     hasRecent,
			"lastBackupTime":      tStr(lastBackupTime),
			"rpoLagSeconds":       rpoLagSeconds,
			"storageConnectivity": storageConnectivity,
			"phase":               phase,
			"clusterReady":        clusterReady,
			"controllersReady":    controllersReady,
			"podsReady":           podsReady,
			"conflicts":           conflicts,
			"unschedulablePods":   unsched,
			"nodeDiskPressure":    diskPressure,
			"quotaOk":             quotaOk,
			"binlogOk":            binlogOk,
			"rbacOk":              rbacOk,
			"currentVersion":      curVer,
			"targetVersion":       tgtVer,
			"versionOk":           verOk,
		},
		"generatedAt": now.Format(time.RFC3339),
		"token":       token,
		"tokenSig":    sig,
	})
}

// ======================== Exported Validation Functions ========================

// ValidatePrecheckToken parses and validates token: ts:op:ns/name.
func ValidatePrecheckToken(token, expectOp, ns, name string, maxSkew time.Duration) bool {
	if token == "" {
		return false
	}
	first := -1
	second := -1
	for i, ch := range token {
		if ch == ':' {
			if first < 0 {
				first = i
				continue
			}
			second = i
			break
		}
	}
	if first <= 0 || second <= first+1 || second >= len(token)-1 {
		return false
	}
	tsStr := token[:first]
	op := token[first+1 : second]
	id := token[second+1:]
	if op != expectOp {
		return false
	}
	if id != fmt.Sprintf("%s/%s", ns, name) {
		return false
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return false
	}
	t := time.Unix(0, ts)
	return time.Since(t) <= maxSkew
}

// GetPrecheckSecretForValidation tries to get secret via k8sClient in gin context.
func GetPrecheckSecretForValidation(c *gin.Context) string {
	v, ok := c.Get("k8sClient")
	if !ok {
		return ""
	}
	cli, ok := v.(client.Client)
	if !ok || cli == nil {
		return ""
	}
	return getPrecheckSecret(c, cli)
}

// VerifyPrecheckTokenForValidation verifies HMAC signature against plain token
func VerifyPrecheckTokenForValidation(secret, token, sig string) bool {
	return verifyPrecheckToken(secret, token, sig)
}
