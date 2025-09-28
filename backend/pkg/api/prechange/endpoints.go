package prechange

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/gin-gonic/gin"
	"sigs.k8s.io/controller-runtime/pkg/client"

	systemtask "github.com/alibaba/polardbx-operator/api/v1/systemtask"
)

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
	if e := k8sClient.Get(c.Request.Context(), client.ObjectKey{Namespace: "polardbx-operator-system", Name: "polardbx-ui-backend-config"}, &settingsCM); e == nil {
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
	// Get cluster CR
	var cluster polardbxv1.PolarDBXCluster
	if e := k8sClient.Get(c.Request.Context(), client.ObjectKey{Namespace: namespace, Name: name}, &cluster); e != nil {
		err = e
		return
	}
	phase = string(cluster.Status.Phase)
	// ClusterReady condition
	for _, cond := range cluster.Status.Conditions {
		if string(cond.Type) == "ClusterReady" && cond.Status == corev1.ConditionTrue {
			clusterReady = true
			break
		}
	}

	// Controllers: Deployments and StatefulSets
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

	// Pods readiness
	podsReady = true
	var podList corev1.PodList
	if e := k8sClient.List(c.Request.Context(), &podList, client.InNamespace(namespace), client.MatchingLabels(map[string]string{"polardbx/name": name})); e == nil {
		for _, p := range podList.Items {
			// Need Ready condition True
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

	// 1) 进行中的备份（非 Finished/Failed/Deleting）
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

	// 2) 系统任务进行中（非 Success）
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

	// 3) 升级在途：Cluster Phase Upgrading
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

	// Pods scheduling state
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

	// Node disk pressure (cluster-wide signal that may affect scheduling)
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

	// ResourceQuota sanity check (namespace-level)
	quotaOk = true
	var rqList corev1.ResourceQuotaList
	if err := k8sClient.List(ctx, &rqList, client.InNamespace(namespace)); err == nil {
		for _, rq := range rqList.Items {
			for resName, hard := range rq.Status.Hard {
				used := rq.Status.Used[resName]
				// Consider near-exhausted when usage exceeds 95%
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

	// Binlog availability (presence of BackupBinlog resource for the cluster)
	binlogOk = false
	var binlogList polardbxv1.PolarDBXBackupBinlogList
	if err := k8sClient.List(ctx, &binlogList, client.InNamespace(namespace)); err == nil {
		for _, b := range binlogList.Items {
			if b.Spec.PxcName == name {
				// Treat existence as basic availability; stricter checks can be added later
				binlogOk = true
				break
			}
		}
	}

	// RBAC/CRD presence probing via list calls; if any of these return a NoMatch-like error, mark false
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
		return "", "", true, "无法获取集群，跳过版本检查"
	}
	currentVersion = cluster.Spec.Topology.Version
	versionOk = true
	if op != "upgrade" {
		return currentVersion, "", true, "非升级操作"
	}
	// extract targetVersion from targetSpec
	if v, ok := targetSpec["targetVersion"].(string); ok {
		targetVersion = v
	}
	if targetVersion == "" {
		return currentVersion, "", false, "升级目标版本未提供"
	}
	cmj, cmn, cpt, cok := parseVersion(currentVersion)
	tmj, tmn, tpt, tok := parseVersion(targetVersion)
	if !cok || !tok {
		return currentVersion, targetVersion, true, "版本号无法解析，跳过严格校验（建议使用 x.y.z 语义化版本）"
	}
	if tmj != cmj {
		return currentVersion, targetVersion, false, "不支持跨主版本升级"
	}
	// equal
	if tmn == cmn && tpt == cpt {
		return currentVersion, targetVersion, false, "目标版本与当前版本一致，无需升级"
	}
	// require target > current
	if tmn < cmn || (tmn == cmn && tpt < cpt) {
		return currentVersion, targetVersion, false, "不支持降级到更低版本"
	}
	return currentVersion, targetVersion, true, "版本路径校验通过"
}

// parseVersion accepts strings like "8.0.18" or "8.0.18-xxx" and returns major/minor/patch
func parseVersion(s string) (major int, minor int, patch int, ok bool) {
	// keep only digits and dots before first non [0-9A-Za-z_.-] char (simple tolerant parser)
	// split by '.' and parse numeric prefix of each token
	parts := strings.Split(s, ".")
	if len(parts) < 2 { // at least major.minor
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
		ok3 = true // allow missing patch
	}
	if !(ok1 && ok2 && ok3) {
		return 0, 0, 0, false
	}
	return mj, mn, pt, true
}

func GetPrechangeChecklist(c *gin.Context) {
	k8sClient, ok := func() (client.Client, bool) {
		v, ok := c.Get("k8sClient")
		if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "kubernetes client not initialized"})
			return nil, false
		}
		cli, ok := v.(client.Client)
		if !ok || cli == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid kubernetes client in context"})
			return nil, false
		}
		return cli, true
	}()
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list backups", "details": err.Error()})
		return
	}

	plan := []map[string]any{
		{"id": "checkStorage", "state": ternary(storageConnectivity == "configured", "ok", "warn"), "message": "HPFS 配置存在"},
		{"id": "checkRecentBackup", "state": ternary(hasRecent, "ok", "warn"), "message": "最近窗口内存在成功的全量备份"},
		{"id": "checkRPO", "state": ternary(rpoOk, "ok", "warn"), "message": "RPO 未超过阈值"},
	}
	resp := gin.H{"namespace": namespace, "name": name, "generatedAt": now.Format(time.RFC3339), "checks": gin.H{
		"hasRecentBackup":     hasRecent,
		"lastBackupTime":      ternary(lastBackupTime != nil, lastBackupTime.UTC().Format(time.RFC3339), ""),
		"rpoLagSeconds":       rpoLagSeconds,
		"storageConnectivity": storageConnectivity,
	}, "plan": plan}
	c.JSON(http.StatusOK, resp)
}

// ---- Unified Precheck API (operation-aware) ----

type PrecheckRequest struct {
	Operation  string         `json:"operation"`  // scale|upgrade|config
	TargetSpec map[string]any `json:"targetSpec"` // optional, reserved
}

// Precheck returns pass/warnings/errors and a lightweight token
func Precheck(c *gin.Context) {
	k8sClient, ok := func() (client.Client, bool) {
		v, ok := c.Get("k8sClient")
		if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "kubernetes client not initialized"})
			return nil, false
		}
		cli, ok := v.(client.Client)
		if !ok || cli == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid kubernetes client in context"})
			return nil, false
		}
		return cli, true
	}()
	if !ok {
		return
	}

	ns := c.Param("namespace")
	name := c.Param("name")

	var req PrecheckRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid precheck request", "details": err.Error()})
		return
	}
	// normalize op
	op := req.Operation
	switch op {
	case "", "scale", "upgrade", "config":
		if op == "" {
			op = "config"
		}
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported operation", "operation": op})
		return
	}

	now := time.Now()
	hasRecent, lastBackupTime, rpoLagSeconds, storageConnectivity, rpoOk, err := computePrechangeChecklist(c, k8sClient, ns, name, 24, now)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to compute checklist", "details": err.Error()})
		return
	}

	warnings := []string{}
	errorsArr := []string{}

	// cluster state
	phase, clusterReady, controllersReady, podsReady, _ := evaluateClusterState(c, k8sClient, ns, name)
	unsched, diskPressure, quotaOk, binlogOk, rbacOk := evaluateCapacityAndDeps(c, k8sClient, ns, name)
	curVer, tgtVer, verOk, verMsg := evaluateVersionCompat(c, k8sClient, ns, name, op, req.TargetSpec)
	conflicts := scanConflicts(c, k8sClient, ns, name)
	if len(conflicts) > 0 {
		warnings = append(warnings, fmt.Sprintf("存在可能冲突的作业 %d 项", len(conflicts)))
	}
	if unsched {
		warnings = append(warnings, "存在不可调度的 Pod，资源或亲和性可能不足")
	}
	if diskPressure {
		warnings = append(warnings, "部分节点存在磁盘压力(NodeDiskPressure)")
	}
	if !quotaOk {
		warnings = append(warnings, "命名空间资源配额接近耗尽（≥95%）")
	}
	if !binlogOk {
		warnings = append(warnings, "未检测到 Binlog 备份配置，PITR 能力可能不可用")
	}
	if !rbacOk {
		warnings = append(warnings, "RBAC/CRD 依赖校验可能失败（请检查 CRD 与权限）")
	}
	if !clusterReady {
		if op == "upgrade" {
			errorsArr = append(errorsArr, "集群未就绪，升级前需达到 ClusterReady")
		} else {
			warnings = append(warnings, "集群未就绪，建议先排查状态")
		}
	}
	if !controllersReady {
		if op == "upgrade" {
			errorsArr = append(errorsArr, "控制器未全部就绪（Deployment/StatefulSet）")
		} else {
			warnings = append(warnings, "控制器未全部就绪（Deployment/StatefulSet）")
		}
	}
	if !podsReady {
		warnings = append(warnings, "存在未就绪的 Pod，请关注")
	}

	if storageConnectivity != "configured" {
		warnings = append(warnings, "未检测到 HPFS 配置，建议配置以保障回退点")
	}
	if !hasRecent {
		warnings = append(warnings, "最近 24 小时内无成功的全量备份")
	}
	if !rpoOk {
		warnings = append(warnings, fmt.Sprintf("RPO 滞后 %d 秒超出阈值", rpoLagSeconds))
	}

	// 升级场景更严格
	if op == "upgrade" {
		if storageConnectivity != "configured" {
			errorsArr = append(errorsArr, "升级前必须配置 HPFS 以保证回退能力")
		}
		if !hasRecent {
			errorsArr = append(errorsArr, "升级前必须具备最近成功的全量备份")
		}
	}

	// Build plan items
	tStr := func(t *time.Time) string {
		if t == nil {
			return ""
		}
		return t.UTC().Format(time.RFC3339)
	}
	plan := []map[string]any{
		{"id": "checkClusterReady", "state": ternary(clusterReady, "ok", ternary(op == "upgrade", "error", "warn")), "message": "ClusterReady 条件为 True"},
		{"id": "checkControllersReady", "state": ternary(controllersReady, "ok", ternary(op == "upgrade", "error", "warn")), "message": "Deployment/StatefulSet 副本就绪"},
		{"id": "checkPodsReady", "state": ternary(podsReady, "ok", "warn"), "message": "所有 Pod 处于 Ready"},
		{"id": "checkStorage", "state": ternary(storageConnectivity == "configured", "ok", ternary(op == "upgrade", "error", "warn")), "message": "HPFS 配置存在"},
		{"id": "checkRecentBackup", "state": ternary(hasRecent, "ok", ternary(op == "upgrade", "error", "warn")), "message": "最近窗口内存在成功的全量备份"},
		{"id": "checkRPO", "state": ternary(rpoOk, "ok", "warn"), "message": "RPO 未超过阈值"},
		{"id": "checkConflicts", "state": ternary(len(conflicts) == 0, "ok", "warn"), "message": "冲突作业扫描"},
		{"id": "checkScheduling", "state": ternary(!unsched, "ok", "warn"), "message": "不可调度 Pod 检查"},
		{"id": "checkNodeDiskPressure", "state": ternary(!diskPressure, "ok", "warn"), "message": "节点磁盘压力"},
		{"id": "checkResourceQuota", "state": ternary(quotaOk, "ok", "warn"), "message": "命名空间配额健康"},
		{"id": "checkBinlogAvailable", "state": ternary(binlogOk, "ok", ternary(op == "upgrade", "warn", "warn")), "message": "PITR Binlog 可用性"},
		{"id": "checkRBAC", "state": ternary(rbacOk, "ok", "warn"), "message": "RBAC/CRD 依赖可用"},
		{"id": "checkVersionCompat", "state": ternary(verOk, "ok", ternary(op == "upgrade", "error", "warn")), "message": verMsg},
	}

	pass := len(errorsArr) == 0
	plain := fmt.Sprintf("%d:%s:%s/%s", time.Now().UnixNano(), op, ns, name)
	token := plain
	secret := getPrecheckSecret(c, k8sClient)
	sig := signPrecheckToken(secret, plain)

	c.JSON(http.StatusOK, gin.H{
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

// ValidatePrecheckToken parses and validates token: ts:op:ns/name.
// Lightweight guard to reduce TOCTOU; not a hard security barrier.
func ValidatePrecheckToken(token, expectOp, ns, name string, maxSkew time.Duration) bool {
	if token == "" {
		return false
	}
	// split ts:op:ns/name
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

func ternary[T any](cond bool, a T, b T) T {
	if cond {
		return a
	}
	return b
}

// Checklist 汇总预变更检查的关键结果
type Checklist struct {
	HasRecentBackup     bool   `json:"hasRecentBackup"`
	RpoLagSeconds       int    `json:"rpoLagSeconds"`
	StorageConnectivity string `json:"storageConnectivity"`
	RpoOk               bool   `json:"rpoOk"`
}

func getPrecheckSecret(c *gin.Context, cli client.Client) string {
	cm := corev1.ConfigMap{}
	if err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: "polardbx-operator-system", Name: "polardbx-ui-backend-config"}, &cm); err == nil {
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
