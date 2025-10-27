package monitoring

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"polardbx-ui-backend/pkg/api/util"
	"polardbx-ui-backend/pkg/config"

	"github.com/gin-gonic/gin"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Bootstrap installs or registers monitoring stack. For now persists a bootstrap plan ConfigMap.
func Bootstrap(c *gin.Context) {
	type req struct {
		Mode string `json:"mode"` // managed|assisted|byo
		Dry  bool   `json:"dryRun"`
		NS   string `json:"namespace"`
		Name string `json:"releaseName"`
	}
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	var r req
	_ = c.ShouldBindJSON(&r)
	if r.NS == "" {
		r.NS = "polardbx-operator-system"
	}

	// Check for existing ongoing bootstrap jobs (idempotent)
	if !r.Dry {
		existingJobs := &batchv1.JobList{}
		labelSelector := client.MatchingLabels{
			"app":       "polardbx-monitor-bootstrap",
			"createdBy": "dashboard",
		}
		if err := cli.List(c.Request.Context(), existingJobs, client.InNamespace(r.NS), labelSelector); err == nil {
			// Look for non-completed jobs
			for _, job := range existingJobs.Items {
				isComplete := false
				isFailed := false
				for _, condition := range job.Status.Conditions {
					if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
						isComplete = true
						break
					}
					if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
						isFailed = true
						break
					}
				}
				// If job is still running, return existing job info
				if !isComplete && !isFailed {
					c.JSON(http.StatusAccepted, gin.H{
						"message":      "monitoring bootstrap already in progress",
						"namespace":    r.NS,
						"targetNs":     "polardbx-monitor",
						"mode":         r.Mode,
						"releaseName":  r.Name,
						"jobName":      job.Name,
						"instructions": "Use kubectl logs -n " + r.NS + " job/" + job.Name + " to see progress",
						"existing":     true,
					})
					return
				}
			}
		}
	}

	// Persist plan (idempotent)
	cm := corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: r.NS, Name: "polardbx-monitoring-plan"}
	if err := cli.Get(c.Request.Context(), key, &cm); err != nil {
		cm = corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: r.NS, Name: "polardbx-monitoring-plan"}, Data: map[string]string{}}
		_ = cli.Create(c.Request.Context(), &cm)
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data["mode"] = r.Mode
	cm.Data["releaseName"] = r.Name
	cm.Data["dryRun"] = map[bool]string{true: "true", false: "false"}[r.Dry]
	_ = cli.Update(c.Request.Context(), &cm)

	// If dry-run, just accept
	if r.Dry {
		c.JSON(http.StatusAccepted, gin.H{"message": "monitoring bootstrap accepted (dry-run)", "namespace": r.NS, "mode": r.Mode, "releaseName": r.Name, "dryRun": r.Dry})
		return
	}

	// Ensure target monitoring namespace exists
	monitorNS := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "polardbx-monitor"}}
	if err := cli.Create(c.Request.Context(), monitorNS); err != nil && !apierrors.IsAlreadyExists(err) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create namespace polardbx-monitor", "details": err.Error()})
		return
	}

	// Create a short-lived Job to run helm install inside cluster
	// Note: requires the Job's ServiceAccount to have sufficient RBAC to install chart resources
	jobName := fmt.Sprintf("polardbx-monitor-bootstrap-%d", time.Now().Unix())
	correlationId := fmt.Sprintf("monitor-%d", time.Now().UnixNano())
	command := strings.Join([]string{
		"set -e",
		"helm version || (echo 'helm not found in image' && exit 1)",
		"helm repo add polardbx https://polardbx-charts.oss-cn-beijing.aliyuncs.com || true",
		"helm repo update",
		"helm upgrade --install polardbx-monitor polardbx/polardbx-monitor --namespace polardbx-monitor --create-namespace",
	}, " && ")

	backoff := int32(0)
	ttl := int32(600)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.NS,
			Name:      jobName,
			Labels: map[string]string{
				"app":           "polardbx-monitor-bootstrap",
				"createdBy":     "dashboard",
				"correlationId": correlationId,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: func(b bool) *bool { return &b }(true),
					Containers: []corev1.Container{{
						Name:            "helm",
						Image:           config.GetGlobalConfig().GetHelmImage(),
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command:         []string{"sh", "-c", command},
					}},
				},
			},
		},
	}
	if err := cli.Create(c.Request.Context(), job); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create helm install job", "details": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"message":       "monitoring bootstrap started",
		"namespace":     r.NS,
		"targetNs":      "polardbx-monitor",
		"mode":          r.Mode,
		"releaseName":   r.Name,
		"jobName":       jobName,
		"correlationId": correlationId,
		"instructions":  "Use kubectl logs -n " + r.NS + " job/" + jobName + " to see progress",
	})
}

// Status summarizes discovered components readiness.
func Status(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	// 监控组件默认部署在 polardbx-monitor，可通过 ?namespace= 覆盖
	ns := util.DefaultNamespace(c, "polardbx-monitor")

	namespaceExists := true
	namespaceError := ""
	if err := cli.Get(c.Request.Context(), client.ObjectKey{Name: ns}, &corev1.Namespace{}); err != nil {
		if apierrors.IsNotFound(err) {
			namespaceExists = false
		} else {
			namespaceExists = false
			namespaceError = err.Error()
		}
	}

	checkDeploy := func(name string) (ready, desired int32, ok bool) {
		dep := appsv1.Deployment{}
		if err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: ns, Name: name}, &dep); err == nil {
			return dep.Status.ReadyReplicas, dep.Status.Replicas, true
		}
		return 0, 0, false
	}
	checkStateful := func(name string) (ready, desired int32, ok bool) {
		sts := appsv1.StatefulSet{}
		if err := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: ns, Name: name}, &sts); err == nil {
			return sts.Status.ReadyReplicas, *sts.Spec.Replicas, true
		}
		return 0, 0, false
	}
	checkService := func(name string) (*corev1.Service, bool) {
		svc := &corev1.Service{}
		ok := cli.Get(c.Request.Context(), client.ObjectKey{Namespace: ns, Name: name}, svc) == nil
		return svc, ok
	}

	checkCRD := func(name, display string) gin.H {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		err := cli.Get(c.Request.Context(), client.ObjectKey{Name: name}, crd)
		if err != nil {
			resp := gin.H{
				"name":          name,
				"displayName":   display,
				"exists":        false,
				"established":   false,
				"namesAccepted": false,
			}
			if !apierrors.IsNotFound(err) {
				resp["error"] = err.Error()
			}
			return resp
		}

		established := false
		namesAccepted := false
		conditions := make([]gin.H, 0, len(crd.Status.Conditions))
		for _, cond := range crd.Status.Conditions {
			if cond.Type == apiextensionsv1.Established && cond.Status == apiextensionsv1.ConditionTrue {
				established = true
			}
			if cond.Type == apiextensionsv1.NamesAccepted && cond.Status == apiextensionsv1.ConditionTrue {
				namesAccepted = true
			}
			conditions = append(conditions, gin.H{
				"type":               string(cond.Type),
				"status":             string(cond.Status),
				"reason":             cond.Reason,
				"message":            cond.Message,
				"lastTransitionTime": cond.LastTransitionTime,
			})
		}

		versions := make([]gin.H, 0, len(crd.Spec.Versions))
		for _, ver := range crd.Spec.Versions {
			versions = append(versions, gin.H{
				"name":    ver.Name,
				"served":  ver.Served,
				"storage": ver.Storage,
			})
		}

		shortNames := make([]string, len(crd.Spec.Names.ShortNames))
		copy(shortNames, crd.Spec.Names.ShortNames)

		return gin.H{
			"name":           name,
			"displayName":    display,
			"exists":         true,
			"group":          crd.Spec.Group,
			"kind":           crd.Spec.Names.Kind,
			"plural":         crd.Spec.Names.Plural,
			"singular":       crd.Spec.Names.Singular,
			"shortNames":     shortNames,
			"scope":          string(crd.Spec.Scope),
			"versions":       versions,
			"storedVersions": crd.Status.StoredVersions,
			"established":    established,
			"namesAccepted":  namesAccepted,
			"conditions":     conditions,
		}
	}

	// Helper to generate access URL for service
	generateServiceAccessURL := func(svcName string) string {
		svc, ok := checkService(svcName)
		if !ok {
			return ""
		}

		// LoadBalancer: return external IP if available
		if svc.Spec.Type == corev1.ServiceTypeLoadBalancer {
			if len(svc.Status.LoadBalancer.Ingress) > 0 {
				ingress := svc.Status.LoadBalancer.Ingress[0]
				if ingress.Hostname != "" {
					return fmt.Sprintf("http://%s", ingress.Hostname)
				}
				if ingress.IP != "" {
					return fmt.Sprintf("http://%s", ingress.IP)
				}
			}
		}

		// NodePort: return first node IP + port (user needs to know node IP)
		if svc.Spec.Type == corev1.ServiceTypeNodePort {
			for _, port := range svc.Spec.Ports {
				if port.NodePort > 0 {
					// Return instruction: user should find node IP and use it
					return fmt.Sprintf("NodePort: %d (需要使用 <node-ip>:%d 访问)", port.NodePort, port.NodePort)
				}
			}
		}

		// ClusterIP: return port-forward instruction
		if svc.Spec.Type == corev1.ServiceTypeClusterIP {
			for _, port := range svc.Spec.Ports {
				return fmt.Sprintf("port-forward svc/%s -n %s %d:3000", svcName, ns, port.Port)
			}
		}

		return ""
	}

	prom := gin.H{"ready": false, "readyReplicas": nil, "replicas": nil, "service": false, "exists": false}
	if r, d, ok := checkStateful("prometheus-k8s"); ok {
		prom["ready"] = r == d
		prom["readyReplicas"] = r
		prom["replicas"] = d
	} else if r, d, ok := checkStateful("kube-prometheus-stack-prometheus"); ok {
		prom["ready"] = r == d
		prom["readyReplicas"] = r
		prom["replicas"] = d
	}
	promSvc, promExists := checkService("prometheus-k8s")
	if !promExists {
		_, promExists = checkService("kube-prometheus-stack-prometheus")
		promSvc, _ = checkService("kube-prometheus-stack-prometheus")
	}
	prom["service"] = promExists
	prom["exists"] = prom["readyReplicas"] != nil || promExists
	if promExists {
		prom["accessUrl"] = generateServiceAccessURL(promSvc.Name)
	}

	graf := gin.H{"ready": false, "readyReplicas": nil, "replicas": nil, "service": false, "exists": false}
	if r, d, ok := checkDeploy("grafana"); ok {
		graf["ready"] = r == d
		graf["readyReplicas"] = r
		graf["replicas"] = d
	} else if r, d, ok := checkDeploy("kube-prometheus-stack-grafana"); ok {
		graf["ready"] = r == d
		graf["readyReplicas"] = r
		graf["replicas"] = d
	}
	grafSvc, grafExists := checkService("grafana")
	if !grafExists {
		_, grafExists = checkService("kube-prometheus-stack-grafana")
		grafSvc, _ = checkService("kube-prometheus-stack-grafana")
	}
	graf["service"] = grafExists
	graf["exists"] = graf["readyReplicas"] != nil || grafExists
	if grafExists {
		graf["accessUrl"] = generateServiceAccessURL(grafSvc.Name)
	}

	am := gin.H{"configured": false}
	_, amExists := checkService("alertmanager-main")
	if !amExists {
		_, amExists = checkService("kube-prometheus-stack-alertmanager")
	}
	am["configured"] = amExists
	am["exists"] = amExists

	crds := gin.H{
		"serviceMonitor":  checkCRD("servicemonitors.monitoring.coreos.com", "ServiceMonitor"),
		"polardbxMonitor": checkCRD("polardbxmonitors.polardbx.aliyun.com", "PolarDBXMonitor"),
	}
	prereq := gin.H{
		"generatedAt": time.Now().UTC().Format(time.RFC3339),
		"crds":        crds,
	}

	c.JSON(http.StatusOK, gin.H{
		"namespace":       ns,
		"namespaceExists": namespaceExists,
		"namespaceError":  namespaceError,
		"components": gin.H{
			"prometheus":   prom,
			"grafana":      graf,
			"alertmanager": am,
		},
		"prerequisites": prereq,
	})
}

// Uninstall removes lightweight markers (stub). In managed mode, a SystemTask will handle teardown later.
func Uninstall(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}
	ns := util.DefaultNamespace(c, "polardbx-operator-system")
	_ = cli.Delete(c.Request.Context(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "polardbx-monitoring-plan"}})
	c.JSON(http.StatusOK, gin.H{"message": "monitoring uninstall request accepted"})
}

// runIOPSBench launches a short-lived pod to estimate write IOPS on node ephemeral storage.
func runIOPSBench(ctx context.Context, cs kubernetes.Interface, ns string) map[string]interface{} {
	name := fmt.Sprintf("iops-bench-%d", rand.Intn(1_000_000))
	script := strings.Join([]string{
		// 不使用 set -e，避免 dd/awk 在某些 busybox 变体返回非零直接终止
		"export LC_ALL=C LANG=C",
		"cd /data",
		"COUNT=${COUNT:-50000}", // 50k ops @4k ≈ 200MB
		"BS=${BS:-4096}",
		"rm -f testfile || true",
		// 尝试使用更通用的 oflag=dsync；若失败则回退到 conv=fdatasync；最终回退裸 dd
		"OUT=$( (dd if=/dev/zero of=testfile bs=$BS count=$COUNT oflag=dsync 2>&1 || dd if=/dev/zero of=testfile bs=$BS count=$COUNT conv=fdatasync 2>&1 || dd if=/dev/zero of=testfile bs=$BS count=$COUNT 2>&1) | tail -1)",
		"SEC=$(echo \"$OUT\" | awk -F', ' '{print $(NF-1)}' | awk '{print $1}')",
		"if [ -z \"$SEC\" ]; then SEC=0; fi",
		"if [ \"$SEC\" = \"0\" ]; then IOPS=0; else IOPS=$(awk -v c=$COUNT -v s=$SEC 'BEGIN{printf(\"%d\", c/s)}'); fi",
		"echo {\\\"write\\\":{\\\"bs\\\":$BS,\\\"ops\\\":$COUNT,\\\"seconds\\\":$SEC,\\\"iops\\\":$IOPS}}",
	}, "; ")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Volumes:       []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
			Containers: []corev1.Container{{
				Name:         "bench",
				Image:        "busybox:1.36",
				Command:      []string{"sh", "-c", script},
				VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
			}},
		},
	}
	_, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return map[string]interface{}{"estimated": false, "ok": false, "message": "无权限或创建 Pod 失败: " + err.Error()}
	}
	defer func() { _ = cs.CoreV1().Pods(ns).Delete(context.Background(), name, metav1.DeleteOptions{}) }()
	// wait for completion
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		p, e := cs.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
		if e == nil {
			phase := p.Status.Phase
			if phase == corev1.PodSucceeded || phase == corev1.PodFailed {
				break
			}
		}
		time.Sleep(1 * time.Second)
	}
	// fetch logs
	logReq := cs.CoreV1().Pods(ns).GetLogs(name, &corev1.PodLogOptions{Container: "bench"})
	rc, e := logReq.Stream(ctx)
	if e != nil {
		return map[string]interface{}{"estimated": false, "ok": false, "message": "无法读取基准日志: " + e.Error()}
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	raw := strings.TrimSpace(string(b))
	// pick last non-empty line that looks like JSON, otherwise extract last {...}
	line := ""
	if raw != "" {
		parts := strings.Split(raw, "\n")
		for i := len(parts) - 1; i >= 0; i-- {
			cand := strings.TrimSpace(parts[i])
			if cand == "" {
				continue
			}
			if strings.HasPrefix(cand, "{") && strings.Contains(cand, "\"iops\"") {
				line = cand
				break
			}
		}
		if line == "" {
			l := strings.LastIndex(raw, "{")
			r := strings.LastIndex(raw, "}")
			if l >= 0 && r > l {
				line = strings.TrimSpace(raw[l : r+1])
			}
		}
	}
	// expected: {"write":{"bs":4096,"ops":50000,"seconds":1.23,"iops":40650}}
	res := map[string]interface{}{"estimated": true, "ok": false, "message": "未能解析输出"}
	if strings.HasPrefix(line, "{") {
		// very small parser
		ok := false
		var iops int64 = 0
		var seconds float64 = 0
		var ops int64 = 0
		var bs int64 = 0
		// parse by splitting (avoid bringing full json dep)
		get := func(key string) string {
			idx := strings.Index(line, key)
			if idx < 0 {
				return ""
			}
			s := line[idx+len(key):]
			s = strings.TrimLeft(s, ":")
			s = strings.TrimLeft(s, " ")
			i := strings.IndexAny(s, ",}")
			if i < 0 {
				return s
			}
			return s[:i]
		}
		if v := get("\"iops\""); v != "" {
			if n, err := strconv.ParseInt(strings.Trim(v, " \""), 10, 64); err == nil {
				iops = n
				ok = true
			}
		}
		if v := get("\"seconds\""); v != "" {
			if f, err := strconv.ParseFloat(strings.Trim(v, " \""), 64); err == nil {
				seconds = f
			}
		}
		if v := get("\"ops\""); v != "" {
			if n, err := strconv.ParseInt(strings.Trim(v, " \""), 10, 64); err == nil {
				ops = n
			}
		}
		if v := get("\"bs\""); v != "" {
			if n, err := strconv.ParseInt(strings.Trim(v, " \""), 10, 64); err == nil {
				bs = n
			}
		}
		// fallback: derive iops if missing but ops/seconds present
		if !ok && ops > 0 && seconds > 0 {
			calc := int64(float64(ops) / seconds)
			if calc > 0 {
				iops = calc
				ok = true
			}
		}
		res = map[string]interface{}{
			"estimated": ok,
			"ok":        ok && iops > 0,
			"iops":      iops,
			"seconds":   seconds,
			"ops":       ops,
			"bs":        bs,
		}
	} else if raw != "" {
		// Fallback: parse seconds directly from dd output and estimate IOPS using defaults
		re := regexp.MustCompile(`([0-9]+\.?[0-9]*)\s*s`)
		matches := re.FindAllStringSubmatch(raw, -1)
		if len(matches) > 0 {
			secStr := matches[len(matches)-1][1]
			if sec, err := strconv.ParseFloat(secStr, 64); err == nil && sec > 0 {
				const defaultOps = 50000
				const defaultBS = 4096
				estIOPS := int64(float64(defaultOps) / sec)
				res = map[string]interface{}{
					"estimated": true,
					"ok":        estIOPS > 0,
					"iops":      estIOPS,
					"seconds":   sec,
					"ops":       defaultOps,
					"bs":        defaultBS,
					"message":   "根据 dd 输出估算 (fallback)",
				}
			}
		}
	}
	return res
}

// Preflight performs lightweight checks before installing/using monitoring stack.
// It provides placeholders for IOPS and clock skew checks and a real AZ distribution summary.
func Preflight(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	// List nodes to infer AZ distribution
	nodes := corev1.NodeList{}
	_ = cli.List(c.Request.Context(), &nodes)

	zonesSet := map[string]struct{}{}
	nodesWithZone := 0
	for _, n := range nodes.Items {
		lbl := n.Labels
		zone := lbl["topology.kubernetes.io/zone"]
		if zone == "" {
			zone = lbl["failure-domain.beta.kubernetes.io/zone"]
		}
		if zone != "" {
			zonesSet[zone] = struct{}{}
			nodesWithZone++
		}
	}
	zones := make([]string, 0, len(zonesSet))
	for z := range zonesSet {
		zones = append(zones, z)
	}

	now := time.Now().UTC().Format(time.RFC3339)

	// IOPS benchmark: switched to placeholder per product decision
	// If later needed, re-enable runIOPSBench and replace the placeholder below.
	iops := gin.H{"estimated": false, "ok": false, "message": "占位：请在监控系统查看磁盘 IOPS"}

	c.JSON(http.StatusOK, gin.H{
		"timestamp": now,
		"az": gin.H{
			"zones":         zones,
			"count":         len(zones),
			"nodeCount":     len(nodes.Items),
			"nodesWithZone": nodesWithZone,
			"hasMultiple":   len(zones) >= 2,
		},
		"clock": gin.H{
			"controllerTime": now,
			"skewAssessed":   false,
			"ok":             true,
			"message":        "未校验集群节点时钟漂移（占位）",
		},
		"iops": iops,
	})
}

// BootstrapStatus returns the status of a monitoring bootstrap job
func BootstrapStatus(c *gin.Context) {
	cli, ok := util.K8sClientFromContext(c)
	if !ok {
		return
	}

	jobName := c.Query("jobName")
	// 安装 Job 位于安装器命名空间，默认 polardbx-operator-system
	namespace := util.DefaultNamespace(c, "polardbx-operator-system")

	if jobName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "jobName parameter is required"})
		return
	}

	// Get the job
	job := &batchv1.Job{}
	key := client.ObjectKey{Namespace: namespace, Name: jobName}
	if err := cli.Get(c.Request.Context(), key, job); err != nil {
		if apierrors.IsNotFound(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found", "jobName": jobName, "namespace": namespace})
			return
		}
		util.HandleK8sError(c, "failed to get job", err)
		return
	}

	// Determine job phase
	phase := "Running"
	var completionTime *metav1.Time
	var failureReason string

	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			phase = "Succeeded"
			completionTime = &condition.LastTransitionTime
			break
		}
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			phase = "Failed"
			failureReason = condition.Message
			completionTime = &condition.LastTransitionTime
			break
		}
	}

	// Check if job is still running by looking at active pods
	if phase == "Running" && job.Status.Active == 0 && job.Status.Succeeded == 0 && job.Status.Failed == 0 {
		phase = "Pending"
	}

	response := gin.H{
		"jobName":   jobName,
		"namespace": namespace,
		"phase":     phase,
		"startTime": job.Status.StartTime,
		"active":    job.Status.Active,
		"succeeded": job.Status.Succeeded,
		"failed":    job.Status.Failed,
	}

	if completionTime != nil {
		response["completionTime"] = completionTime
	}

	if failureReason != "" {
		response["failureReason"] = failureReason
	}

	// Add conditions for detailed status
	response["conditions"] = job.Status.Conditions

	c.JSON(http.StatusOK, response)
}

// BootstrapLogs returns the logs of a monitoring bootstrap job
func BootstrapLogs(c *gin.Context) {
	cs, ok := util.ClientsetFromContext(c)
	if !ok {
		return
	}

	jobName := c.Query("jobName")
	// 安装 Job 位于安装器命名空间，默认 polardbx-operator-system
	namespace := util.DefaultNamespace(c, "polardbx-operator-system")
	tailLines := int64(100) // Default to last 100 lines

	if jobName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "jobName parameter is required"})
		return
	}

	if tailParam := c.Query("tailLines"); tailParam != "" {
		if parsed, err := strconv.ParseInt(tailParam, 10, 64); err == nil && parsed > 0 {
			tailLines = parsed
		}
	}

	// List pods created by this job
	pods, err := cs.CoreV1().Pods(namespace).List(c.Request.Context(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	if err != nil {
		util.HandleK8sError(c, "failed to list job pods", err)
		return
	}

	if len(pods.Items) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "no pods found for job", "jobName": jobName})
		return
	}

	// Get logs from the first pod (usually there's only one for our jobs)
	pod := pods.Items[0]

	logOptions := &corev1.PodLogOptions{
		TailLines: &tailLines,
	}

	// If pod has multiple containers, get logs from the first one
	if len(pod.Spec.Containers) > 0 {
		logOptions.Container = pod.Spec.Containers[0].Name
	}

	logReq := cs.CoreV1().Pods(namespace).GetLogs(pod.Name, logOptions)
	rc, err := logReq.Stream(c.Request.Context())
	if err != nil {
		util.HandleK8sError(c, "failed to get pod logs", err)
		return
	}
	defer rc.Close()

	logs, err := io.ReadAll(rc)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read logs", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"jobName":   jobName,
		"namespace": namespace,
		"podName":   pod.Name,
		"logs":      string(logs),
		"tailLines": tailLines,
	})
}
