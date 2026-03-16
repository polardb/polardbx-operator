package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-dashboard-backend/pkg/k8s"
)

var ErrMetricsNotAvailable = errors.New("metrics-server not available")

type ClusterResourceUsage struct {
	Available   bool   `json:"available"`
	Source      string `json:"source"`
	TimestampMs int64  `json:"timestampMs"`
	Message     string `json:"message,omitempty"`

	PodsTotal         int `json:"podsTotal,omitempty"`
	PodsWithMetrics   int `json:"podsWithMetrics,omitempty"`
	ContainersMatched int `json:"containersMatched,omitempty"`

	CPU    CPUUsage    `json:"cpu"`
	Memory MemoryUsage `json:"memory"`
}

type CPUUsage struct {
	UsageCores    float64 `json:"usageCores"`
	RequestsCores float64 `json:"requestsCores"`
	LimitsCores   float64 `json:"limitsCores"`

	PctOfRequests *float64 `json:"pctOfRequests,omitempty"`
	PctOfLimits   *float64 `json:"pctOfLimits,omitempty"`
}

type MemoryUsage struct {
	UsageBytes    int64   `json:"usageBytes"`
	UsageGiB      float64 `json:"usageGiB"`
	RequestsBytes int64   `json:"requestsBytes"`
	LimitsBytes   int64   `json:"limitsBytes"`

	PctOfRequests *float64 `json:"pctOfRequests,omitempty"`
	PctOfLimits   *float64 `json:"pctOfLimits,omitempty"`
}

var metricsPodGVR = schema.GroupVersionResource{
	Group:    "metrics.k8s.io",
	Version:  "v1beta1",
	Resource: "pods",
}

func GetClusterResourceUsage(ctx context.Context, cli client.Client, dyn dynamic.Interface, namespace, clusterName string) (*ClusterResourceUsage, error) {
	now := time.Now()
	resp := &ClusterResourceUsage{
		Available:   true,
		Source:      "metrics-server",
		TimestampMs: now.UnixMilli(),
	}

	pods, err := k8s.ListPodsForPolarDBXClusterWithContext(ctx, cli, namespace, clusterName)
	if err != nil {
		return nil, err
	}
	resp.PodsTotal = len(pods)

	allowed := map[string]struct{}{
		"engine": {},
		"server": {},
	}

	type denom struct {
		cpuReqCores float64
		cpuLimCores float64
		memReqBytes int64
		memLimBytes int64
	}

	total := denom{}
	podContainerWanted := map[string]map[string]struct{}{}

	for _, pod := range pods {
		for _, ctn := range pod.Spec.Containers {
			if _, ok := allowed[ctn.Name]; !ok {
				continue
			}
			if _, ok := podContainerWanted[pod.Name]; !ok {
				podContainerWanted[pod.Name] = map[string]struct{}{}
			}
			podContainerWanted[pod.Name][ctn.Name] = struct{}{}

			if v, ok := ctn.Resources.Requests[corev1.ResourceCPU]; ok {
				total.cpuReqCores += float64(v.MilliValue()) / 1000.0
			}
			if v, ok := ctn.Resources.Limits[corev1.ResourceCPU]; ok {
				total.cpuLimCores += float64(v.MilliValue()) / 1000.0
			}
			if v, ok := ctn.Resources.Requests[corev1.ResourceMemory]; ok {
				total.memReqBytes += v.Value()
			}
			if v, ok := ctn.Resources.Limits[corev1.ResourceMemory]; ok {
				total.memLimBytes += v.Value()
			}
			resp.ContainersMatched++
		}
	}

	var cpuUsageCores float64
	var memUsageBytes int64
	podsWithMetrics := 0
	podsMissingMetrics := 0

	for _, pod := range pods {
		want := podContainerWanted[pod.Name]
		if len(want) == 0 {
			continue
		}
		u, err := dyn.Resource(metricsPodGVR).Namespace(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			if isMetricsNotAvailable(err) {
				return &ClusterResourceUsage{
					Available:   false,
					Source:      "none",
					TimestampMs: now.UnixMilli(),
					Message:     "metrics-server 未安装或不可用（metrics.k8s.io）",
					PodsTotal:   len(pods),
				}, ErrMetricsNotAvailable
			}
			if apierrors.IsNotFound(err) {
				podsMissingMetrics++
				continue
			}
			return nil, err
		}

		gotAny := false
		containers, ok := extractContainersUsage(u)
		if !ok {
			podsMissingMetrics++
			continue
		}
		for name, usage := range containers {
			if _, ok := want[name]; !ok {
				continue
			}
			cpuUsageCores += usage.cpuCores
			memUsageBytes += usage.memBytes
			gotAny = true
		}
		if gotAny {
			podsWithMetrics++
		} else {
			podsMissingMetrics++
		}
	}

	resp.PodsWithMetrics = podsWithMetrics
	resp.CPU = CPUUsage{
		UsageCores:    cpuUsageCores,
		RequestsCores: total.cpuReqCores,
		LimitsCores:   total.cpuLimCores,
	}
	resp.Memory = MemoryUsage{
		UsageBytes:    memUsageBytes,
		UsageGiB:      float64(memUsageBytes) / (1024.0 * 1024.0 * 1024.0),
		RequestsBytes: total.memReqBytes,
		LimitsBytes:   total.memLimBytes,
	}

	if total.cpuReqCores > 0 {
		v := (cpuUsageCores / total.cpuReqCores) * 100.0
		resp.CPU.PctOfRequests = &v
	}
	if total.cpuLimCores > 0 {
		v := (cpuUsageCores / total.cpuLimCores) * 100.0
		resp.CPU.PctOfLimits = &v
	}
	if total.memReqBytes > 0 {
		v := (float64(memUsageBytes) / float64(total.memReqBytes)) * 100.0
		resp.Memory.PctOfRequests = &v
	}
	if total.memLimBytes > 0 {
		v := (float64(memUsageBytes) / float64(total.memLimBytes)) * 100.0
		resp.Memory.PctOfLimits = &v
	}

	if podsMissingMetrics > 0 && podsWithMetrics > 0 {
		resp.Message = fmt.Sprintf("部分 Pod 暂无 metrics：%d/%d", podsMissingMetrics, resp.PodsTotal)
	}
	if podsWithMetrics == 0 && resp.PodsTotal > 0 && resp.Available {
		resp.Message = "metrics-server 已安装，但尚未采集到 Pod 指标（请稍后刷新）"
	}

	return resp, nil
}

type containerUsage struct {
	cpuCores float64
	memBytes int64
}

func extractContainersUsage(u *unstructured.Unstructured) (map[string]containerUsage, bool) {
	items, found, err := unstructured.NestedSlice(u.Object, "containers")
	if err != nil || !found {
		return nil, false
	}
	out := map[string]containerUsage{}
	for _, it := range items {
		m, ok := it.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		if name == "" {
			continue
		}
		usageMap, _ := m["usage"].(map[string]interface{})
		if len(usageMap) == 0 {
			continue
		}

		cpuStr, _ := usageMap["cpu"].(string)
		memStr, _ := usageMap["memory"].(string)

		var cpuCores float64
		var memBytes int64
		if cpuStr != "" {
			if q, err := resource.ParseQuantity(cpuStr); err == nil {
				cpuCores = float64(q.MilliValue()) / 1000.0
			}
		}
		if memStr != "" {
			if q, err := resource.ParseQuantity(memStr); err == nil {
				memBytes = q.Value()
			}
		}

		out[name] = containerUsage{cpuCores: cpuCores, memBytes: memBytes}
	}
	return out, true
}

func isMetricsNotAvailable(err error) bool {
	if err == nil {
		return false
	}
	if meta.IsNoMatchError(err) {
		return true
	}
	if apierrors.IsNotFound(err) {
		// metrics API absent often returns 404 with this message
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "requested resource") || strings.Contains(msg, "could not find") {
			return true
		}
	}
	// Some clients wrap errors; inspect string as last resort.
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "metrics.k8s.io") && strings.Contains(msg, "not found")
}
