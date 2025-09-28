package k8s

import (
	"context"
	"fmt"
	"io"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	kubescheme = runtime.NewScheme()
)

func init() {
	// 添加标准的Kubernetes API
	_ = scheme.AddToScheme(kubescheme)
	// 添加PolarDB-X自定义API
	_ = polardbxv1.AddToScheme(kubescheme)
}

// ClientProvider defines the interface for creating a Kubernetes client.
type ClientProvider interface {
	NewClientFromKubeconfig(kubeconfigData []byte) (client.Client, error)
}

// DefaultClientProvider is the default implementation of ClientProvider.
type DefaultClientProvider struct{}

// NewClientFromKubeconfig creates a new typed Kubernetes client from a kubeconfig byte slice.
func NewClientFromKubeconfig(kubeconfig []byte) (client.Client, error) {
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create rest config from kubeconfig: %w", err)
	}

	c, err := client.New(restConfig, client.Options{Scheme: kubescheme})
	if err != nil {
		return nil, fmt.Errorf("failed to create new client: %w", err)
	}
	return c, nil
}

// NewClientsFromKubeconfig creates a controller-runtime client and a standard clientset
// from the given kubeconfig bytes.
func NewClientsFromKubeconfig(kubeconfig []byte) (client.Client, kubernetes.Interface, error) {
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create rest config from kubeconfig: %w", err)
	}

	c, err := client.New(restConfig, client.Options{Scheme: kubescheme})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create new client: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create new clientset: %w", err)
	}

	return c, clientset, nil
}

// NewAllClientsFromKubeconfig creates a controller-runtime client, standard clientset, and dynamic client
// from the given kubeconfig bytes.
func NewAllClientsFromKubeconfig(kubeconfig []byte) (client.Client, kubernetes.Interface, dynamic.Interface, error) {
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create rest config from kubeconfig: %w", err)
	}

	c, err := client.New(restConfig, client.Options{Scheme: kubescheme})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create new client: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create new clientset: %w", err)
	}

	dynClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create new dynamic client: %w", err)
	}

	return c, clientset, dynClient, nil
}

// GetPodLogsWithContext retrieves logs from a specified pod with context.
func GetPodLogsWithContext(ctx context.Context, clientset kubernetes.Interface, namespace, podName, containerName string, tailLines int64) (string, error) {
	opts := &corev1.PodLogOptions{
		Container: containerName,
	}
	if tailLines > 0 {
		opts.TailLines = &tailLines
	}

	req := clientset.CoreV1().Pods(namespace).GetLogs(podName, opts)
	podLogs, err := req.Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("error in opening log stream: %w", err)
	}
	defer podLogs.Close()

	logBytes, err := io.ReadAll(podLogs)
	if err != nil {
		return "", fmt.Errorf("error in reading log stream: %w", err)
	}

	return string(logBytes), nil
}

// Deprecated: use GetPodLogsWithContext
func GetPodLogs(clientset kubernetes.Interface, namespace, podName, containerName string, tailLines int64) (string, error) {
	return GetPodLogsWithContext(context.Background(), clientset, namespace, podName, containerName, tailLines)
}

func ListPolarDBXClusters(c client.Client, namespace string) ([]polardbxv1.PolarDBXCluster, error) {
	var clusterList polardbxv1.PolarDBXClusterList
	if err := c.List(context.TODO(), &clusterList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return clusterList.Items, nil
}

func CreatePolarDBXCluster(c client.Client, namespace string, cluster *polardbxv1.PolarDBXCluster) (*polardbxv1.PolarDBXCluster, error) {
	if cluster.Namespace == "" {
		cluster.Namespace = namespace
	}
	err := c.Create(context.TODO(), cluster)
	return cluster, err
}

func GetPolarDBXCluster(c client.Client, namespace, name string) (*polardbxv1.PolarDBXCluster, error) {
	var cluster polardbxv1.PolarDBXCluster
	err := c.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, &cluster)
	if err != nil {
		return nil, err
	}
	return &cluster, nil
}

func DeletePolarDBXCluster(c client.Client, namespace, name string) error {
	var cluster polardbxv1.PolarDBXCluster
	cluster.Name = name
	cluster.Namespace = namespace
	return c.Delete(context.TODO(), &cluster)
}

func UpdatePolarDBXCluster(c client.Client, namespace string, cluster *polardbxv1.PolarDBXCluster) (*polardbxv1.PolarDBXCluster, error) {
	if cluster.Namespace == "" {
		cluster.Namespace = namespace
	}
	err := c.Update(context.TODO(), cluster)
	return cluster, err
}

func PatchPolarDBXCluster(c client.Client, namespace, name string, patchData []byte) (*polardbxv1.PolarDBXCluster, error) {
	cluster := &polardbxv1.PolarDBXCluster{}
	err := c.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, cluster)
	if err != nil {
		return nil, err
	}

	patch := client.RawPatch(types.MergePatchType, patchData)
	err = c.Patch(context.TODO(), cluster, patch)
	if err != nil {
		return nil, err
	}

	// 重新获取更新后的资源
	updatedCluster := &polardbxv1.PolarDBXCluster{}
	err = c.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, updatedCluster)
	if err != nil {
		return nil, err
	}

	return updatedCluster, nil
}

// Context-aware variants for cluster operations
func ListPolarDBXClustersWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.PolarDBXCluster, error) {
	var clusterList polardbxv1.PolarDBXClusterList
	if err := c.List(ctx, &clusterList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return clusterList.Items, nil
}

func CreatePolarDBXClusterWithContext(ctx context.Context, c client.Client, namespace string, cluster *polardbxv1.PolarDBXCluster) (*polardbxv1.PolarDBXCluster, error) {
	if cluster.Namespace == "" {
		cluster.Namespace = namespace
	}
	if err := c.Create(ctx, cluster); err != nil {
		return nil, err
	}
	return cluster, nil
}

func GetPolarDBXClusterWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.PolarDBXCluster, error) {
	var cluster polardbxv1.PolarDBXCluster
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &cluster); err != nil {
		return nil, err
	}
	return &cluster, nil
}

func DeletePolarDBXClusterWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	var cluster polardbxv1.PolarDBXCluster
	cluster.Name = name
	cluster.Namespace = namespace
	return c.Delete(ctx, &cluster)
}

func UpdatePolarDBXClusterWithContext(ctx context.Context, c client.Client, namespace string, cluster *polardbxv1.PolarDBXCluster) (*polardbxv1.PolarDBXCluster, error) {
	if cluster.Namespace == "" {
		cluster.Namespace = namespace
	}
	if err := c.Update(ctx, cluster); err != nil {
		return nil, err
	}
	return cluster, nil
}

func PatchPolarDBXClusterWithContext(ctx context.Context, c client.Client, namespace, name string, patchData []byte) (*polardbxv1.PolarDBXCluster, error) {
	cluster := &polardbxv1.PolarDBXCluster{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cluster); err != nil {
		return nil, err
	}
	patch := client.RawPatch(types.MergePatchType, patchData)
	if err := c.Patch(ctx, cluster, patch); err != nil {
		return nil, err
	}
	updated := &polardbxv1.PolarDBXCluster{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, updated); err != nil {
		return nil, err
	}
	return updated, nil
}

func ListPolarDBXBackups(c client.Client, namespace, clusterName string) ([]polardbxv1.PolarDBXBackup, error) {
	var backupList polardbxv1.PolarDBXBackupList
	opts := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels{
			"polardbx/name": clusterName,
		},
	}
	if err := c.List(context.TODO(), &backupList, opts...); err != nil {
		return nil, err
	}
	return backupList.Items, nil
}

func CreatePolarDBXBackup(c client.Client, namespace string, backup *polardbxv1.PolarDBXBackup) (*polardbxv1.PolarDBXBackup, error) {
	if backup.Namespace == "" {
		backup.Namespace = namespace
	}
	err := c.Create(context.TODO(), backup)
	return backup, err
}

// CreatePolarDBXBackupDryRun performs a dry-run to trigger webhook validation without persisting the object.
func CreatePolarDBXBackupDryRun(c client.Client, namespace string, backup *polardbxv1.PolarDBXBackup) (*polardbxv1.PolarDBXBackup, error) {
	if backup.Namespace == "" {
		backup.Namespace = namespace
	}
	opts := &client.CreateOptions{DryRun: []string{metav1.DryRunAll}}
	err := c.Create(context.TODO(), backup, opts)
	return backup, err
}

func DeletePolarDBXBackup(c client.Client, namespace, name string) error {
	backup := &polardbxv1.PolarDBXBackup{}
	backup.Name = name
	backup.Namespace = namespace
	return c.Delete(context.TODO(), backup)
}

func ListPolarDBXParameters(c client.Client, namespace string) ([]polardbxv1.PolarDBXParameter, error) {
	var paramList polardbxv1.PolarDBXParameterList
	if err := c.List(context.TODO(), &paramList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return paramList.Items, nil
}

func GetPolarDBXParameter(c client.Client, namespace, name string) (*polardbxv1.PolarDBXParameter, error) {
	var param polardbxv1.PolarDBXParameter
	err := c.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, &param)
	if err != nil {
		return nil, err
	}
	return &param, nil
}

func CreatePolarDBXParameter(c client.Client, namespace string, param *polardbxv1.PolarDBXParameter) (*polardbxv1.PolarDBXParameter, error) {
	if param.Namespace == "" {
		param.Namespace = namespace
	}
	err := c.Create(context.TODO(), param)
	return param, err
}

func UpdatePolarDBXParameter(c client.Client, namespace string, param *polardbxv1.PolarDBXParameter) (*polardbxv1.PolarDBXParameter, error) {
	if param.Namespace == "" {
		param.Namespace = namespace
	}
	err := c.Update(context.TODO(), param)
	return param, err
}

func DeletePolarDBXParameter(c client.Client, namespace, name string) error {
	var param polardbxv1.PolarDBXParameter
	param.Name = name
	param.Namespace = namespace
	return c.Delete(context.TODO(), &param)
}

func ListPodsForPolarDBXCluster(c client.Client, namespace, clusterName string) ([]corev1.Pod, error) {
	var podList corev1.PodList
	opts := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels{
			"polardbx/name": clusterName,
		},
	}
	if err := c.List(context.TODO(), &podList, opts...); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

// XStore Management Functions

func ListXStores(c client.Client, namespace string) ([]polardbxv1.XStore, error) {
	var xstoreList polardbxv1.XStoreList
	if err := c.List(context.TODO(), &xstoreList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return xstoreList.Items, nil
}

func CreateXStore(c client.Client, namespace string, xstore *polardbxv1.XStore) (*polardbxv1.XStore, error) {
	if xstore.Namespace == "" {
		xstore.Namespace = namespace
	}
	err := c.Create(context.TODO(), xstore)
	return xstore, err
}

func GetXStore(c client.Client, namespace, name string) (*polardbxv1.XStore, error) {
	var xstore polardbxv1.XStore
	err := c.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, &xstore)
	if err != nil {
		return nil, err
	}
	return &xstore, nil
}

func UpdateXStore(c client.Client, namespace string, xstore *polardbxv1.XStore) (*polardbxv1.XStore, error) {
	err := c.Update(context.TODO(), xstore)
	return xstore, err
}

func DeleteXStore(c client.Client, namespace, name string) error {
	xstore := &polardbxv1.XStore{}
	xstore.Name = name
	xstore.Namespace = namespace
	return c.Delete(context.TODO(), xstore)
}

// PolarDBXMonitor Management Functions

func ListPolarDBXMonitors(c client.Client, namespace string) ([]polardbxv1.PolarDBXMonitor, error) {
	var monitorList polardbxv1.PolarDBXMonitorList
	if err := c.List(context.TODO(), &monitorList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return monitorList.Items, nil
}

func CreatePolarDBXMonitor(c client.Client, namespace string, monitor *polardbxv1.PolarDBXMonitor) (*polardbxv1.PolarDBXMonitor, error) {
	if monitor.Namespace == "" {
		monitor.Namespace = namespace
	}
	err := c.Create(context.TODO(), monitor)
	return monitor, err
}

func GetPolarDBXMonitor(c client.Client, namespace, name string) (*polardbxv1.PolarDBXMonitor, error) {
	var monitor polardbxv1.PolarDBXMonitor
	err := c.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, &monitor)
	if err != nil {
		return nil, err
	}
	return &monitor, nil
}

func UpdatePolarDBXMonitor(c client.Client, namespace string, monitor *polardbxv1.PolarDBXMonitor) (*polardbxv1.PolarDBXMonitor, error) {
	err := c.Update(context.TODO(), monitor)
	return monitor, err
}

func DeletePolarDBXMonitor(c client.Client, namespace, name string) error {
	monitor := &polardbxv1.PolarDBXMonitor{}
	monitor.Name = name
	monitor.Namespace = namespace
	return c.Delete(context.TODO(), monitor)
}

// PolarDBXBackupSchedule Management Functions

func ListPolarDBXBackupSchedules(c client.Client, namespace string) ([]polardbxv1.PolarDBXBackupSchedule, error) {
	var scheduleList polardbxv1.PolarDBXBackupScheduleList
	if err := c.List(context.TODO(), &scheduleList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return scheduleList.Items, nil
}

func CreatePolarDBXBackupSchedule(c client.Client, namespace string, schedule *polardbxv1.PolarDBXBackupSchedule) (*polardbxv1.PolarDBXBackupSchedule, error) {
	if schedule.Namespace == "" {
		schedule.Namespace = namespace
	}
	err := c.Create(context.TODO(), schedule)
	return schedule, err
}

func GetPolarDBXBackupSchedule(c client.Client, namespace, name string) (*polardbxv1.PolarDBXBackupSchedule, error) {
	var schedule polardbxv1.PolarDBXBackupSchedule
	err := c.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, &schedule)
	if err != nil {
		return nil, err
	}
	return &schedule, nil
}

func UpdatePolarDBXBackupSchedule(c client.Client, namespace string, schedule *polardbxv1.PolarDBXBackupSchedule) (*polardbxv1.PolarDBXBackupSchedule, error) {
	err := c.Update(context.TODO(), schedule)
	return schedule, err
}

func DeletePolarDBXBackupSchedule(c client.Client, namespace, name string) error {
	schedule := &polardbxv1.PolarDBXBackupSchedule{}
	schedule.Name = name
	schedule.Namespace = namespace
	return c.Delete(context.TODO(), schedule)
}

// Context-aware variants for BackupSchedule
func ListPolarDBXBackupSchedulesWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.PolarDBXBackupSchedule, error) {
	var list polardbxv1.PolarDBXBackupScheduleList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func CreatePolarDBXBackupScheduleWithContext(ctx context.Context, c client.Client, namespace string, schedule *polardbxv1.PolarDBXBackupSchedule) (*polardbxv1.PolarDBXBackupSchedule, error) {
	if schedule.Namespace == "" {
		schedule.Namespace = namespace
	}
	if err := c.Create(ctx, schedule); err != nil {
		return nil, err
	}
	return schedule, nil
}

func GetPolarDBXBackupScheduleWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.PolarDBXBackupSchedule, error) {
	var item polardbxv1.PolarDBXBackupSchedule
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &item); err != nil {
		return nil, err
	}
	return &item, nil
}

func UpdatePolarDBXBackupScheduleWithContext(ctx context.Context, c client.Client, namespace string, schedule *polardbxv1.PolarDBXBackupSchedule) (*polardbxv1.PolarDBXBackupSchedule, error) {
	if err := c.Update(ctx, schedule); err != nil {
		return nil, err
	}
	return schedule, nil
}

func DeletePolarDBXBackupScheduleWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.PolarDBXBackupSchedule{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}

// PolarDBXParameter Template Management Functions

func ListPolarDBXParameterTemplates(c client.Client, namespace string) ([]polardbxv1.PolarDBXParameterTemplate, error) {
	var templateList polardbxv1.PolarDBXParameterTemplateList
	if err := c.List(context.TODO(), &templateList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return templateList.Items, nil
}

func CreatePolarDBXParameterTemplate(c client.Client, namespace string, template *polardbxv1.PolarDBXParameterTemplate) (*polardbxv1.PolarDBXParameterTemplate, error) {
	if template.Namespace == "" {
		template.Namespace = namespace
	}
	err := c.Create(context.TODO(), template)
	return template, err
}

func GetPolarDBXParameterTemplate(c client.Client, namespace, name string) (*polardbxv1.PolarDBXParameterTemplate, error) {
	var template polardbxv1.PolarDBXParameterTemplate
	err := c.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, &template)
	if err != nil {
		return nil, err
	}
	return &template, nil
}

func UpdatePolarDBXParameterTemplate(c client.Client, namespace string, template *polardbxv1.PolarDBXParameterTemplate) (*polardbxv1.PolarDBXParameterTemplate, error) {
	err := c.Update(context.TODO(), template)
	return template, err
}

func DeletePolarDBXParameterTemplate(c client.Client, namespace, name string) error {
	template := &polardbxv1.PolarDBXParameterTemplate{}
	template.Name = name
	template.Namespace = namespace
	return c.Delete(context.TODO(), template)
}

// SystemTask Management Functions

func ListSystemTasks(c client.Client, namespace string) ([]polardbxv1.SystemTask, error) {
	var taskList polardbxv1.SystemTaskList
	if err := c.List(context.TODO(), &taskList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return taskList.Items, nil
}

func CreateSystemTask(c client.Client, namespace string, task *polardbxv1.SystemTask) (*polardbxv1.SystemTask, error) {
	if task.Namespace == "" {
		task.Namespace = namespace
	}
	err := c.Create(context.TODO(), task)
	return task, err
}

func GetSystemTask(c client.Client, namespace, name string) (*polardbxv1.SystemTask, error) {
	var task polardbxv1.SystemTask
	err := c.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, &task)
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func UpdateSystemTask(c client.Client, namespace string, task *polardbxv1.SystemTask) (*polardbxv1.SystemTask, error) {
	err := c.Update(context.TODO(), task)
	return task, err
}

func DeleteSystemTask(c client.Client, namespace, name string) error {
	task := &polardbxv1.SystemTask{}
	task.Name = name
	task.Namespace = namespace
	return c.Delete(context.TODO(), task)
}

// PolarDBXLogCollector Management Functions

func ListPolarDBXLogCollectors(c client.Client, namespace string) ([]polardbxv1.PolarDBXLogCollector, error) {
	var collectorList polardbxv1.PolarDBXLogCollectorList
	if err := c.List(context.TODO(), &collectorList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return collectorList.Items, nil
}

func CreatePolarDBXLogCollector(c client.Client, namespace string, collector *polardbxv1.PolarDBXLogCollector) (*polardbxv1.PolarDBXLogCollector, error) {
	if collector.Namespace == "" {
		collector.Namespace = namespace
	}
	err := c.Create(context.TODO(), collector)
	return collector, err
}

func GetPolarDBXLogCollector(c client.Client, namespace, name string) (*polardbxv1.PolarDBXLogCollector, error) {
	var collector polardbxv1.PolarDBXLogCollector
	err := c.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, &collector)
	if err != nil {
		return nil, err
	}
	return &collector, nil
}

func UpdatePolarDBXLogCollector(c client.Client, namespace string, collector *polardbxv1.PolarDBXLogCollector) (*polardbxv1.PolarDBXLogCollector, error) {
	err := c.Update(context.TODO(), collector)
	return collector, err
}

func DeletePolarDBXLogCollector(c client.Client, namespace, name string) error {
	collector := &polardbxv1.PolarDBXLogCollector{}
	collector.Name = name
	collector.Namespace = namespace
	return c.Delete(context.TODO(), collector)
}

// PolarDBXBackupBinlog functions

func ListPolarDBXBackupBinlogs(c client.Client, namespace string) ([]polardbxv1.PolarDBXBackupBinlog, error) {
	var binlogList polardbxv1.PolarDBXBackupBinlogList
	if err := c.List(context.TODO(), &binlogList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return binlogList.Items, nil
}

func CreatePolarDBXBackupBinlog(c client.Client, namespace string, binlog *polardbxv1.PolarDBXBackupBinlog) (*polardbxv1.PolarDBXBackupBinlog, error) {
	if binlog.Namespace == "" {
		binlog.Namespace = namespace
	}
	err := c.Create(context.TODO(), binlog)
	return binlog, err
}

func GetPolarDBXBackupBinlog(c client.Client, namespace, name string) (*polardbxv1.PolarDBXBackupBinlog, error) {
	var binlog polardbxv1.PolarDBXBackupBinlog
	err := c.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, &binlog)
	if err != nil {
		return nil, err
	}
	return &binlog, nil
}

func UpdatePolarDBXBackupBinlog(c client.Client, namespace string, binlog *polardbxv1.PolarDBXBackupBinlog) (*polardbxv1.PolarDBXBackupBinlog, error) {
	err := c.Update(context.TODO(), binlog)
	return binlog, err
}

func DeletePolarDBXBackupBinlog(c client.Client, namespace, name string) error {
	binlog := &polardbxv1.PolarDBXBackupBinlog{}
	binlog.Name = name
	binlog.Namespace = namespace
	return c.Delete(context.TODO(), binlog)
}

// XStoreBackupBinlog (Standard Edition Incremental Log Backup)
func ListXStoreBackupBinlogs(c client.Client, namespace string) ([]polardbxv1.XStoreBackupBinlog, error) {
	var list polardbxv1.XStoreBackupBinlogList
	if err := c.List(context.TODO(), &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func CreateXStoreBackupBinlog(c client.Client, namespace string, obj *polardbxv1.XStoreBackupBinlog) (*polardbxv1.XStoreBackupBinlog, error) {
	if obj.Namespace == "" {
		obj.Namespace = namespace
	}
	if err := c.Create(context.TODO(), obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func GetXStoreBackupBinlog(c client.Client, namespace, name string) (*polardbxv1.XStoreBackupBinlog, error) {
	var out polardbxv1.XStoreBackupBinlog
	if err := c.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func UpdateXStoreBackupBinlog(c client.Client, namespace string, obj *polardbxv1.XStoreBackupBinlog) (*polardbxv1.XStoreBackupBinlog, error) {
	if obj.Namespace == "" {
		obj.Namespace = namespace
	}
	if err := c.Update(context.TODO(), obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func DeleteXStoreBackupBinlog(c client.Client, namespace, name string) error {
	obj := &polardbxv1.XStoreBackupBinlog{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(context.TODO(), obj)
}

// XStoreFollower Management Functions for DN Replica Fault Recovery

func ListXStoreFollowers(c client.Client, namespace string) ([]polardbxv1.XStoreFollower, error) {
	var followerList polardbxv1.XStoreFollowerList
	if err := c.List(context.TODO(), &followerList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return followerList.Items, nil
}

func CreateXStoreFollower(c client.Client, namespace string, follower *polardbxv1.XStoreFollower) (*polardbxv1.XStoreFollower, error) {
	if follower.Namespace == "" {
		follower.Namespace = namespace
	}
	err := c.Create(context.TODO(), follower)
	return follower, err
}

func GetXStoreFollower(c client.Client, namespace, name string) (*polardbxv1.XStoreFollower, error) {
	var follower polardbxv1.XStoreFollower
	err := c.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, &follower)
	if err != nil {
		return nil, err
	}
	return &follower, nil
}

func UpdateXStoreFollower(c client.Client, namespace string, follower *polardbxv1.XStoreFollower) (*polardbxv1.XStoreFollower, error) {
	err := c.Update(context.TODO(), follower)
	return follower, err
}

func DeleteXStoreFollower(c client.Client, namespace, name string) error {
	follower := &polardbxv1.XStoreFollower{}
	follower.Name = name
	follower.Namespace = namespace
	return c.Delete(context.TODO(), follower)
}

// XStoreBackup Management Functions for Storage-level Backup (完善备份模块)

func ListXStoreBackups(c client.Client, namespace string) ([]polardbxv1.XStoreBackup, error) {
	var backupList polardbxv1.XStoreBackupList
	if err := c.List(context.TODO(), &backupList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return backupList.Items, nil
}

func CreateXStoreBackup(c client.Client, namespace string, backup *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error) {
	if backup.Namespace == "" {
		backup.Namespace = namespace
	}
	err := c.Create(context.TODO(), backup)
	return backup, err
}

func GetXStoreBackup(c client.Client, namespace, name string) (*polardbxv1.XStoreBackup, error) {
	var backup polardbxv1.XStoreBackup
	err := c.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, &backup)
	if err != nil {
		return nil, err
	}
	return &backup, nil
}

func UpdateXStoreBackup(c client.Client, namespace string, backup *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error) {
	err := c.Update(context.TODO(), backup)
	return backup, err
}

func DeleteXStoreBackup(c client.Client, namespace, name string) error {
	backup := &polardbxv1.XStoreBackup{}
	backup.Name = name
	backup.Namespace = namespace
	return c.Delete(context.TODO(), backup)
}

// PolarDBXClusterKnobs operations for performance tuning

func GetClusterKnobsList(c client.Client) (*polardbxv1.PolarDBXClusterKnobsList, error) {
	knobsList := &polardbxv1.PolarDBXClusterKnobsList{}
	err := c.List(context.TODO(), knobsList)
	return knobsList, err
}

func CreateClusterKnobs(c client.Client, knobs *polardbxv1.PolarDBXClusterKnobs) (*polardbxv1.PolarDBXClusterKnobs, error) {
	err := c.Create(context.TODO(), knobs)
	return knobs, err
}

func GetClusterKnobs(c client.Client, namespace, name string) (*polardbxv1.PolarDBXClusterKnobs, error) {
	knobs := &polardbxv1.PolarDBXClusterKnobs{}
	err := c.Get(context.TODO(), client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, knobs)
	return knobs, err
}

func UpdateClusterKnobs(c client.Client, knobs *polardbxv1.PolarDBXClusterKnobs) (*polardbxv1.PolarDBXClusterKnobs, error) {
	err := c.Update(context.TODO(), knobs)
	return knobs, err
}

func DeleteClusterKnobs(c client.Client, namespace, name string) error {
	knobs := &polardbxv1.PolarDBXClusterKnobs{}
	knobs.Name = name
	knobs.Namespace = namespace
	return c.Delete(context.TODO(), knobs)
}

// Context-aware variants for Backup core CRUD
func ListPolarDBXBackupsWithContext(ctx context.Context, c client.Client, namespace, clusterName string) ([]polardbxv1.PolarDBXBackup, error) {
	var list polardbxv1.PolarDBXBackupList
	opts := []client.ListOption{client.InNamespace(namespace), client.MatchingLabels{"polardbx/name": clusterName}}
	if err := c.List(ctx, &list, opts...); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func CreatePolarDBXBackupWithContext(ctx context.Context, c client.Client, namespace string, backup *polardbxv1.PolarDBXBackup) (*polardbxv1.PolarDBXBackup, error) {
	if backup.Namespace == "" {
		backup.Namespace = namespace
	}
	if err := c.Create(ctx, backup); err != nil {
		return nil, err
	}
	return backup, nil
}

func CreatePolarDBXBackupDryRunWithContext(ctx context.Context, c client.Client, namespace string, backup *polardbxv1.PolarDBXBackup) (*polardbxv1.PolarDBXBackup, error) {
	if backup.Namespace == "" {
		backup.Namespace = namespace
	}
	opts := &client.CreateOptions{DryRun: []string{metav1.DryRunAll}}
	if err := c.Create(ctx, backup, opts); err != nil {
		return nil, err
	}
	return backup, nil
}

func DeletePolarDBXBackupWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.PolarDBXBackup{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}

// Context-aware variants for Parameters CRUD
func ListPolarDBXParametersWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.PolarDBXParameter, error) {
	var list polardbxv1.PolarDBXParameterList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}
func GetPolarDBXParameterWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.PolarDBXParameter, error) {
	var obj polardbxv1.PolarDBXParameter
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &obj); err != nil {
		return nil, err
	}
	return &obj, nil
}
func CreatePolarDBXParameterWithContext(ctx context.Context, c client.Client, namespace string, param *polardbxv1.PolarDBXParameter) (*polardbxv1.PolarDBXParameter, error) {
	if param.Namespace == "" {
		param.Namespace = namespace
	}
	if err := c.Create(ctx, param); err != nil {
		return nil, err
	}
	return param, nil
}
func UpdatePolarDBXParameterWithContext(ctx context.Context, c client.Client, namespace string, param *polardbxv1.PolarDBXParameter) (*polardbxv1.PolarDBXParameter, error) {
	if err := c.Update(ctx, param); err != nil {
		return nil, err
	}
	return param, nil
}
func DeletePolarDBXParameterWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.PolarDBXParameter{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}

// Context-aware variants for ParameterTemplates CRUD
func ListPolarDBXParameterTemplatesWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.PolarDBXParameterTemplate, error) {
	var list polardbxv1.PolarDBXParameterTemplateList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}
func GetPolarDBXParameterTemplateWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.PolarDBXParameterTemplate, error) {
	var obj polardbxv1.PolarDBXParameterTemplate
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &obj); err != nil {
		return nil, err
	}
	return &obj, nil
}
func CreatePolarDBXParameterTemplateWithContext(ctx context.Context, c client.Client, namespace string, tpl *polardbxv1.PolarDBXParameterTemplate) (*polardbxv1.PolarDBXParameterTemplate, error) {
	if tpl.Namespace == "" {
		tpl.Namespace = namespace
	}
	if err := c.Create(ctx, tpl); err != nil {
		return nil, err
	}
	return tpl, nil
}
func UpdatePolarDBXParameterTemplateWithContext(ctx context.Context, c client.Client, namespace string, tpl *polardbxv1.PolarDBXParameterTemplate) (*polardbxv1.PolarDBXParameterTemplate, error) {
	if err := c.Update(ctx, tpl); err != nil {
		return nil, err
	}
	return tpl, nil
}
func DeletePolarDBXParameterTemplateWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.PolarDBXParameterTemplate{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}

// Context-aware variants for SystemTask CRUD
func ListSystemTasksWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.SystemTask, error) {
	var list polardbxv1.SystemTaskList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}
func GetSystemTaskWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.SystemTask, error) {
	var obj polardbxv1.SystemTask
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &obj); err != nil {
		return nil, err
	}
	return &obj, nil
}
func CreateSystemTaskWithContext(ctx context.Context, c client.Client, namespace string, task *polardbxv1.SystemTask) (*polardbxv1.SystemTask, error) {
	if task.Namespace == "" {
		task.Namespace = namespace
	}
	if err := c.Create(ctx, task); err != nil {
		return nil, err
	}
	return task, nil
}
func UpdateSystemTaskWithContext(ctx context.Context, c client.Client, namespace string, task *polardbxv1.SystemTask) (*polardbxv1.SystemTask, error) {
	if err := c.Update(ctx, task); err != nil {
		return nil, err
	}
	return task, nil
}
func DeleteSystemTaskWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.SystemTask{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}

// Context-aware variants for LogCollector CRUD
func ListPolarDBXLogCollectorsWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.PolarDBXLogCollector, error) {
	var list polardbxv1.PolarDBXLogCollectorList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}
func CreatePolarDBXLogCollectorWithContext(ctx context.Context, c client.Client, namespace string, obj *polardbxv1.PolarDBXLogCollector) (*polardbxv1.PolarDBXLogCollector, error) {
	if obj.Namespace == "" {
		obj.Namespace = namespace
	}
	if err := c.Create(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}
func GetPolarDBXLogCollectorWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.PolarDBXLogCollector, error) {
	var out polardbxv1.PolarDBXLogCollector
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
func UpdatePolarDBXLogCollectorWithContext(ctx context.Context, c client.Client, namespace string, obj *polardbxv1.PolarDBXLogCollector) (*polardbxv1.PolarDBXLogCollector, error) {
	if err := c.Update(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}
func DeletePolarDBXLogCollectorWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	inst := &polardbxv1.PolarDBXLogCollector{}
	inst.Name = name
	inst.Namespace = namespace
	return c.Delete(ctx, inst)
}

// Context-aware variants for BackupBinlog CRUD
func ListPolarDBXBackupBinlogsWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.PolarDBXBackupBinlog, error) {
	var list polardbxv1.PolarDBXBackupBinlogList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}
func CreatePolarDBXBackupBinlogWithContext(ctx context.Context, c client.Client, namespace string, obj *polardbxv1.PolarDBXBackupBinlog) (*polardbxv1.PolarDBXBackupBinlog, error) {
	if obj.Namespace == "" {
		obj.Namespace = namespace
	}
	if err := c.Create(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}
func GetPolarDBXBackupBinlogWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.PolarDBXBackupBinlog, error) {
	var out polardbxv1.PolarDBXBackupBinlog
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
func UpdatePolarDBXBackupBinlogWithContext(ctx context.Context, c client.Client, namespace string, obj *polardbxv1.PolarDBXBackupBinlog) (*polardbxv1.PolarDBXBackupBinlog, error) {
	if err := c.Update(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// Context-aware variants for XStore CRUD
func ListXStoresWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.XStore, error) {
	var list polardbxv1.XStoreList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}
func CreateXStoreWithContext(ctx context.Context, c client.Client, namespace string, x *polardbxv1.XStore) (*polardbxv1.XStore, error) {
	if x.Namespace == "" {
		x.Namespace = namespace
	}
	if err := c.Create(ctx, x); err != nil {
		return nil, err
	}
	return x, nil
}
func GetXStoreWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.XStore, error) {
	var out polardbxv1.XStore
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
func UpdateXStoreWithContext(ctx context.Context, c client.Client, namespace string, x *polardbxv1.XStore) (*polardbxv1.XStore, error) {
	if err := c.Update(ctx, x); err != nil {
		return nil, err
	}
	return x, nil
}
func DeleteXStoreWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.XStore{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}

// Context-aware variants for XStoreBackup CRUD
func ListXStoreBackupsWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.XStoreBackup, error) {
	var list polardbxv1.XStoreBackupList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}
func CreateXStoreBackupWithContext(ctx context.Context, c client.Client, namespace string, obj *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error) {
	if obj.Namespace == "" {
		obj.Namespace = namespace
	}
	if err := c.Create(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}
func GetXStoreBackupWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.XStoreBackup, error) {
	var out polardbxv1.XStoreBackup
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
func UpdateXStoreBackupWithContext(ctx context.Context, c client.Client, namespace string, obj *polardbxv1.XStoreBackup) (*polardbxv1.XStoreBackup, error) {
	if err := c.Update(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}
func DeleteXStoreBackupWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.XStoreBackup{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}

// Context-aware variants for PolarDBXMonitor CRUD
func ListPolarDBXMonitorsWithContext(ctx context.Context, c client.Client, namespace string) ([]polardbxv1.PolarDBXMonitor, error) {
	var list polardbxv1.PolarDBXMonitorList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}
func CreatePolarDBXMonitorWithContext(ctx context.Context, c client.Client, namespace string, m *polardbxv1.PolarDBXMonitor) (*polardbxv1.PolarDBXMonitor, error) {
	if m.Namespace == "" {
		m.Namespace = namespace
	}
	if err := c.Create(ctx, m); err != nil {
		return nil, err
	}
	return m, nil
}
func GetPolarDBXMonitorWithContext(ctx context.Context, c client.Client, namespace, name string) (*polardbxv1.PolarDBXMonitor, error) {
	var out polardbxv1.PolarDBXMonitor
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
func UpdatePolarDBXMonitorWithContext(ctx context.Context, c client.Client, namespace string, m *polardbxv1.PolarDBXMonitor) (*polardbxv1.PolarDBXMonitor, error) {
	if err := c.Update(ctx, m); err != nil {
		return nil, err
	}
	return m, nil
}
func DeletePolarDBXMonitorWithContext(ctx context.Context, c client.Client, namespace, name string) error {
	obj := &polardbxv1.PolarDBXMonitor{}
	obj.Name = name
	obj.Namespace = namespace
	return c.Delete(ctx, obj)
}
