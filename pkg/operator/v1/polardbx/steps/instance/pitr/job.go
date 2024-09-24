package pitr

import (
	"fmt"
	polarxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/backupbinlog"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms/security"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	"github.com/alibaba/polardbx-operator/pkg/pitr"
	"github.com/pkg/errors"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
)

const (
	HttpServerPort = 13333
	PortName       = "server"
	ContainerName  = "job"
)

func NewJobName(name string) string {
	result := fmt.Sprintf("sys-pitr-%s", name)
	if len(result) > 63 {
		result = security.MustSha1Hash(result)
	}
	return result
}

func CreateTaskConfig(rc *polardbxv1reconcile.Context, pxcBackup *polarxv1.PolarDBXBackup) (*pitr.TaskConfig, error) {
	polardbx := rc.MustGetPolarDBX()
	var namespace string
	var sinkName string
	var sinkType string
	var binlogChecksum string
	var heartbeatSName string
	binlogSource := polardbx.Spec.Restore.BinlogSource
	if binlogSource != nil {
		namespace = binlogSource.Namespace
		if binlogSource.StorageProvider != nil {
			sinkType = string(binlogSource.StorageProvider.StorageName)
			sinkName = binlogSource.StorageProvider.Sink
		}
		binlogChecksum = binlogSource.Checksum
		heartbeatSName = binlogSource.HeartbeatSName
	}
	if namespace == "" {
		namespace = polardbx.Namespace
	}
	// try get from PolarDBXBackupBinlog object
	var backupBinlogs polarxv1.PolarDBXBackupBinlogList
	err := rc.Client().List(rc.Context(), &backupBinlogs, client.InNamespace(namespace), client.MatchingLabels{
		meta.LabelName: pxcBackup.Spec.Cluster.Name,
		meta.LabelUid:  string(pxcBackup.Spec.Cluster.UID),
	})
	if err != nil {
		return nil, err
	}
	//pxcBackup.Spec.Cluster.Name
	if sinkName == "" {
		if len(backupBinlogs.Items) > 0 {
			backupBinlog := backupBinlogs.Items[0]
			sinkName = backupBinlog.Spec.StorageProvider.Sink
			sinkType = string(backupBinlog.Spec.StorageProvider.StorageName)
		}
	}
	if sinkName == "" {
		if polardbx.Spec.Restore.StorageProvider != nil {
			sinkName = polardbx.Spec.Restore.StorageProvider.Sink
			sinkType = string(polardbx.Spec.Restore.StorageProvider.StorageName)
		}
	}
	if binlogChecksum == "" {
		if len(backupBinlogs.Items) > 0 {
			binlogChecksum = backupBinlogs.Items[0].Spec.BinlogChecksum
		}
	}
	if binlogChecksum == "" {
		binlogChecksum = "crc32"
	}

	var xstoreBackups polarxv1.XStoreBackupList
	err = rc.Client().List(rc.Context(), &xstoreBackups, client.InNamespace(rc.Namespace()), client.MatchingLabels{
		meta.LabelName:      pxcBackup.Spec.Cluster.Name,
		meta.LabelTopBackup: pxcBackup.Name,
	})
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to get xstore backup list, polardbx name = %s, backup name = %s", pxcBackup.Spec.Cluster.Name, pxcBackup.Name))
	}

	if heartbeatSName == "" {
		heartbeatSName = backupbinlog.Sname
	}

	xstoreConfigs := generateXStoreTaskConfigs(xstoreBackups.Items, rc.Config().Backup().GetRestorePodSuffix(), heartbeatSName)

	var currentXStores polarxv1.XStoreList
	err = rc.Client().List(rc.Context(), &currentXStores, client.InNamespace(namespace), client.MatchingLabels{
		meta.LabelName: pxcBackup.Spec.Cluster.Name,
	})
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to list xstores of namespace = %s , polardbx = %s", pxcBackup.Namespace, pxcBackup.Spec.Cluster.Name))
	}
	for _, xstore := range currentXStores.Items {
		xStoreConfig, ok := xstoreConfigs[xstore.Name]
		if ok && xStoreConfig.XStoreUid == string(xstore.UID) {
			for podName, podVolume := range xstore.Status.BoundVolumes {
				pod, ok := xStoreConfig.Pods[podName]
				if ok {
					pod.Host = podVolume.Host
					pod.LogDir = filepath.Join(podVolume.HostPath, "log")
					if xstore.Spec.Config.Dynamic.LogDataSeparation {
						pod.LogDir = filepath.Join(podVolume.LogHostPath, "log")
					}
				}
			}
		}
	}

	taskConfig := pitr.TaskConfig{
		Namespace:      namespace,
		PxcName:        pxcBackup.Spec.Cluster.Name,
		PxcUid:         string(pxcBackup.Spec.Cluster.UID),
		SinkName:       sinkName,
		SinkType:       sinkType,
		HpfsEndpoint:   rc.Config().Store().HostPathFileServiceEndpoint(),
		FsEndpoint:     rc.Config().Store().FilestreamServiceEndpoint(),
		BinlogChecksum: binlogChecksum,
		HttpServerPort: HttpServerPort,
		XStores:        xstoreConfigs,
		Timestamp:      uint64(rc.MustParseRestoreTime().Unix()),
	}
	return &taskConfig, nil
}

func generateXStoreTaskConfigs(dnBackups []polarxv1.XStoreBackup, restorePodSuffix string, heartbeatSname string) map[string]*pitr.XStoreConfig {
	dnConfigs := make(map[string]*pitr.XStoreConfig, len(dnBackups))
	for _, backup := range dnBackups {
		xstoreName := backup.Spec.XStore.Name
		globalConsistent := true
		if strings.HasSuffix(xstoreName, "gms") || strings.HasPrefix(xstoreName, "pxc-xdb-m") {
			globalConsistent = false
		}
		podName := backup.Status.TargetPod
		if podName == "" {
			podName = backup.Spec.XStore.Name + restorePodSuffix
		}
		dnConfig := pitr.XStoreConfig{
			GlobalConsistent:    globalConsistent,
			XStoreName:          xstoreName,
			XStoreUid:           string(backup.Spec.XStore.UID),
			BackupSetStartIndex: uint64(backup.Status.CommitIndex),
			HeartbeatSname:      heartbeatSname,
			Pods: map[string]*pitr.PodConfig{
				podName: {
					PodName: podName,
				},
			},
		}
		dnConfigs[dnConfig.XStoreName] = &dnConfig
	}
	return dnConfigs
}

func newJobLabels(polardbx *polarxv1.PolarDBXCluster) map[string]string {
	return map[string]string{
		meta.LabelName:    polardbx.Name,
		meta.LabelJobType: string(meta.PitrPrepareBinlogJobType),
	}
}

func GetJobAndPod(rc *polardbxv1reconcile.Context) (*batchv1.Job, *corev1.Pod, error) {
	polardbx := rc.MustGetPolarDBX()
	var job batchv1.Job
	jobName := NewJobName(polardbx.Name)
	err := rc.Client().Get(rc.Context(), types.NamespacedName{Namespace: rc.Namespace(), Name: jobName}, &job)
	if err != nil {
		return nil, nil, errors.Wrap(err, fmt.Sprintf("failed to get job, jobName = %s", jobName))
	}
	var podList corev1.PodList
	labels := newJobLabels(polardbx)
	err = rc.Client().List(rc.Context(), &podList, client.InNamespace(rc.Namespace()), client.MatchingLabels(labels))
	if err != nil {
		return nil, nil, errors.Wrap(err, fmt.Sprintf("failed to get job pod jobName = %s", jobName))
	}
	if len(podList.Items) > 0 {
		pod := podList.Items[0]
		err := k8shelper.CheckControllerReference(&pod, &job)
		if err != nil {
			return nil, nil, errors.Wrap(err, fmt.Sprintf("failed to check pod owner jobName = %s", jobName))
		}
		return &job, &pod, nil
	}
	return &job, nil, errors.New("job pod is not found")
}

func CreatePrepareBinlogJob(rc *polardbxv1reconcile.Context, config *pitr.TaskConfig) *batchv1.Job {
	polardbx := rc.MustGetPolarDBX()
	pitrAnnotationConfig := pitr.MustMarshalJSON(config)
	labels := newJobLabels(polardbx)
	var jobTTL int32 = 30
	var jobParallelism int32 = 1
	var completions int32 = 1
	var backOffLimit int32 = 0
	volumes := []corev1.Volume{
		{
			Name: "spill",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				DownwardAPI: &corev1.DownwardAPIVolumeSource{
					Items: []corev1.DownwardAPIVolumeFile{
						{
							Path: "config.json",
							FieldRef: &corev1.ObjectFieldSelector{
								FieldPath: fmt.Sprintf("metadata.annotations['%s']", meta.AnnotationPitrConfig),
							},
						},
					},
				},
			},
		},
	}
	mountMode := corev1.MountPropagationHostToContainer
	container := corev1.Container{
		Name:    ContainerName,
		Image:   rc.Config().Images().DefaultJobImage(),
		Command: []string{"/polardbx-job"},
		Args:    []string{"-job-type=" + string(meta.PitrPrepareBinlogJobType)},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "config",
				MountPath: "/workspace/conf/",
				ReadOnly:  true,
			},
			{
				Name:             "spill",
				MountPath:        "/workspace/spill",
				MountPropagation: &mountMode,
			},
		},
		Ports: []corev1.ContainerPort{
			{
				Name:          PortName,
				ContainerPort: HttpServerPort,
			},
		},
		ReadinessProbe: &corev1.Probe{
			InitialDelaySeconds: 20,
			TimeoutSeconds:      5,
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Port: intstr.FromString(PortName),
					Path: "/status",
				},
			},
		},
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("200m"),
				corev1.ResourceMemory: resource.MustParse("100Mi"),
			},
		},
	}
	job := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NewJobName(polardbx.Name),
			Namespace: rc.Namespace(),
			Annotations: map[string]string{
				meta.AnnotationPitrConfig: pitrAnnotationConfig,
			},
			Labels: labels,
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: &jobTTL,
			Parallelism:             &jobParallelism,
			Completions:             &completions,
			BackoffLimit:            &backOffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						meta.AnnotationPitrConfig: pitrAnnotationConfig,
					},
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Volumes:          volumes,
					Containers:       []corev1.Container{container},
					RestartPolicy:    corev1.RestartPolicyNever,
					ImagePullSecrets: polardbx.Spec.Topology.Nodes.DN.Template.ImagePullSecrets,
				},
			},
		},
	}
	return &job
}
