package instance

import (
	"fmt"
	polarxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/backupbinlog"
	k8shelper "github.com/alibaba/polardbx-operator/pkg/k8s/helper"
	"github.com/alibaba/polardbx-operator/pkg/meta/core/gms/security"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"github.com/alibaba/polardbx-operator/pkg/pitr"
	"github.com/pkg/errors"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	HttpServerPort = 13333
	PortName       = "server"
	ContainerName  = "job"
)

func NewJobName(name string) string {
	result := fmt.Sprintf("xstore-pitr-%s", name)
	if len(result) > 63 {
		result = security.MustSha1Hash(result)
	}
	return result
}

func CreateTaskConfig(rc *xstorev1reconcile.Context, xstoreBackup *polarxv1.XStoreBackup) (*pitr.TaskConfig, error) {
	xstore := rc.MustGetXStore()
	var namespace string
	var sinkName string
	var sinkType string
	var binlogChecksum string
	binlogSource := xstore.Spec.Restore.BinlogSource
	if binlogSource != nil {
		namespace = binlogSource.Namespace
		if binlogSource.StorageProvider != nil {
			sinkType = string(binlogSource.StorageProvider.StorageName)
			sinkName = binlogSource.StorageProvider.Sink
		}
		binlogChecksum = binlogSource.Checksum
	}
	if namespace == "" {
		namespace = xstore.Namespace
	}

	// try get from PolarDBXBackupBinlog object
	var backupBinlogs polarxv1.XStoreBackupBinlogList

	err := rc.Client().List(rc.Context(), &backupBinlogs, client.InNamespace(namespace), client.MatchingLabels{
		meta.LabelName: xstoreBackup.Spec.XStore.Name,
		meta.LabelUid:  string(xstoreBackup.Spec.XStore.UID),
	})
	if err != nil {
		return nil, err
	}
	if sinkName == "" {
		if len(backupBinlogs.Items) > 0 {
			backupBinlog := backupBinlogs.Items[0]
			sinkName = backupBinlog.Spec.StorageProvider.Sink
			sinkType = string(backupBinlog.Spec.StorageProvider.StorageName)
		}
	}
	if sinkName == "" {
		if xstore.Spec.Restore.StorageProvider != nil {
			sinkName = xstore.Spec.Restore.StorageProvider.Sink
			sinkType = string(xstore.Spec.Restore.StorageProvider.StorageName)
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
	podName := xstoreBackup.Status.TargetPod
	if podName == "" {
		podName = xstoreBackup.Spec.XStore.Name + "-cand-0"
	}

	dnConfigs := make(map[string]*pitr.XStoreConfig, 1)
	dnConfig := pitr.XStoreConfig{
		GlobalConsistent:    true,
		XStoreName:          xstoreBackup.Spec.XStore.Name,
		XStoreUid:           string(xstoreBackup.Spec.XStore.UID),
		BackupSetStartIndex: uint64(xstoreBackup.Status.CommitIndex),
		HeartbeatSname:      backupbinlog.Sname,
		Pods: map[string]*pitr.PodConfig{
			podName: {
				PodName: podName,
			},
		},
	}
	dnConfigs[dnConfig.XStoreName] = &dnConfig

	taskConfig := pitr.TaskConfig{
		Namespace:      namespace,
		PxcName:        xstoreBackup.Spec.XStore.Name,
		PxcUid:         string(xstoreBackup.Spec.XStore.UID),
		SinkName:       sinkName,
		SinkType:       sinkType,
		HpfsEndpoint:   rc.Config().Store().HostPathFileServiceEndpoint(),
		FsEndpoint:     rc.Config().Store().FilestreamServiceEndpoint(),
		BinlogChecksum: binlogChecksum,
		XStores:        dnConfigs,
		HttpServerPort: HttpServerPort,
		Timestamp:      uint64(rc.MustParseRestoreTime().Unix()),
	}
	return &taskConfig, nil
}

func newJobLabels(xstore *polarxv1.XStore) map[string]string {
	return map[string]string{
		meta.LabelName:    xstore.Name,
		meta.LabelJobType: string(meta.PitrPrepareBinlogForXStoreJobType),
	}
}

func CreatePrepareBinlogJob(rc *xstorev1reconcile.Context, config *pitr.TaskConfig) *batchv1.Job {
	xstore := rc.MustGetXStore()
	pitrAnnotationConfig := pitr.MustMarshalJSON(config)
	labels := newJobLabels(xstore)
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
		Args:    []string{"-job-type=" + string(meta.PitrPrepareBinlogForXStoreJobType)},
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
			Name:      NewJobName(xstore.Name),
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
					ImagePullSecrets: xstore.Spec.Topology.Template.Spec.ImagePullSecrets,
				},
			},
		},
	}
	return &job
}

func GetJobAndPod(rc *xstorev1reconcile.Context) (*batchv1.Job, *corev1.Pod, error) {
	xstore := rc.MustGetXStore()
	var job batchv1.Job
	jobName := NewJobName(xstore.Name)
	err := rc.Client().Get(rc.Context(), types.NamespacedName{Namespace: rc.Namespace(), Name: jobName}, &job)
	if err != nil {
		return nil, nil, errors.Wrap(err, fmt.Sprintf("failed to get job, jobName = %s", jobName))
	}
	var podList corev1.PodList
	labels := newJobLabels(xstore)
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
