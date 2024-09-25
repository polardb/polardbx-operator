package importbackupset

import (
	"fmt"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/factory"
	"github.com/alibaba/polardbx-operator/pkg/util/json"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
	"k8s.io/utils/ptr"
	"strconv"
	"time"
)

func buildBackupSet(backupSet *BackupSet) factory.MetadataBackup {
	backupSetName := backupSet.BackupSetId
	polardbxClusterMetadata := factory.PolarDBXClusterMetadata{
		Name: backupSet.InsName,
		UID:  types.UID(backupSet.BackupSetId),
		Spec: &polardbxv1.PolarDBXClusterSpec{
			ProtocolVersion: intstr.FromString("8.0"),
			Topology: polardbx.Topology{
				Nodes: polardbx.TopologyNodes{
					CN: polardbx.TopologyNodeCN{
						Replicas: pointer.Int32(2),
						Template: polardbx.CNTemplate{
							Image: backupSet.CNImage,
						},
					},
					DN: polardbx.TopologyNodeDN{
						Replicas: int32(len(backupSet.DnBackupSets)),
						Template: polardbx.XStoreTemplate{
							Image: backupSet.DNImage,
						},
					},
					CDC: &polardbx.TopologyNodeCDC{
						Replicas: intstr.FromInt32(2),
						Template: polardbx.CDCTemplate{
							Image: backupSet.CDCImage,
						},
					},
				},
			},
		},
	}
	backupSet.PxcUid = string(polardbxClusterMetadata.UID)
	location, err := time.LoadLocation("UTC")
	if err != nil {
		panic(err)
	}
	startTime, err := time.ParseInLocation("2006-01-02T15:04:05Z", backupSet.BackupStartTime, location)
	if err != nil {
		panic(err)
	}
	BackupRootPath := "polardbx-backup" + "/" + backupSet.InsName + "/" + backupSetName + "-" + startTime.Format("20060102150405")
	backupSet.BackupRootPath = BackupRootPath
	xstoreMetadataList := make([]factory.XstoreMetadata, 0)
	for i, dnBackupSet := range backupSet.DnBackupSets {
		uid := types.UID(strconv.FormatInt(int64(dnBackupSet.HostInstanceId), 10))
		targetPod := dnBackupSet.DnName + "-cand-0"
		lastCommitIndex, err := strconv.ParseInt(dnBackupSet.CommitIndex, 10, 63)
		if err != nil {
			panic(fmt.Errorf("failed to parse commitIndex, dnName %s , commitIndex string %s, err %+v", dnBackupSet.DnName, dnBackupSet.CommitIndex, err))
		}
		xstoreMetadataList = append(xstoreMetadataList, factory.XstoreMetadata{
			Name:            dnBackupSet.DnName,
			UID:             uid,
			BackupName:      backupSetName + "-dn-" + strconv.FormatInt(int64(i), 10),
			TargetPod:       targetPod,
			LastCommitIndex: lastCommitIndex,
		})
		backupSet.DnBackupSets[i].XStoreUid = string(uid)
		backupSet.DnBackupSets[i].TargetPod = targetPod
	}
	uid := types.UID(strconv.FormatInt(int64(backupSet.GmsBackupSet.HostInstanceId), 10))
	targetPod := backupSet.GmsBackupSet.DnName + "-cand-0"
	lastCommitIndex, err := strconv.ParseInt(backupSet.GmsBackupSet.CommitIndex, 10, 63)
	if err != nil {
		panic(fmt.Errorf("failed to parse commitIndex, dnName %s , commitIndex string %s, err %+v", backupSet.GmsBackupSet.DnName, backupSet.GmsBackupSet.CommitIndex, err))
	}
	xstoreMetadataList = append(xstoreMetadataList, factory.XstoreMetadata{
		Name:            backupSet.GmsBackupSet.DnName,
		UID:             uid,
		BackupName:      backupSetName + "-gms-0",
		TargetPod:       targetPod,
		LastCommitIndex: lastCommitIndex,
	})
	backupSet.GmsBackupSet.XStoreUid = string(uid)
	backupSet.GmsBackupSet.TargetPod = targetPod
	endTime, err := time.ParseInLocation("2006-01-02T15:04:05Z", backupSet.BackupEndTime, location)
	if err != nil {
		panic(err)
	}
	return factory.MetadataBackup{
		PolarDBXClusterMetadata:    polardbxClusterMetadata,
		XstoreMetadataList:         xstoreMetadataList,
		BackupSetName:              backupSetName,
		BackupRootPath:             BackupRootPath,
		StartTime:                  ptr.To(metav1.NewTime(startTime)),
		EndTime:                    ptr.To(metav1.NewTime(endTime)),
		LatestRecoverableTimestamp: ptr.To(metav1.NewTime(endTime)),
	}
}

func GenerateBackupSetUploadObjs(backupSet *BackupSet) []UploadObj {
	uploadObjs := make([]UploadObj, 0)
	//build upload metadata upload objs
	metadataBackup := buildBackupSet(backupSet)
	filepathPrefix := backupSet.BackupRootPath
	uploadObjs = append(uploadObjs, UploadObj{
		Content:  []byte(json.Convert2JsonString(metadataBackup)),
		Filepath: filepathPrefix + "/" + "metadata",
	})
	dnBackupSets := make([]DnBackupSet, 0, len(backupSet.DnBackupSets)+1)
	dnBackupSets = append(dnBackupSets, backupSet.DnBackupSets...)
	dnBackupSets = append(dnBackupSets, backupSet.GmsBackupSet)
	//build fullbackup upload objs
	for _, dnBackupSet := range dnBackupSets {
		downloadUrl := GetDownloadUrl(dnBackupSet.InnerFullDownloadUrl, dnBackupSet.PubFullDownloadUrl)
		if downloadUrl == "" {
			panic(fmt.Sprintf("failed to get download url of backupset dnName %s", dnBackupSet.DnName))
		}
		uploadObjs = append(uploadObjs, UploadObj{
			DownloadURL: downloadUrl,
			Filepath:    filepathPrefix + "/" + "fullbackup" + "/" + dnBackupSet.DnName + ".xbstream",
		})
	}
	return uploadObjs
}
