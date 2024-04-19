package factory

import (
	"errors"
	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbxv1polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/convention"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/meta"
	"github.com/alibaba/polardbx-operator/pkg/util/name"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type PolarDBXClusterMetadata struct {
	// Name records name of original pxc
	Name string `json:"name,omitempty"`

	// UID records uid of original pxc
	UID types.UID `json:"uid,omitempty"`

	// Spec records the topology from original pxc
	Spec *polardbxv1.PolarDBXClusterSpec `json:"spec,omitempty"`

	// Secrets records account and password for pxc
	Secrets []polardbxv1polardbx.PrivilegeItem `json:"secrets,omitempty"`
}

type XstoreMetadata struct {
	// Name records name of original xstore
	Name string `json:"name,omitempty"`

	// UID records uid of original xstore
	UID types.UID `json:"uid,omitempty"`

	// BackupName records name of xstore backup of original xstore
	BackupName string `json:"backupName,omitempty"`

	// LastCommitIndex records the last binlog index during full backup
	LastCommitIndex int64 `json:"lastCommitIndex,omitempty"`

	// Secrets records account and password for xstore
	Secrets []polardbxv1polardbx.PrivilegeItem `json:"secrets,omitempty"`

	// TargetPod denotes the pod where backup was performed
	TargetPod string `json:"targetPod,omitempty"`
}

// MetadataBackup defines metadata to be uploaded during backup
type MetadataBackup struct {

	// PolarDBXClusterMetadata records metadata of pxc which backed up
	PolarDBXClusterMetadata PolarDBXClusterMetadata `json:"polarDBXClusterMetadata,omitempty"`

	// XstoreMetadataList records metadata of each xstore which backed up
	XstoreMetadataList []XstoreMetadata `json:"xstoreMetadataList,omitempty"`

	// BackupSetName records name of pxb
	BackupSetName string `json:"backupSetName,omitempty"`

	// BackupRootPath records the root path for pxb
	BackupRootPath string `json:"backupRootPath,omitempty"`

	// StartTime records start time of backup
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// EndTime records end time of backup
	EndTime *metav1.Time `json:"endTime,omitempty"`

	// LatestRecoverableTimestamp records the latest timestamp that can recover from current backup set
	LatestRecoverableTimestamp *metav1.Time `json:"latestRecoverableTimestamp,omitempty"`
}

func (m *MetadataBackup) GetXstoreNameList() []string {
	xstoreNameList := make([]string, len(m.XstoreMetadataList))
	for i, xstoreMetadata := range m.XstoreMetadataList {
		xstoreNameList[i] = xstoreMetadata.Name
	}
	return xstoreNameList
}

func (m *MetadataBackup) GetXstoreMetadataByName(xstoreName string) (*XstoreMetadata, error) {
	for i := range m.XstoreMetadataList {
		if m.XstoreMetadataList[i].Name == xstoreName {
			return &m.XstoreMetadataList[i], nil
		}
	}
	return nil, errors.New("no such metadata related to xstore " + xstoreName)
}

func (f *objectFactory) NewPolarDBXBackupBySchedule() (*polardbxv1.PolarDBXBackup, error) {
	backupSchedule := f.rc.MustGetPolarDBXBackupSchedule()
	backupName := name.NewSplicedName(
		name.WithTokens(backupSchedule.Spec.BackupSpec.Cluster.Name, "backup",
			backupSchedule.Status.NextBackupTime.Format("200601021504")),
		name.WithPrefix("scheduled-backup"),
	)
	backup := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: backupSchedule.Namespace,
			Name:      backupName,
			Labels: map[string]string{
				meta.LabelBackupSchedule: backupSchedule.Name,
			},
		},
		Spec: *backupSchedule.Spec.BackupSpec.DeepCopy(),
	}
	return backup, nil
}

func (f *objectFactory) NewXStoreBackup(
	xstore *polardbxv1.XStore) (*polardbxv1.XStoreBackup, error) {
	backup := f.rc.MustGetPolarDBXBackup()
	xstoreBackup := &polardbxv1.XStoreBackup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: backup.Namespace,
			Name: name.NewSplicedName(
				name.WithTokens(backup.Name, xstore.Name),
				name.WithPrefix("xstore-backup"),
			),
			Labels: map[string]string{
				meta.LabelName:            backup.Spec.Cluster.Name,
				meta.LabelTopBackup:       backup.Name,
				meta.LabelBackupXStore:    xstore.Name,
				meta.LabelBackupXStoreUID: string(xstore.UID),
			},
		},
		Spec: polardbxv1.XStoreBackupSpec{
			XStore: polardbxv1.XStoreReference{
				Name: xstore.Name,
				UID:  xstore.UID,
			},
			RetentionTime:       backup.Spec.RetentionTime,
			StorageProvider:     backup.Spec.StorageProvider,
			Engine:              xstore.Spec.Engine,
			PreferredBackupRole: backup.Spec.PreferredBackupRole,
		},
	}

	return xstoreBackup, nil
}

func (f *objectFactory) newDummyAnnotation() map[string]string {
	return map[string]string{
		meta.AnnotationDummyBackup: "true",
	}
}

func (f *objectFactory) NewDummyPolarDBXBackup(metadata *MetadataBackup) (*polardbxv1.PolarDBXBackup, error) {
	if metadata == nil {
		return nil, errors.New("not enough information to create dummy polardbx backup")
	}
	polardbx := f.rc.MustGetPolarDBX()
	polardbxBackup := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: name.NewSplicedName(
				name.WithTokens(metadata.BackupSetName, "dummy"),
				name.WithPrefix("dummy-backup"),
			),
			Namespace:   polardbx.Namespace,
			Annotations: f.newDummyAnnotation(),
		},
		Spec: polardbxv1.PolarDBXBackupSpec{
			Cluster: polardbxv1.PolarDBXClusterReference{
				Name: metadata.PolarDBXClusterMetadata.Name,
				UID:  metadata.PolarDBXClusterMetadata.UID,
			},
			StorageProvider: *polardbx.Spec.Restore.StorageProvider,
		},
		Status: polardbxv1.PolarDBXBackupStatus{
			Phase:                      polardbxv1.BackupDummy,
			BackupRootPath:             polardbx.Spec.Restore.From.BackupSetPath,
			ClusterSpecSnapshot:        metadata.PolarDBXClusterMetadata.Spec,
			XStores:                    metadata.GetXstoreNameList(),
			Backups:                    make(map[string]string),
			LatestRecoverableTimestamp: metadata.LatestRecoverableTimestamp,
			StartTime:                  metadata.StartTime,
			EndTime:                    metadata.EndTime,
		},
	}
	return polardbxBackup, nil
}

func (f *objectFactory) NewDummyXstoreBackup(xstoreName string, polardbxBackup *polardbxv1.PolarDBXBackup,
	metadata *MetadataBackup) (*polardbxv1.XStoreBackup, error) {
	if metadata == nil {
		return nil, errors.New("not enough information to create dummy xstore backup")
	}
	xstoreMetadata, err := metadata.GetXstoreMetadataByName(xstoreName)
	if err != nil {
		return nil, err
	}
	xstoreBackup := &polardbxv1.XStoreBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: name.NewSplicedName(
				name.WithTokens(polardbxBackup.Name, xstoreName),
				name.WithPrefix("dummy-xstore-backup"),
			),
			Namespace:   polardbxBackup.Namespace,
			Annotations: f.newDummyAnnotation(),
			Labels: map[string]string{
				meta.LabelName:            polardbxBackup.Spec.Cluster.Name,
				meta.LabelTopBackup:       polardbxBackup.Name,
				meta.LabelBackupXStore:    xstoreName,
				meta.LabelBackupXStoreUID: string(xstoreMetadata.UID),
			},
		},
		Spec: polardbxv1.XStoreBackupSpec{
			XStore: polardbxv1.XStoreReference{
				Name: xstoreName,
				UID:  xstoreMetadata.UID,
			},
			StorageProvider: polardbxBackup.Spec.StorageProvider,
		},
		Status: polardbxv1.XStoreBackupStatus{
			Phase:          polardbxv1.XStoreBackupDummy,
			CommitIndex:    xstoreMetadata.LastCommitIndex,
			BackupRootPath: metadata.BackupRootPath,
			TargetPod:      xstoreMetadata.TargetPod,
		},
	}
	return xstoreBackup, nil
}

func (f *objectFactory) NewDummySecretBackup(sourceSecretName string, metadata *MetadataBackup) (*corev1.Secret, error) {
	polardbx := f.rc.MustGetPolarDBX()

	var dummySecretName string
	var sourceSecrets *[]polardbxv1polardbx.PrivilegeItem
	accounts := make(map[string]string)

	if sourceSecretName == metadata.PolarDBXClusterMetadata.Name {
		// secret of cn
		dummySecretName = name.NewSplicedName(
			name.WithTokens(metadata.BackupSetName, "dummy"),
			name.WithPrefix("dummy-secret"),
		)
		sourceSecrets = &metadata.PolarDBXClusterMetadata.Secrets
	} else {
		// secret of dn
		dummySecretName = name.NewSplicedName(
			name.WithTokens(metadata.BackupSetName, "dummy", sourceSecretName),
			name.WithPrefix("dummy-xstore-secret"),
		)
		xstoreMetadata, err := metadata.GetXstoreMetadataByName(sourceSecretName)
		if err != nil {
			return nil, err
		}
		sourceSecrets = &xstoreMetadata.Secrets
	}
	for _, item := range *sourceSecrets {
		accounts[item.Username] = item.Password
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dummySecretName,
			Namespace: polardbx.Namespace,
			Labels:    convention.ConstLabels(polardbx),
		},
		StringData: accounts,
	}, nil
}
