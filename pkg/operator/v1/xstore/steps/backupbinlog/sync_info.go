package backupbinlog

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	xstorev1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/backupbinlog"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	hpfs "github.com/alibaba/polardbx-operator/pkg/hpfs/proto"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	"github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	xstorev1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/reconcile"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strconv"
	"strings"
)

func SyncInfoToXStore(rc *xstorev1reconcile.BackupBinlogContext, xstore xstorev1.XStore, info *backupbinlog.Info, infoHash string, hpfsClient hpfs.HpfsServiceClient) error {
	if xstore.Status.BoundVolumes != nil {
		for _, boundVolume := range xstore.Status.BoundVolumes {
			workDir := filepath.Join(boundVolume.HostPath, "log")
			if xstore.Spec.Config.Dynamic.LogDataSeparation {
				workDir = filepath.Join(boundVolume.LogHostPath, "log")
			}
			resp, err := hpfsClient.GetWatcherInfoHash(rc.Context(), &hpfs.GetWatcherInfoHashRequest{
				Host:   &hpfs.Host{NodeName: boundVolume.Host},
				LogDir: workDir,
			})
			if err != nil {
				return err
			}
			if resp.Status.Code != hpfs.Status_OK {
				return errors.New("GetWatcherInfoHash status not ok: " + resp.Status.Code.String())
			}
			if infoHash != resp.GetHash() {
				podInfo := *info
				podInfo.PodName = boundVolume.Pod
				//check if logger by pod name
				splitPodNames := strings.Split(podInfo.PodName, "-")
				if len(splitPodNames) >= 2 {
					if splitPodNames[len(splitPodNames)-2] == "log" {
						podInfo.SinkType = config.SinkTypeNone
					}
				}
				infoContent := getInfoContent(podInfo)
				_, err := hpfsClient.OpenBackupBinlog(rc.Context(), &hpfs.OpenBackupBinlogRequest{
					Host:    &hpfs.Host{NodeName: boundVolume.Host},
					LogDir:  workDir,
					Content: infoContent,
				})
				if err != nil {
					return err
				}
				if resp.Status.Code != hpfs.Status_OK {
					return errors.New("OpenBackupBinlog status not ok: " + resp.Status.Code.String())
				}
			}
		}

	}
	return nil
}

func writeInfoField(buf *bytes.Buffer, fieldName string, value string) {
	buf.Write([]byte(fieldName))
	buf.Write([]byte("="))
	buf.Write([]byte(value))
	buf.Write([]byte("\n"))
}

func getInfoContent(info backupbinlog.Info) string {
	buf := &bytes.Buffer{}
	writeInfoField(buf, backupbinlog.InfoNamespace, info.Namespace)
	writeInfoField(buf, backupbinlog.InfoPxcName, info.XStoreName)
	writeInfoField(buf, backupbinlog.InfoPxcUid, info.XStoreUid)
	writeInfoField(buf, backupbinlog.InfoXStoreName, info.XStoreName)
	writeInfoField(buf, backupbinlog.InfoXStoreUid, info.XStoreUid)
	writeInfoField(buf, backupbinlog.InfoPodName, info.PodName)
	writeInfoField(buf, backupbinlog.InfoBinlogChecksum, info.BinlogChecksum)
	writeInfoField(buf, backupbinlog.InfoSinkType, info.SinkType)
	writeInfoField(buf, backupbinlog.InfoSinkName, info.SinkName)
	writeInfoField(buf, backupbinlog.InfoXStoreUid, info.XStoreUid)
	writeInfoField(buf, backupbinlog.InfoLocalExpireLogSeconds, strconv.FormatInt(info.LocalExpireLogSeconds, 10))
	writeInfoField(buf, backupbinlog.InfoMaxLocalBinlogCount, strconv.FormatInt(info.MaxLocalBinlogCount, 10))
	writeInfoField(buf, backupbinlog.InfoForbidPurge, strconv.FormatBool(info.ForbidPurge))
	return buf.String()
}

func generateInfo(backupBinlog *xstorev1.XStoreBackupBinlog, xStore *xstorev1.XStore) (*backupbinlog.Info, *string, error) {
	expireLogHours := backupBinlog.Spec.LocalExpireLogHours
	expireLogSeconds := int64(expireLogHours.IntValue()) * 3600
	if expireLogSeconds == 0 {
		parsedVal, err := strconv.ParseFloat(expireLogHours.String(), 64)
		if err != nil {
			return nil, nil, err
		}
		expireLogSeconds = int64(parsedVal * 3600)
	}
	sinkName := config.SinkTypeNone
	if backupBinlog.Spec.StorageProvider.Sink != "" {
		sinkName = backupBinlog.Spec.StorageProvider.Sink
	}
	sinkType := config.SinkTypeNone
	if string(backupBinlog.Spec.StorageProvider.StorageName) != "" {
		sinkType = string(backupBinlog.Spec.StorageProvider.StorageName)
	}
	forbidPurge := xStore.Labels[meta.LabelBinlogPurgeLock] == meta.BinlogPurgeLock
	xstoreInfo := &backupbinlog.Info{
		Namespace:             backupBinlog.Namespace,
		XStoreName:            xStore.Name,
		XStoreUid:             string(xStore.UID),
		SinkName:              sinkName,
		SinkType:              sinkType,
		BinlogChecksum:        backupBinlog.Spec.BinlogChecksum,
		PxcName:               xStore.Name,
		PxcUid:                string(xStore.UID),
		LocalExpireLogSeconds: expireLogSeconds,
		MaxLocalBinlogCount:   int64(backupBinlog.Spec.MaxLocalBinlogCount),
		ForbidPurge:           forbidPurge,
	}
	infoJson, _ := json.Marshal(xstoreInfo)
	xstoreInfoHash := fmt.Sprintf("%x", md5.Sum(infoJson))
	return xstoreInfo, &xstoreInfoHash, nil
}

var SyncInfo = NewStepBinder("SyncInfo",
	func(rc *xstorev1reconcile.BackupBinlogContext, flow control.Flow) (reconcile.Result, error) {
		xstore := rc.GetActiveXStore()
		if xstore != nil {
			hpfsClient, err := rc.XStoreContext().GetHpfsClient()
			if err != nil {
				return flow.RetryErr(err, "failed to get hpfs client")
			}
			backupBinlog := rc.MustGetXStoreBackupBinlog()
			xstoreInfo, xstoreInfoHash, err := generateInfo(backupBinlog, xstore)
			if err != nil {
				return flow.RetryErr(err, "failed to generateInfo")
			}
			err = SyncInfoToXStore(rc, *xstore, xstoreInfo, *xstoreInfoHash, hpfsClient)
			if err != nil {
				flow.Logger().Error(err, "failed to sync info to xstore", "xstoreName", xstore.Name)
			}
		}
		return flow.Continue("SyncInfo.")
	})
