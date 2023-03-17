package backupbinlog

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	v1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/backupbinlog"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	hpfs "github.com/alibaba/polardbx-operator/pkg/hpfs/proto"
	"github.com/alibaba/polardbx-operator/pkg/k8s/control"
	polardbxv1reconcile "github.com/alibaba/polardbx-operator/pkg/operator/v1/polardbx/reconcile"
	xstoremeta "github.com/alibaba/polardbx-operator/pkg/operator/v1/xstore/meta"
	"google.golang.org/grpc"
	"path/filepath"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strconv"
	"strings"
	"time"
)

func GetHpfsClient(rc *polardbxv1reconcile.Context) (hpfs.HpfsServiceClient, error) {
	hpfsConn, err := grpc.Dial(rc.Config().Store().HostPathFileServiceEndpoint(), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return hpfs.NewHpfsServiceClient(hpfsConn), nil
}

func SyncInfoToXStore(rc *polardbxv1reconcile.Context, xstore *v1.XStore, info *backupbinlog.Info, infoHash string, hpfsClient hpfs.HpfsServiceClient) error {
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

func getInfoContent(info backupbinlog.Info) string {
	buf := &bytes.Buffer{}
	writeInfoField(buf, backupbinlog.InfoNamespace, info.Namespace)
	writeInfoField(buf, backupbinlog.InfoPxcName, info.PxcName)
	writeInfoField(buf, backupbinlog.InfoPxcUid, info.PxcUid)
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

func writeInfoField(buf *bytes.Buffer, fieldName string, value string) {
	buf.Write([]byte(fieldName))
	buf.Write([]byte("="))
	buf.Write([]byte(value))
	buf.Write([]byte("\n"))
}

func generateInfo(backupBinlog *v1.PolarDBXBackupBinlog, xStores []*v1.XStore) (map[string]*backupbinlog.Info, map[string]string, error) {
	expireLogHours := backupBinlog.Spec.LocalExpireLogHours
	expireLogSeconds := int64(expireLogHours.IntValue()) * 3600
	if expireLogSeconds == 0 {
		parsedVal, err := strconv.ParseFloat(expireLogHours.String(), 64)
		if err != nil {
			return nil, nil, err
		}
		expireLogSeconds = int64(parsedVal * 3600)
	}
	dnPodInfoMap := map[string]*backupbinlog.Info{}
	dnPodInfoHashMap := map[string]string{}

	sinkName := config.SinkTypeNone
	if backupBinlog.Spec.StorageProvider.Sink != "" {
		sinkName = backupBinlog.Spec.StorageProvider.Sink
	}
	sinkType := config.SinkTypeNone
	if string(backupBinlog.Spec.StorageProvider.StorageName) != "" {
		sinkType = string(backupBinlog.Spec.StorageProvider.StorageName)
	}

	if xStores != nil {
		for _, xstore := range xStores {
			forbidPurge := xstore.Labels[xstoremeta.LabelBinlogPurgeLock] == xstoremeta.BinlogPurgeLock
			info := &backupbinlog.Info{
				Namespace:             backupBinlog.Namespace,
				XStoreName:            xstore.Name,
				XStoreUid:             string(xstore.UID),
				SinkName:              sinkName,
				SinkType:              sinkType,
				BinlogChecksum:        backupBinlog.Spec.BinlogChecksum,
				PxcName:               backupBinlog.Spec.PxcName,
				PxcUid:                backupBinlog.Spec.PxcUid,
				LocalExpireLogSeconds: expireLogSeconds,
				MaxLocalBinlogCount:   int64(backupBinlog.Spec.MaxLocalBinlogCount),
				ForbidPurge:           forbidPurge,
			}
			dnPodInfoMap[xstore.Name] = info
			infoJson, _ := json.Marshal(info)
			dnPodInfoHashMap[xstore.Name] = fmt.Sprintf("%x", md5.Sum(infoJson))
		}
	}
	return dnPodInfoMap, dnPodInfoHashMap, nil
}

var SyncInfo = polardbxv1reconcile.NewStepBinder("AddFinalizer", func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
	hpfsClient, err := GetHpfsClient(rc)
	if err != nil {
		return flow.RetryErr(err, "failed to get hpfs client")
	}
	dnMap, err := rc.GetDNMap()
	if err != nil {
		return flow.RetryErr(err, "failed to get dn map")
	}
	gms, err := rc.GetGMS()
	if err != nil {
		return flow.RetryErr(err, "failed to get gms")
	}
	xstores := make([]*v1.XStore, 0, len(dnMap)+1)
	if gms.GetDeletionTimestamp().IsZero() {
		xstores = append(xstores, gms)
	}
	for _, v := range dnMap {
		if v.GetDeletionTimestamp().IsZero() {
			xstores = append(xstores, v)
		}
	}
	backupBinlog := rc.MustGetPolarDBXBackupBinlog()
	xstoreInfos, xstoreInfoHashes, err := generateInfo(backupBinlog, xstores)
	if err != nil {
		return flow.RetryErr(err, "failed to generateInfo")
	}
	for _, xstore := range xstores {
		xstoreInfo, ok := xstoreInfos[xstore.Name]
		if !ok {
			continue
		}
		err := SyncInfoToXStore(rc, xstore, xstoreInfo, xstoreInfoHashes[xstore.Name], hpfsClient)
		if err != nil {
			flow.Logger().Error(err, "failed to sync info to xstore", "xstoreName", xstore.Name)
		}
	}
	return flow.Continue("SyncInfo.")
})

var RunningRoute = polardbxv1reconcile.NewStepBinder("AddFinalizer", func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
	backupBinlog := rc.MustGetPolarDBXBackupBinlog()
	now := time.Now()
	checkInterval, err := rc.Config().Backup().CheckBinlogExpiredFileInterval()
	if err != nil {
		flow.Logger().Error(err, "failed to get check backup binlog interval")
		return flow.Pass()
	}

	if now.Unix()-int64(backupBinlog.Status.CheckExpireFileLastTime) > int64(checkInterval.Seconds()) {
		backupBinlog.Status.Phase = v1.BackupBinlogPhaseCheckExpiredFile
		rc.MarkPolarDBXChanged()
	}

	return flow.RetryAfter(5*time.Second, "RunningRoute.")
})

var CloseBackupBinlog = polardbxv1reconcile.NewStepBinder("CloseBackupBinlog", func(rc *polardbxv1reconcile.Context, flow control.Flow) (reconcile.Result, error) {
	hpfsClient, err := GetHpfsClient(rc)
	if err != nil {
		return flow.RetryErr(err, "failed to get hpfs client")
	}
	dnMap, err := rc.GetDNMap()
	if err != nil {
		return flow.RetryErr(err, "failed to get dn map")
	}
	gms, err := rc.GetGMS()
	if err != nil {
		return flow.RetryErr(err, "failed to get gms")
	}
	xstores := make([]*v1.XStore, 0, len(dnMap)+1)
	if gms.GetDeletionTimestamp().IsZero() {
		xstores = append(xstores, gms)
	}
	for _, v := range dnMap {
		if v.GetDeletionTimestamp().IsZero() {
			xstores = append(xstores, v)
		}
	}
	for _, xstore := range xstores {
		for _, boundVolume := range xstore.Status.BoundVolumes {
			workDir := filepath.Join(boundVolume.HostPath, "log")
			if xstore.Spec.Config.Dynamic.LogDataSeparation {
				workDir = filepath.Join(boundVolume.LogHostPath, "log")
			}
			resp, err := hpfsClient.CloseBackupBinlog(rc.Context(), &hpfs.CloseBackupBinlogRequest{
				Host:   &hpfs.Host{NodeName: boundVolume.Host},
				LogDir: workDir,
			})
			if err != nil {
				return flow.RetryErr(err, "failed to close backup binlog", "logDir", workDir, "nodeName", boundVolume.Host)
			}
			if resp.Status.Code != hpfs.Status_OK {
				return flow.RetryErr(errors.New("CloseBackupBinlogRequest status not ok: "+resp.Status.Code.String()), "logDir", workDir, "nodeName", boundVolume.Host)
			}

		}
	}
	return flow.Continue("CloseBackupBinlog.")
})
