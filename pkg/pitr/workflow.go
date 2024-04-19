package pitr

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/algo"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/tx"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/common"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	hpfs "github.com/alibaba/polardbx-operator/pkg/hpfs/proto"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	pkgErrors "github.com/pkg/errors"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	CopyBufferSize = 1 << 20
)

type Step func(pCtx *Context) error

func MustMarshalJSON(obj interface{}) string {
	b, err := json.Marshal(obj)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func LoadAllBinlog(pCtx *Context) error {
	pCtx.Logger.Info("LoadAllBinlog...")
	taskConfig := pCtx.TaskConfig
	if taskConfig == nil {
		return errors.New("taskConfig must not be nil")
	}
	hpfsClient, conn, err := NewHpfsClient(taskConfig.HpfsEndpoint)
	if err != nil {
		return pkgErrors.Wrap(err, fmt.Sprintf("failed to get hpfs client, endpoint=%s", taskConfig.HpfsEndpoint))
	}
	if conn != nil {
		defer conn.Close()
	}
	filestreamIp, filestreamPort := common.ParseNetAddr(taskConfig.FsEndpoint)
	restoreBinlogs := make([]RestoreBinlog, 0)
	for _, xStore := range taskConfig.XStores {
		if xStore.GlobalConsistent {
			pCtx.ConsistentXStoreCount += 1
		}
		for _, pod := range xStore.Pods {
			var binlogSources []BinlogSource
			if pod.Host != "" {
				pCtx.Logger.Info("list local binlog list", "pod", pod.PodName, "host", pod.Host, "logDir", pod.LogDir)
				resp, err := hpfsClient.ListLocalBinlogList(context.Background(), &hpfs.ListLocalBinlogListRequest{
					Host:   &hpfs.Host{NodeName: pod.Host},
					LogDir: pod.LogDir,
				})
				if err != nil {
					pCtx.Logger.Error(err, "failed to list local binlog list", "pod", pod.PodName, "host", pod.Host, "logDir", pod.LogDir)
					return err
				}
				if resp.Version != "" {
					for _, binlogFile := range resp.GetBinlogFiles() {
						binlogFilename := filepath.Base(binlogFile)
						absoluteFilepath := filepath.Join(pod.LogDir, binlogFilename)
						binlogSources = append(binlogSources, BinlogSource{
							Filename: binlogFilename,
							LSource: &LocalSource{
								FsIp:         filestreamIp,
								FsPort:       filestreamPort,
								NodeName:     pod.Host,
								DataFilepath: absoluteFilepath,
							},
							BinlogChecksum: taskConfig.BinlogChecksum,
							Version:        resp.Version,
							Timestamp:      taskConfig.Timestamp,
							StartIndex:     xStore.BackupSetStartIndex,
						})
					}
				}
				pCtx.Logger.Info("finish list local binlog list", "pod", pod.PodName, "host", pod.Host, "logDir", pod.LogDir, "response", MustMarshalJSON(resp))
			}

			pCtx.Logger.Info("remote binlog list", "pod", pod.PodName, "host", pod.Host, "logDir", pod.LogDir)
			remoteResp, err := hpfsClient.ListRemoteBinlogList(context.Background(), &hpfs.ListRemoteBinlogListRequest{
				Namespace:  taskConfig.Namespace,
				PxcName:    taskConfig.PxcName,
				PxcUid:     taskConfig.PxcUid,
				XStoreName: xStore.XStoreName,
				XStoreUid:  xStore.XStoreUid,
				SinkName:   taskConfig.SinkName,
				SinkType:   taskConfig.SinkType,
				PodName:    pod.PodName,
			})
			if err != nil {
				pCtx.Logger.Error(err, "failed to list local binlog list", "pod", pod.PodName, "host", pod.Host, "logDir", pod.LogDir)
				return err
			}
			pCtx.Logger.Info("finish remote binlog list", "pod", pod.PodName, "host", pod.Host, "logDir", pod.LogDir, "resp", MustMarshalJSON(remoteResp))
			versionFilesMap := map[string]map[string][]string{}
			for _, file := range remoteResp.GetFiles() {
				elements := strings.Split(file, "/")
				if len(elements) < 4 {
					err := errors.New(fmt.Sprintf("invaid filepath = %s", file))
					pCtx.Logger.Error(err, "failed to get version")
					return err
				}
				version := elements[len(elements)-4]
				_, ok := versionFilesMap[version]
				if !ok {
					versionFilesMap[version] = map[string][]string{}
				}
				filename := filepath.Base(file)
				index := 0
				if strings.HasSuffix(filename, ".txt") {
					filename = filename[:len(filename)-4]
					index = 1
				}
				if versionFilesMap[version][filename] == nil {
					versionFilesMap[version][filename] = make([]string, 2)
				}
				versionFilesMap[version][filename][index] = file
			}

			for version, versionVal := range versionFilesMap {
				for filename, vals := range versionVal {
					dataFilepath := vals[0]
					metaFilepath := vals[1]
					if dataFilepath == "" || metaFilepath == "" {
						err := fmt.Errorf("invalid filepath, dataFilepath = %s, metaFilepath = %s", dataFilepath, metaFilepath)
						return err
					}
					binlogSources = append(binlogSources, BinlogSource{
						Filename: filename,
						RSource: &RemoteSource{
							FsIp:   filestreamIp,
							FsPort: filestreamPort,
							Sink: &config.Sink{
								Name: taskConfig.SinkName,
								Type: taskConfig.SinkType,
							},
							DataFilepath: dataFilepath,
							MetaFilepath: metaFilepath,
						},
						BinlogChecksum: taskConfig.BinlogChecksum,
						Version:        version,
						Timestamp:      taskConfig.Timestamp,
						StartIndex:     xStore.BackupSetStartIndex,
					})
				}
			}
			groupedBinlogSources := groupBinlogSources(binlogSources)
			for k, bs := range groupedBinlogSources {
				version, err := strconv.ParseInt(k, 10, 64)
				if err != nil {
					return fmt.Errorf("failed to parse version = %s", k)
				}
				restoreBinlogs = append(restoreBinlogs, RestoreBinlog{
					GlobalConsistent: xStore.GlobalConsistent,
					PxcName:          taskConfig.PxcName,
					PxcUid:           taskConfig.PxcUid,
					XStoreName:       xStore.XStoreName,
					XStoreUid:        xStore.XStoreUid,
					PodName:          pod.PodName,
					StartIndex:       xStore.BackupSetStartIndex,
					Timestamp:        taskConfig.Timestamp,
					Version:          uint64(version),
					HeartbeatSname:   xStore.HeartbeatSname,
					Sources:          bs,
					spillFilepath:    filepath.Join(taskConfig.SpillDirectory, uuid.New().String()),
				})
			}
		}
	}
	pCtx.RestoreBinlogs = restoreBinlogs
	return nil
}

func groupBinlogSources(sources []BinlogSource) map[string][]BinlogSource {
	result := map[string][]BinlogSource{}
	for _, source := range sources {
		_, ok := result[source.Version]
		if !ok {
			result[source.Version] = []BinlogSource{}
		}
		result[source.Version] = append(result[source.Version], source)
	}

	for k := range result {
		sort.Slice(result[k], func(i, j int) bool {
			return result[k][i].GetBinlogFileNum() < result[k][j].GetBinlogFileNum()
		})
		tmpSources := make([]BinlogSource, 0)
		for _, source := range result[k] {
			if len(tmpSources) > 0 && tmpSources[len(tmpSources)-1].Filename == source.Filename {
				if tmpSources[len(tmpSources)-1].RSource == nil {
					tmpSources[len(tmpSources)-1].RSource = source.RSource
				} else if tmpSources[len(tmpSources)-1].LSource == nil {
					tmpSources[len(tmpSources)-1].LSource = source.LSource
				}
				continue
			}
			tmpSources = append(tmpSources, source)
		}
		result[k] = tmpSources
	}
	return result
}

func NewHpfsClient(endpoint string) (hpfs.HpfsServiceClient, *grpc.ClientConn, error) {
	hpfsConn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	return hpfs.NewHpfsServiceClient(hpfsConn), hpfsConn, nil
}

func SelectBinlogForStandard(pCtx *Context) error {
	pCtx.Logger.Info("SelectBinlogForStandard...")
	flags := map[string]bool{}
	newRestoreBinlogs := make([]RestoreBinlog, 0, len(pCtx.RestoreBinlogs)/2)
	for _, restoreBinlog := range pCtx.RestoreBinlogs {
		_, ok := flags[restoreBinlog.XStoreName]
		if !ok {
			err := restoreBinlog.SearchByTimestampAndIndex()
			if err != nil {
				pCtx.Logger.Error(err, "failed to SearchByTimestamp", "pxcName", restoreBinlog.PxcName, "xStoreName", restoreBinlog.XStoreName, "podName", restoreBinlog.PodName, "version", restoreBinlog.Version)
				return err
			}
			newRestoreBinlogs = append(newRestoreBinlogs, restoreBinlog)
			flags[restoreBinlog.XStoreName] = true
		}
	}
	pCtx.RestoreBinlogs = newRestoreBinlogs
	if len(pCtx.RestoreBinlogs) != len(pCtx.TaskConfig.XStores) {
		// failed. some xStore binlog does not exist between the backup set start index and the timestamp
		restoreBinlogXstores := make([]string, 0, len(pCtx.RestoreBinlogs))
		for _, restoreBinlog := range pCtx.RestoreBinlogs {
			restoreBinlogXstores = append(restoreBinlogXstores, restoreBinlog.XStoreName)
		}
		slices.Sort(restoreBinlogXstores)
		configXStores := make([]string, 0, len(pCtx.TaskConfig.XStores))
		for xStoreName, _ := range pCtx.TaskConfig.XStores {
			configXStores = append(configXStores, xStoreName)
		}
		slices.Sort(configXStores)
		err := errors.New("failed get proper binlogs")
		pCtx.Logger.Error(err, "", "expect", configXStores, "actual", restoreBinlogXstores)
		return err
	}
	return nil

}
func PrepareBinlogMeta(pCtx *Context) error {
	pCtx.Logger.Info("PrepareBinlogMeta...")
	flags := map[string]bool{}
	newRestoreBinlogs := make([]RestoreBinlog, 0, len(pCtx.RestoreBinlogs)/2)
	for _, restoreBinlog := range pCtx.RestoreBinlogs {
		_, ok := flags[restoreBinlog.XStoreName]
		if !ok {
			err := restoreBinlog.SearchByTimestamp()
			if err != nil {
				pCtx.Logger.Error(err, "failed to SearchByTimestamp", "pxcName", restoreBinlog.PxcName, "xStoreName", restoreBinlog.XStoreName, "podName", restoreBinlog.PodName, "version", restoreBinlog.Version)
				return err
			}
			if restoreBinlog.CheckValid() {
				err := restoreBinlog.SpillResultSources()
				if err != nil {
					pCtx.Logger.Error(err, fmt.Sprintf("failed to spill result sources of podName = %s, version = %d", restoreBinlog.PodName, restoreBinlog.Version))
					return err
				}
				newRestoreBinlogs = append(newRestoreBinlogs, restoreBinlog)
				flags[restoreBinlog.XStoreName] = true
			} else {
				pCtx.Logger.Info("invalid searchByTimestamp result", "pxcName", restoreBinlog.PxcName, "xStoreName", restoreBinlog.XStoreName, "podName", restoreBinlog.PodName, "version", restoreBinlog.Version)
			}
		}
	}
	pCtx.RestoreBinlogs = newRestoreBinlogs
	if len(pCtx.RestoreBinlogs) != len(pCtx.TaskConfig.XStores) {
		// failed. some xStore binlog does not exist between the backup set start index and the timestamp
		restoreBinlogXstores := make([]string, 0, len(pCtx.RestoreBinlogs))
		for _, restoreBinlog := range pCtx.RestoreBinlogs {
			restoreBinlogXstores = append(restoreBinlogXstores, restoreBinlog.XStoreName)
		}
		slices.Sort(restoreBinlogXstores)
		configXStores := make([]string, 0, len(pCtx.TaskConfig.XStores))
		for xStoreName, _ := range pCtx.TaskConfig.XStores {
			configXStores = append(configXStores, xStoreName)
		}
		slices.Sort(configXStores)
		err := errors.New("failed get proper binlogs")
		pCtx.Logger.Error(err, "", "expect", configXStores, "actual", restoreBinlogXstores)
		return err
	}
	return nil
}

func CollectInterestedTxEvents(pCtx *Context) error {
	pCtx.Logger.Info("CollectInterestedTxEvents...")
	if !pCtx.NeedConsistentPoint() {
		for i, _ := range pCtx.RestoreBinlogs {
			pCtx.RestoreBinlogs[i].LoadResultSources()
		}
		pCtx.Logger.Info("Skip CollectInterestedTxEvents...")
		return nil
	}
	xidCntMap := map[uint64]int{}
	xidTsMap := map[uint64]tx.Event{}
	for k, rb := range pCtx.RestoreBinlogs {
		if !rb.GlobalConsistent {
			continue
		}
		rb.LoadResultSources()
		xidCntMapInner := map[uint64]bool{}
		for _, rs := range rb.ResultSources {
			for _, ht := range rs.HeartbeatTxEvents {
				if ht.Type == tx.Commit && ht.Ts > 0 {
					xid := ht.XID
					xidTsMap[xid] = ht
					if _, innerOk := xidCntMapInner[xid]; !innerOk {
						_, ok := xidCntMap[xid]
						if !ok {
							xidCntMap[xid] = 0
						}
						xidCntMap[xid] = xidCntMap[xid] + 1
					}
				}
			}
		}
		rb.SpillResultSources()
		pCtx.RestoreBinlogs[k] = rb
	}
	candidateHeartbeatTxEvents := make([]tx.Event, 0)
	for xid, cnt := range xidCntMap {
		// ignore meta db
		if cnt == len(pCtx.RestoreBinlogs)-1 {
			candidateHeartbeatTxEvents = append(candidateHeartbeatTxEvents, xidTsMap[xid])
		}
	}
	sort.Slice(candidateHeartbeatTxEvents, func(i, j int) bool {
		return candidateHeartbeatTxEvents[i].Ts < candidateHeartbeatTxEvents[j].Ts
	})
	if len(candidateHeartbeatTxEvents) < 2 {
		return errors.New(fmt.Sprintf("heartbeat count expect >= 2, actual = %d", len(candidateHeartbeatTxEvents)))
	}
	beginXid := candidateHeartbeatTxEvents[len(candidateHeartbeatTxEvents)-2].XID
	endXid := candidateHeartbeatTxEvents[len(candidateHeartbeatTxEvents)-1].XID
	pCtx.CpHeartbeatXid = endXid
	for j, rb := range pCtx.RestoreBinlogs {
		err := rb.LoadResultSources()
		pCtx.RestoreBinlogs[j] = rb
		if err != nil {
			return err
		}
		if !rb.GlobalConsistent {
			continue
		}
		for k, rs := range rb.ResultSources {
			if len(rs.RangeTxEvents) > 0 {
				newRangeTxEvents := make([]tx.Event, 0)
				startIndex := math.MaxInt
				endIndex := math.MaxInt
				for i, txEvent := range rs.RangeTxEvents {
					switch txEvent.XID {
					case beginXid:
						if txEvent.Type == tx.Prepare {
							startIndex = i
						}
					case endXid:
						if txEvent.Type == tx.Commit {
							endIndex = i
						}
					}
					if i >= startIndex && i <= endIndex {
						newRangeTxEvents = append(newRangeTxEvents, txEvent)
					}
				}
				rs.RangeTxEvents = newRangeTxEvents
				rb.ResultSources[k] = rs
			}
		}
		pCtx.RestoreBinlogs[j] = rb
	}
	return nil
}

type myTransactionParser struct {
	sources []BinlogSource
}

func (tp *myTransactionParser) Parse(h tx.EventHandler) error {
	for _, source := range tp.sources {
		for _, txEvent := range source.RangeTxEvents {
			h(&txEvent)
		}
	}
	return nil
}

func Checkpoint(pCtx *Context) error {
	pCtx.Logger.Info("Checkpoint...")
	if !pCtx.NeedConsistentPoint() {
		pCtx.Logger.Info("Skip Checkpoint...")
		byteBuf := &bytes.Buffer{}
		binary.Write(byteBuf, binary.LittleEndian, uint32(0))
		pCtx.RecoverTxsBytes = byteBuf.Bytes()
		pCtx.Logger.Info(fmt.Sprintf("Write Empty RecoverTxsBytes Length = %d", len(pCtx.RecoverTxsBytes)))
		return nil
	}
	txParsers := map[string]tx.TransactionEventParser{}
	for _, restoreBinlog := range pCtx.RestoreBinlogs {
		if !restoreBinlog.GlobalConsistent {
			continue
		}
		txParsers[restoreBinlog.XStoreName] = &myTransactionParser{
			sources: restoreBinlog.ResultSources,
		}
	}
	recoverableTxs, borders, err := algo.NewSeekConsistentPoint(txParsers, pCtx.CpHeartbeatXid).Perform()
	if err != nil {
		return err
	}
	pCtx.RecoverTxsBytes, err = algo.SerializeCpResult(recoverableTxs, borders)
	if err != nil {
		return err
	}
	pCtx.Borders = borders
	for i := 0; i < len(pCtx.RestoreBinlogs); i++ {
		restoreBinlog := pCtx.RestoreBinlogs[i]
		if !restoreBinlog.GlobalConsistent {
			continue
		}
		eOffset, ok := pCtx.Borders[restoreBinlog.XStoreName]
		if !ok {
			err := fmt.Errorf("failed to get event offset of xstore name = %s", restoreBinlog.XStoreName)
			pCtx.Logger.Error(err, "")
			return err
		}
		var found bool
		for j := 0; j < len(restoreBinlog.ResultSources); j++ {
			if restoreBinlog.ResultSources[j].getBinlogFilename() == eOffset.File {
				offset := eOffset.Offset
				restoreBinlog.ResultSources[j].TruncateLength = &offset
				pCtx.RestoreBinlogs[i] = restoreBinlog
				found = true
				break
			}
		}
		if !found {
			err := fmt.Errorf("impossible. event offset = %s", eOffset.String())
			pCtx.Logger.Error(err, "failed to find binlog source")
			return err
		}
	}
	return nil
}

type httpLogger struct {
	mux    *http.ServeMux
	logger logr.Logger
}

func (h *httpLogger) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestInfo := map[string]string{}
	requestInfo["host"] = r.Host
	requestInfo["path"] = r.URL.Path
	vals := r.URL.Query()
	for k, val := range vals {
		requestInfo[k] = fmt.Sprintf("%+v", val)
	}
	requestId := vals.Get("requestid")
	h.logger.Info(MustMarshalJSON(requestInfo), "requestid", requestId)
	begin := time.Now().UnixMilli()
	h.mux.ServeHTTP(w, r)
	h.logger.Info(fmt.Sprintf("time cost %d ms", time.Now().UnixMilli()-begin), "requestid", requestId)
}

func FinishAndStartHttpServer(pCtx *Context) error {
	pCtx.Logger.Info("FinishAndStartHttpServer...")
	mux := &http.ServeMux{}
	mux.HandleFunc("/status", func(writer http.ResponseWriter, request *http.Request) {
		if pCtx.LastErr == nil {
			writer.WriteHeader(http.StatusOK)
			writer.Write([]byte("success"))
		} else {
			writer.WriteHeader(http.StatusInternalServerError)
			writer.Write([]byte(fmt.Sprintf("%+v", pCtx.LastErr)))
		}
	})

	mux.HandleFunc("/lastErr", func(writer http.ResponseWriter, request *http.Request) {
		if pCtx.LastErr != nil {
			writer.Write([]byte(fmt.Sprintf("%+v", pCtx.LastErr)))
		}
		writer.Write([]byte(""))
	})

	mux.HandleFunc("/context", func(writer http.ResponseWriter, request *http.Request) {
		writer.Write([]byte(MustMarshalJSON(pCtx)))
	})

	mux.HandleFunc("/config", func(writer http.ResponseWriter, request *http.Request) {
		writer.Write([]byte(MustMarshalJSON(pCtx.TaskConfig)))
	})

	mux.HandleFunc("/binlogs", func(writer http.ResponseWriter, request *http.Request) {
		query := request.URL.Query()
		xStore := query.Get("xstore")
		if xStore == "" {
			writer.Write([]byte("xstore param is required"))
			return
		}
		for _, restoreBinlog := range pCtx.RestoreBinlogs {
			if restoreBinlog.XStoreName == xStore {
				files := make([]HttpBinlogFileInfo, 0, len(restoreBinlog.ResultSources))
				for _, source := range restoreBinlog.ResultSources {
					trueLength := *source.GetTrueLength()
					files = append(files, HttpBinlogFileInfo{
						Filename: source.getBinlogFilename(),
						Length:   &trueLength,
					})
				}
				writer.Write([]byte(MustMarshalJSON(files)))
				return
			}
		}
		writer.Write([]byte("[]"))
	})

	mux.HandleFunc("/download/binlog", func(writer http.ResponseWriter, request *http.Request) {
		query := request.URL.Query()
		xStore := query.Get("xstore")
		if xStore == "" {
			writer.WriteHeader(http.StatusNotFound)
			writer.Write([]byte("xstore param is required"))
			return
		}
		filename := query.Get("filename")
		if filename == "" {
			writer.WriteHeader(http.StatusNotFound)
			writer.Write([]byte("filename param is required"))
		}
		onlyMeta := query.Get("only_meta")

		for _, restoreBinlog := range pCtx.RestoreBinlogs {
			if restoreBinlog.XStoreName == xStore {
				for _, source := range restoreBinlog.ResultSources {
					if filename == source.getBinlogFilename() {
						if strings.EqualFold(onlyMeta, "true") {
							binlogSourceJsonBytes := []byte(MustMarshalJSON(source))
							writer.Write(binlogSourceJsonBytes)
							return
						}
						reader, err := source.OpenStream()
						if err != nil {
							pCtx.Logger.Error(err, "failed to open stream", "xstoreName", xStore, "filename", filename)
							writer.WriteHeader(http.StatusInternalServerError)
							return
						}
						length := int64(*source.GetTrueLength())
						writer.Header().Add("Content-Length", strconv.FormatInt(length, 10))
						writer.WriteHeader(http.StatusAccepted)
						io.CopyBuffer(writer, io.LimitReader(reader, length), make([]byte, CopyBufferSize))
						reader.Close()
						return
					}
				}
			}
		}
		writer.WriteHeader(http.StatusNotFound)
	})

	mux.HandleFunc("/download/recovertxs", func(writer http.ResponseWriter, request *http.Request) {
		if pCtx.RecoverTxsBytes != nil {
			writer.WriteHeader(http.StatusOK)
			gw, _ := gzip.NewWriterLevel(writer, gzip.BestSpeed)
			defer gw.Close()
			gw.Write(pCtx.RecoverTxsBytes)
		} else {
			writer.WriteHeader(http.StatusNotFound)
		}
	})

	mux.HandleFunc("/exit", func(writer http.ResponseWriter, request *http.Request) {
		if !pCtx.Closed.CAS(false, true) {
			return
		}
		exitCode := 0
		if pCtx.LastErr != nil {
			exitCode = 1
		}
		go func() {
			time.Sleep(2 * time.Second)
			os.Exit(exitCode)
		}()
	})
	listenAddr := fmt.Sprintf(":%d", pCtx.TaskConfig.HttpServerPort)
	pCtx.Logger.Info("start http server, listen " + listenAddr)
	err := http.ListenAndServe(listenAddr, &httpLogger{mux: mux, logger: pCtx.Logger})
	return err
}
