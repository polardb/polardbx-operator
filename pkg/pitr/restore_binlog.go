package pitr

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/algo"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/tx"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/backupbinlog"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/common"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/filestream"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const CheckRangeSeconds = 5

var CutIgnoredEventType = map[string]bool{
	"Consensus":              true,
	"PreviousConsensusIndex": true,
}

type LocalSource struct {
	FsIp         string `json:"fs_ip,omitempty"`
	FsPort       int    `json:"fs_port,omitempty"`
	NodeName     string `json:"node_name,omitempty"`
	DataFilepath string `json:"data_filepath,omitempty"`
}

type RemoteSource struct {
	FsIp         string       `json:"fs_ip,omitempty"`
	FsPort       int          `json:"fs_port,omitempty"`
	Sink         *config.Sink `json:"Sink,omitempty"`
	MetaFilepath string       `json:"meta_filepath,omitempty"`
	DataFilepath string       `json:"data_filepath,omitempty"`
}

type BinlogSource struct {
	Filename          string        `json:"filename,omitempty"`
	LSource           *LocalSource  `json:"local_source,omitempty"`
	RSource           *RemoteSource `json:"remote_source,omitempty"`
	BinlogChecksum    string        `json:"binlog_checksum,omitempty"`
	RangeStartOffset  *uint32       `json:"range_start_offset,omitempty"`
	RangeEndOffset    *uint32       `json:"range_end_offset,omitempty"`
	TruncateLength    *uint64       `json:"truncate_length,omitempty"`
	Length            uint64        `json:"Length,omitempty"`
	Version           string        `json:"version,omitempty"`
	Hash              string        `json:"hash,omitempty"`
	Timestamp         uint64        `json:"timestamp,omitempty"`
	StartIndex        uint64        `json:"start_index,omitempty"`
	HeartbeatTxEvents []tx.Event    `json:"heartbeat_tx_events,omitempty"`
	RangeTxEvents     []tx.Event    `json:"range_tx_events,omitempty"`
}

func (b *BinlogSource) GetTrueLength() *uint64 {
	if b.TruncateLength != nil {
		return b.TruncateLength
	}
	if b.RangeEndOffset != nil {
		offset := uint64(*b.RangeEndOffset)
		return &offset
	}
	return &b.Length
}

func (b *BinlogSource) Copy() *BinlogSource {
	tmp := *b
	return &tmp
}

func (b *BinlogSource) String() string {
	jsonContent, _ := json.Marshal(b)
	return string(jsonContent)
}

func (b *BinlogSource) OpenStream() (io.ReadCloser, error) {
	reader, err := b.OpenRemoteStream()
	if err != nil {
		fmt.Println(err)
	}
	if reader == nil {
		reader, err = b.OpenLocalStream()
	}
	return reader, err
}

func (b *BinlogSource) OpenRemoteStream() (io.ReadCloser, error) {
	if b.RSource != nil {
		fileClient := filestream.NewFileClient(b.RSource.FsIp, b.RSource.FsPort, nil)
		fileClient.InitWaitChan()
		action := filestream.GetClientActionBySinkType(b.RSource.Sink.Type)
		if action == filestream.InvalidAction {
			panic(fmt.Sprintf("invalid sinkType %s", b.RSource.Sink.Type))
		}
		metadata := filestream.ActionMetadata{
			Action:    action,
			Filepath:  b.RSource.MetaFilepath,
			RequestId: uuid.New().String(),
			Sink:      b.RSource.Sink.Name,
		}
		metaReader, metaWriter := io.Pipe()
		go func() {
			defer metaWriter.Close()
			_, err := fileClient.Download(metaWriter, metadata)
			if err != nil {
				fmt.Println(err)
			}
		}()
		err := fileClient.WaitForDownload()
		if err != nil {
			return nil, err
		}
		defer metaReader.Close()
		metaBytes, err := io.ReadAll(metaReader)
		if err != nil {
			return nil, err
		}
		var bf backupbinlog.BinlogFile
		err = json.Unmarshal(metaBytes, &bf)
		if err != nil {
			return nil, err
		}
		b.Timestamp = bf.EventTimestamp
		b.Hash = bf.Sha256
		b.Version = bf.Version
		b.StartIndex = bf.StartIndex
		dataReader, dataWriter := io.Pipe()
		go func() {
			defer dataWriter.Close()
			_, err = fileClient.Download(dataWriter, filestream.ActionMetadata{
				Action:    action,
				Filepath:  b.RSource.DataFilepath,
				RequestId: uuid.New().String(),
				Sink:      b.RSource.Sink.Name,
			})
			if err != nil {
				fmt.Println(err)
			}
		}()
		err = fileClient.WaitForDownload()
		if err != nil {
			fmt.Println(err)
			dataReader.Close()
			return nil, err
		}
		b.Length = fileClient.GetLastLen()
		return dataReader, nil
	}
	return nil, errors.New("invalid remote source")
}

func (b *BinlogSource) OpenLocalStream() (io.ReadCloser, error) {
	if b.LSource != nil {
		fileClient := filestream.NewFileClient(b.LSource.FsIp, b.LSource.FsPort, nil)
		fileClient.InitWaitChan()

		var err error
		dataReader, dataWriter := io.Pipe()
		metadata := filestream.ActionMetadata{
			Action:       filestream.DownloadRemote,
			Filepath:     b.LSource.DataFilepath,
			RequestId:    uuid.New().String(),
			RedirectAddr: filestream.GetRemoteAddrByNodeName(b.LSource.NodeName),
		}
		go func() {
			defer dataWriter.Close()
			_, err = fileClient.Download(dataWriter, metadata)
			if err != nil {
				fmt.Println(err)
			}
		}()
		err = fileClient.WaitForDownload()
		if err != nil {
			dataReader.Close()
			return nil, err
		}
		headBytes, err := common.ReadBytes(dataReader, backupbinlog.BufferSizeBytes)
		if len(headBytes) > 0 {
			startIndex, eventTimestamp, err := backupbinlog.GetBinlogFileBeginInfo(headBytes, filepath.Base(metadata.Filepath), b.BinlogChecksum)
			if err != nil {
				return nil, err
			}
			b.StartIndex = startIndex
			b.Timestamp = eventTimestamp
		}
		dataReader.Close()
		dataReaderAgain, dataWriterAgain := io.Pipe()

		go func() {
			defer dataWriterAgain.Close()
			_, err = fileClient.Download(dataWriterAgain, metadata)
			if err != nil {
				fmt.Println(err)
			}
		}()
		err = fileClient.WaitForDownload()
		if err != nil {
			dataReaderAgain.Close()
			return nil, err
		}
		b.Length = fileClient.GetLastLen()
		return dataReaderAgain, nil
	}
	return nil, nil
}

func (b *BinlogSource) getBinlogFilename() string {
	myFilepath := ""
	if b.LSource != nil {
		myFilepath = b.LSource.DataFilepath
	}
	if b.RSource != nil {
		myFilepath = b.RSource.DataFilepath
	}
	if myFilepath == "" {
		panic(errors.New("empty binlog filename"))
	}
	return filepath.Base(myFilepath)
}

func (b *BinlogSource) GetBinlogFileNum() int64 {
	filename := b.getBinlogFilename()
	splitStrs := strings.Split(filename, ".")
	if len(splitStrs) != 2 {
		panic(fmt.Sprintf("invalid binlog filename %s", filename))
	}
	num, err := strconv.ParseInt(splitStrs[1], 10, 64)
	if err != nil {
		panic(errors.Wrap(err, fmt.Sprintf("failed to parse int, filename = %s ", filename)))
	}
	return num
}

type RestoreBinlog struct {
	GlobalConsistent bool           `json:"global_consistent,omitempty"`
	PxcName          string         `json:"pxc_name,omitempty"`
	PxcUid           string         `json:"pxc_uid,omitempty"`
	XStoreName       string         `json:"xstore_name,omitempty"`
	XStoreUid        string         `json:"xstore_uid,omitempty"`
	PodName          string         `json:"pod_name,omitempty"`
	StartIndex       uint64         `json:"start_index,omitempty"`
	Timestamp        uint64         `json:"timestamp,omitempty"`
	Version          uint64         `json:"version,omitempty"`
	HeartbeatSname   string         `json:"heartbeat_sname,omitempty"`
	Sources          []BinlogSource `json:"sources,omitempty"`
	ResultSources    []BinlogSource `json:"result_sources,omitempty"`
	spillFilepath    string         `json:"spill_filepath,omitempty"`
}

func newRestoreBinlog(pxcName string, pxcUid string, xStoreName string, xStoreUid string, podName string, startIndex uint64, timestamp uint64, version uint64, heartbeatSname string, sources []BinlogSource) *RestoreBinlog {
	if sources == nil {
		sources = make([]BinlogSource, 0)
	}
	return &RestoreBinlog{
		PxcName:        pxcName,
		PxcUid:         pxcUid,
		XStoreName:     xStoreName,
		XStoreUid:      xStoreUid,
		PodName:        podName,
		StartIndex:     startIndex,
		Timestamp:      timestamp,
		Version:        version,
		HeartbeatSname: heartbeatSname,
		Sources:        sources,
	}
}

func (r *RestoreBinlog) SetSpillFilepath(spillFilepath string) {
	r.spillFilepath = spillFilepath
}

func (r *RestoreBinlog) GetSpillFilepath(spillFilepath string) string {
	return r.spillFilepath
}

func (r *RestoreBinlog) SpillResultSources() error {
	if r.ResultSources != nil {
		bytes, _ := json.Marshal(r.ResultSources)
		err := os.WriteFile(r.spillFilepath, bytes, 0644)
		if err != nil {
			return err
		}
		r.ResultSources = nil
	}
	return nil
}

func (r *RestoreBinlog) LoadResultSources() error {
	if r.ResultSources == nil {
		bytes, err := os.ReadFile(r.spillFilepath)
		if err != nil {
			return err
		}
		err = json.Unmarshal(bytes, &r.ResultSources)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *RestoreBinlog) SortSources() {
	if len(r.Sources) > 0 {
		sort.Slice(r.Sources, func(i, j int) bool {
			return r.Sources[i].GetBinlogFileNum() < r.Sources[j].GetBinlogFileNum()
		})
	}
}

func (r *RestoreBinlog) SearchByTimestamp() error {
	r.SortSources()
	result := make([]BinlogSource, 0)
	var preBinlogSource *BinlogSource
	for _, binlogSource := range r.Sources {
		reader, err := binlogSource.OpenStream()
		if reader == nil {
			if err == nil {
				err = errors.New("")
			}
			return errors.Wrap(err, fmt.Sprintf("failed to get reader, binlog sources=%s", binlogSource.String()))
		}
		reader.Close()
		if r.StartIndex > binlogSource.StartIndex {
			preBinlogSource = binlogSource.Copy()
			continue
		}

		if r.Timestamp-CheckRangeSeconds <= binlogSource.Timestamp {
			if len(result) > 0 {
				r.searchRangeInfo(&result[len(result)-1], r.Timestamp-CheckRangeSeconds, r.Timestamp)
			}
			if r.Timestamp < binlogSource.Timestamp {
				break
			}
		}

		if r.StartIndex <= binlogSource.StartIndex {
			if r.StartIndex < binlogSource.StartIndex && len(result) == 0 {
				if preBinlogSource == nil && len(result) == 0 {
					return errors.New("startIndex is smaller than binlog start index")
				}
				result = append(result, *preBinlogSource)
			}
			result = append(result, binlogSource)
			preBinlogSource = nil
		}
	}
	if len(result) == 0 && preBinlogSource != nil && preBinlogSource.StartIndex <= r.StartIndex {
		result = append(result, *preBinlogSource)
	}
	if len(result) > 0 {
		lastBinlogSource := result[len(result)-1]
		if lastBinlogSource.RangeStartOffset == nil && lastBinlogSource.RangeEndOffset == nil {
			beginTimestamp := r.Timestamp - CheckRangeSeconds
			if !r.GlobalConsistent {
				beginTimestamp = 0
			}
			r.searchRangeInfo(&result[len(result)-1], beginTimestamp, r.Timestamp)
		}
	}
	r.ResultSources = result
	return nil
}

func (r *RestoreBinlog) searchRangeInfo(binlogSource *BinlogSource, startTs uint64, endTs uint64) {
	reader, err := binlogSource.OpenStream()
	if err != nil || reader == nil {
		panic(fmt.Sprintf("failed to get reader stream, binlogSource=%s", binlogSource.String()))
	}
	defer reader.Close()
	opts := []binlog.LogEventScannerOption{
		binlog.WithBinlogFile(binlogSource.getBinlogFilename()),
		binlog.WithChecksumAlgorithm(binlogSource.BinlogChecksum),
		binlog.WithLogEventHeaderFilter(func(header event.LogEventHeader) bool {
			headerTs := uint64(header.EventTimestamp())
			if startTs <= headerTs && headerTs < endTs {
				if binlogSource.RangeStartOffset == nil {
					offset := header.EventEndPosition() - header.TotalEventLength()
					binlogSource.RangeStartOffset = &offset
				}
				return true
			}
			if headerTs >= endTs {
				if _, ok := CutIgnoredEventType[header.EventType()]; ok {
					if binlogSource.RangeEndOffset == nil {
						offset := header.EventEndPosition() - header.TotalEventLength()
						binlogSource.RangeEndOffset = &offset
					}
					reader.Close()
				}
			}
			return false
		}),
	}
	scanner, err := binlog.NewLogEventScanner(bufio.NewReader(reader), opts...)
	if err != nil {
		panic(fmt.Sprintf("failed to get binlog event scanner, binlogSource=%s", binlogSource.String()))
	}

	heartbeatTxEvents := make([]tx.Event, 0)
	rangeTxEvents := make([]tx.Event, 0)
	var latestHeartbeatPrepareTxEvent *tx.Event
	uniqueMap := map[uint64]bool{}
	eventHandler := func(txEvent *tx.Event) error {
		if !r.GlobalConsistent {
			return nil
		}
		copiedTxEvent := *txEvent
		if copiedTxEvent.Type == tx.Prepare && copiedTxEvent.HeartbeatRowsLogEvents != nil {
			for _, rowsEv := range copiedTxEvent.HeartbeatRowsLogEvents {
				if updateRowsEv, ok := rowsEv.(*event.UpdateRowsEvent); ok {
					hrow, err := algo.ExtractHeartbeatFieldsFromRowsEvent(updateRowsEv)
					if err != nil {
						return fmt.Errorf("unable to extract heartbeat rows event: %w", err)
					}
					if bytes.Equal([]byte(r.HeartbeatSname), []byte(hrow.Sname)) {
						latestHeartbeatPrepareTxEvent = &copiedTxEvent
						break
					}
				}
			}
		}

		if copiedTxEvent.Type == tx.Prepare {
			rangeTxEvents = append(rangeTxEvents, copiedTxEvent)
		}

		if copiedTxEvent.Type == tx.Commit {
			_, exist := uniqueMap[copiedTxEvent.XID]
			if latestHeartbeatPrepareTxEvent != nil && copiedTxEvent.XID == latestHeartbeatPrepareTxEvent.XID {
				if (copiedTxEvent.Ts >> 22 / 1000) >= endTs {
					if binlogSource.RangeEndOffset == nil {
						header := copiedTxEvent.Raw.EventHeader()
						offset := header.EventEndPosition() - header.TotalEventLength()
						binlogSource.RangeEndOffset = &offset
					}
					return tx.StopParse
				}
				if !exist {
					heartbeatTxEvents = append(heartbeatTxEvents, *latestHeartbeatPrepareTxEvent, copiedTxEvent)
				} else {
					heartbeatTxEvents[len(heartbeatTxEvents)-1].Ts = copiedTxEvent.Ts
				}
			}
			if !exist {
				rangeTxEvents = append(rangeTxEvents, copiedTxEvent)
			}
			uniqueMap[copiedTxEvent.XID] = true

		}
		return nil
	}
	err = tx.NewTransactionEventParser(scanner).Parse(eventHandler)
	if err != nil {
		fmt.Println("NewTransactionEventParser Parse err=", err)
	}
	if len(heartbeatTxEvents) > 0 {
		binlogSource.HeartbeatTxEvents = heartbeatTxEvents
	}
	if len(rangeTxEvents) > 0 {
		binlogSource.RangeTxEvents = rangeTxEvents
	}
}

func (r *RestoreBinlog) CheckValid() bool {
	var hasRangeStart bool
	var hasRangeEnd bool
	if r.ResultSources != nil {
		for _, resultSource := range r.ResultSources {
			if resultSource.RangeStartOffset != nil {
				hasRangeStart = true
			}
			if resultSource.RangeEndOffset != nil {
				hasRangeEnd = true
			}
		}
	}
	return hasRangeEnd && hasRangeStart
}
