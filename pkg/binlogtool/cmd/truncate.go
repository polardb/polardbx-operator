//go:build polardbx

/*
Copyright 2022 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cmd

import (
	"errors"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
	"github.com/spf13/cobra"
	"io"
	"os"
	"strconv"
	"strings"
)

var (
	inputBinlogFile      string
	binlogChecksum       string
	truncateEndOffset    string
	truncateEndTimestamp uint32
	truncateEndTSO       uint64
	outputBinlogFile     string
)

var truncateCmd = &cobra.Command{
	Use:   "truncate",
	Short: "Truncate binlog by end offset or timestamp",
	Long:  "Truncate binlog by end offset or timestamp",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return errors.New("please specify a binlog file")
		}
		inputBinlogFile = args[0]
		return nil
	},

	Run: wrap(func(cmd *cobra.Command, args []string) error {
		opts := []binlog.LogEventScannerOption{
			binlog.WithBinlogFile(inputBinlogFile),
			binlog.WithChecksumAlgorithm(binlogChecksum),
			// does not parse any event body but return a raw event
			binlog.WithScanMode(binlog.ScanModeRaw),
		}

		if truncateEndOffset == "" && truncateEndTimestamp <= 0 && truncateEndTSO <= 0 {
			return errors.New("end-offset or end-ts or end-tso must be specified")
		}

		if outputBinlogFile == "" {
			return errors.New("output file must be specified")
		}

		endOffset, err := parseHybridOffset([]string{inputBinlogFile}, truncateEndOffset)
		if err != nil {
			return err
		}
		if endOffset != nil {
			opts = append(opts, binlog.WithEndPos(endOffset.Offset))
		}

		var lastEvent event.LogEvent
		lazyScanner := binlog.NewLazyLogEventScanCloser(
			func() (io.ReadCloser, error) {
				f, err := os.Open(inputBinlogFile)
				if err != nil {
					return nil, err
				}
				return utils.NewSeekableBufferReader(f), nil
			},
			0,
			opts...,
		)
		defer lazyScanner.Close()
		defer func() {
			fmt.Printf("LAST EVENT TIMESTAMP: %d\n", lastEvent.EventHeader().EventTimestamp())
		}()

		f, err := os.OpenFile(outputBinlogFile, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		defer f.Close()
		writer, err := binlog.NewRawLogEventWriter(f)
		defer writer.Flush()

		writer.WriteCommonHeader()

		for {
			_, ev, err := lazyScanner.Next()
			if err != nil {
				if err == binlog.EOF {
					break
				}
				return err
			}
			lastEvent = ev
			if truncateEndTimestamp > 0 && ev.EventHeader().EventTimestamp() > truncateEndTimestamp {
				return nil
			}

			if truncateEndTSO > 0 && ev.EventHeader().EventTypeCode() == spec.ROWS_QUERY_LOG_EVENT {
				queryEvent := ev.EventData().(event.RawLogEventData)
				eventContent := string(queryEvent)
				/**
				TSO in cdc binlog is in rows query log event and as follows:
					CTS::714859135699727161616796565716735180810000000000000000
				Notice: the first character in the event content is byte `1` not `C`, so I use eventContent[1:6] to
				extract the prefix and then parse the first 19 chars to uint64.
				*/
				if strings.HasPrefix(eventContent[1:6], "CTS::") {
					tso, err := strconv.ParseUint(eventContent[6:25], 10, 64)
					if err != nil {
						return err
					}

					if tso > truncateEndTSO {
						return nil
					}
				}
			}

			writer.Write(ev)
		}
		return nil
	}),
}

func init() {
	truncateCmd.Flags().StringVar(&binlogChecksum, "checksum", "crc32", "binary log checksum (ignored for binary log version v1, v3 and v4 after 3.6.1)")
	truncateCmd.Flags().StringVarP(&outputBinlogFile, "output", "o", "", "The output binlog after cut")
	truncateCmd.Flags().StringVar(&truncateEndOffset, "end-offset", "", "offset offset in bytes")
	truncateCmd.Flags().Uint32Var(&truncateEndTimestamp, "end-ts", 0, "end timestamp in seconds (compared with event header)")
	truncateCmd.Flags().Uint64Var(&truncateEndTSO, "end-tso", 0, "end tso (compared with event rows query info)")

	rootCmd.AddCommand(truncateCmd)
}
