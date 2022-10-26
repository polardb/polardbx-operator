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
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/algo"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/tx"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

var (
	seekHbBinlogChecksum    string
	seekHbBinlogStartOffset uint64
	seekHbBinlogEndOffset   uint64
	seekHbBinlogFile        string
	seekHbCommitTs          uint64
	seekHbLargerThanGivenTs bool
	seekHbTs                uint64
	seekHbSname             string
)

func init() {
	seekHbCmd.Flags().StringVar(&seekHbBinlogChecksum, "checksum", "crc32", "binary log checksum (ignored for binary log version v1, v3 and v4 after 3.6.1)")
	seekHbCmd.Flags().Uint64Var(&seekHbBinlogStartOffset, "start-offset", 0, "start offset in bytes")
	seekHbCmd.Flags().Uint64Var(&seekHbBinlogEndOffset, "end-offset", 0, "end offset in bytes")
	seekHbCmd.Flags().StringVar(&seekHbSname, "sname", "", "unique sname (write event) to locate heartbeat transaction")
	seekHbCmd.Flags().Uint64Var(&seekHbCommitTs, "commit-ts", 0, "commit timestamp (from TSO) to locate heartbeat transaction")
	seekHbCmd.Flags().BoolVar(&seekHbLargerThanGivenTs, "larger", false, "find the heartbeat larger than give commit ts (must be used with --commit-ts)")
	seekHbCmd.Flags().Uint64Var(&seekHbTs, "time", 0, "timestamp in seconds to locate heartbeat transaction (approximately)")

	rootCmd.AddCommand(seekHbCmd)
}

var seekHbCmd = &cobra.Command{
	Use:   "seekhb [flags] file",
	Short: "Locate heartbeat transaction",
	Long:  "Locate heartbeat transaction",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return errors.New("please specify the binlog file")
		}
		seekHbBinlogFile = args[0]
		return nil
	},
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		opts := []binlog.LogEventScannerOption{
			binlog.WithBinlogFile(seekHbBinlogFile),
			binlog.WithChecksumAlgorithm(seekHbBinlogChecksum),
		}
		if seekHbBinlogStartOffset > 0 {
			opts = append(opts, binlog.WithProbeFirstHeader(4096))
		}
		if seekHbBinlogEndOffset > 0 {
			opts = append(opts, binlog.WithEndPos(seekHbBinlogEndOffset))
		}

		s := binlog.NewLazyLogEventScanCloser(
			func() (io.ReadCloser, error) {
				f, err := os.Open(seekHbBinlogFile)
				if err != nil {
					return nil, err
				}
				return utils.NewSeekableBufferReader(f), nil
			},
			seekHbBinlogStartOffset,
			opts...,
		)
		defer s.Close()

		var seekOpt algo.LocateHeartbeatOption
		if seekHbCommitTs > 0 {
			policy := algo.ExactCommitTS
			if seekHbLargerThanGivenTs {
				policy = algo.LargerCommitTS
			}
			seekOpt = algo.WithCommitTSPolicy(seekHbCommitTs, policy)
		} else if seekHbTs > 0 {
			seekOpt = algo.WithCommitTSPolicy(uint64(utils.NewTSOTimeFromMillis(seekHbTs*1000)), algo.AroundCommitTS)
		} else if len(seekHbSname) > 0 {
			seekOpt = algo.WithUniqueWritePolicy(seekHbSname)
		} else {
			return errors.New("no policy")
		}

		txid, commitTs, eventOffset, row, err := algo.NewLocateHeartbeat(tx.NewTransactionEventParser(s), seekOpt).Perform()
		if err != nil {
			return err
		}
		fmt.Printf("TRANSACTION ID: %d\n", txid)
		if commitTs > 0 {
			fmt.Printf("COMMIT TS: %d\n", commitTs)
		}
		if eventOffset != nil {
			fmt.Printf("EVENT OFFSET: %s\n", eventOffset.String())
		}
		if row != nil {
			if row.Timestamp.IsZero() {
				fmt.Printf("TIMESTAMP (IN ROW): %s\n", "NULL")
			} else {
				fmt.Printf("HEARTBEAT TIMESTAMP (IN ROW): %s\n", row.Timestamp)
				fmt.Printf("HEARTBEAT TIMESTAMP(UNIX): %d\n", row.Timestamp.Unix())
			}
		}
		return nil
	}),
}
