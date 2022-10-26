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
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/meta"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/tx"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

var (
	txDumpBinlogFiles       []string
	txDumpBinlogStartOffset string
	txDumpBinlogEndOffset   string
	txDumpBinlogChecksum    string
	txDumpOutputFile        string
	txDumpBinaryFormat      bool
)

func init() {
	txDumpCmd.Flags().StringVar(&txDumpBinlogChecksum, "checksum", "crc32", "binary log checksum (ignored for binary log version v1, v3 and v4 after 3.6.1)")
	txDumpCmd.Flags().StringVarP(&txDumpOutputFile, "output", "o", "", "output file")
	txDumpCmd.Flags().StringVar(&txDumpBinlogStartOffset, "start-offset", "", "start offset in bytes")
	txDumpCmd.Flags().StringVar(&txDumpBinlogEndOffset, "end-offset", "", "end offset in bytes")
	txDumpCmd.Flags().BoolVar(&txDumpBinaryFormat, "bin", false, "write txDumpaction events in binary format (only when output file is specified)")

	rootCmd.AddCommand(txDumpCmd)
}

func parseHybridOffset(files []string, s string) (*binlog.EventOffset, error) {
	if len(s) == 0 {
		return nil, nil
	}
	if len(files) == 1 {
		offset, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			off, err := parseOffset(s)
			if err != nil {
				return nil, err
			}
			if off.File != path.Base(files[0]) {
				return nil, errors.New("offset invalid: file not match")
			}
			return off, nil
		}
		return &binlog.EventOffset{File: path.Base(files[0]), Offset: offset}, nil
	}
	off, err := parseOffset(s)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if off.File == path.Base(f) {
			return off, nil
		}
	}
	return nil, errors.New("offset invalid: file not match")
}

var txDumpCmd = &cobra.Command{
	Use:   "txdump [flags] file [files...]",
	Short: "Extract 2PC events",
	Long:  "Extract 2PC events",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return errors.New("please specify a binlog file")
		}
		txDumpBinlogFiles = args
		return nil
	},
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		startOffset, err := parseHybridOffset(txDumpBinlogFiles, txDumpBinlogStartOffset)
		if err != nil {
			return err
		}
		endOffset, err := parseHybridOffset(txDumpBinlogFiles, txDumpBinlogEndOffset)
		if err != nil {
			return err
		}
		binlogs, err := meta.BinaryLogFilesConsistentAndContinuousInName(txDumpBinlogFiles)
		if err != nil {
			return err
		}
		var startOffsetBf, endOffsetBf meta.BinlogFile
		if startOffset != nil {
			startOffsetBf, err = meta.ParseBinlogFile(startOffset.File)
			if err != nil {
				return err
			}
		}
		if endOffset != nil {
			endOffsetBf, err = meta.ParseBinlogFile(endOffset.File)
			if err != nil {
				return err
			}
		}
		if startOffset != nil && endOffset != nil {
			if startOffsetBf.Index > endOffsetBf.Index {
				return errors.New("invalid binlog range: end offset < start offset")
			}
		}
		// Filter out binlog files.
		binlogs = utils.FilterSlice(binlogs, func(bf *meta.BinlogFile) bool {
			if startOffset != nil && bf.Index < startOffsetBf.Index {
				return false
			}
			if endOffset != nil && bf.Index > endOffsetBf.Index {
				return false
			}
			return true
		})

		scanners := make([]binlog.LogEventScanCloser, 0, len(binlogs))
		for i := range binlogs {
			bf := binlogs[i]
			opts := []binlog.LogEventScannerOption{
				binlog.WithBinlogFile(bf.File),
				binlog.WithChecksumAlgorithm(txDumpBinlogChecksum),
			}
			var bfStartOffset uint64 = 0
			if startOffset != nil && startOffset.File == bf.FileName() {
				bfStartOffset = startOffset.Offset
				opts = append(opts, binlog.WithProbeFirstHeader(4096))
			}
			if endOffset != nil && endOffset.File == bf.FileName() {
				opts = append(opts, binlog.WithEndPos(endOffset.Offset))
			}

			s := binlog.NewLazyLogEventScanCloser(
				func() (io.ReadCloser, error) {
					f, err := os.Open(bf.File)
					if err != nil {
						return nil, err
					}
					return utils.NewSeekableBufferReader(f), nil
				},
				bfStartOffset,
				opts...,
			)
			scanners = append(scanners, s)
		}

		s := binlog.NewMultiLogEventScanCloser(scanners...)

		var of io.Writer
		if txDumpOutputFile != "" {
			f, err := os.OpenFile(txDumpOutputFile, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
			if err != nil {
				return err
			}
			defer f.Close()
			of = f
		} else {
			of = os.Stdout
		}
		w := bufio.NewWriter(of)
		defer w.Flush()

		if txDumpBinaryFormat && len(txDumpOutputFile) == 0 {
			_, _ = fmt.Fprintln(os.Stderr, "WARN: no output file specified, ignore binary format flag")
		}

		if txDumpBinaryFormat && len(txDumpOutputFile) > 0 {
			filenames := utils.ConvertSlice(binlogs, func(t meta.BinlogFile) string {
				return t.File
			})
			ew, err := tx.NewBinaryTransactionEventWriter(of, filenames)
			if err != nil {
				return err
			}
			defer ew.Flush()
			return tx.NewTransactionEventParser(s).Parse(func(ev *tx.Event) error {
				return ew.Write(*ev)
			})
		} else {
			return tx.NewTransactionEventParser(s).Parse(func(ev *tx.Event) error {
				_, err := w.WriteString(ev.String() + "\n")
				return err
			})
		}
	}),
}
