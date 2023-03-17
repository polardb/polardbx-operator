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
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/algo"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/meta"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/tx"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

func parseOffset(s string) (*binlog.EventOffset, error) {
	if s == "" {
		return nil, nil
	}

	colonIdx := strings.IndexRune(s, ':')
	if colonIdx < 0 {
		return nil, errors.New("invalid binlog offset")
	}
	file, offsetStr := s[:colonIdx], s[colonIdx+1:]
	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		return nil, err
	}
	return &binlog.EventOffset{File: file, Offset: offset}, nil
}

type binlogRange struct {
	Start *binlog.EventOffset
	End   *binlog.EventOffset
}

func parseBinlogRange(s string) (binlogRange, error) {
	seps := strings.Split(s, ",")
	if len(seps) > 2 {
		return binlogRange{}, errors.New("more than 2")
	}

	if len(seps) == 1 {
		startOff, err := parseOffset(seps[0])
		if err != nil {
			return binlogRange{}, err
		}
		return binlogRange{Start: startOff}, nil
	} else {
		startOff, err := parseOffset(seps[0])
		if err != nil {
			return binlogRange{}, err
		}
		endOff, err := parseOffset(seps[0])
		if err != nil {
			return binlogRange{}, err
		}
		if strings.Split(startOff.File, ".")[0] != strings.Split(endOff.File, ".")[0] {
			return binlogRange{}, errors.New("range invalid: basename not match")
		}
		if endOff.File < startOff.File || (startOff.File == endOff.File && startOff.Offset > endOff.Offset) {
			return binlogRange{}, errors.New("range invalid: end < start")
		}
		return binlogRange{Start: startOff, End: endOff}, nil
	}
}

type binlogRangesValue struct {
	lastDir string
	ranges  map[string]binlogRange
}

func (v *binlogRangesValue) Set(s string) error {
	if s == "" {
		return nil
	}

	if v.lastDir == "" {
		return errors.New("directory is not provided")
	}

	r, err := parseBinlogRange(s)
	if err != nil {
		return err
	}
	if r.Start != nil || r.End != nil {
		v.ranges[v.lastDir] = r
	}

	v.lastDir = ""
	return nil
}

func (v *binlogRangesValue) String() string {
	return ""
}

func (v *binlogRangesValue) Type() string {
	return ""
}

type binlogFileValue binlogRangesValue

func (v *binlogFileValue) Set(s string) error {
	v.lastDir = s
	return nil
}

func (v *binlogFileValue) String() string {
	return ""
}

func (v *binlogFileValue) Type() string {
	return ""
}

var (
	seekCpDirectory     string
	seekCpBinlogRanges  map[string]binlogRange
	seekCpHeartbeatTxid uint64
	seekCpBinEvents     bool
	seekCpVerbose       bool
	seekCpOutput        string
)

func init() {
	seekCpBinlogRanges = make(map[string]binlogRange)
	brv := &binlogRangesValue{ranges: seekCpBinlogRanges}

	seekCpCmd.Flags().Var((*binlogFileValue)(brv), "binlog-stream", "target binlog stream, must be followed with --binlog-range")
	seekCpCmd.Flags().Var(brv, "binlog-range", `binlog range with format [[file1:start_index],[file2:end_index], e.g. 
"mysql-bin.000000:123"                      -- [mysql-bin.000000:123, EOF]
"mysql-bin.000000:123,mysql-bin.000001:456" -- [mysql-bin.000000:123, mysql-bin.000001:456]
",mysql-bin.000001:456"                     -- [BEGIN, mysql-bin.000002:456]`)
	seekCpCmd.Flags().BoolVar(&seekCpBinEvents, "binary-events", false, "use binary format events instead (end with .evs)")
	seekCpCmd.Flags().Uint64Var(&seekCpHeartbeatTxid, "heartbeat-txid", 0, "heartbeat transaction id")
	seekCpCmd.Flags().BoolVarP(&seekCpVerbose, "verbose", "v", false, "verbose mode")
	seekCpCmd.Flags().StringVar(&seekCpOutput, "output", "", "output file (binary format)")

	rootCmd.AddCommand(seekCpCmd)
}

var sortedBinaryLogsByStream map[string][]meta.BinlogFile

func printBinlogFiles() {
	for streamName, binaryLogs := range sortedBinaryLogsByStream {
		r := seekCpBinlogRanges[streamName]
		fmt.Printf("  %s: ", streamName)
		if len(binaryLogs) > 1 {
			if r.Start != nil && r.End != nil {
				fmt.Printf("%s-%s\n", r.Start.String(), r.End.String())
			} else if r.Start != nil {
				fmt.Printf("%s-%s\n", r.Start.String(), binaryLogs[len(binaryLogs)-1].String())
			} else if r.End != nil {
				fmt.Printf("%s-%s\n", binaryLogs[0].String(), r.End.String())
			} else {
				fmt.Printf("%s-%s\n", binaryLogs[0].String(), binaryLogs[len(binaryLogs)-1].String())
			}
		} else {
			if r.Start != nil && r.End != nil {
				fmt.Printf("%s:[%d-%d]\n", binaryLogs[0].String(), r.Start.Offset, r.End.Offset)
			} else if r.Start != nil {
				fmt.Printf("%s:[%d-]\n", binaryLogs[0].String(), r.Start.Offset)
			} else if r.End != nil {
				fmt.Printf("%s:[-%d]\n", binaryLogs[0].String(), r.End.Offset)
			} else {
				fmt.Printf("%s\n", binaryLogs[0].String())
			}
		}
	}
}

func buildBinlogFiles() map[string][]meta.BinlogFile {
	dir, err := os.Open(seekCpDirectory)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer dir.Close()

	m := make(map[string][]meta.BinlogFile)
	entries, err := dir.ReadDir(-1)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		files, err := listBinlogFiles(path.Join(seekCpDirectory, entry.Name()))
		if err != nil {
			fmt.Println("List binlog files in " + entry.Name() + " failed: " + err.Error())
			os.Exit(1)
		}
		if len(files) == 0 {
			continue
		}
		m[entry.Name()] = files
	}

	return m
}

func writeRecoverableConsistentPoint(recoverableTxs []uint64, borders map[string]binlog.EventOffset) error {
	if len(seekCpOutput) == 0 {
		return nil
	}

	// Layout
	// TXID LENGTH 						-- 4 bytes
	// REPEAT
	//		TXID						-- 8 bytes
	// STREAM LENGTH					-- 2 bytes
	// REPEAT
	//		STREAM NAME LEN				-- 1 byte
	//		STREAM NAME					-- len bytes
	//		OFFSET BINLOG FILE NAME LEN	-- 1 byte
	//		OFFSET BINLOG FILE NAME		-- len bytes
	//		OFFSET						-- 8 bytes

	f, err := os.OpenFile(seekCpOutput, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	gw, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
	if err != nil {
		return err
	}
	defer gw.Close()
	bytes, err := algo.SerializeCpResult(recoverableTxs, borders)
	if err != nil {
		return err
	}
	gw.Write(bytes)
	return nil
}

func doSeekConsistentPoint(txParsers map[string]tx.TransactionEventParser, heartbeatTxid uint64) {
	fmt.Println("=================== SEEK CONSISTENT POINTS ===================")

	recoverableTxs, borders, err := algo.NewSeekConsistentPoint(txParsers, heartbeatTxid).Perform()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "ERROR: "+err.Error())
		os.Exit(1)
	}

	fmt.Printf("TOTAL RECOVERABLE TRANSACTIONS: %d\n", len(recoverableTxs))
	if seekCpVerbose {
		for i := 0; i < len(recoverableTxs); i += 5 {
			r := i + 5
			if r > len(recoverableTxs) {
				r = len(recoverableTxs)
			}
			fmt.Printf("  %s\n", utils.JoinIntegerSequence(recoverableTxs[i:r], ", "))
		}
		fmt.Println()
	}

	fmt.Println("BINLOG INDEXES: ")
	for name, border := range borders {
		fmt.Printf("  %s:  %s\n", name, border.String())
	}
	fmt.Println()

	if err := writeRecoverableConsistentPoint(recoverableTxs, borders); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "ERROR: "+err.Error())
		os.Exit(1)
	}
}

func listBinlogFiles(dir string) ([]meta.BinlogFile, error) {
	d, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	filenames, err := d.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(filenames); i++ {
		filenames[i] = path.Join(dir, filenames[i])
	}
	return meta.BinaryLogFilesConsistentAndContinuousInName(filenames)
}

func buildTransactionParsers(name string, binlogFiles []meta.BinlogFile) (tx.TransactionEventParser, error) {
	basename := binlogFiles[0].Basename
	r := seekCpBinlogRanges[name]
	startBinlogOffset, endBinlogOffset := r.Start, r.End
	var startBf, endBf meta.BinlogFile
	var err error
	if startBinlogOffset != nil {
		if strings.Split(startBinlogOffset.File, ".")[0] != basename {
			return nil, errors.New("start offset invalid: basename not match")
		}
		if startBinlogOffset.File < binlogFiles[0].String() ||
			startBinlogOffset.File > binlogFiles[len(binlogFiles)-1].String() {
			return nil, errors.New("start offset invalid: range exceeds")
		}
		startBf, err = meta.ParseBinlogFile(startBinlogOffset.File)
		if err != nil {
			return nil, err
		}
	}
	if endBinlogOffset != nil {
		if strings.Split(endBinlogOffset.File, ".")[0] != basename {
			return nil, errors.New("end offset invalid: basename not match")
		}
		if endBinlogOffset.File < binlogFiles[0].String() ||
			endBinlogOffset.File > binlogFiles[len(binlogFiles)-1].String() {
			return nil, errors.New("end offset invalid: range exceeds")
		}
		endBf, err = meta.ParseBinlogFile(endBinlogOffset.File)
		if err != nil {
			return nil, err
		}
	}

	scanners := make([]binlog.LogEventScanner, 0, len(binlogFiles))
	for _, file := range binlogFiles {
		if startBinlogOffset != nil && file.Index < startBf.Index {
			continue
		}
		if endBinlogOffset != nil && file.Index > endBf.Index {
			continue
		}

		startOffset := uint64(0)
		opts := []binlog.LogEventScannerOption{
			binlog.WithBinlogFile(file.String()),
			binlog.WithLogEventHeaderFilter(tx.TransactionEventParserHeaderFilter()),
			binlog.EnableChecksumValidation,
		}
		if startBinlogOffset != nil && file.Index == startBf.Index {
			startOffset = startBinlogOffset.Offset
		}
		if endBinlogOffset != nil && file.Index == endBf.Index {
			opts = append(opts, binlog.WithEndPos(endBinlogOffset.Offset))
		}

		filePath := file.File
		scanners = append(scanners, binlog.NewLazyLogEventScanCloser(
			func() (io.ReadCloser, error) {
				f, err := os.Open(filePath)
				if err != nil {
					return nil, err
				}
				return utils.NewSeekableBufferReader(f), nil
			},
			startOffset,
			opts...))
	}

	return tx.NewTransactionEventParser(binlog.NewMultiLogEventScanner(scanners...)), nil
}

func buildAllTransactionParsers(binlogFiles map[string][]meta.BinlogFile) (map[string]tx.TransactionEventParser, error) {
	txParsers := make(map[string]tx.TransactionEventParser)
	for name, files := range binlogFiles {
		p, err := buildTransactionParsers(name, files)
		if err != nil {
			return nil, err
		}
		txParsers[name] = p
	}

	return txParsers, nil
}

func findAndCollectAllBinlogFiles() {
	fmt.Println("==================== COLLECT BINARY LOGS =====================")

	sortedBinaryLogsByStream = buildBinlogFiles()

	if len(sortedBinaryLogsByStream) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "ERROR: no binlog files found")
		os.Exit(1)
	}

	fmt.Printf("TOTAL BINLOG DIRECTORIES: %d\n", len(sortedBinaryLogsByStream))

	printBinlogFiles()

	fmt.Println()

	fileCnt := 0
	for _, files := range sortedBinaryLogsByStream {
		fileCnt += len(files)
	}

	fmt.Printf("TOTAL BINARY LOG FILES: %d\n", fileCnt)
	fmt.Println()
}

func buildBinaryEventFiles() map[string]string {
	fmt.Println("=================== COLLECT BINARY EVENTS ====================")

	dir, err := os.Open(seekCpDirectory)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer dir.Close()

	m := make(map[string]string)
	entries, err := dir.ReadDir(-1)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "ERROR: unable to list dir, "+err.Error())
		os.Exit(1)
	}

	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".evs") {
			streamName := strings.Split(e.Name(), ".")[0]
			m[streamName] = path.Join(seekCpDirectory, e.Name())
		}
	}

	fmt.Printf("TOTAL BINARY EVENT STREAMS: %d\n", len(m))

	fmt.Println()

	return m
}

var seekCpCmd = &cobra.Command{
	Use:   "seekcp [flags] directory",
	Short: "Seek consistent point from binary logs / transaction events",
	Long:  "Seek consistent point from binary logs / transaction events",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("please specify the directory")
		}
		seekCpDirectory = args[0]
		return nil
	},
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		var txParsers map[string]tx.TransactionEventParser
		if seekCpBinEvents {
			// Build from binary events.
			binEventFiles := buildBinaryEventFiles()

			txParsers = make(map[string]tx.TransactionEventParser)
			for streamName, eventFile := range binEventFiles {
				f, err := os.Open(eventFile)
				if err != nil {
					return err
				}
				//goland:noinspection ALL
				defer f.Close()
				p, err := tx.NewBinaryTransactionEventParser(bufio.NewReader(f))
				if err != nil {
					return err
				}
				txParsers[streamName] = p
			}
		} else {
			// Build from binary logs.
			findAndCollectAllBinlogFiles()

			var err error
			txParsers, err = buildAllTransactionParsers(sortedBinaryLogsByStream)
			if err != nil {
				return err
			}
		}

		doSeekConsistentPoint(txParsers, seekCpHeartbeatTxid)
		return nil
	}),
}
