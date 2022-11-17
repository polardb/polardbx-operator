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
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/str"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
	"github.com/spf13/cobra"
	"io"
	"os"
)

var (
	consistentPointFile string
)

func init() {
	cpShowCmd.Flags().StringVarP(&consistentPointFile, "file", "f", "", "file contains recoverable transaction ids")

	rootCmd.AddCommand(cpShowCmd)
}

// =========Consistent point file layout===========
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
func parseConsistentPoint(gr io.Reader) (map[uint64]int, map[string]binlog.EventOffset, error) {
	var length uint32
	if err := layout.Number(&length).FromStream(gr); err != nil {
		return nil, nil, err
	}

	txidsMap := make(map[uint64]int)

	var txid uint64
	txidL := layout.Number(&txid)
	for i := 0; i < int(length); i++ {
		if err := txidL.FromStream(gr); err != nil {
			return nil, nil, err
		}
		txidsMap[txid] = 1
	}

	var streamLength uint16
	if err := layout.Number(&streamLength).FromStream(gr); err != nil {
		return nil, nil, err
	}
	borders := make(map[string]binlog.EventOffset, streamLength)
	var streamNameLength uint8
	var streamName str.Str
	var binlogNameLength uint8
	var binlogName str.Str
	var offset uint64
	decl := layout.Decl(
		layout.Number(&streamNameLength),
		layout.Bytes(&streamNameLength, &streamName),
		layout.Number(&binlogNameLength),
		layout.Bytes(&binlogNameLength, &binlogName),
		layout.Number(&offset))
	for i := 0; i < int(streamLength); i++ {
		if err := decl.FromStream(gr); err != nil {
			return nil, nil, err
		}
		borders[streamName.String()] = binlog.EventOffset{
			File:   binlogName.String(),
			Offset: offset,
		}
	}

	return txidsMap, borders, nil
}

var cpShowCmd = &cobra.Command{
	Use:   "cpshow [flags] file",
	Short: "View consistent point file",
	Long:  "View consistent point file",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(consistentPointFile) == 0 {
			return errors.New("please provide the file")
		}
		return nil
	},
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		f, err := os.Open(consistentPointFile)
		if err != nil {
			return err
		}
		defer f.Close()

		r := bufio.NewReader(f)
		gr, err := gzip.NewReader(r)
		if err != nil {
			return err
		}
		defer gr.Close()

		recoverableTxids, borders, err := parseConsistentPoint(gr)
		if err != nil {
			return err
		}

		fmt.Printf("TOTAL RECOVERABLE TRANSACTIONS: %d\n", len(recoverableTxids))
		txids := make([]uint64, 0)
		for txid, _ := range recoverableTxids {
			txids = append(txids, txid)
		}
		fmt.Printf("  %s\n", utils.JoinIntegerSequence(txids, ", "))

		fmt.Println()

		fmt.Println("BINLOG INDEXES: ")
		for name, border := range borders {
			fmt.Printf("  %s:  %s\n", name, border.String())
		}
		fmt.Println()
		return nil
	}),
}
