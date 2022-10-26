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

	"github.com/spf13/cobra"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/tx"
)

var (
	txShowFile  string
	txShowSkip  int
	txShowCount int
	txShowTxid  uint64
)

func init() {
	txShowCmd.Flags().Uint64Var(&txShowTxid, "txid", 0, "target transaction id")
	txShowCmd.Flags().IntVarP(&txShowSkip, "skip", "s", 0, "skip the first N events")
	txShowCmd.Flags().IntVarP(&txShowCount, "count", "n", 0, "show the specified N events")

	rootCmd.AddCommand(txShowCmd)
}

var txShowCmd = &cobra.Command{
	Use:   "txshow [flags] file",
	Short: "View transaction logs in binary format",
	Long:  "View transaction logs in binary format",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("please provide a file")
		}
		txShowFile = args[0]
		return nil
	},
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		f, err := os.Open(txShowFile)
		if err != nil {
			return err
		}
		defer f.Close()

		var r io.Reader
		r = bufio.NewReader(f)

		of := bufio.NewWriter(os.Stdout)
		defer of.Flush()

		p, err := tx.NewBinaryTransactionEventParser(r)
		if err != nil {
			return err
		}
		i := 0
		return p.Parse(func(event *tx.Event) error {
			defer func() {
				i++
			}()
			if txShowTxid > 0 {
				if event.XID == txShowTxid {
					_, _ = fmt.Fprintln(of, event.String())
				}
				return nil
			} else {
				if i < txShowSkip {
					return nil
				}
				_, _ = fmt.Fprintln(of, event.String())
				if txShowCount > 0 && i >= txShowSkip+txShowCount {
					return tx.StopParse
				}
				return nil
			}
		})
	}),
}
