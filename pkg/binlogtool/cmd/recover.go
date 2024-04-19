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
	"database/sql"
	"errors"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/tx"
)

var (
	recoverConsistentPointFile string
	recoverHost                string
	recoverPort                int
	recoverUser                string
	recoverPasswd              string
	recoverableTxids           map[uint64]int
)

func init() {
	recoverCmd.Flags().Bool("help", false, "help for "+recoverCmd.Name())
	recoverCmd.Flags().StringVarP(&recoverConsistentPointFile, "file", "f", "", "file contains recoverable transaction ids")
	if err := recoverCmd.MarkFlagRequired("file"); err != nil {
		panic(err)
	}

	recoverCmd.Flags().StringVarP(&recoverHost, "host", "h", "127.0.0.1", "host")
	recoverCmd.Flags().IntVarP(&recoverPort, "port", "P", 3306, "port")
	recoverCmd.Flags().StringVarP(&recoverUser, "user", "u", "root", "username")
	recoverCmd.Flags().StringVarP(&recoverPasswd, "password", "p", "", "password")

	rootCmd.AddCommand(recoverCmd)
}

func readRecoverableTxids(file string) (map[uint64]int, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}

	var length uint32
	if err := layout.Number(&length).FromStream(gr); err != nil {
		return nil, err
	}

	txidsMap := make(map[uint64]int)

	var txid uint64
	txidL := layout.Number(&txid)
	for i := 0; i < int(length); i++ {
		if err := txidL.FromStream(gr); err != nil {
			return nil, err
		}
		txidsMap[txid] = 1
	}

	return txidsMap, nil
}

type xaTrans struct {
	formatId uint32
	gtrid    []byte
	bqual    []byte
}

func listPreparedXATransactions(db *sql.DB) ([]xaTrans, error) {
	rs, err := db.Query("XA RECOVER")
	if err != nil {
		return nil, fmt.Errorf("failed to query: %w", err)
	}
	defer rs.Close()

	xaTransList := make([]xaTrans, 0)
	var formatId uint32
	var gtridLen, bqualLen int
	var data []byte
	for rs.Next() {
		err := rs.Scan(&formatId, &gtridLen, &bqualLen, &data)
		if err != nil {
			return nil, fmt.Errorf("failed to scan result: %w", err)
		}
		if gtridLen+bqualLen != len(data) {
			return nil, fmt.Errorf("failed to extract XA transaction: %w", errors.New("size not match"))
		}
		fmt.Sprintf("XA RECOVER DATA: %s\n", data)
		xaTransList = append(xaTransList, xaTrans{
			formatId: formatId,
			gtrid:    data[:gtridLen],
			bqual:    data[gtridLen:],
		})
	}

	return xaTransList, nil
}

//goland:noinspection SqlDialectInspection,SqlNoDataSourceInspection
var recoverCmd = &cobra.Command{
	Use:   "recover [flags]",
	Short: "Recover XA transactions with consistent point",
	Long:  "Recover XA transactions with consistent point",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(recoverConsistentPointFile) == 0 {
			return errors.New("please provide the file")
		}
		return nil
	},
	PreRun: wrap(func(cmd *cobra.Command, args []string) error {
		var err error
		recoverableTxids, err = readRecoverableTxids(recoverConsistentPointFile)
		if err != nil {
			return err
		}
		return nil
	}),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/",
			recoverUser, recoverPasswd, recoverHost, recoverPort))
		if err != nil {
			return fmt.Errorf("failed to open db: %w", err)
		}
		defer db.Close()

		xaTransList, err := listPreparedXATransactions(db)
		if err != nil {
			return fmt.Errorf("failed to list prepared XA transactions: %w", err)
		}

		for _, xaTrans := range xaTransList {
			txid, err := tx.ParseXIDFromGtrid(xaTrans.gtrid)
			if err != nil {
				return fmt.Errorf("failed to parse xid from gtrid: %w", err)
			}
			if _, ok := recoverableTxids[txid]; ok {
				commitSQL := fmt.Sprintf("XA COMMIT '%s', '%s',%d",
					xaTrans.gtrid, xaTrans.bqual, xaTrans.formatId)
				fmt.Printf("Commit SQL : %s\n", commitSQL)
				_, err := db.Exec(commitSQL)
				if err != nil {
					return fmt.Errorf("failed to commit XA transaction: %w", err)
				}
			}
		}

		return nil
	}),
}
