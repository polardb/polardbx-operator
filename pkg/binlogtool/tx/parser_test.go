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

package tx

import (
	"bufio"
	"fmt"
	"os"
	"testing"
)

func parseBinlogAndOutputTransLog(t *testing.T, binlogFile string, outputFile string) {
	f, err := os.Open(binlogFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	scanner, err := binlog.NewLogEventScanner(bufio.NewReader(f),
		binlog.WithBinlogFile(binlogFile),
		binlog.WithLogEventHeaderFilter(TransactionEventParserHeaderFilter()),
	)
	if err != nil {
		t.Fatal(err)
	}

	output, err := os.OpenFile(outputFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0755)
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	w := bufio.NewWriter(output)
	defer w.Flush()

	parser := NewTransactionEventParser(scanner)
	if err := parser.Parse(func(event *Event) error {
		_, err := fmt.Fprintln(w, event)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTransactionEventParser_Parse(t *testing.T) {
	const binlogFile = "/Users/shunjie.dsj/Documents/mysql-bin.000351"
	parseBinlogAndOutputTransLog(t, binlogFile, "/tmp/trans.57.log")
}

func TestTransactionEventParser_Parse_MySQL8(t *testing.T) {
	const binlogFile = "/Users/shunjie.dsj/Documents/master-bin.000288"
	parseBinlogAndOutputTransLog(t, binlogFile, "/tmp/trans.80.log")
}

func TestTransactionEventParser_ParseSpeed(t *testing.T) {
	const binlogFile = "/Users/shunjie.dsj/Documents/mysql-bin.000351"
	f, err := os.Open(binlogFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	scanner, err := binlog.NewLogEventScanner(bufio.NewReaderSize(f, 8192),
		binlog.WithLogEventHeaderFilter(TransactionEventParserHeaderFilter()),
	)
	if err != nil {
		t.Fatal(err)
	}

	parser := NewTransactionEventParser(scanner)
	if err := parser.Parse(func(event *Event) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
