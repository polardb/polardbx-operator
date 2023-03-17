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

package algo

import (
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog"
	"os"
	"testing"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/tx"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

func testLocateHeartbeat_Perform(t *testing.T, binlogFile string, opt LocateHeartbeatOption, expectTxid uint64) {
	f, err := os.Open(binlogFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	scanner, err := binlog.NewLogEventScanner(
		utils.NewSeekableBufferReader(f),
		binlog.WithBinlogFile(binlogFile),
		binlog.WithLogEventHeaderFilter(tx.TransactionEventParserHeaderFilter()),
	)
	if err != nil {
		t.Fatal(err)
	}
	parser := tx.NewTransactionEventParser(scanner)

	lh := NewLocateHeartbeat(parser, opt)
	txid, _, _, _, err := lh.Perform()
	if err != nil && err != ErrTargetHeartbeatTransactionNotFound {
		t.Fatal(err)
	}

	if expectTxid != txid {
		t.Fatalf("expect: %d, actual: %d", expectTxid, txid)
	}
}

func TestLocateHeartbeat_Perform(t *testing.T) {
	const binlogFile = "/Users/shunjie.dsj/Documents/mysql-bin.000351"
	testLocateHeartbeat_Perform(t, binlogFile, WithCommitTSPolicy(6914146952644919360, ExactCommitTS), 1445212168193581057)
}
