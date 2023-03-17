package pitr

import (
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog"
	"github.com/go-logr/logr"
	"go.uber.org/atomic"
)

type Context struct {
	TaskConfig            *TaskConfig
	Logger                logr.Logger
	RestoreBinlogs        []RestoreBinlog
	ConsistentXStoreCount int
	CpHeartbeatXid        uint64
	Borders               map[string]binlog.EventOffset
	LastErr               error
	RecoverTxsBytes       []byte
	Closed                atomic.Bool
}

func (pCtx *Context) NeedConsistentPoint() bool {
	return pCtx.ConsistentXStoreCount > 1
}
