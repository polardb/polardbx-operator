/*
Copyright 2021 Alibaba Group Holding Limited.

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
