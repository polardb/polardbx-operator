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

package binlog

import (
	"errors"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
)

func parseEvent(fde *event.FormatDescriptionEvent, header event.LogEventHeader, rawData []byte, strict bool) (event.LogEvent, error) {
	switch header.EventTypeCode() {
	case spec.FORMAT_DESCRIPTION_EVENT:
		return event.ExtractLogEventFromBlock[event.FormatDescriptionEvent](fde, header, rawData, strict)
	case spec.START_EVENT_V3:
		return event.ExtractLogEventFromBlock[event.StartEvent](fde, header, rawData, strict)
	case spec.QUERY_EVENT:
		return event.ExtractLogEventFromBlock[event.QueryEvent](fde, header, rawData, strict)
	case spec.STOP_EVENT:
		return event.ExtractLogEventFromBlock[event.StopEvent](fde, header, rawData, strict)
	case spec.ROTATE_EVENT:
		return event.ExtractLogEventFromBlock[event.RotateEvent](fde, header, rawData, strict)
	case spec.INTVAR_EVENT:
		return event.ExtractLogEventFromBlock[event.IntvarEvent](fde, header, rawData, strict)
	case spec.LOAD_EVENT:
		return event.ExtractLogEventFromBlock[event.LoadEvent](fde, header, rawData, strict)
	case spec.SLAVE_EVENT:
		return event.ExtractLogEventFromBlock[event.SlaveEvent](fde, header, rawData, strict)
	case spec.CREATE_FILE_EVENT:
		return event.ExtractLogEventFromBlock[event.CreateFileEvent](fde, header, rawData, strict)
	case spec.APPEND_BLOCK_EVENT:
		return event.ExtractLogEventFromBlock[event.AppendBlockEvent](fde, header, rawData, strict)
	case spec.EXEC_LOAD_EVENT:
		return event.ExtractLogEventFromBlock[event.ExecLoadEvent](fde, header, rawData, strict)
	case spec.DELETE_FILE_EVENT:
		return event.ExtractLogEventFromBlock[event.DeleteFileEvent](fde, header, rawData, strict)
	case spec.NEW_LOAD_EVENT:
		return event.ExtractLogEventFromBlock[event.NewLoadEvent](fde, header, rawData, strict)
	case spec.RAND_EVENT:
		return event.ExtractLogEventFromBlock[event.RandEvent](fde, header, rawData, strict)
	case spec.USER_VAR_EVENT:
		return event.ExtractLogEventFromBlock[event.UserVarEvent](fde, header, rawData, strict)
	case spec.XID_EVENT:
		return event.ExtractLogEventFromBlock[event.XIDEvent](fde, header, rawData, strict)
	case spec.BEGIN_LOAD_QUERY_EVENT:
		return event.ExtractLogEventFromBlock[event.BeginLoadQueryEvent](fde, header, rawData, strict)
	case spec.EXECUTE_LOAD_QUERY_EVENT:
		return event.ExtractLogEventFromBlock[event.ExecuteLoadQueryEvent](fde, header, rawData, strict)
	case spec.TABLE_MAP_EVENT:
		return event.ExtractLogEventFromBlock[event.TableMapEvent](fde, header, rawData, strict)
	case spec.PRE_GA_WRITE_ROWS_EVENT:
		return event.ExtractLogEventFromBlock[event.WriteRowsEvent](fde, header, rawData, strict)
	case spec.PRE_GA_UPDATE_ROWS_EVENT:
		return event.ExtractLogEventFromBlock[event.UpdateRowsEvent](fde, header, rawData, strict)
	case spec.PRE_GA_DELETE_ROWS_EVENT:
		return event.ExtractLogEventFromBlock[event.DeleteRowsEvent](fde, header, rawData, strict)
	case spec.WRITE_ROWS_EVENT_V1:
		return event.ExtractLogEventFromBlock[event.WriteRowsEvent](fde, header, rawData, strict)
	case spec.UPDATE_ROWS_EVENT_V1:
		return event.ExtractLogEventFromBlock[event.UpdateRowsEvent](fde, header, rawData, strict)
	case spec.DELETE_ROWS_EVENT_V1:
		return event.ExtractLogEventFromBlock[event.DeleteRowsEvent](fde, header, rawData, strict)
	case spec.WRITE_ROWS_EVENT_V2:
		return event.ExtractLogEventFromBlock[event.WriteRowsEvent](fde, header, rawData, strict)
	case spec.UPDATE_ROWS_EVENT_V2:
		return event.ExtractLogEventFromBlock[event.UpdateRowsEvent](fde, header, rawData, strict)
	case spec.DELETE_ROWS_EVENT_V2:
		return event.ExtractLogEventFromBlock[event.DeleteRowsEvent](fde, header, rawData, strict)
	case spec.INCIDENT_EVENT:
		return event.ExtractLogEventFromBlock[event.IncidentEvent](fde, header, rawData, strict)
	case spec.HEARTBEAT_LOG_EVENT:
		return event.ExtractLogEventFromBlock[event.HeartbeatEvent](fde, header, rawData, strict)
	case spec.ROWS_QUERY_LOG_EVENT:
		return event.ExtractLogEventFromBlock[event.RowsQueryEvent](fde, header, rawData, strict)
	case spec.GTID_LOG_EVENT:
		return event.ExtractLogEventFromBlock[event.GTIDLogEvent](fde, header, rawData, strict)
	case spec.ANONYMOUS_GTID_LOG_EVENT:
		return event.ExtractLogEventFromBlock[event.AnonymousGTIDEvent](fde, header, rawData, strict)
	case spec.PREVIOUS_GTIDS_LOG_EVENT:
		return event.ExtractLogEventFromBlock[event.PreviousGTIDsEvent](fde, header, rawData, strict)
	case spec.TRANSACTION_CONTEXT_EVENT:
		return event.ExtractLogEventFromBlock[event.TransactionPayloadEvent](fde, header, rawData, strict)
	case spec.VIEW_CHANGE_EVENT:
		return event.ExtractLogEventFromBlock[event.ViewChangeEvent](fde, header, rawData, strict)
	case spec.XA_PREPARE_LOG_EVENT:
		return event.ExtractLogEventFromBlock[event.XAPrepareEvent](fde, header, rawData, strict)
	case spec.SEQUENCE_EVENT:
		return event.ExtractLogEventFromBlock[event.SequenceEvent](fde, header, rawData, strict)
	case spec.GCN_LOG_EVENT:
		return event.ExtractLogEventFromBlock[event.GCNEvent](fde, header, rawData, strict)
	case spec.PARTIAL_UPDATE_ROWS_EVENT:
		return event.ExtractLogEventFromBlock[event.PartialUpdateRowsEvent](fde, header, rawData, strict)
	case spec.TRANSACTION_PAYLOAD_EVENT:
		return event.ExtractLogEventFromBlock[event.TransactionPayloadEvent](fde, header, rawData, strict)
	case spec.HEARTBEAT_LOG_EVENT_V2:
		return event.ExtractLogEventFromBlock[event.HeartbeatEventV2](fde, header, rawData, strict)
	case spec.CONSENSUS_LOG_EVENT:
		return event.ExtractLogEventFromBlock[event.ConsensusEvent](fde, header, rawData, strict)
	case spec.PREVIOUS_CONSENSUS_INDEX_LOG_EVENT:
		return event.ExtractLogEventFromBlock[event.PreviousConsensusIndexEvent](fde, header, rawData, strict)
	case spec.CONSENSUS_CLUSTER_INFO_EVENT:
		return event.ExtractLogEventFromBlock[event.ConsensusClusterInfoEvent](fde, header, rawData, strict)
	case spec.CONSENSUS_EMPTY_EVENT:
		return event.ExtractLogEventFromBlock[event.ConsensusEmptyEvent](fde, header, rawData, strict)
	case spec.GROUP_UPDATE_ROWS_EVENT:
		return event.ExtractLogEventFromBlock[event.GroupUpdateRows](fde, header, rawData, strict)
	case spec.PREVIOUS_PREPARED_XIDS_EVENT:
		return event.ExtractLogEventFromBlock[event.PreviousPreparedXIDsEvent](fde, header, rawData, strict)
	case spec.UNKNOWN_EVENT, spec.IGNORABLE_LOG_EVENT:
		return nil, errors.New("unknown/ignorable event")
	default:
		return nil, errors.New("unknown/ignorable event")
	}
}

func parseEventWithCache(layoutCache []*eventLayout, fde *event.FormatDescriptionEvent, header event.LogEventHeader, rawData []byte, strict bool) (event.LogEvent, error) {
	el := layoutCache[header.EventTypeCode()]
	if el == nil {
		return parseEvent(fde, header, rawData, strict)
	}
	switch header.EventTypeCode() {
	case spec.FORMAT_DESCRIPTION_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.FormatDescriptionEvent](el.event, el.l, header, rawData, strict)
	case spec.START_EVENT_V3:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.StartEvent](el.event, el.l, header, rawData, strict)
	case spec.QUERY_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.QueryEvent](el.event, el.l, header, rawData, strict)
	case spec.STOP_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.StopEvent](el.event, el.l, header, rawData, strict)
	case spec.ROTATE_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.RotateEvent](el.event, el.l, header, rawData, strict)
	case spec.INTVAR_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.IntvarEvent](el.event, el.l, header, rawData, strict)
	case spec.LOAD_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.LoadEvent](el.event, el.l, header, rawData, strict)
	case spec.SLAVE_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.SlaveEvent](el.event, el.l, header, rawData, strict)
	case spec.CREATE_FILE_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.CreateFileEvent](el.event, el.l, header, rawData, strict)
	case spec.APPEND_BLOCK_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.AppendBlockEvent](el.event, el.l, header, rawData, strict)
	case spec.EXEC_LOAD_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.ExecLoadEvent](el.event, el.l, header, rawData, strict)
	case spec.DELETE_FILE_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.DeleteFileEvent](el.event, el.l, header, rawData, strict)
	case spec.NEW_LOAD_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.NewLoadEvent](el.event, el.l, header, rawData, strict)
	case spec.RAND_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.RandEvent](el.event, el.l, header, rawData, strict)
	case spec.USER_VAR_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.UserVarEvent](el.event, el.l, header, rawData, strict)
	case spec.XID_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.XIDEvent](el.event, el.l, header, rawData, strict)
	case spec.BEGIN_LOAD_QUERY_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.BeginLoadQueryEvent](el.event, el.l, header, rawData, strict)
	case spec.EXECUTE_LOAD_QUERY_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.ExecuteLoadQueryEvent](el.event, el.l, header, rawData, strict)
	case spec.TABLE_MAP_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.TableMapEvent](el.event, el.l, header, rawData, strict)
	case spec.PRE_GA_WRITE_ROWS_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.WriteRowsEvent](el.event, el.l, header, rawData, strict)
	case spec.PRE_GA_UPDATE_ROWS_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.UpdateRowsEvent](el.event, el.l, header, rawData, strict)
	case spec.PRE_GA_DELETE_ROWS_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.DeleteRowsEvent](el.event, el.l, header, rawData, strict)
	case spec.WRITE_ROWS_EVENT_V1:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.WriteRowsEvent](el.event, el.l, header, rawData, strict)
	case spec.UPDATE_ROWS_EVENT_V1:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.UpdateRowsEvent](el.event, el.l, header, rawData, strict)
	case spec.DELETE_ROWS_EVENT_V1:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.DeleteRowsEvent](el.event, el.l, header, rawData, strict)
	case spec.WRITE_ROWS_EVENT_V2:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.WriteRowsEvent](el.event, el.l, header, rawData, strict)
	case spec.UPDATE_ROWS_EVENT_V2:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.UpdateRowsEvent](el.event, el.l, header, rawData, strict)
	case spec.DELETE_ROWS_EVENT_V2:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.DeleteRowsEvent](el.event, el.l, header, rawData, strict)
	case spec.INCIDENT_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.IncidentEvent](el.event, el.l, header, rawData, strict)
	case spec.HEARTBEAT_LOG_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.HeartbeatEvent](el.event, el.l, header, rawData, strict)
	case spec.ROWS_QUERY_LOG_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.RowsQueryEvent](el.event, el.l, header, rawData, strict)
	case spec.GTID_LOG_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.GTIDLogEvent](el.event, el.l, header, rawData, strict)
	case spec.ANONYMOUS_GTID_LOG_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.AnonymousGTIDEvent](el.event, el.l, header, rawData, strict)
	case spec.PREVIOUS_GTIDS_LOG_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.PreviousGTIDsEvent](el.event, el.l, header, rawData, strict)
	case spec.TRANSACTION_CONTEXT_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.TransactionPayloadEvent](el.event, el.l, header, rawData, strict)
	case spec.VIEW_CHANGE_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.ViewChangeEvent](el.event, el.l, header, rawData, strict)
	case spec.XA_PREPARE_LOG_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.XAPrepareEvent](el.event, el.l, header, rawData, strict)
	case spec.SEQUENCE_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.SequenceEvent](el.event, el.l, header, rawData, strict)
	case spec.GCN_LOG_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.GCNEvent](el.event, el.l, header, rawData, strict)
	case spec.PARTIAL_UPDATE_ROWS_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.PartialUpdateRowsEvent](el.event, el.l, header, rawData, strict)
	case spec.TRANSACTION_PAYLOAD_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.TransactionPayloadEvent](el.event, el.l, header, rawData, strict)
	case spec.HEARTBEAT_LOG_EVENT_V2:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.HeartbeatEventV2](el.event, el.l, header, rawData, strict)
	case spec.CONSENSUS_LOG_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.ConsensusEvent](el.event, el.l, header, rawData, strict)
	case spec.PREVIOUS_CONSENSUS_INDEX_LOG_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.PreviousConsensusIndexEvent](el.event, el.l, header, rawData, strict)
	case spec.CONSENSUS_CLUSTER_INFO_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.ConsensusClusterInfoEvent](el.event, el.l, header, rawData, strict)
	case spec.CONSENSUS_EMPTY_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.ConsensusEmptyEvent](el.event, el.l, header, rawData, strict)
	case spec.GROUP_UPDATE_ROWS_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.GroupUpdateRows](el.event, el.l, header, rawData, strict)
	case spec.PREVIOUS_PREPARED_XIDS_EVENT:
		return event.ExtractLogEventFromBlockWithLayoutCache[event.PreviousPreparedXIDsEvent](el.event, el.l, header, rawData, strict)
	case spec.UNKNOWN_EVENT, spec.IGNORABLE_LOG_EVENT:
		return nil, errors.New("unknown/ignorable event")
	default:
		return nil, errors.New("unknown/ignorable event")
	}
}
