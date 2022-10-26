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
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/layout"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
)

type eventLayout struct {
	event any
	l     *layout.Layout
}

func fromBlockWith[E any](el *eventLayout, bs []byte, e *E) (int, error) {
	defer func() {
		var e E
		*(el.event.(*E)) = e
	}()

	n, err := el.l.FromBlock(bs)
	if err != nil {
		return 0, err
	}
	*e = *(el.event.(*E))
	return n, nil
}

func newEventLayout[E any, LV event.LayoutViewEvent[E]](version uint32, code uint8, fde *event.FormatDescriptionEvent) eventLayout {
	var e E
	return eventLayout{event: &e, l: LV(&e).Layout(version, code, fde)}
}

func newEventBodyLayout(version uint32, code byte, fde *event.FormatDescriptionEvent) (eventLayout, bool) {
	switch code {
	case spec.FORMAT_DESCRIPTION_EVENT:
		return newEventLayout[event.FormatDescriptionEvent](version, code, fde), true
	case spec.START_EVENT_V3:
		return newEventLayout[event.StartEvent](version, code, fde), true
	case spec.QUERY_EVENT:
		return newEventLayout[event.QueryEvent](version, code, fde), true
	case spec.STOP_EVENT:
		return newEventLayout[event.StopEvent](version, code, fde), true
	case spec.ROTATE_EVENT:
		return newEventLayout[event.RotateEvent](version, code, fde), true
	case spec.INTVAR_EVENT:
		return newEventLayout[event.IntvarEvent](version, code, fde), true
	case spec.LOAD_EVENT:
		return newEventLayout[event.LoadEvent](version, code, fde), true
	case spec.SLAVE_EVENT:
		return newEventLayout[event.SlaveEvent](version, code, fde), true
	case spec.CREATE_FILE_EVENT:
		return newEventLayout[event.CreateFileEvent](version, code, fde), true
	case spec.APPEND_BLOCK_EVENT:
		return newEventLayout[event.AppendBlockEvent](version, code, fde), true
	case spec.EXEC_LOAD_EVENT:
		return newEventLayout[event.ExecLoadEvent](version, code, fde), true
	case spec.DELETE_FILE_EVENT:
		return newEventLayout[event.DeleteFileEvent](version, code, fde), true
	case spec.NEW_LOAD_EVENT:
		return newEventLayout[event.NewLoadEvent](version, code, fde), true
	case spec.RAND_EVENT:
		return newEventLayout[event.RandEvent](version, code, fde), true
	case spec.USER_VAR_EVENT:
		return newEventLayout[event.UserVarEvent](version, code, fde), true
	case spec.XID_EVENT:
		return newEventLayout[event.XIDEvent](version, code, fde), true
	case spec.BEGIN_LOAD_QUERY_EVENT:
		return newEventLayout[event.BeginLoadQueryEvent](version, code, fde), true
	case spec.EXECUTE_LOAD_QUERY_EVENT:
		return newEventLayout[event.ExecuteLoadQueryEvent](version, code, fde), true
	case spec.TABLE_MAP_EVENT:
		return newEventLayout[event.TableMapEvent](version, code, fde), true
	case spec.PRE_GA_WRITE_ROWS_EVENT:
		return newEventLayout[event.WriteRowsEvent](version, code, fde), true
	case spec.PRE_GA_UPDATE_ROWS_EVENT:
		return newEventLayout[event.UpdateRowsEvent](version, code, fde), true
	case spec.PRE_GA_DELETE_ROWS_EVENT:
		return newEventLayout[event.DeleteRowsEvent](version, code, fde), true
	case spec.WRITE_ROWS_EVENT_V1:
		return newEventLayout[event.WriteRowsEvent](version, code, fde), true
	case spec.UPDATE_ROWS_EVENT_V1:
		return newEventLayout[event.UpdateRowsEvent](version, code, fde), true
	case spec.DELETE_ROWS_EVENT_V1:
		return newEventLayout[event.DeleteRowsEvent](version, code, fde), true
	case spec.WRITE_ROWS_EVENT_V2:
		return newEventLayout[event.WriteRowsEvent](version, code, fde), true
	case spec.UPDATE_ROWS_EVENT_V2:
		return newEventLayout[event.UpdateRowsEvent](version, code, fde), true
	case spec.DELETE_ROWS_EVENT_V2:
		return newEventLayout[event.DeleteRowsEvent](version, code, fde), true
	case spec.INCIDENT_EVENT:
		return newEventLayout[event.IncidentEvent](version, code, fde), true
	case spec.HEARTBEAT_LOG_EVENT:
		return newEventLayout[event.HeartbeatEvent](version, code, fde), true
	case spec.ROWS_QUERY_LOG_EVENT:
		return newEventLayout[event.RowsQueryEvent](version, code, fde), true
	case spec.GTID_LOG_EVENT:
		return newEventLayout[event.GTIDLogEvent](version, code, fde), true
	case spec.ANONYMOUS_GTID_LOG_EVENT:
		return newEventLayout[event.AnonymousGTIDEvent](version, code, fde), true
	case spec.PREVIOUS_GTIDS_LOG_EVENT:
		return newEventLayout[event.PreviousGTIDsEvent](version, code, fde), true
	case spec.TRANSACTION_CONTEXT_EVENT:
		return newEventLayout[event.TransactionPayloadEvent](version, code, fde), true
	case spec.VIEW_CHANGE_EVENT:
		return newEventLayout[event.ViewChangeEvent](version, code, fde), true
	case spec.XA_PREPARE_LOG_EVENT:
		return newEventLayout[event.XAPrepareEvent](version, code, fde), true
	case spec.SEQUENCE_EVENT:
		return newEventLayout[event.SequenceEvent](version, code, fde), true
	case spec.GCN_LOG_EVENT:
		return newEventLayout[event.GCNEvent](version, code, fde), true
	case spec.PARTIAL_UPDATE_ROWS_EVENT:
		return newEventLayout[event.PartialUpdateRowsEvent](version, code, fde), true
	case spec.TRANSACTION_PAYLOAD_EVENT:
		return newEventLayout[event.TransactionPayloadEvent](version, code, fde), true
	case spec.HEARTBEAT_LOG_EVENT_V2:
		return newEventLayout[event.HeartbeatEventV2](version, code, fde), true
	case spec.CONSENSUS_LOG_EVENT:
		return newEventLayout[event.ConsensusEvent](version, code, fde), true
	case spec.PREVIOUS_CONSENSUS_INDEX_LOG_EVENT:
		return newEventLayout[event.PreviousConsensusIndexEvent](version, code, fde), true
	case spec.CONSENSUS_CLUSTER_INFO_EVENT:
		return newEventLayout[event.ConsensusClusterInfoEvent](version, code, fde), true
	case spec.CONSENSUS_EMPTY_EVENT:
		return newEventLayout[event.ConsensusEmptyEvent](version, code, fde), true
	case spec.GROUP_UPDATE_ROWS_EVENT:
		return newEventLayout[event.GroupUpdateRows](version, code, fde), true
	case spec.PREVIOUS_PREPARED_XIDS_EVENT:
		return newEventLayout[event.PreviousPreparedXIDsEvent](version, code, fde), true
	case spec.UNKNOWN_EVENT, spec.IGNORABLE_LOG_EVENT:
		return eventLayout{}, false
	default:
		return eventLayout{}, false
	}
}

func newEventBodyLayoutCache(version uint32, fde *event.FormatDescriptionEvent) []*eventLayout {
	m := make([]*eventLayout, 256)
	for i := 0; i < 256; i++ {
		if el, ok := newEventBodyLayout(version, byte(i), fde); ok {
			m[byte(i)] = &el
		}
	}
	return m
}
