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
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/rows"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/str"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/bitmap"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"
)

var (
	showBinlogFile               string
	showBinlogStartOffset        uint64
	showBinlogEndOffset          uint64
	showBinlogInterestEventTypes []string
	showBinlogIncludeHeader      bool
	showBinlogScanHeaderOnly     bool
	showBinlogPrintHeaderOnly    bool
	showBinlogSkip               int
	showBinlogPrecede            int
	showBinlogCount              int
	showBinlogCharset            string
	showBinlogChecksum           string
	showBinlogValidateChecksum   bool
	showOutputFile               string
	showNoOutput                 bool
	showDecodeRows               bool
	showRowsTargetSchema         string
	showRowsTargetTable          string
	showDecodeBlob               bool
	showStartTimestamp           uint32
	showEndTimestamp             uint32
	showRowsPredicate            string
	showBinlogTailCount          int
	showQuery                    string
)

func init() {
	showCmd.Flags().Uint64Var(&showBinlogStartOffset, "start-offset", 0, "start offset in bytes")
	showCmd.Flags().Uint64Var(&showBinlogEndOffset, "end-offset", 0, "end offset in bytes")
	showCmd.Flags().StringSliceVarP(&showBinlogInterestEventTypes, "event", "e", nil, "interested event types")
	showCmd.Flags().BoolVar(&showBinlogIncludeHeader, "header", false, "show headers")
	showCmd.Flags().BoolVar(&showBinlogScanHeaderOnly, "header-only", false, "show headers only (scan only headers)")
	showCmd.Flags().BoolVar(&showBinlogPrintHeaderOnly, "print-header-only", false, "show headers only (print only headers)")
	showCmd.Flags().IntVarP(&showBinlogSkip, "skip", "s", 0, "skip the first s events")
	showCmd.Flags().IntVarP(&showBinlogCount, "count", "n", -1, "show n events (negative means no limit)")
	showCmd.Flags().StringVar(&showBinlogCharset, "charset", "utf-8", "charset to decode strings")
	showCmd.Flags().StringVar(&showBinlogChecksum, "checksum", "crc32", "binary log checksum (ignored for binary log version v1, v3 and v4 after 3.6.1)")
	showCmd.Flags().BoolVar(&showBinlogValidateChecksum, "validate-checksum", false, "turn on checksum validation")
	showCmd.Flags().StringVarP(&showOutputFile, "output", "o", "", "output file")
	showCmd.Flags().BoolVar(&showNoOutput, "no-output", false, "no output")
	showCmd.Flags().IntVarP(&showBinlogPrecede, "precede", "b", 0, "print n events before the start offset (only probe 64K bytes ahead)")
	showCmd.Flags().BoolVar(&showDecodeRows, "decode-rows", false, "print decoded rows (must allow table map event)")
	showCmd.Flags().StringVar(&showRowsTargetTable, "table", "", "filter table map event and rows event with matched table (regex)")
	showCmd.Flags().StringVar(&showRowsTargetSchema, "schema", "", "filter table map event and rows event with matched schema (regex)")
	showCmd.Flags().BoolVar(&showDecodeBlob, "decode-blobs", false, "print decoded blobs (change charset with --charset)")
	showCmd.Flags().Uint32Var(&showStartTimestamp, "start-ts", 0, "start timestamp in seconds (compared with event header)")
	showCmd.Flags().Uint32Var(&showEndTimestamp, "end-ts", 0, "end timestamp in seconds (compared with event header)")
	showCmd.Flags().StringVarP(&showRowsPredicate, "predicate", "p", "", `predicate to filter the rows event by values (must be specified with --decode-rows),
in the format of @i=value[,@j=value,...], where i is the column index start with 1`)
	showCmd.Flags().IntVarP(&showBinlogTailCount, "tail", "t", -1, "show tail n events")
	showCmd.Flags().StringVar(&showQuery, "query", "", "filter query event and rows query event with regex")
	rootCmd.AddCommand(showCmd)
}

const showProbeSize = 65536

var (
	showRowsTableRe            *regexp.Regexp
	showRowsSchemaRe           *regexp.Regexp
	showQueryRe                *regexp.Regexp
	showRowsPredicateKeyValues map[int]string
)

func wrapRegex(s string) string {
	if !strings.HasPrefix(s, "^") {
		s = "^" + s
	}
	if !strings.HasSuffix(s, "$") {
		s += "$"
	}
	return s
}

var showCmd = &cobra.Command{
	Use:   "show [flags] file",
	Short: "Show binlog events",
	Long:  "Show binlog events",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("please specify a binlog file")
		}
		showBinlogFile = args[0]
		return nil
	},
	PreRun: wrap(func(cmd *cobra.Command, args []string) error {
		var err error
		if showBinlogScanHeaderOnly {
			if showDecodeRows {
				return errors.New("--header-only and --decode-rows are not compatible")
			}
			if showRowsTargetSchema != "" {
				_, _ = fmt.Fprintln(os.Stderr, "WARN: ignore --schema values because --header-only is specified")
			}
			if showRowsTargetTable != "" {
				_, _ = fmt.Fprintln(os.Stderr, "WARN: ignore --table values because --header-only is specified")
			}
			if showQuery != "" {
				_, _ = fmt.Fprintln(os.Stderr, "WARN: ignore --query values because --header-only is specified")
			}
		} else {
			if showRowsPredicate != "" {
				if !showDecodeRows {
					return errors.New("--predicate must be applied with --decode-rows")
				}

				showRowsPredicateKeyValues, err = parsePredicate(showRowsPredicate)
				if err != nil {
					return fmt.Errorf("failed to parse predicates: %w", err)
				}
			}
			if showRowsTargetTable != "" {
				if showRowsTableRe, err = regexp.Compile(wrapRegex(showRowsTargetTable)); err != nil {
					return fmt.Errorf("failed to parse regex: %w", err)
				}
			}
			if showRowsTargetSchema != "" {
				if showRowsSchemaRe, err = regexp.Compile(wrapRegex(showRowsTargetSchema)); err != nil {
					return fmt.Errorf("failed to parse regex: %w", err)
				}
			}
			if showQuery != "" {
				if showQueryRe, err = regexp.Compile(showQuery); err != nil {
					return fmt.Errorf("faield to parse regex: %w", err)
				}
			}
			if showBinlogCharset != "utf-8" {
				enc, err := lookupEncoding(showBinlogCharset)
				if err != nil {
					return err
				}
				str.DefaultEncoding = enc
			}
			if showDecodeBlob {
				str.DecodeBlob = true
			}
		}
		return nil
	}),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		opts := []binlog.LogEventScannerOption{
			binlog.WithBinlogFile(showBinlogFile),
			binlog.WithChecksumAlgorithm(showBinlogChecksum),
		}

		if showBinlogStartOffset > 0 {
			// Enable probe when start offset specified.
			opts = append(opts, binlog.WithProbeFirstHeader(showProbeSize))
		}
		if showBinlogEndOffset > 0 {
			opts = append(opts, binlog.WithEndPos(showBinlogEndOffset))
		}

		// Control if table map events are displayed when decoded rows.
		var showTableMapEvents = true

		if showBinlogInterestEventTypes != nil {
			eventTypeCodes := utils.ConvertAndConcatenateSlice(showBinlogInterestEventTypes, guessEventType)
			eventTypeCodes = utils.SortedSlice(utils.DistinctSlice(eventTypeCodes))
			if showDecodeRows && !slices.Contains(eventTypeCodes, spec.TABLE_MAP_EVENT) {
				eventTypeCodes = append(eventTypeCodes, spec.TABLE_MAP_EVENT)
				showTableMapEvents = false
			}
			if (showRowsTargetSchema != "" || showRowsTargetTable != "") &&
				!slices.Contains(eventTypeCodes, spec.TABLE_MAP_EVENT) {
				return errors.New("enable table map / rows event filter but table map event is not in the interested types")
			}
			opts = append(opts, binlog.WithInterestedLogEventTypes(eventTypeCodes...))
		}
		if showStartTimestamp > 0 {
			opts = append(opts, binlog.WithLogEventHeaderFilter(func(header event.LogEventHeader) bool {
				return header.EventTimestamp() >= showStartTimestamp
			}))
		}
		if showBinlogValidateChecksum {
			opts = append(opts, binlog.EnableChecksumValidation)
		}
		if showBinlogScanHeaderOnly {
			opts = append(opts, binlog.WithScanMode(binlog.ScanModeHeaderOnly))
		}

		startOffset := showBinlogStartOffset
		if showBinlogPrecede > 0 {
			// Backoff probe size bytes.
			if startOffset >= showProbeSize {
				startOffset -= showProbeSize
			} else {
				startOffset = 0
			}
		}

		lazyScanner := binlog.NewLazyLogEventScanCloser(
			func() (io.ReadCloser, error) {
				f, err := os.Open(showBinlogFile)
				if err != nil {
					return nil, err
				}
				return utils.NewSeekableBufferReader(f), nil
			},
			startOffset,
			opts...,
		)
		defer lazyScanner.Close()

		var w *bufio.Writer
		if !showNoOutput {
			var of io.Writer
			if showOutputFile != "" {
				f, err := os.OpenFile(showOutputFile, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
				if err != nil {
					return err
				}
				defer f.Close()
				of = f
			} else {
				of = os.Stdout
			}
			w = bufio.NewWriter(of)
			defer w.Flush()
		}

		predicates := make([]binlog.LogEventFilterFunc, 0)

		if showQueryRe != nil {
			predicates = append(predicates, func(off binlog.EventOffset, ev event.LogEvent) bool {
				if ev.EventHeader().EventTypeCode() == spec.QUERY_EVENT {
					queryEv := ev.EventData().(*event.QueryEvent)
					return showQueryRe.MatchString(queryEv.Query.String())
				} else if ev.EventHeader().EventTypeCode() == spec.ROWS_QUERY_LOG_EVENT {
					queryEv := ev.EventData().(*event.RowsQueryEvent)
					return showQueryRe.MatchString(queryEv.Query.String())
				}
				return true
			})
		}

		tables := make(map[uint64]*event.TableMapEvent)
		recordTables := showRowsSchemaRe != nil || showRowsTableRe != nil || showDecodeRows
		if !showBinlogScanHeaderOnly {
			if recordTables {
				predicates = append(predicates,
					// Filter and record table.
					func(off binlog.EventOffset, ev event.LogEvent) bool {
						if ev.EventHeader().EventTypeCode() == spec.TABLE_MAP_EVENT {
							if tableMapEvent, ok := ev.EventData().(*event.TableMapEvent); ok {
								if showRowsSchemaRe != nil && !showRowsSchemaRe.MatchString(tableMapEvent.Schema.String()) {
									tables[tableMapEvent.TableID] = nil
									return false
								} else if showRowsTableRe != nil && !showRowsTableRe.MatchString(tableMapEvent.Table.String()) {
									tables[tableMapEvent.TableID] = nil
									return false
								} else {
									tables[tableMapEvent.TableID] = tableMapEvent
									return true
								}
							}
						}
						return true
					},
					// Filter rows event.
					func(off binlog.EventOffset, ev event.LogEvent) bool {
						if rowsEvent, ok := ev.EventData().(event.RowsEventVariants); ok {
							table, ok := tables[rowsEvent.AsRowsEvent().TableID]
							// Table not recorded or table not filtered.
							return !ok || table != nil
						}
						return true
					},
				)
			}
		}

		var s binlog.LogEventScanner = lazyScanner
		if len(predicates) > 0 {
			s = binlog.NewFilterLogEventScanner(s, predicates...)
		}

		if !showBinlogScanHeaderOnly {
			if showDecodeRows {
				s = binlog.NewMutateLogEventScanner(s, func(off binlog.EventOffset, ev event.LogEvent) (event.LogEvent, error) {
					// errTableFilteredOut should never happen
					return decodeRowsEvents(tables, ev)
				})
			}
			if showRowsPredicate != "" {
				s = binlog.NewFilterLogEventScanner(s, func(off binlog.EventOffset, ev event.LogEvent) bool {
					if rowsEvent, ok := ev.EventData().(rows.DecodedRowsEvent); ok {
						rows := rowsEvent.GetDecodedRows()
						for _, row := range rows {
							ok, err := matchesRow(row, rowsEvent.AsRowsEvent().Columns, showRowsPredicateKeyValues)
							if err == nil && ok {
								return true
							}
						}
						return false
					}
					return true
				})
			}

			var afterPredicates []binlog.LogEventFilterFunc
			if recordTables {
				afterPredicates = append(afterPredicates, // Clean table if it's an end statement.
					func(off binlog.EventOffset, ev event.LogEvent) bool {
						if rowsEvent, ok := ev.EventData().(event.RowsEventVariants); ok {
							if rowsEvent.AsRowsEvent().Flags&spec.STMT_END_F > 0 {
								tables = make(map[uint64]*event.TableMapEvent)
							}
						}
						return true
					})

			}
			if !showTableMapEvents {
				afterPredicates = append(afterPredicates,
					func(off binlog.EventOffset, ev event.LogEvent) bool {
						return ev.EventHeader().EventTypeCode() != spec.TABLE_MAP_EVENT
					})
			}
			if len(afterPredicates) > 0 {
				s = binlog.NewFilterLogEventScanner(s, afterPredicates...)
			}
		}

		if showBinlogPrecede > 0 {
			var pivotOff binlog.EventOffset
			var pivotEv event.LogEvent
			s = binlog.NewMultiLogEventScanner(
				// Events before start offset.
				binlog.NewTailLogEventScanner(
					binlog.NewMutateLogEventScanner(s, func(off binlog.EventOffset, ev event.LogEvent) (event.LogEvent, error) {
						if off.Offset-uint64(ev.EventHeader().TotalEventLength()) >= showBinlogStartOffset {
							pivotOff, pivotEv = off, ev
							return nil, binlog.EOF
						}
						return ev, nil
					}),
					showBinlogPrecede,
				),
				// Captured pivot event.
				binlog.NewOneEventScanner(&pivotOff, &pivotEv),
				// Events afterwards.
				s,
			)
		}

		if showBinlogSkip > 0 || showBinlogCount >= 0 {
			s = binlog.NewLimitedLogEventScanner(s, showBinlogSkip, showBinlogCount)
		}

		if showBinlogTailCount >= 0 {
			s = binlog.NewTailLogEventScanner(s, showBinlogTailCount)
		}

		for {
			off, ev, err := s.Next()
			if err != nil {
				if err == binlog.EOF {
					break
				}
				return err
			}

			if showEndTimestamp > 0 && ev.EventHeader().EventTimestamp() > showEndTimestamp {
				return nil
			}

			// Print event.
			if !showNoOutput {
				if err = showPrintEvent(w, off, ev); err != nil {
					return err
				}
			}
		}

		return nil
	}),
}

func parsePredicate(predicate string) (map[int]string, error) {
	m := make(map[int]string)
	for _, s := range strings.Split(predicate, ",") {
		s = strings.TrimSpace(s)
		kv := strings.SplitN(s, "=", 2)
		if len(kv) != 2 {
			return nil, errors.New("invalid predicate \"" + s + "\"")
		}
		if !strings.HasPrefix(kv[0], "@") {
			return nil, errors.New("invalid predicate \"" + s + "\"")
		}
		i, err := strconv.Atoi(kv[0][1:])
		if err != nil {
			return nil, fmt.Errorf("invalid predicate \"%s\": %w", s, err)
		}
		if _, ok := m[i-1]; ok {
			return nil, fmt.Errorf("invalid predicate \"%s\": duplicate column index", s)
		}
		m[i-1] = kv[1]
	}
	return m, nil
}

func matchesColumn(value any, expect string) (bool, error) {
	if value == nil {
		return expect == "NULL", nil
	}
	switch v := value.(type) {
	case uint8:
		e, err := strconv.ParseInt(expect, 10, 64)
		if err != nil {
			return false, fmt.Errorf("not parsable value: %w", err)
		}
		return uint64(v) == uint64(e), nil
	case uint16:
		e, err := strconv.ParseInt(expect, 10, 64)
		if err != nil {
			return false, fmt.Errorf("not parsable value: %w", err)
		}
		return uint64(v) == uint64(e), nil
	case uint32:
		e, err := strconv.ParseInt(expect, 10, 64)
		if err != nil {
			return false, fmt.Errorf("not parsable value: %w", err)
		}
		return uint64(v) == uint64(e), nil
	case uint64:
		e, err := strconv.ParseInt(expect, 10, 64)
		if err != nil {
			return false, fmt.Errorf("not parsable value: %w", err)
		}
		return v == uint64(e), nil
	case str.Str:
		return v.String() == expect, nil
	case float32:
		e, err := strconv.ParseFloat(expect, 64)
		if err != nil {
			return false, fmt.Errorf("not parsable value: %w", err)
		}
		return float64(v) == e, nil
	case float64:
		e, err := strconv.ParseFloat(expect, 64)
		if err != nil {
			return false, fmt.Errorf("not parsable value: %w", err)
		}
		return v == e, nil
	default:
		return false, fmt.Errorf("unsupported column value type: %t", v)
	}
}

func matchesRow(row rows.Row, columns bitmap.Bitmap, expect map[int]string) (bool, error) {
	j := 0
	for i := 0; i < columns.Len(); i++ {
		var value any
		if !columns.Get(i) {
			value = nil
		} else {
			value = row[j].Value
			j++
		}

		v, ok := expect[i]
		if !ok {
			continue
		}

		ok, err := matchesColumn(value, v)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func showPrintEvent(w io.Writer, off binlog.EventOffset, ev event.LogEvent) error {
	var err error
	if showBinlogIncludeHeader {
		_, err = fmt.Fprintf(w, "[%s] Length=%d StartPos=%d EndPos=%d ServerID=%d Timestamp=%d %s\n",
			ev.EventHeader().EventType(),
			ev.EventHeader().TotalEventLength(),
			off.Offset-uint64(ev.EventHeader().TotalEventLength()),
			off.Offset,
			ev.EventHeader().EventServerID(),
			ev.EventHeader().EventTimestamp(),
			utils.JsonPrettyFormat(ev))
	} else if showBinlogScanHeaderOnly || showBinlogPrintHeaderOnly {
		_, err = fmt.Fprintf(w, "[%s] Length=%d StartPos=%d EndPos=%d ServerID=%d Timestamp=%d\n",
			ev.EventHeader().EventType(),
			ev.EventHeader().TotalEventLength(),
			off.Offset-uint64(ev.EventHeader().TotalEventLength()),
			off.Offset,
			ev.EventHeader().EventServerID(),
			ev.EventHeader().EventTimestamp(),
		)
	} else {
		_, err = fmt.Fprintf(w, "[%s] Length=%d StartPos=%d EndPos=%d ServerID=%d Timestamp=%d %s\n",
			ev.EventHeader().EventType(),
			ev.EventHeader().TotalEventLength(),
			off.Offset-uint64(ev.EventHeader().TotalEventLength()),
			off.Offset,
			ev.EventHeader().EventServerID(),
			ev.EventHeader().EventTimestamp(),
			utils.JsonPrettyFormat(ev.EventData()))
	}
	return err
}

var (
	errTableNotFound    = errors.New("table not found")
	errTableFilteredOut = errors.New("table filtered out")
)

func decodeRowsEvents(tables map[uint64]*event.TableMapEvent, ev event.LogEvent) (event.LogEvent, error) {
	var table *event.TableMapEvent
	if rowsEvent, ok := ev.EventData().(event.RowsEventVariants); ok {
		table, ok = tables[rowsEvent.AsRowsEvent().TableID]
		if !ok {
			return nil, errTableNotFound
		}
		if table == nil {
			return ev, errTableFilteredOut
		}
	} else {
		return ev, nil
	}

	switch ev.EventHeader().EventTypeCode() {
	case spec.PRE_GA_DELETE_ROWS_EVENT, spec.DELETE_ROWS_EVENT_V1, spec.DELETE_ROWS_EVENT_V2:
		rowsEvent := ev.EventData().(*event.DeleteRowsEvent)
		body, err := rows.DecodeDeleteRowsEvent(rowsEvent, table)
		if err != nil {
			return nil, fmt.Errorf("decode rows error: %w", err)
		}
		ev, _ = event.NewLogEvent(ev.EventHeader(), *body)
		return ev, nil
	case spec.PRE_GA_WRITE_ROWS_EVENT, spec.WRITE_ROWS_EVENT_V1, spec.WRITE_ROWS_EVENT_V2:
		rowsEvent := ev.EventData().(*event.WriteRowsEvent)
		body, err := rows.DecodeWriteRowsEvent(rowsEvent, table)
		if err != nil {
			return nil, fmt.Errorf("decode rows error: %w", err)
		}
		ev, _ = event.NewLogEvent(ev.EventHeader(), *body)
		return ev, nil
	case spec.PRE_GA_UPDATE_ROWS_EVENT, spec.UPDATE_ROWS_EVENT_V1, spec.UPDATE_ROWS_EVENT_V2:
		rowsEvent := ev.EventData().(*event.UpdateRowsEvent)
		body, err := rows.DecodeUpdateRowsEvent(rowsEvent, table)
		if err != nil {
			return nil, fmt.Errorf("decode rows error: %w", err)
		}
		ev, _ = event.NewLogEvent(ev.EventHeader(), *body)
		return ev, nil
	default:
		return ev, nil
	}
}
