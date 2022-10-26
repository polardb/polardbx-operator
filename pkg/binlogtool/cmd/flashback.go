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
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/spf13/cobra"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/bitmap"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/utils"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/event"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/meta"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/rows"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/str"
)

var (
	flashbackChecksum string
	flashbackFiles    []string
	flashbackDuration time.Duration
	flashbackSchema   string
	flashbackTable    string
)

func init() {
	flashbackCmd.Flags().DurationVarP(&flashbackDuration, "back", "b", 5*time.Minute, "duration before now to flashback to")
	flashbackCmd.Flags().StringVar(&flashbackSchema, "schema", "", "target schema (regex)")
	flashbackCmd.Flags().StringVar(&flashbackTable, "table", "", "target table (regex)")
	flashbackCmd.Flags().StringVar(&flashbackChecksum, "checksum", "crc32", "binlog checksum algorithm (ignored for binary log version v1, v3 and v4 after 3.6.1)")

	rootCmd.AddCommand(flashbackCmd)
}

var (
	flashbackTime        time.Time
	flashbackBinlogFiles []meta.BinlogFile
	flashbackSchemaRe    *regexp.Regexp
	flashbackTableRe     *regexp.Regexp
)

var flashbackCmd = &cobra.Command{
	Use:   "flashback [flags] file [files...]",
	Short: "Generate SQL statements to rollback delete operations. (experimental)",
	Long:  "Generate SQL statements to rollback delete operations. (experimental)",
	PreRun: wrap(func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.New("please specify a binlog file")
		}
		flashbackFiles = args
		var err error
		flashbackBinlogFiles, err = meta.ParseBinlogFilesAndSortByIndex(flashbackFiles...)
		if err != nil {
			return err
		}

		flashbackTime = time.Now().Add(-flashbackDuration)

		if flashbackSchema != "" {
			flashbackSchemaRe, err = regexp.Compile(wrapRegex(flashbackSchema))
			if err != nil {
				return fmt.Errorf("unable to parse regex: %w", err)
			}
		}
		if flashbackTable != "" {
			flashbackTableRe, err = regexp.Compile(wrapRegex(flashbackTable))
			if err != nil {
				return fmt.Errorf("unable to parse regex: %w", err)
			}
		}

		return nil
	}),
	Run: wrap(func(cmd *cobra.Command, args []string) error {
		flashbackTimestamp := uint32(flashbackTime.Unix())

		// Build binlog file scanners.
		scanners := make([]binlog.LogEventScanCloser, 0, len(flashbackBinlogFiles))
		for i := range flashbackBinlogFiles {
			bf := flashbackBinlogFiles[i]
			s := binlog.NewLazyLogEventScanCloser(
				func() (io.ReadCloser, error) {
					f, err := os.Open(bf.File)
					if err != nil {
						return nil, err
					}
					return utils.NewSeekableBufferReader(f), nil
				},
				0,
				binlog.WithChecksumAlgorithm(flashbackChecksum),
				binlog.WithBinlogFile(bf.File),
				binlog.WithLogEventHeaderFilter(func(header event.LogEventHeader) bool {
					return header.EventTimestamp() >= flashbackTimestamp
				}),
				binlog.WithInterestedLogEventTypes(utils.ConvertAndConcatenateSlice([]string{"rows", "tablemap"}, guessEventType)...),
			)

			scanners = append(scanners, s)
		}

		sc := binlog.NewMultiLogEventScanCloser(scanners...)
		defer sc.Close()

		// Table context and row event mutate.
		tables := make(map[uint64]*event.TableMapEvent)
		results := make([]string, 0)
		s := binlog.NewFilterLogEventScanner(
			binlog.NewMutateLogEventScanner(
				binlog.NewFilterLogEventScanner(sc,
					// Filter and record table.
					func(off binlog.EventOffset, ev event.LogEvent) bool {
						if ev.EventHeader().EventTypeCode() == spec.TABLE_MAP_EVENT {
							if tableMapEvent, ok := ev.EventData().(*event.TableMapEvent); ok {
								if flashbackSchemaRe != nil && !flashbackSchemaRe.MatchString(tableMapEvent.Schema.String()) {
									tables[tableMapEvent.TableID] = nil
									return false
								} else if flashbackTableRe != nil && !flashbackTableRe.MatchString(tableMapEvent.Table.String()) {
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
				),
				// Decode delete rows events.
				func(off binlog.EventOffset, ev event.LogEvent) (event.LogEvent, error) {
					// errTableFilteredOut should never happen
					switch ev.EventHeader().EventTypeCode() {
					case spec.PRE_GA_DELETE_ROWS_EVENT, spec.DELETE_ROWS_EVENT_V1, spec.DELETE_ROWS_EVENT_V2:
						return decodeRowsEvents(tables, ev)
					default:
						return ev, nil
					}
				}),
			// Clean table if it's an end statement. Also generate result for rows event.
			func(off binlog.EventOffset, ev event.LogEvent) bool {
				if rowsEvent, ok := ev.EventData().(event.RowsEventVariants); ok {
					if deleteRowsEvent, ok := rowsEvent.(*rows.DecodedDeleteRowsEvent); ok {
						table := tables[rowsEvent.AsRowsEvent().TableID]
						results = append(results, newRollbackStatement(table, deleteRowsEvent)...)
					}

					if rowsEvent.AsRowsEvent().Flags&spec.STMT_END_F > 0 {
						tables = make(map[uint64]*event.TableMapEvent)
					}
				}
				return true
			},
		)

		// Run to end.
		for {
			_, _, err := s.Next()
			if err != nil {
				if err == binlog.EOF {
					break
				}
				return err
			}
		}

		// Print result (reverse).
		w := bufio.NewWriter(os.Stdout)
		defer w.Flush()
		for i := len(results) - 1; i >= 0; i-- {
			_, _ = fmt.Fprintln(w, results[i])
		}

		return nil
	}),
}

// Copied from go-mysql driver

const digits01 = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
const digits10 = "0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999"

func appendDateTime(buf []byte, t time.Time) ([]byte, error) {
	year, month, day := t.Date()
	hour, min, sec := t.Clock()
	nsec := t.Nanosecond()

	if year < 1 || year > 9999 {
		return buf, errors.New("year is not in the range [1, 9999]: " + strconv.Itoa(year)) // use errors.New instead of fmt.Errorf to avoid year escape to heap
	}
	year100 := year / 100
	year1 := year % 100

	var localBuf [len("2006-01-02T15:04:05.999999999")]byte // does not escape
	localBuf[0], localBuf[1], localBuf[2], localBuf[3] = digits10[year100], digits01[year100], digits10[year1], digits01[year1]
	localBuf[4] = '-'
	localBuf[5], localBuf[6] = digits10[month], digits01[month]
	localBuf[7] = '-'
	localBuf[8], localBuf[9] = digits10[day], digits01[day]

	if hour == 0 && min == 0 && sec == 0 && nsec == 0 {
		return append(buf, localBuf[:10]...), nil
	}

	localBuf[10] = ' '
	localBuf[11], localBuf[12] = digits10[hour], digits01[hour]
	localBuf[13] = ':'
	localBuf[14], localBuf[15] = digits10[min], digits01[min]
	localBuf[16] = ':'
	localBuf[17], localBuf[18] = digits10[sec], digits01[sec]

	if nsec == 0 {
		return append(buf, localBuf[:19]...), nil
	}
	nsec100000000 := nsec / 100000000
	nsec1000000 := (nsec / 1000000) % 100
	nsec10000 := (nsec / 10000) % 100
	nsec100 := (nsec / 100) % 100
	nsec1 := nsec % 100
	localBuf[19] = '.'

	// milli second
	localBuf[20], localBuf[21], localBuf[22] =
		digits01[nsec100000000], digits10[nsec1000000], digits01[nsec1000000]
	// micro second
	localBuf[23], localBuf[24], localBuf[25] =
		digits10[nsec10000], digits01[nsec10000], digits10[nsec100]
	// nano second
	localBuf[26], localBuf[27], localBuf[28] =
		digits01[nsec100], digits10[nsec1], digits01[nsec1]

	// trim trailing zeros
	n := len(localBuf)
	for n > 0 && localBuf[n-1] == '0' {
		n--
	}

	return append(buf, localBuf[:n]...), nil
}

func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		// Grow buffer exponentially
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}

func escapeBytesQuotes(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

func escapeStringQuotes(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

func interpolateParams(query string, args []driver.Value) (string, error) {
	// Number of ? should be same to len(args)
	if strings.Count(query, "?") != len(args) {
		return "", driver.ErrSkip
	}

	buf := make([]byte, 0, 1024)
	buf = buf[:0]
	argPos := 0
	var err error

	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf = append(buf, query[i:]...)
			break
		}
		buf = append(buf, query[i:i+q]...)
		i += q

		arg := args[argPos]
		argPos++

		if arg == nil {
			buf = append(buf, "NULL"...)
			continue
		}

		switch v := arg.(type) {
		case int8:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int16:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int32:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case uint8:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case uint16:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case uint32:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case uint64:
			// Handle uint64 explicitly because our custom ConvertValue emits unsigned values
			buf = strconv.AppendUint(buf, v, 10)
		case float32:
			buf = strconv.AppendFloat(buf, float64(v), 'g', -1, 32)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case time.Time:
			if v.IsZero() {
				buf = append(buf, "'0000-00-00'"...)
			} else {
				buf = append(buf, '\'')
				buf, err = appendDateTime(buf, v)
				if err != nil {
					return "", err
				}
				buf = append(buf, '\'')
			}
		case json.RawMessage:
			buf = append(buf, '\'')
			buf = escapeBytesQuotes(buf, v)
			buf = append(buf, '\'')
		case str.Str:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				buf = append(buf, '\'')
				buf = escapeBytesQuotes(buf, v)
				buf = append(buf, '\'')
			}
		case str.Blob:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				buf = append(buf, "_binary'"...)
				buf = escapeBytesQuotes(buf, v)
				buf = append(buf, '\'')
			}
		case []byte:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				buf = append(buf, "_binary'"...)
				buf = escapeBytesQuotes(buf, v)
				buf = append(buf, '\'')
			}
		case string:
			buf = append(buf, '\'')
			buf = escapeStringQuotes(buf, v)
			buf = append(buf, '\'')
		case bitmap.Bitmap:
			buf = append(buf, '\'')
			buf = append(buf, v.String()...)
			buf = append(buf, '\'')
		case decimal.Decimal:
			buf = append(buf, v.String()...)
		case time.Duration:
			buf = append(buf, '\'')
			buf = append(buf, fmt.Sprintf("%d:%d:%d.%d", v/time.Hour, v/time.Minute, v/time.Second, v/time.Microsecond*100000)...)
			buf = append(buf, '\'')
		default:
			return "", driver.ErrSkip
		}
	}
	if argPos != len(args) {
		return "", driver.ErrSkip
	}
	return string(buf), nil
}

func repeat(s string, n int) []string {
	r := make([]string, 0, n)
	for i := 0; i < n; i++ {
		r = append(r, s)
	}
	return r
}

func newRollbackStatement(table *event.TableMapEvent, deleteRows *rows.DecodedDeleteRowsEvent) []string {
	statements := make([]string, 0, len(deleteRows.DecodedRows))
	for _, row := range deleteRows.DecodedRows {
		s := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES (\n%s\n);", table.Schema.String(), table.Table.String(),
			strings.Join(repeat("\t?", len(row)), ", \n"))
		values := utils.ConvertSlice(row, func(t rows.Column) driver.Value {
			return t.Value
		})
		stmt, err := interpolateParams(s, values)
		if err != nil {
			panic("unable to interpolate param: " + err.Error())
		}
		statements = append(statements, stmt)
	}
	return statements
}
