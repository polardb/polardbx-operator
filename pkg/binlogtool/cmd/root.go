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
	"os"
	"runtime/pprof"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"

	"github.com/alibaba/polardbx-operator/pkg/binlogtool/binlog/spec"
)

var eventTypeMap = map[string]uint8{}

func init() {
	for i := 0; i <= 255; i++ {
		s := spec.EventTypeName(byte(i))
		s = strings.ToLower(s)
		s = strings.ReplaceAll(s, "_", "")
		eventTypeMap[s] = uint8(i)
	}
	delete(eventTypeMap, "unrecognized")
}

func guessEventType(s string) []uint8 {
	code, err := strconv.ParseInt(strings.TrimSpace(s), 10, 8)
	if err == nil {
		return []uint8{uint8(code)}
	}
	// To lower (camel case), remove underscore (snake case).
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "_", "")
	switch s {
	case "writerows":
		return []uint8{spec.PRE_GA_WRITE_ROWS_EVENT, spec.WRITE_ROWS_EVENT_V1, spec.WRITE_ROWS_EVENT_V2}
	case "updaterows":
		return []uint8{spec.PRE_GA_UPDATE_ROWS_EVENT, spec.UPDATE_ROWS_EVENT_V1, spec.UPDATE_ROWS_EVENT_V2}
	case "deleterows":
		return []uint8{spec.PRE_GA_DELETE_ROWS_EVENT, spec.DELETE_ROWS_EVENT_V1, spec.DELETE_ROWS_EVENT_V2}
	case "rows":
		return []uint8{
			spec.PRE_GA_WRITE_ROWS_EVENT, spec.WRITE_ROWS_EVENT_V1, spec.WRITE_ROWS_EVENT_V2,
			spec.PRE_GA_UPDATE_ROWS_EVENT, spec.UPDATE_ROWS_EVENT_V1, spec.UPDATE_ROWS_EVENT_V2,
			spec.PRE_GA_DELETE_ROWS_EVENT, spec.DELETE_ROWS_EVENT_V1, spec.DELETE_ROWS_EVENT_V2,
		}
	}
	return []uint8{eventTypeMap[s]}
}

var encodingMap = map[string]encoding.Encoding{
	"utf8":      encoding.Replacement,
	"gbk":       simplifiedchinese.GBK,
	"gb18030":   simplifiedchinese.GB18030,
	"hzgb2312":  simplifiedchinese.HZGB2312,
	"big5":      traditionalchinese.Big5,
	"euckr":     korean.EUCKR,
	"eucjp":     japanese.EUCJP,
	"iso2022jp": japanese.ISO2022JP,
	"shiftjis":  japanese.ShiftJIS,
}

func lookupEncoding(name string) (encoding.Encoding, error) {
	s := strings.ToLower(name)
	s = strings.ReplaceAll(s, "_", "")
	s = strings.ReplaceAll(s, "-", "")
	enc, ok := encodingMap[s]
	if ok {
		return enc, nil
	}
	return nil, errors.New("encoding not found: " + name)
}

var rootCmd = &cobra.Command{
	Use:   "bb",
	Short: "Yet another binlog utility, fast and with modern features.",
	Long:  "Yet another binlog utility, fast and with modern features.",
}

var (
	profileCpu bool
	profileMem bool
)

func init() {
	rootCmd.PersistentFlags().BoolVar(&profileCpu, "profile.cpu", false, "Profile CPU and output to cpu.profile")
	rootCmd.PersistentFlags().BoolVar(&profileMem, "profile.mem", false, "Profile memory and output to mem.profile")
}

func Run() {
	_ = rootCmd.ParseFlags(os.Args)
	if profileCpu {
		f, err := os.OpenFile("cpu.profile", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "ERROR: "+err.Error())
			os.Exit(1)
		}
		defer f.Close()
		w := bufio.NewWriter(f)
		defer w.Flush()
		if err = pprof.StartCPUProfile(w); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "ERROR: "+err.Error())
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
	}

	if profileMem {
		defer func() {
			f, err := os.OpenFile("mem.profile", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, "ERROR: "+err.Error())
				os.Exit(1)
			}
			defer f.Close()
			w := bufio.NewWriter(f)
			defer w.Flush()
			if err := pprof.WriteHeapProfile(w); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, "ERROR: "+err.Error())
				os.Exit(1)
			}
		}()
	}

	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "ERROR: "+err.Error())
		os.Exit(1)
	}
}

func wrap(runE func(cmd *cobra.Command, args []string) error) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		if err := runE(cmd, args); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "ERROR: "+err.Error())
			os.Exit(1)
		}
	}
}
