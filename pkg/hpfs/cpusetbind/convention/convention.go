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

package convention

import (
	"bufio"
	"bytes"
	"strings"
)

const (
	NodeLabelIsolateCpu        = "polardbx/isolate-cpu"
	NodeLabelIsolateCpuDisable = "polardbx/isolate-cpu-disable"
	NodeLabelIsolateCpuReset   = "polardbx/isolate-cpu-reset"
)

const (
	KubeletContainerRuntimeEndpoint = "container-runtime-endpoint"
	KubeletReservedCpus             = "reserved-cpus"
)

const (
	PodAnnotationIsolateCpuCpuSet = "polardbx/isolate-cpu-cpuset"
)

func ParseKVConfig(content []byte) map[string]string {
	result := map[string]string{}
	if len(content) == 0 {
		return result
	}
	var advance int
	var line []byte
	for advance < len(content) {
		content = content[advance:]
		advance, line, _ = bufio.ScanLines(content, false)
		if advance == 0 {
			advance, line, _ = bufio.ScanLines(content, true)
		}
		separateIndex := bytes.IndexByte(line, '=')
		if separateIndex > 0 && separateIndex < len(line) {
			result[string(line[0:separateIndex])] = strings.Trim(strings.TrimSpace(string(line[separateIndex+1:])), "\"")
		}
	}
	return result
}
