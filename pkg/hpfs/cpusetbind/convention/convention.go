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
