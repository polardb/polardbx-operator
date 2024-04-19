package cpusetbind

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"sort"
	"strconv"
	"strings"
)

const AllCpuSet = "-1"

func UniqueAndSort(cpus []int) []int {
	if len(cpus) == 0 {
		return cpus
	}
	uniqueSet := map[int]bool{}
	for _, cpu := range cpus {
		uniqueSet[cpu] = true
	}
	result := make([]int, 0, len(uniqueSet))
	for k, _ := range uniqueSet {
		result = append(result, k)
	}
	sort.Ints(result)
	return result
}

func Convert2CpuSetFormat(cpuSet []int) string {
	if len(cpuSet) == 0 {
		return AllCpuSet
	}
	byteBuf := bytes.Buffer{}
	byteBuf.WriteString(strconv.FormatInt(int64(cpuSet[0]), 10))
	cpuSetLen := len(cpuSet)
	for i := 1; i < cpuSetLen; i++ {
		if cpuSet[i]-cpuSet[i-1] == 1 {
			if (i+1 < cpuSetLen) && cpuSet[i+1]-cpuSet[i] == 1 {
				// the end has not been reached
				continue
			} else {
				// the end of the `-`
				byteBuf.WriteString(fmt.Sprintf("-%d", cpuSet[i]))
			}
		} else {
			// add another `,`
			byteBuf.WriteString(fmt.Sprintf(",%d", cpuSet[i]))
		}
	}
	return byteBuf.String()
}

func ParseCpuSetFormat(cpuSet string) ([]int, error) {
	result := make([]int, 0)
	cpus := strings.Split(cpuSet, ",")
	for _, cpu := range cpus {
		if cpu != "" {
			if strings.Contains(cpu, "-") {
				splittedStr := strings.Split(cpu, "-")
				start, err := strconv.Atoi(strings.TrimSpace(splittedStr[0]))
				if err != nil {
					return nil, errors.Wrap(err, fmt.Sprintf("failed to convert start, cpu = %s", cpu))
				}
				end, err := strconv.Atoi(strings.TrimSpace(splittedStr[1]))
				if err != nil {
					return nil, errors.Wrap(err, fmt.Sprintf("failed to convert end, cpu = %s", cpu))
				}
				for i := start; i <= end; i++ {
					result = append(result, i)
				}
			} else {
				value, err := strconv.Atoi(strings.TrimSpace(cpu))
				if err != nil {
					return nil, errors.Wrap(err, fmt.Sprintf("failed to convert, cpu = %s", cpu))
				}
				result = append(result, value)
			}
		}
	}
	if len(result) > 0 {
		sort.Ints(result)
	}
	return result, nil
}
