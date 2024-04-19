package cpusetbind

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

var NumaNodeFilePath = "/sys/devices/system/node"
var OnlineNumaNodeFilepath = NumaNodeFilePath + "/online"
var NumaNodeCpuListFilepath = NumaNodeFilePath + "/node%d/cpulist"

func getOnlineNumaNodes() (string, error) {
	bytes, err := os.ReadFile(OnlineNumaNodeFilepath)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(bytes)), nil
}

func IsNumaOpen() (bool, error) {
	onlineNodes, err := getOnlineNumaNodes()
	if err != nil {
		return false, err
	}
	// check if onlineNodes is just a number
	if onlineNodes != "" {
		_, err := strconv.Atoi(onlineNodes)
		if err != nil {
			return true, nil
		}
	}
	return false, nil
}

func GetNumaNodeCpus() (map[int][]int, error) {
	onlineNodes, err := getOnlineNumaNodes()
	if err != nil {
		return nil, err
	}
	nodes, err := ParseCpuSetFormat(onlineNodes)
	if err != nil {
		return nil, err
	}
	result := map[int][]int{}
	for _, n := range nodes {
		cpuListFilepath := fmt.Sprintf(NumaNodeCpuListFilepath, n)
		bytes, err := os.ReadFile(cpuListFilepath)
		if err != nil {
			return nil, err
		}
		cpuList, err := ParseCpuSetFormat(string(bytes))
		if err != nil {
			return nil, err
		}
		if len(cpuList) > 0 {
			sort.Ints(cpuList)
		}
		result[n] = cpuList
	}
	return result, nil
}

func GetPodPreferNuma() (map[string]int, error) {
	pids, err := GetPids()
	if err != nil {
		return nil, err
	}
	result := map[string]int{}
	for _, pid := range pids {
		podName, err := GetPodNameByPid(pid)
		if err != nil {
			continue
		}
		numaNode, err := GetFirstPageNumaNodeNumByPid(pid)
		if err != nil {
			continue
		}
		if podName != "" && numaNode != "" {
			numaNodeInt, err := strconv.Atoi(numaNode)
			if err != nil {
				continue
			}
			result[podName] = numaNodeInt
		}
	}
	return result, nil
}
