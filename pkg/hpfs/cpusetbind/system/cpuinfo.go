package cpusetbind

import (
	"fmt"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sort"
	"strconv"
	"strings"
)

const CpuInfoFilepath = "/proc/cpuinfo"
const CpuPhysicalIdFilepathFormat = "/sys/devices/system/cpu/cpu%d/topology/physical_package_id"
const CpuCoreIdFilepathFormat = "/sys/devices/system/cpu/cpu%d/topology/core_id"

const (
	KeyProcessor  = "processor"
	KeyPhysicalId = "physical id"
	KeyCoreId     = "core id"
	KeyModelName  = "model name"
)

var globalCpuInfo *CpuInfo

type CpuInfo struct {
	CpuItems []*CpuItem `json:"cpu_items,omitempty"`
	// key: ${socket id}
	SocketCpuMap map[int][]*CpuItem `json:"socket_cpu_map,omitempty"`
	// key: ${socket id}-${core id}
	CoreCpuMap map[string][]*CpuItem `json:"core_cpu_map,omitempty"`
	// key: numa node, value: sockets
	NumaSocketMap map[int][]int `json:"numa_socket_map,omitempty"`
	// socket list
	SocketList []int `json:"socket_list,omitempty"`
	// numa start index map
	NumaStartIndexMap map[int]int `json:"numa_start_index_map,omitempty"`
	// socket core list
	SocketCoreMap map[int][]int `json:"socket_core_map,omitempty"`
	Sockets       int           `json:"sockets,omitempty"`
	Cores         int           `json:"cores,omitempty"`
	Processors    int           `json:"processors,omitempty"`
	NumaNodes     int           `json:"numa_node,omitempty"`
}

func NewCpuInfo(cpuItems []*CpuItem, socketCpuMap map[int][]*CpuItem, coreCpuMap map[string][]*CpuItem, numaSocketMap map[int][]int) *CpuInfo {
	cpuInfo := &CpuInfo{
		CpuItems:      cpuItems,
		SocketCpuMap:  socketCpuMap,
		CoreCpuMap:    coreCpuMap,
		NumaSocketMap: numaSocketMap,
		Sockets:       len(socketCpuMap),
		Cores:         len(coreCpuMap),
		Processors:    len(cpuItems),
		NumaNodes:     len(numaSocketMap),
	}
	socketList := make([]int, 0)
	numaStartIndexMap := map[int]int{}
	if numaSocketMap != nil && len(numaSocketMap) > 0 {
		for n, v := range numaSocketMap {
			numaStartIndexMap[n] = len(socketList)
			for _, vv := range v {
				socketList = append(socketList, vv)
			}
		}
	} else {
		for k, _ := range socketCpuMap {
			socketList = append(socketList, k)
		}
	}
	socketCoreMap := map[int][]int{}
	for _, val := range socketList {
		coreUniqMap := map[int]bool{}
		for _, cpuItem := range cpuInfo.SocketCpuMap[val] {
			coreUniqMap[cpuItem.CoreId] = true
		}
		cores := make([]int, 0)
		for k, _ := range coreUniqMap {
			cores = append(cores, k)
		}
		sort.Ints(cores)
		socketCoreMap[val] = cores
	}
	cpuInfo.SocketCoreMap = socketCoreMap
	cpuInfo.SocketList = socketList
	cpuInfo.NumaStartIndexMap = numaStartIndexMap
	return cpuInfo
}

type CpuItem struct {
	Processor  int    `json:"processor"`
	CoreId     int    `json:"core_id"`
	PhysicalId int    `json:"physical_id"`
	ModelName  string `json:"model_name"`
	NumaNode   int    `json:"numa_node"`
}

func GetCpuInfo() *CpuInfo {
	return globalCpuInfo
}

func SetCpuInfo(cpuInfo *CpuInfo) {
	globalCpuInfo = cpuInfo
}

func initCpuInfo(cpuInfoFilepath string) *CpuInfo {
	logger := zap.New(zap.UseDevMode(true)).WithName("cpuinfo")
	logger.Info("read cpuinfo file and parse", "filepath", cpuInfoFilepath)
	content, err := ReadFile(cpuInfoFilepath)
	if err != nil {
		logger.Error(err, "failed to read file", "filepath", cpuInfoFilepath)
		panic(err)
	}
	lines := strings.Split(content, NewLineSeparator)

	cpuItems := make([]*CpuItem, 0)
	cpuItem := &CpuItem{}

	hasPhysicalId := false
	hasCoreId := false
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			if cpuItem.ModelName != "" {
				updatePhysicalId(!hasPhysicalId, cpuItem)
				hasPhysicalId = false
				updateCordId(!hasCoreId, cpuItem)
				hasCoreId = false
				cpuItems = append(cpuItems, cpuItem)
				cpuItem = &CpuItem{}
			}
		}
		kv := strings.Split(line, ":")
		if len(kv) == 2 {
			key := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])
			switch key {
			case KeyProcessor:
				intVal, err := strconv.ParseInt(value, 10, 32)
				if err != nil {
					logger.Info("failed to parse line", "line", line)
					panic(err)
				}
				cpuItem.Processor = int(intVal)
				cpuItem.ModelName = "unknown"
				break
			case KeyPhysicalId:
				intVal, err := strconv.ParseInt(value, 10, 32)
				if err != nil {
					logger.Info("failed to parse line", "line", line)
					panic(err)
				}
				cpuItem.PhysicalId = int(intVal)
				hasPhysicalId = true
				break
			case KeyCoreId:
				intVal, err := strconv.ParseInt(value, 10, 32)
				if err != nil {
					logger.Info("failed to parse line", "line", line)
					panic(err)
				}
				cpuItem.CoreId = int(intVal)
				hasCoreId = true
				break
			case KeyModelName:
				cpuItem.ModelName = value
				break
			default:
			}
		}
	}
	if cpuItem.ModelName != "" {
		updatePhysicalId(!hasPhysicalId, cpuItem)
		updateCordId(!hasCoreId, cpuItem)
		cpuItems = append(cpuItems, cpuItem)
		cpuItem = &CpuItem{}
	}
	socketCpuMap := map[int][]*CpuItem{}
	coreIdProcessorMap := map[string][]*CpuItem{}
	for _, cpuItem := range cpuItems {
		// for socket cpu map
		_, ok := socketCpuMap[cpuItem.PhysicalId]
		if !ok {
			socketCpuMap[cpuItem.PhysicalId] = make([]*CpuItem, 0)
		}
		socketCpuMap[cpuItem.PhysicalId] = append(socketCpuMap[cpuItem.PhysicalId], cpuItem)
		// for core cpu map
		coreIdKey := fmt.Sprintf("%d-%d", cpuItem.PhysicalId, cpuItem.CoreId)
		_, ok = coreIdProcessorMap[coreIdKey]
		if !ok {
			coreIdProcessorMap[coreIdKey] = make([]*CpuItem, 0)
		}
		coreIdProcessorMap[coreIdKey] = append(coreIdProcessorMap[coreIdKey], cpuItem)
	}
	numaOpen, _ := IsNumaOpen()
	numaSocketMap := map[int][]int{}
	if numaOpen {
		numaCpuMap, err := GetNumaNodeCpus()
		if err != nil {
			panic(err)
		}
		processor2NumaMap := map[int]int{}
		for k, v := range numaCpuMap {
			numaSocketMap[k] = make([]int, 0)
			if len(v) > 0 {
				for _, processor := range v {
					processor2NumaMap[processor] = k
				}
			}
		}
		flag := map[int]bool{}
		for _, cpuItem := range cpuItems {
			cpuItem.NumaNode = processor2NumaMap[cpuItem.Processor]
			if !flag[cpuItem.PhysicalId] {
				numaSocketMap[cpuItem.NumaNode] = append(numaSocketMap[cpuItem.NumaNode], cpuItem.PhysicalId)
				flag[cpuItem.PhysicalId] = true
			}
		}
	}
	cpuInfo := NewCpuInfo(cpuItems, socketCpuMap, coreIdProcessorMap, numaSocketMap)
	return cpuInfo
}

func updatePhysicalId(needDo bool, cpuItem *CpuItem) {
	if needDo {
		physicalIdFilepath := fmt.Sprintf(CpuPhysicalIdFilepathFormat, cpuItem.Processor)
		physicalId, err := ReadIntFromFile(physicalIdFilepath)
		if err != nil {
			logger.Error(err, "failed to read int from file", "filepath", physicalIdFilepath)
			panic(err)
		}
		cpuItem.PhysicalId = int(physicalId)
	}
}

func updateCordId(needDo bool, cpuItem *CpuItem) {
	if needDo {
		coreIdFilepath := fmt.Sprintf(CpuCoreIdFilepathFormat, cpuItem.Processor)
		coreId, err := ReadIntFromFile(coreIdFilepath)
		if err != nil {
			logger.Error(err, "failed to read int from file", "filepath", coreIdFilepath)
			panic(err)
		}
		cpuItem.CoreId = int(coreId)
	}
}

func InitCpuInfo() *CpuInfo {
	cpuInfo := initCpuInfo(CpuInfoFilepath)
	SetCpuInfo(cpuInfo)
	logger.Info("show cpuinfo", "cpuinfo", cpuInfo)
	return cpuInfo
}
