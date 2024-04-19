package pool

import (
	"errors"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	cpusetbind "github.com/alibaba/polardbx-operator/pkg/hpfs/cpusetbind/system"
	"sort"
)

type NormalCpuSetPool struct {
	cpuInfo      *cpusetbind.CpuInfo
	allocatedMap map[int]bool
}

func NewNormalCpuSetPool(cpuInfo *cpusetbind.CpuInfo) *NormalCpuSetPool {
	return &NormalCpuSetPool{
		cpuInfo:      cpuInfo,
		allocatedMap: map[int]bool{},
	}
}

func (c *NormalCpuSetPool) Name() string {
	return string(config.Normal)
}

func (c *NormalCpuSetPool) MarkAllocated(cpus []int) {
	for _, cpu := range cpus {
		c.allocatedMap[cpu] = true
	}
}

func (c *NormalCpuSetPool) TestAvailable(cpus []int) bool {
	for _, cpu := range cpus {
		if _, ok := c.allocatedMap[cpu]; ok {
			return false
		}
	}
	return true
}

func (c *NormalCpuSetPool) AllocateProcessors(processors int) ([]int, error) {
	preSocketIndex := 0
	sockerPreCoreMap := map[int]int{}
	sockerPreCoreMap[preSocketIndex] = 0
	allocatedProcessors := make([]int, 0, processors)
	for i := 0; i < processors; i++ {
		processor, socketIndex, coreIndex := c.allocateOneProcessor(preSocketIndex, sockerPreCoreMap[preSocketIndex])
		if processor == -1 {
			return nil, errors.New("failed to allocate")
		}
		allocatedProcessors = append(allocatedProcessors, processor)
		sockerPreCoreMap[socketIndex] = coreIndex
		socketNum := c.cpuInfo.SocketList[socketIndex]
		if !c.TestAllocateProcessor(socketNum, c.cpuInfo.SocketCoreMap[socketNum][coreIndex]) {
			preSocketIndex = (socketIndex + 1) % c.cpuInfo.Sockets
		}
	}
	sort.Ints(allocatedProcessors)
	return allocatedProcessors, nil
}

func (c *NormalCpuSetPool) TestAllocateProcessor(socket int, core int) bool {
	socketCoreKey := generateSocketCoreKey(socket, core)
	cpuItems, ok := c.cpuInfo.CoreCpuMap[socketCoreKey]
	if ok {
		for _, cpuItem := range cpuItems {
			if !c.allocatedMap[cpuItem.Processor] {
				return true
			}
		}
	}
	return false
}

func (c *NormalCpuSetPool) allocateOneProcessor(socketIndex int, coreIndex int) (int, int, int) {
	for i := 0; i < c.cpuInfo.Sockets; i++ {
		currentSocketIndex := (socketIndex + i) % c.cpuInfo.Sockets
		coresLen := len(c.cpuInfo.SocketCoreMap[c.cpuInfo.SocketList[currentSocketIndex]])
		for j := 0; j < coresLen; j++ {
			currentCoreIndex := (coreIndex + j) % coresLen
			socketNum := c.cpuInfo.SocketList[currentSocketIndex]
			socketCoreKey := generateSocketCoreKey(socketNum, c.cpuInfo.SocketCoreMap[socketNum][currentCoreIndex])
			cpuItems, ok := c.cpuInfo.CoreCpuMap[socketCoreKey]
			if ok {
				for _, cpuItem := range cpuItems {
					if !c.allocatedMap[cpuItem.Processor] {
						c.allocatedMap[cpuItem.Processor] = true
						return cpuItem.Processor, currentSocketIndex, currentCoreIndex
					}
				}
			}
		}
	}
	return -1, -1, -1
}

func (c *NormalCpuSetPool) SetPreferredNumaNode(socket int) {
	// do nothing
}

func (c *NormalCpuSetPool) ConsiderNuma() bool {
	return false
}

func generateSocketCoreKey(socket int, core int) string {
	return fmt.Sprintf("%d-%d", socket, core)
}
