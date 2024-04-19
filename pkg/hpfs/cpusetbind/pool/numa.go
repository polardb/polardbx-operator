package pool

import (
	"errors"
	"fmt"
	"github.com/alibaba/polardbx-operator/pkg/hpfs/config"
	cpusetbind "github.com/alibaba/polardbx-operator/pkg/hpfs/cpusetbind/system"
	"sort"
)

type NumaCpuSetPool struct {
	NormalCpuSetPool
	preferredNumaNode int
}

func NewNumaCpuSetPool(cpuInfo *cpusetbind.CpuInfo) *NumaCpuSetPool {
	return &NumaCpuSetPool{
		NormalCpuSetPool: NormalCpuSetPool{
			cpuInfo:      cpuInfo,
			allocatedMap: map[int]bool{},
		},
		preferredNumaNode: 0,
	}
}

func (c *NumaCpuSetPool) Name() string {
	return string(config.NumaNodePrefer)
}

func (c *NumaCpuSetPool) AllocateProcessors(processors int) ([]int, error) {
	preferredNumaNode := c.preferredNumaNode
	startIndex, ok := c.cpuInfo.NumaStartIndexMap[preferredNumaNode]
	if !ok {
		return nil, errors.New(fmt.Sprintf("failed to get numa start index, numa node = %d", preferredNumaNode))
	}
	preSocket := startIndex
	socketPreCoreMap := map[int]int{}
	socketPreCoreMap[preSocket] = 0
	allocatedProcessors := make([]int, 0, processors)
	for i := 0; i < processors; i++ {
		processor, socketIndex, coreIndex := c.allocateOneProcessor(preSocket, socketPreCoreMap[preSocket])
		if processor == -1 {
			return nil, errors.New("failed to allocate")
		}
		allocatedProcessors = append(allocatedProcessors, processor)
		socketPreCoreMap[socketIndex] = coreIndex
		preSocket = socketIndex
	}
	sort.Ints(allocatedProcessors)
	return allocatedProcessors, nil
}

func (c *NumaCpuSetPool) SetPreferredNumaNode(numaNode int) {
	c.preferredNumaNode = numaNode
}

func (c *NumaCpuSetPool) ConsiderNuma() bool {
	return true
}
