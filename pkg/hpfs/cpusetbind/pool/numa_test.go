package pool

import (
	"fmt"
	cpusetbind "github.com/alibaba/polardbx-operator/pkg/hpfs/cpusetbind/system"
	. "github.com/onsi/gomega"
	"testing"
)

func mockNumaCpuInfo() *cpusetbind.CpuInfo {
	cpuItems := []*cpusetbind.CpuItem{
		{
			Processor:  0,
			CoreId:     0,
			PhysicalId: 0,
			ModelName:  "0",
		},
		{
			Processor:  1,
			CoreId:     0,
			PhysicalId: 0,
			ModelName:  "0",
		},
		{
			Processor:  2,
			CoreId:     1,
			PhysicalId: 0,
			ModelName:  "0",
		},
		{
			Processor:  3,
			CoreId:     1,
			PhysicalId: 0,
			ModelName:  "0",
		},
		{
			Processor:  4,
			CoreId:     0,
			PhysicalId: 1,
			ModelName:  "0",
		},
		{
			Processor:  5,
			CoreId:     0,
			PhysicalId: 1,
			ModelName:  "0",
		},
		{
			Processor:  6,
			CoreId:     1,
			PhysicalId: 1,
			ModelName:  "0",
		},
		{
			Processor:  7,
			CoreId:     1,
			PhysicalId: 1,
			ModelName:  "0",
		},
		{
			Processor:  8,
			CoreId:     0,
			PhysicalId: 2,
			ModelName:  "0",
		},
		{
			Processor:  9,
			CoreId:     0,
			PhysicalId: 2,
			ModelName:  "0",
		},
		{
			Processor:  10,
			CoreId:     1,
			PhysicalId: 2,
			ModelName:  "0",
		},
		{
			Processor:  11,
			CoreId:     1,
			PhysicalId: 2,
			ModelName:  "0",
		},
		{
			Processor:  12,
			CoreId:     0,
			PhysicalId: 3,
			ModelName:  "0",
		},
		{
			Processor:  13,
			CoreId:     0,
			PhysicalId: 3,
			ModelName:  "0",
		},
		{
			Processor:  14,
			CoreId:     1,
			PhysicalId: 3,
			ModelName:  "0",
		},
		{
			Processor:  70,
			CoreId:     1,
			PhysicalId: 3,
			ModelName:  "0",
		},
	}
	socketCpuMap := map[int][]*cpusetbind.CpuItem{}
	coreIdProcessorMap := map[string][]*cpusetbind.CpuItem{}
	for _, cpuItem := range cpuItems {
		// for socket cpu map
		_, ok := socketCpuMap[cpuItem.PhysicalId]
		if !ok {
			socketCpuMap[cpuItem.PhysicalId] = make([]*cpusetbind.CpuItem, 0)
		}
		socketCpuMap[cpuItem.PhysicalId] = append(socketCpuMap[cpuItem.PhysicalId], cpuItem)
		// for core cpu map
		coreIdKey := fmt.Sprintf("%d-%d", cpuItem.PhysicalId, cpuItem.CoreId)
		_, ok = coreIdProcessorMap[coreIdKey]
		if !ok {
			coreIdProcessorMap[coreIdKey] = make([]*cpusetbind.CpuItem, 0)
		}
		coreIdProcessorMap[coreIdKey] = append(coreIdProcessorMap[coreIdKey], cpuItem)
	}

	numaSocketMap := map[int][]int{
		0: {0, 2},
		1: {1, 3},
	}

	cpuInfo := cpusetbind.NewCpuInfo(cpuItems, socketCpuMap, coreIdProcessorMap, numaSocketMap)
	return cpuInfo
}

func TestNumaCpuSetPool18(t *testing.T) {
	g := NewGomegaWithT(t)
	cpuInfo := mockNumaCpuInfo()
	cpuSetPool := NewNumaCpuSetPool(cpuInfo)
	cpuSetPool.SetPreferredNumaNode(1)
	cpuSetPool.AllocateProcessors(8)
	canAllocate := cpuSetPool.TestAllocateProcessor(1, 0) || cpuSetPool.TestAllocateProcessor(1, 1) || cpuSetPool.TestAllocateProcessor(3, 0) || cpuSetPool.TestAllocateProcessor(3, 1)
	g.Expect(canAllocate).Should(BeEquivalentTo(false))
}

func TestNumaCpuSetPool08(t *testing.T) {
	g := NewGomegaWithT(t)
	cpuInfo := mockNumaCpuInfo()
	cpuSetPool := NewNumaCpuSetPool(cpuInfo)
	cpuSetPool.SetPreferredNumaNode(0)
	cpuSetPool.AllocateProcessors(8)
	canAllocate := cpuSetPool.TestAllocateProcessor(0, 0) || cpuSetPool.TestAllocateProcessor(0, 1) || cpuSetPool.TestAllocateProcessor(2, 0) || cpuSetPool.TestAllocateProcessor(2, 1)
	g.Expect(canAllocate).Should(BeEquivalentTo(false))
}

func TestNumaCpuSetPool19(t *testing.T) {
	g := NewGomegaWithT(t)
	cpuInfo := mockNumaCpuInfo()
	cpuSetPool := NewNumaCpuSetPool(cpuInfo)
	cpuSetPool.SetPreferredNumaNode(1)
	cpuSetPool.AllocateProcessors(9)
	canAllocate := cpuSetPool.TestAllocateProcessor(1, 0) || cpuSetPool.TestAllocateProcessor(1, 1) || cpuSetPool.TestAllocateProcessor(3, 0) || cpuSetPool.TestAllocateProcessor(3, 1)
	g.Expect(canAllocate).Should(BeEquivalentTo(false))
	canAllocate = cpuSetPool.TestAllocateProcessor(0, 0)
	g.Expect(canAllocate).Should(BeEquivalentTo(true))
	cpuSetPool.AllocateProcessors(1)
	canAllocate = cpuSetPool.TestAllocateProcessor(0, 0)
	g.Expect(canAllocate).Should(BeEquivalentTo(false))
}

func TestNumaCpuSetPool09(t *testing.T) {
	g := NewGomegaWithT(t)
	cpuInfo := mockNumaCpuInfo()
	cpuSetPool := NewNumaCpuSetPool(cpuInfo)
	cpuSetPool.SetPreferredNumaNode(0)
	cpuSetPool.AllocateProcessors(9)
	canAllocate := cpuSetPool.TestAllocateProcessor(0, 0) || cpuSetPool.TestAllocateProcessor(0, 1) || cpuSetPool.TestAllocateProcessor(2, 0) || cpuSetPool.TestAllocateProcessor(2, 1)
	g.Expect(canAllocate).Should(BeEquivalentTo(false))
	canAllocate = cpuSetPool.TestAllocateProcessor(1, 0)
	g.Expect(canAllocate).Should(BeEquivalentTo(true))
	cpuSetPool.AllocateProcessors(1)
	canAllocate = cpuSetPool.TestAllocateProcessor(1, 0)
	g.Expect(canAllocate).Should(BeEquivalentTo(false))
}
