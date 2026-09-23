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

package pool

import (
	"fmt"
	cpusetbind "github.com/alibaba/polardbx-operator/pkg/hpfs/cpusetbind/system"
	. "github.com/onsi/gomega"
	"testing"
)

func mockCpuInfo() *cpusetbind.CpuInfo {
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
	cpuInfo := cpusetbind.NewCpuInfo(cpuItems, socketCpuMap, coreIdProcessorMap, nil)
	return cpuInfo
}

func TestNormalCpuSetPool(t *testing.T) {
	g := NewGomegaWithT(t)
	cpuInfo := mockCpuInfo()
	cpuSetPool := NewNormalCpuSetPool(cpuInfo)
	cpuSetPool.AllocateProcessors(6)
	canAllocate := cpuSetPool.TestAllocateProcessor(0, 0) || cpuSetPool.TestAllocateProcessor(1, 0) || cpuSetPool.TestAllocateProcessor(0, 1)
	g.Expect(canAllocate).Should(BeEquivalentTo(false))
}
