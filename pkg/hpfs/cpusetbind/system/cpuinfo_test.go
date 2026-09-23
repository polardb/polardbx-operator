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

package cpusetbind

import (
	. "github.com/onsi/gomega"
	"testing"
)

func TestGetCpuInfoNoNuma(t *testing.T) {
	g := NewGomegaWithT(t)
	cpuInfo := initCpuInfo("./testfiles/cpu/cpuinfo")
	logger.Info("show cpu info", "cpuInfo", cpuInfo)
	g.Expect(len(cpuInfo.CpuItems)).Should(BeEquivalentTo(104))
	g.Expect(len(cpuInfo.SocketCpuMap)).Should(BeEquivalentTo(2))
	g.Expect(len(cpuInfo.SocketCpuMap[0])).Should(BeEquivalentTo(52))
	g.Expect(len(cpuInfo.SocketCpuMap[1])).Should(BeEquivalentTo(52))
	g.Expect(len(cpuInfo.CoreCpuMap)).Should(BeEquivalentTo(52))
	g.Expect(len(cpuInfo.NumaSocketMap)).Should(BeEquivalentTo(0))
	g.Expect(len(cpuInfo.SocketCoreMap)).Should(BeEquivalentTo(2))
	g.Expect(len(cpuInfo.SocketCoreMap[0])).Should(BeEquivalentTo(26))
	g.Expect(len(cpuInfo.SocketCoreMap[1])).Should(BeEquivalentTo(26))
	g.Expect(cpuInfo.NumaNodes).Should(BeEquivalentTo(0))
	g.Expect(cpuInfo.Sockets).Should(BeEquivalentTo(2))
	g.Expect(cpuInfo.Cores).Should(BeEquivalentTo(52))
	g.Expect(cpuInfo.Processors).Should(BeEquivalentTo(104))
}

func TestGetCpuInfoWithNuma(t *testing.T) {
	NumaNodeFilePath = "./testfiles/node"
	OnlineNumaNodeFilepath = NumaNodeFilePath + "/online"
	NumaNodeCpuListFilepath = NumaNodeFilePath + "/node%d/cpulist"
	g := NewGomegaWithT(t)
	cpuInfo := initCpuInfo("./testfiles/cpu/cpuinfo")
	g.Expect(cpuInfo.NumaNodes).Should(BeEquivalentTo(2))
	g.Expect(len(cpuInfo.NumaSocketMap)).Should(BeEquivalentTo(2))
	g.Expect(len(cpuInfo.CpuItems)).Should(BeEquivalentTo(104))
	g.Expect(len(cpuInfo.SocketCpuMap)).Should(BeEquivalentTo(2))
	g.Expect(len(cpuInfo.SocketCpuMap[0])).Should(BeEquivalentTo(52))
	g.Expect(len(cpuInfo.SocketCpuMap[1])).Should(BeEquivalentTo(52))
	g.Expect(len(cpuInfo.CoreCpuMap)).Should(BeEquivalentTo(52))
	g.Expect(len(cpuInfo.SocketCoreMap)).Should(BeEquivalentTo(2))
	g.Expect(len(cpuInfo.SocketCoreMap[0])).Should(BeEquivalentTo(26))
	g.Expect(len(cpuInfo.SocketCoreMap[1])).Should(BeEquivalentTo(26))
	g.Expect(cpuInfo.Sockets).Should(BeEquivalentTo(2))
	g.Expect(cpuInfo.Cores).Should(BeEquivalentTo(52))
	g.Expect(cpuInfo.Processors).Should(BeEquivalentTo(104))

}
