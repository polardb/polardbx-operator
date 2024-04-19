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
