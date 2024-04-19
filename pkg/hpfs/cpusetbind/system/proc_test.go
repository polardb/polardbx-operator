package cpusetbind

import (
	. "github.com/onsi/gomega"
	"testing"
)

func TestGetNumaNodeRight(t *testing.T) {
	g := NewGomegaWithT(t)
	result := GetNumaNode("56284e99b000 default file=/opt/alibaba/dragonwell-11.0.17.13+8-GA/bin/java anon=1 dirty=1 active=0 N0=1 kernelpagesize_kB=4")
	g.Expect(result).Should(BeEquivalentTo("0"))
	result = GetNumaNode("56284e99b000 default file=/opt/alibaba/dragonwell-11.0.17.13+8-GA/bin/java anon=1 dirty=1 N12=10 kernelpagesize_kB=4")
	g.Expect(result).Should(BeEquivalentTo("12"))
}

func TestGetNumaNodeWrong(t *testing.T) {
	g := NewGomegaWithT(t)
	result := GetNumaNode("56284e99b000 default file=/opt/alibaba/dragonwell-11.0.17.13+8-GA/bin/java anon=1 dirty=1 active=0 NN=1 kernelpagesize_kB=4")
	g.Expect(result).Should(BeEquivalentTo(""))
	result = GetNumaNode("56284e99b000 default file=/opt/alibaba/dragonwell-11.0.17.13+8-GA/bin/java anon=1 dirty=1 NN=10 kernelpagesize_kB=4")
	g.Expect(result).Should(BeEquivalentTo(""))
	result = GetNumaNode("")
	g.Expect(result).Should(BeEquivalentTo(""))
}
