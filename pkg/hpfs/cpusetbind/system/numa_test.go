package cpusetbind

import (
	. "github.com/onsi/gomega"
	"path/filepath"
	"testing"
)

func TestNumaOpen(t *testing.T) {
	g := NewGomegaWithT(t)
	testFileDir := "./testfiles"
	fileResultMap := map[string]bool{
		"nodeonline0":    false,
		"nodeonline1":    false,
		"nodeonline2":    false,
		"nodeonline3":    true,
		"nodeonelinexxx": false,
	}
	fileErrMap := map[string]bool{
		"nodeonline0":    false,
		"nodeonline1":    false,
		"nodeonline2":    false,
		"nodeonline3":    false,
		"nodeonelinexxx": true,
	}
	for k, v := range fileResultMap {
		nodeOnlineFilepath := filepath.Join(testFileDir, k)
		OnlineNumaNodeFilepath = nodeOnlineFilepath
		result, err := IsNumaOpen()
		g.Expect(result).Should(BeEquivalentTo(v))
		hasErr := fileErrMap[k]
		g.Expect(err != nil).Should(BeEquivalentTo(hasErr))
	}
}

func TestGetNumaCpuList(t *testing.T) {
	g := NewGomegaWithT(t)
	NumaNodeFilePath = "./testfiles/node/"
	OnlineNumaNodeFilepath = NumaNodeFilePath + "/online"
	NumaNodeCpuListFilepath = NumaNodeFilePath + "/node%d/cpulist"
	numaCpuMap, err := GetNumaNodeCpus()
	g.Expect(err).Should(BeNil())
	g.Expect(Convert2CpuSetFormat(numaCpuMap[0])).Should(BeEquivalentTo("0-51"))
	g.Expect(Convert2CpuSetFormat(numaCpuMap[1])).Should(BeEquivalentTo("52-103"))
}
