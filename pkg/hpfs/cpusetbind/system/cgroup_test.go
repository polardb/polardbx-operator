package cpusetbind

import (
	. "github.com/onsi/gomega"
	"path/filepath"
	"testing"
)

type cpusetFormatParamWrap struct {
	formatString string
	cpuSetInt    []int
}

func TestConvertCpuSetFormat(t *testing.T) {
	g := NewGomegaWithT(t)
	params := []*cpusetFormatParamWrap{
		{formatString: "1-5", cpuSetInt: []int{1, 2, 3, 4, 5}},
		{formatString: "1-5", cpuSetInt: []int{1, 4, 5, 2, 3}},
		{formatString: "1,5,7", cpuSetInt: []int{1, 5, 7}},
		{formatString: "1-5,7", cpuSetInt: []int{1, 5, 7, 2, 3, 4}},
		{formatString: "0-1,3-5,7", cpuSetInt: []int{1, 5, 7, 3, 4, 0}},
	}
	for _, val := range params {
		g.Expect(Convert2CpuSetFormat(UniqueAndSort(val.cpuSetInt))).Should(BeEquivalentTo(val.formatString))
	}
}

func TestParseCpuSetFormat(t *testing.T) {
	g := NewGomegaWithT(t)
	result, _ := ParseCpuSetFormat("1-3,5,7-10")
	g.Expect(result).Should(BeEquivalentTo([]int{1, 2, 3, 5, 7, 8, 9, 10}))
	result, _ = ParseCpuSetFormat("11")
	g.Expect(result).Should(BeEquivalentTo([]int{11}))
	result, _ = ParseCpuSetFormat("11-12")
	g.Expect(result).Should(BeEquivalentTo([]int{11, 12}))
	result, _ = ParseCpuSetFormat("0,9")
	g.Expect(result).Should(BeEquivalentTo([]int{0, 9}))
}

func TestFilepathJoin(t *testing.T) {
	g := NewGomegaWithT(t)
	g.Expect(filepath.Join("/11/", "/1")).Should(BeEquivalentTo("/11/1"))
}
