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
