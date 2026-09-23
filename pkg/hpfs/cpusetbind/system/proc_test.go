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
